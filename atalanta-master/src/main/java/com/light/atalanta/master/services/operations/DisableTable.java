/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations;

import com.google.common.base.Preconditions;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.TableNotExistsException;
import com.light.atalanta.exceptions.NoRetryException;
import com.light.atalanta.master.services.TableLock;
import com.light.atalanta.master.services.UniqueNameThreadFactory;
import com.light.atalanta.master.services.grpc.RegionServerConnector;
import com.light.atalanta.master.services.grpc.RsHealthChecker;
import com.light.atalanta.master.services.recovery.RecoveryRSContext;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.master.vo.DataConfiguration;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.master.vo.GrpcConfiguration;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.util.ZookeeperStatus;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.STATUS_PREFIX;
import static com.light.atalanta.services.ZooKeeperContextHelper.ASSIGNED_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.CREATED_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.DISABLED_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.DISABLING_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.ENABLED_REGION_STATUS;
import static com.light.atalanta.util.ZookeeperStatus.*;

/**
 * Runs runSyncThenAsyncOnError() for a single synchronous attempt that throws on failure,
 * then switches to asynchronous retries. Asynchronous retries run in a single global thread.
 */
@SuppressWarnings("rawtypes")
public class DisableTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DisableTable.class);

    private final DataConfiguration dataConf;
    private final GrpcConfiguration grpcConf;

    private final ZookeeperHelper zooHelper;
    private final ZooKeeperMasterContext zooContext;

    private final RegionServerConnector regionServerConnector;
    private final RecoveryRSContext recoveryRSContext;

    private final ThreadFactory threadFactory = new UniqueNameThreadFactory("atalanta-disable-table-pool");
    private final BlockingDeque<TableLock> tableToDisableDeque;
    private final ExecutorService executorService;

    public DisableTable(AtalantaDBConfiguration conf, ZookeeperHelper zooHelper, ZooKeeperMasterContext zooContext, RecoveryRSContext recoveryRSContext, RegionServerConnector regionServerConnector, ExecutorService executorService) {
        this.dataConf = conf.getDataConfiguration();
        this.grpcConf = conf.getGrpcConfiguration();
        this.zooHelper = zooHelper;
        this.zooContext = zooContext;
        this.regionServerConnector = regionServerConnector;
        this.recoveryRSContext = recoveryRSContext;
        this.executorService = executorService;
        tableToDisableDeque = new LinkedBlockingDeque<>();
        Thread thread = new Thread(this::asyncDisable, "atalanta-master-disable-table");
        thread.start();
    }

    // Asynchronous retries for the operation
    private void asyncDisable() {
        try {
            while (true) {
                do {
                    final TableLock tableLock = tableToDisableDeque.take();
                    final TableName tableName = tableLock.getTableName();
                    try {
                        if (zooHelper.checkExists(ZooKeeperContextHelper.getTablePath(tableName))) {
                            if (zooContext.getTableStatus(tableName).equals(DISABLING)) {
                                disable(tableName);
                                // Release the lock in case of success
                                tableLock.unlockAndReleaseRef();
                            }
                        }
                    } catch (NoRetryException ex) {
                        // Stop making retries, release the lock
                        tableLock.unlockAndReleaseRef();
                        LOGGER.error("Error disabling table async: '{}', no retries", tableName, ex);
                    } catch (Exception ex) {
                        // Continue making async retries, requeue the table, don't release the lock
                        LOGGER.error(
                                "Error disabling table async: '{}', waiting {} ms and retry", tableName,
                                dataConf.getDisableTableRetryDelayMs()
                        );
                        Thread.sleep(dataConf.getDisableTableRetryDelayMs());
                        tableToDisableDeque.add(tableLock);
                    }
                } while (!tableToDisableDeque.isEmpty());
            }
        } catch (InterruptedException e) {
            LOGGER.info("AtalantaDB asyncDisable interrupted");
        }
    }

    // Tries to run an operation synchronously for one time, releases lock if there is success
    // Switches to asynchronous retries on error (if the error is not of type NoRetryException)
    public void runSyncThenAsyncOnError(TableLock tableLock) throws OperationException, NoRetryException {
        final TableName tableName = tableLock.getTableName();
        try {
            disable(tableName);
            // Release the lock in case of success
            tableLock.unlockAndReleaseRef();
        } catch (NoRetryException ex) {
            // Don't go for async retries, release the lock
            tableLock.unlockAndReleaseRef();
            LOGGER.error("Error disabling table sync: '{}', no retries", tableName, ex);
            throw ex;
        } catch (OperationException ex) {
            // Switching to the async retries, _don't_ release the lock
            tableToDisableDeque.add(tableLock);
            LOGGER.error("Error disabling table sync: '{}', switching to the async retries", tableName, ex);
            throw ex;
        }
    }

    private void disable(TableName tableName) throws NoRetryException, OperationException {
        ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers = new ConcurrentHashMap<>();
        try {
            // Check whether operations for table are allowed
            recoveryRSContext.checkIncomeTableRequestsEnabled(tableName);
            LOGGER.info("Start disabling table: '{}'", tableName);

            // Check the table and its status
            AtomicBoolean retryMode = new AtomicBoolean(checkTableExists(tableName));

            // Plan the target region state
            RegionsDistributionStatus distribution = getWantedRegionsDistributionStatus(tableName, retryMode.get());

            // Pre-commit the table and regions for disabling
            makePreCommits(tableName, distribution.getRegionsToPreCommitAndDisable(), retryMode.get());

            // Disable regions on region servers in parallel and wait, commit the regions and the table
            disableRegionsAndCommitTable(executorService, distribution, tableName, pingCheckers);
        } catch (NoRetryException ex) {
            throw new NoRetryException(String.format(
                    "Error disabling table: '%s': %s", tableName, ex.getMessage()), ex.getCause()
            );
        } catch (Exception ex) {
            throw new OperationException(
                    "Error disabling table: '" + tableName + "'", ex
            );
        } finally {
            pingCheckers.values().forEach(RsHealthChecker::close);
        }
    }

    private boolean checkTableExists(TableName tableName) throws Exception {
        String zkPath = ZooKeeperContextHelper.getTablePath(tableName);
        boolean tableExists;
        try {
            tableExists = zooHelper.checkExists(zkPath);
        } catch (Exception ex) {
            throw new Exception("unable to check table exists", ex);
        }
        if (!tableExists) {
            throw new NoRetryException(new TableNotExistsException(tableName));
        }
        ZookeeperStatus status = ZookeeperStatus.fromString(zooHelper.getNotNullString(zkPath));
        boolean retryMode = false;
        switch (status) {
            case DISABLING:
                retryMode = true;
                break;
            case CREATED:
            case ASSIGNED:
            case ENABLED:
                break;
            case DISABLED:
                throw new NoRetryException("table already disabled");
            case CREATING:
                throw new NoRetryException("table is creating for now");
            case SPLITTING:
                throw new NoRetryException("table is splitting for now");
            case MODIFYING:
                throw new NoRetryException("table is modifying for now");
            case ENABLING:
                throw new NoRetryException("table is enabling for now");
            case RECOVERING:
                throw new NoRetryException("table is recovering for now");
            case DROPPING:
                throw new NoRetryException("table is dropping for now");
            case ASSIGNING:
                throw new NoRetryException("table regions is moving for now");
            default:
                throw new NoRetryException("unexpected status: " + status);
        }
        return retryMode;
    }

    private void makePreCommits(TableName tableName, List<RegionDistributionModel> regionsToPreCommit, boolean retryMode) throws Exception {
        List<CuratorOp> plan = new ArrayList<>();
        LOGGER.debug("Disabling table '{}': pre-commits started", tableName);
            // Pre-commit the table
            if (!retryMode) {
                plan.add(zooHelper.setDataTrans(ZooKeeperContextHelper.getTablePath(tableName), STATUS_PREFIX + DISABLING.getValue()));
            } else {
                LOGGER.debug("Disabling table '{}': retry mode detected", tableName);
            }
            // Pre-commit the regions
            List<RegionDistributionModel> preCommitedRegions = new ArrayList<>();
            List<RegionServerModel> preCommitedRS = new ArrayList<>();
            for (RegionDistributionModel region : regionsToPreCommit) {
                RegionServerModel currentRS = region.getRegionServerModel();
                if (!preCommitedRS.contains(currentRS)) {
                    List<RegionModel> regionsOnSameRsToPreCommit = regionsToPreCommit.stream()
                            .filter(rdm -> rdm.getRegionServerModel().equals(currentRS))
                            .map(RegionDistributionModel::getRegionModel)
                            .collect(Collectors.toList());
                    plan.add(zooContext.setRegionStatusesTrans(tableName, regionsOnSameRsToPreCommit, currentRS, DISABLING_REGION_STATUS));
                    preCommitedRS.add(currentRS);
                }
                // Create a new region from the current one, with a new status only
                RegionModel sourceRegion = region.getRegionModel();
                RegionModel preCommitedRegion = new RegionModel(sourceRegion.getName(), sourceRegion.getReplicaNum(),
                        DISABLING_REGION_STATUS,
                        sourceRegion.getBeginKey(),
                        sourceRegion.getEndKey(),
                        sourceRegion.getDataBlockSize(),
                        sourceRegion.getCompressType(),
                        sourceRegion.getRecordCompressionType(),
                        sourceRegion.getDictionary(),
                        sourceRegion.getRecordTargetCount());
                preCommitedRegions.add(new RegionDistributionModel(preCommitedRegion, region.getRegionServerModel()));
            }
            // Run the pre-commits
            if (!plan.isEmpty()) {
                Preconditions.checkArgument(zooHelper.forOperations(plan).size() == plan.size(), "unexpected transaction error");
            }
            // Update the statuses of the regions passed to the method
            regionsToPreCommit.clear();
            regionsToPreCommit.addAll(preCommitedRegions);
        LOGGER.debug("Disabling table '{}': finish pre-commits", tableName);
    }

    private RegionsDistributionStatus getWantedRegionsDistributionStatus(TableName tableName, boolean retryMode) throws Exception {
        List<RegionDistributionModel> currentDistribution = zooContext.getRegionDistributionWithStatuses(tableName);
        if (retryMode) {
            // If the table is being disabled again
            if (currentDistribution.isEmpty()) {
                // But there are no regions at all
                throw new Exception("regions not found");
            }
            // Check that there are no unexpected statuses
            long unexpectedStatusCount = currentDistribution.stream()
                    .filter(r -> r.getRegionModel().getStatusId() != DISABLING_REGION_STATUS)
                    .filter(r -> r.getRegionModel().getStatusId() != DISABLED_REGION_STATUS)
                    .count();
            Preconditions.checkArgument(unexpectedStatusCount == 0, "some regions of table not in DISABLING/DISABLED status");
            // Get the regions in pre-commit status
            List<RegionDistributionModel> preCommitDistribution = currentDistribution.stream()
                    .filter(r -> r.getRegionModel().getStatusId() == DISABLING_REGION_STATUS)
                    .collect(Collectors.toList());
            // All regions are committed
            if (preCommitDistribution.isEmpty()) {
                LOGGER.warn("Disabling table '{}': table has all regions commited, commiting table ...", tableName);
            }
            return new RegionsDistributionStatus.Builder()
                    .regionsToDisableOnly(preCommitDistribution)
                    .allRegions(currentDistribution)
                    .build();
        } else {
            // Check that there are no unexpected statuses
            long unexpectedStatusCount = currentDistribution.stream()
                    .filter(r -> r.getRegionModel().getStatusId() != CREATED_REGION_STATUS)
                    .filter(r -> r.getRegionModel().getStatusId() != ASSIGNED_REGION_STATUS)
                    .filter(r -> r.getRegionModel().getStatusId() != ENABLED_REGION_STATUS)
                    .count();
            Preconditions.checkArgument(unexpectedStatusCount == 0, "some regions of table not in CREATED/ASSIGNED/ENABLED status");
            // If the table is disabled for the first time, get all regions
            List<RegionDistributionModel> initialDistribution = currentDistribution.stream()
                    .filter(r -> r.getRegionModel().getStatusId() == CREATED_REGION_STATUS
                            || r.getRegionModel().getStatusId() == ASSIGNED_REGION_STATUS
                            || r.getRegionModel().getStatusId() == ENABLED_REGION_STATUS)
                    .collect(Collectors.toList());
            return new RegionsDistributionStatus.Builder()
                    .regionsToPreCommitAndDisable(initialDistribution)
                    .allRegions(currentDistribution)
                    .build();
        }
    }

    private synchronized void commitRegions(TableName tableName, List<RegionDistributionModel> regions) throws Exception {
        zooContext.setRegionStatus(tableName, regions, DISABLED_REGION_STATUS);
    }

    private void disableRegionsAndCommitTable(ExecutorService executorService, RegionsDistributionStatus regionsDistributionStatus, TableName tableName, ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers) throws Exception {
        // All replicas
        final List<RegionDistributionModel> distributionToAct = new ArrayList<>();
        distributionToAct.addAll(regionsDistributionStatus.getRegionsToDisableOnly());
        distributionToAct.addAll(regionsDistributionStatus.getRegionsToPreCommitAndDisable());

        // Start pinging the related RS
        regionsDistributionStatus.getRegionsToDisableOnly().stream()
                .map(RegionDistributionModel::getRegionServerModel)
                .forEach(it -> addPingChecker(pingCheckers, it));
        regionsDistributionStatus.getRegionsToPreCommitAndDisable().stream()
                .map(RegionDistributionModel::getRegionServerModel)
                .forEach(it -> addPingChecker(pingCheckers, it));

        // IMPORTANT: stop leader -> unassign _0 -> unassign _1/_2
        Map<String, List<RegionDistributionModel>> replicasByRegion = distributionToAct.stream()
                .collect(Collectors.groupingBy(
                        rdm -> rdm.getRegionModel().getName(),
                        LinkedHashMap::new,
                        Collectors.toList()
                ));
        createDisableTableRegionsTasks(
                executorService,
                replicasByRegion,
                regionsDistributionStatus.getAllRegions(),
                tableName,
                pingCheckers
        );

        // Commit the table
        try {
            LOGGER.debug("Disabling table '{}': commiting table", tableName);
            zooHelper.setData(ZooKeeperContextHelper.getTablePath(tableName), STATUS_PREFIX + DISABLED.getValue());
        } catch (Exception ex) {
            throw new Exception("unable to commit table in ZooKeeper", ex);
        }

        LOGGER.info("Done disabling table: '{}'", tableName);
    }

    /**
     * Disables regions on region servers asynchronously and commits them on success.
     *
     * @param tableName           table whose regions are disabled
     */
    private void createDisableTableRegionsTasks(ExecutorService executorService,
                                                Map<String, List<RegionDistributionModel>> replicasByRegion,
                                                List<RegionDistributionModel> allRegions,
                                                TableName tableName,
                                                ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers) throws Exception {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Map.Entry<String, List<RegionDistributionModel>> entry : replicasByRegion.entrySet()) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    disableRegionReplicas(entry.getKey(), entry.getValue(), allRegions, tableName);
                } catch (Throwable ex) {
                    LOGGER.error("Disabling table '{}': unable to disable region '{}'", tableName, entry.getKey(), ex);
                    throw new CompletionException(ex);
                }
            }, executorService));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        RsHealthChecker.checkRsErrors(pingCheckers);
    }

    private void disableRegionReplicas(String regionName,
                                       List<RegionDistributionModel> replicas,
                                       List<RegionDistributionModel> allRegions,
                                       TableName tableName) throws Throwable {
        RegionDistributionModel leader = replicas.stream()
                .filter(rdm -> rdm.getRegionModel().getReplicaNum() == 0)
                .findFirst()
                .orElse(null);

        if (leader != null) {
            String stopLeaderLogStr = String.format("Disabling table '%s': stopping leader '%s'", tableName, leader.toShortString());
            regionServerConnector.monitor(tableName, stopLeaderLogStr, leader, StringUtils.EMPTY, (stub, req, stream) ->
                    stub.get().stopRegion(req, stream), false
            );

            String reservedAddresses = zooContext.getReservedAddresses(allRegions, regionName, dataConf.getReplicationFactor());
            disableAndCommitRegion(tableName, leader, reservedAddresses);
        }

        List<RegionDistributionModel> reservedReplicas = replicas.stream()
                .filter(rdm -> rdm.getRegionModel().getReplicaNum() != 0)
                .sorted(Comparator.comparingInt(rdm -> rdm.getRegionModel().getReplicaNum()))
                .collect(Collectors.toList());
        for (RegionDistributionModel reservedReplica : reservedReplicas) {
            disableAndCommitRegion(tableName, reservedReplica, StringUtils.EMPTY);
        }
    }

    private void disableAndCommitRegion(TableName tableName, RegionDistributionModel region, String reservedAddresses) throws Throwable {
        // disableRegion == unassignRegion
        String disableTableStr = String.format("Disabling table '%s': disabling region '%s'", tableName, region.toShortString());
        regionServerConnector.monitor(tableName, disableTableStr, region, reservedAddresses, (stub, req, stream) ->
                stub.get().disableRegion(req, stream)
        );
        if (!Thread.currentThread().isInterrupted()) {
            commitRegions(tableName, Collections.singletonList(region));
        }
    }

    // Run RS monitoring threads
    private void addPingChecker(ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers, RegionServerModel rs) {
        if (!pingCheckers.containsKey(rs)) {
            pingCheckers.put(rs, new RsHealthChecker(rs, grpcConf));
        }
    }

    /**
     * Result of checking the current region distribution.
     */
    private static final class RegionsDistributionStatus {

        // Regions to pre-commit and disable
        private final List<RegionDistributionModel> regionsToPreCommitAndDisable;

        // Regions to disable
        private final List<RegionDistributionModel> regionsToDisableOnly;

        // All regions
        private final List<RegionDistributionModel> allRegions;

        private RegionsDistributionStatus(RegionsDistributionStatus.Builder builder) {
            this.regionsToPreCommitAndDisable = builder.regionsToPreCommitAndDisable;
            this.regionsToDisableOnly = builder.regionsToDisableOnly;
            this.allRegions = builder.allRegions;
        }

        public static final class Builder {

            private List<RegionDistributionModel> regionsToPreCommitAndDisable = new ArrayList<>();

            private List<RegionDistributionModel> regionsToDisableOnly = new ArrayList<>();

            private List<RegionDistributionModel> allRegions = new ArrayList<>();

            private Builder() {
            }

            public RegionsDistributionStatus.Builder regionsToPreCommitAndDisable(List<RegionDistributionModel> regionsToPreCommitAndDisable) {
                this.regionsToPreCommitAndDisable = regionsToPreCommitAndDisable;
                return this;
            }

            public RegionsDistributionStatus.Builder regionsToDisableOnly(List<RegionDistributionModel> regionsToDisableOnly) {
                this.regionsToDisableOnly = regionsToDisableOnly;
                return this;
            }

            public RegionsDistributionStatus.Builder allRegions(List<RegionDistributionModel> allRegions) {
                this.allRegions = allRegions;
                return this;
            }

            public RegionsDistributionStatus build() {
                return new RegionsDistributionStatus(this);
            }
        }

        public List<RegionDistributionModel> getRegionsToPreCommitAndDisable() {
            return regionsToPreCommitAndDisable;
        }

        public List<RegionDistributionModel> getRegionsToDisableOnly() {
            return regionsToDisableOnly;
        }

        public List<RegionDistributionModel> getAllRegions() {
            return allRegions;
        }
    }
}
