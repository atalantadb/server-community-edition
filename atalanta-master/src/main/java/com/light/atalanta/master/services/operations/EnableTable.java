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
import com.light.atalanta.util.FutureUtil;
import com.light.atalanta.util.ZookeeperStatus;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.STATUS_PREFIX;
import static com.light.atalanta.services.ZooKeeperContextHelper.DISABLED_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.ENABLED_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.ENABLING_REGION_STATUS;
import static com.light.atalanta.util.ZookeeperStatus.*;

/**
 * Runs runSyncThenAsyncOnError() for a single synchronous attempt that throws on failure,
 * then switches to asynchronous retries. Asynchronous retries run in a single global thread.
 */
@SuppressWarnings("rawtypes")
public class EnableTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EnableTable.class);

    private final DataConfiguration dataConf;
    private final GrpcConfiguration grpcConf;

    private final ZookeeperHelper zooHelper;
    private final ZooKeeperMasterContext zooContext;

    private final RegionServerConnector regionServerConnector;
    private final RecoveryRSContext recoveryRSContext;

    private final ThreadFactory threadFactory = new UniqueNameThreadFactory("atalanta-enable-table-pool");

    private final BlockingDeque<TableLock> tableToEnableDeque;
    private final ExecutorService executorService;

    public EnableTable(AtalantaDBConfiguration conf, ZookeeperHelper zooHelper, ZooKeeperMasterContext zooContext, RecoveryRSContext recoveryRSContext, RegionServerConnector regionServerConnector, ExecutorService executorService) {
        this.dataConf = conf.getDataConfiguration();
        this.grpcConf = conf.getGrpcConfiguration();
        this.zooHelper = zooHelper;
        this.zooContext = zooContext;
        this.regionServerConnector = regionServerConnector;
        this.recoveryRSContext = recoveryRSContext;
        this.executorService = executorService;
        tableToEnableDeque = new LinkedBlockingDeque<>();
        Thread thread = new Thread(this::asyncEnable, "atalanta-master-enable-table");
        thread.start();
    }

    // Asynchronous retries for the operation
    private void asyncEnable() {
        try {
            while (true) {
                do {
                    TableLock tableLock = tableToEnableDeque.take();
                    final TableName tableName = tableLock.getTableName();
                    try {
                        if (zooHelper.checkExists(ZooKeeperContextHelper.getTablePath(tableName))) {
                            if (zooContext.getTableStatus(tableName).equals(ENABLING)) {
                                enable(tableName);
                                // Release the lock in case of success
                                tableLock.unlockAndReleaseRef();
                            }
                        }
                    } catch (NoRetryException ex) {
                        // Stop making retries, release the lock
                        tableLock.unlockAndReleaseRef();
                        LOGGER.error("Error enabling table async: '{}', no retries", tableName, ex);
                    } catch (Exception ex) {
                        // Continue making async retries, requeue the table, don't release the lock
                        LOGGER.error(
                                "Error enabling table async: '{}', waiting {} ms and retry", tableName,
                                dataConf.getEnableTableRetryDelayMs()
                        );
                        Thread.sleep(dataConf.getEnableTableRetryDelayMs());
                        tableToEnableDeque.add(tableLock);
                    }
                } while (!tableToEnableDeque.isEmpty());
            }
        } catch (InterruptedException e) {
            LOGGER.info("AtalantaDB asyncEnable interrupted");
        }
    }

    // Tries to run an operation synchronously for one time, releases lock if there is success
    // Switches to asynchronous retries on error (if the error is not of type NoRetryException)
    public void runSyncThenAsyncOnError(TableLock tableLock) throws OperationException, NoRetryException {
        final TableName tableName = tableLock.getTableName();
        try {
            enable(tableName);
            // Release the lock in case of success
            // Release the ref, since we won't be using that lock again
            tableLock.unlockAndReleaseRef();
        } catch (NoRetryException ex) {
            // Don't go for async retries, release the lock
            // Release the ref, since we won't be using that lock again
            tableLock.unlockAndReleaseRef();
            LOGGER.error("Error enabling table sync: '{}', no retries", tableName, ex);
            throw ex;
        } catch (OperationException ex) {
            // Switching to the async retries, _don't_ release the lock
            tableToEnableDeque.add(tableLock);
            LOGGER.error("Error enabling table sync: '{}', switching to the async retries", tableName, ex);
            throw ex;
        }
    }

    private void enable(TableName tableName) throws NoRetryException, OperationException {
        ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers = new ConcurrentHashMap<>();
        try {
            // Check whether operations for table are allowed
            recoveryRSContext.checkIncomeTableRequestsEnabled(tableName);
            LOGGER.info("Start enabling table: '{}'", tableName);

            // Check the table and its status
            AtomicBoolean retryMode = new AtomicBoolean(checkTableExists(tableName));

            // Plan the target region state
            RegionsDistributionStatus distribution = getWantedRegionsDistributionStatus(tableName, retryMode.get());

            // Pre-commit the table and regions for enabling
            makePreCommits(tableName, distribution.getRegionsToPreCommitAndEnable(), retryMode.get());

            // Enable regions on region servers in parallel and wait, commit the regions and the table
            enableRegionsAndCommitTable(executorService, distribution, tableName, pingCheckers);
        }  catch (NoRetryException ex) {
            throw new NoRetryException(String.format(
                    "Error enabling table: '%s': %s", tableName, ex.getMessage()), ex.getCause()
            );
        } catch (Exception ex) {
            throw new OperationException(
                    "Error enabling table: '" + tableName + "'", ex
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
            case ENABLING:
                retryMode = true;
                break;
            case DISABLED:
                break;
            case ENABLED:
            case ASSIGNED:
            case CREATED:
                throw new NoRetryException("table already enabled");
            case CREATING:
                throw new NoRetryException("table is creating for now");
            case SPLITTING:
                throw new NoRetryException("table is splitting for now");
            case MODIFYING:
                throw new NoRetryException("table is modifying for now");
            case DISABLING:
                throw new NoRetryException("table is disabling for now");
            case DROPPING:
                throw new NoRetryException("table is dropping for now");
            case ASSIGNING:
                throw new NoRetryException("table regions is moving for now");
            case RECOVERING:
                throw new NoRetryException("table is recovering for now");
            default:
                throw new NoRetryException("unexpected status: " + status);
        }
        return retryMode;
    }

    private void makePreCommits(TableName tableName, List<RegionDistributionModel> regionsToPreCommit, boolean retryMode) throws Exception {
        List<CuratorOp> plan = new ArrayList<>();
        LOGGER.debug("Enabling table '{}': pre-commits started", tableName);

        // Pre-commit the table
        if (!retryMode) {
            plan.add(zooHelper.setDataTrans(ZooKeeperContextHelper.getTablePath(tableName), STATUS_PREFIX + ENABLING.getValue()));
        } else {
            LOGGER.debug("Enabling table '{}': retry mode detected", tableName);
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
                plan.add(zooContext.setRegionStatusesTrans(tableName, regionsOnSameRsToPreCommit, currentRS, ENABLING_REGION_STATUS));
                preCommitedRS.add(currentRS);
            }
            // Create a new region from the current one, with a new status only
            RegionModel sourceRegion = region.getRegionModel();
            RegionModel preCommitedRegion = new RegionModel(sourceRegion.getName(),
                    sourceRegion.getReplicaNum(),
                    ENABLING_REGION_STATUS,
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
        LOGGER.debug("Enabling table '{}': finish pre-commits", tableName);
    }

    private RegionsDistributionStatus getWantedRegionsDistributionStatus(TableName tableName, boolean retryMode) throws Exception {
        List<RegionDistributionModel> currentDistribution = zooContext.getRegionDistributionWithStatuses(tableName);
        if (retryMode) {
            // If the table is being enabled again
            if (currentDistribution.isEmpty()) {
                // But there are no regions at all
                throw new Exception("regions not found");
            }
            // Check that there are no unexpected statuses
            long unexpectedStatusCount = currentDistribution.stream()
                    .filter(r -> r.getRegionModel().getStatusId() != ENABLING_REGION_STATUS)
                    .filter(r -> r.getRegionModel().getStatusId() != ENABLED_REGION_STATUS)
                    .count();
            Preconditions.checkArgument(unexpectedStatusCount == 0, "some regions of table not in ENABLING/ENABLED status");
            // Get the regions in pre-commit status
            List<RegionDistributionModel> preCommitDistribution = currentDistribution.stream()
                    .filter(r -> r.getRegionModel().getStatusId() == ENABLING_REGION_STATUS)
                    .collect(Collectors.toList());
            // All regions are committed
            if (preCommitDistribution.isEmpty()) {
                LOGGER.warn("Enabling table '{}': table has all regions commited, commiting table ...", tableName);
            }
            return new RegionsDistributionStatus.Builder()
                    .regionsToEnableOnly(preCommitDistribution)
                    .allRegions(currentDistribution)
                    .build();
        } else {
            // Check that there are no unexpected statuses
            long unexpectedStatusCount = currentDistribution.stream()
                    .filter(r -> r.getRegionModel().getStatusId() != DISABLED_REGION_STATUS)
                    .count();
            Preconditions.checkArgument(unexpectedStatusCount == 0, "some regions of table not in DISABLED status");
            // If the table is enabled for the first time, get all regions
            List<RegionDistributionModel> initialDistribution = currentDistribution.stream()
                    .filter(r -> r.getRegionModel().getStatusId() == DISABLED_REGION_STATUS)
                    .collect(Collectors.toList());
            return new RegionsDistributionStatus.Builder()
                    .regionsToPreCommitAndEnable(initialDistribution)
                    .allRegions(currentDistribution)
                    .build();
        }
    }

    private synchronized void commitRegions(TableName tableName, List<RegionDistributionModel> regions) throws Exception {
        zooContext.setRegionStatus(tableName, regions, ENABLED_REGION_STATUS);
    }

    private void enableRegionsAndCommitTable(ExecutorService executorService, RegionsDistributionStatus regionsDistributionStatus, TableName tableName, ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers) throws Exception {
        // All replicas
        final List<RegionDistributionModel> distributionToAct = new ArrayList<>();
        distributionToAct.addAll(regionsDistributionStatus.getRegionsToEnableOnly());
        distributionToAct.addAll(regionsDistributionStatus.getRegionsToPreCommitAndEnable());

        // Start pinging the related RS
        regionsDistributionStatus.getRegionsToEnableOnly().stream()
                .map(RegionDistributionModel::getRegionServerModel)
                .forEach(it -> addPingChecker(pingCheckers, it));
        regionsDistributionStatus.getRegionsToPreCommitAndEnable().stream()
                .map(RegionDistributionModel::getRegionServerModel)
                .forEach(it -> addPingChecker(pingCheckers, it));

        // Map leader replicas to the core addresses of their reserves
        Map<RegionDistributionModel, String> leaderToReservedMapping = new HashMap<>();
        // Reserve replicas wrapped in a map
        Map<RegionDistributionModel, String> reservedMapping = new HashMap<>();

        // Fill both mappings depending on whether the replica is a leader or a reserve
        distributionToAct.forEach(rdm -> {
            if (rdm.getRegionModel().getReplicaNum() == 0) {
                // Save the reserve replica addresses to the map
                leaderToReservedMapping.put(rdm, zooContext.getReservedAddresses(regionsDistributionStatus.getAllRegions(), rdm.getRegionModel().getName(), dataConf.getReplicationFactor()));
            } else {
                // Write an empty string for a reserve replica
                reservedMapping.put(rdm, StringUtils.EMPTY);
            }
        });

        // Wait for the reserve replicas to be enabled first, and only then for the leaders
        LOGGER.debug("Enabling table '{}': start enabling reserved replicas", tableName);
        createEnableTableRegionsTasks(executorService, reservedMapping, tableName, pingCheckers);
        LOGGER.debug("Enabling table '{}': start enabling leader replicas", tableName);
        createEnableTableRegionsTasks(executorService, leaderToReservedMapping, tableName, pingCheckers);

        // Commit the table
        try {
            LOGGER.debug("Enabling table '{}': commiting table", tableName);
            zooHelper.setData(ZooKeeperContextHelper.getTablePath(tableName), STATUS_PREFIX + ENABLED.getValue());
        } catch (Exception ex) {
            throw new Exception("unable to commit table in ZooKeeper", ex);
        }

        LOGGER.info("Done enabling table: '{}'", tableName);
    }

    /**
     * Enables regions on region servers asynchronously and commits them on success.
     *
     * @param distributionMapping key: distribution of regions across region servers,
     *                            value: set of reserve replicas, if any
     * @param tableName           table whose regions are enabled
     */
    private void createEnableTableRegionsTasks(ExecutorService executorService, Map<RegionDistributionModel, String> distributionMapping, TableName tableName, ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers) throws Exception {
        List<CompletableFuture<RegionDistributionModel>> futures = new ArrayList<>();
        for (RegionDistributionModel region : distributionMapping.keySet()) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    // enableRegion == distributeRegion
                    // distributeRegion != startRegion
                    String createTableStr = String.format("Enabling table '%s': enabling region '%s'", tableName, region.toShortString());
                    regionServerConnector.monitor(tableName, createTableStr, region, distributionMapping.get(region), (stub, req, stream) ->
                            stub.get().distributeRegion(req, stream)
                    );
                } catch (Throwable ex) {
                    LOGGER.error("Enabling table '{}': unable to enable region '{}'", tableName, region.toShortString(), ex);
                    throw new CompletionException(ex);
                }
                // Commit the region if the previous step succeeded and the thread was not interrupted
                return Thread.currentThread().isInterrupted() ? null : region;

            }, executorService));
        }
        List<RegionDistributionModel> commitRegions = FutureUtil.waitAndGet(futures);
        List<RegionDistributionModel> commitNewRegions = commitRegions.stream()
                .map(region -> {
                    RegionModel oldRegion = region.getRegionModel();
                    RegionModel newRegion = new RegionModel(oldRegion.getName(),
                            oldRegion.getReplicaNum(),
                            ENABLING_REGION_STATUS,
                            oldRegion.getBeginKey(),
                            oldRegion.getEndKey(),
                            oldRegion.getDataBlockSize(),
                            oldRegion.getCompressType(),
                            oldRegion.getRecordCompressionType(),
                            oldRegion.getDictionary(),
                            oldRegion.getRecordTargetCount());
                    return new RegionDistributionModel(newRegion, region.getRegionServerModel());
                }).collect(Collectors.toList());
        RsHealthChecker.checkRsErrors(pingCheckers);
        commitRegions(tableName, commitNewRegions);
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

        // Regions to pre-commit and enable
        private final List<RegionDistributionModel> regionsToPreCommitAndEnable;

        // Regions to enable
        private final List<RegionDistributionModel> regionsToEnableOnly;

        // All regions
        private final List<RegionDistributionModel> allRegions;

        private RegionsDistributionStatus(Builder builder) {
            this.regionsToPreCommitAndEnable = builder.regionsToPreCommitAndEnable;
            this.regionsToEnableOnly = builder.regionsToEnableOnly;
            this.allRegions = builder.allRegions;
        }

        public static final class Builder {

            private List<RegionDistributionModel> regionsToPreCommitAndEnable = new ArrayList<>();

            private List<RegionDistributionModel> regionsToEnableOnly = new ArrayList<>();

            private List<RegionDistributionModel> allRegions = new ArrayList<>();

            private Builder() {
            }

            public EnableTable.RegionsDistributionStatus.Builder regionsToPreCommitAndEnable(List<RegionDistributionModel> regionsToPreCommitAndEnable) {
                this.regionsToPreCommitAndEnable = regionsToPreCommitAndEnable;
                return this;
            }

            public EnableTable.RegionsDistributionStatus.Builder regionsToEnableOnly(List<RegionDistributionModel> regionsToEnableOnly) {
                this.regionsToEnableOnly = regionsToEnableOnly;
                return this;
            }

            public EnableTable.RegionsDistributionStatus.Builder allRegions(List<RegionDistributionModel> allRegions) {
                this.allRegions = allRegions;
                return this;
            }

            public EnableTable.RegionsDistributionStatus build() {
                return new EnableTable.RegionsDistributionStatus(this);
            }
        }

        public List<RegionDistributionModel> getRegionsToPreCommitAndEnable() {
            return regionsToPreCommitAndEnable;
        }

        public List<RegionDistributionModel> getRegionsToEnableOnly() {
            return regionsToEnableOnly;
        }

        public List<RegionDistributionModel> getAllRegions() {
            return allRegions;
        }
    }
}
