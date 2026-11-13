/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations;

import com.google.common.base.Preconditions;
import com.light.atalanta.*;
import com.light.atalanta.exceptions.TableNotExistsException;
import com.light.atalanta.exceptions.NoRetryException;
import com.light.atalanta.master.services.TableLock;
import com.light.atalanta.master.services.UniqueNameThreadFactory;
import com.light.atalanta.master.services.grpc.RegionServerConnector;
import com.light.atalanta.master.services.recovery.RecoveryRSContext;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.util.ZookeeperStatus;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.master.vo.DataConfiguration;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.util.FutureUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.STATUS_PREFIX;

import static com.light.atalanta.util.ZookeeperStatus.DROPPING;
import static com.light.atalanta.services.ZooKeeperContextHelper.DISABLED_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.DROPPED_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.DROPPING_REGION_STATUS;

/**
 * Runs runSyncThenAsyncOnError() for a single synchronous attempt that throws on failure,
 * then switches to asynchronous retries. Asynchronous retries run in a single global thread.
 */
@SuppressWarnings("rawtypes")
public class DropTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DropTable.class);

    private final DataConfiguration dataConf;

    private final ZookeeperHelper zooHelper;
    private final ZooKeeperMasterContext zooContext;

    private final RegionServerConnector regionServerConnector;
    private final RecoveryRSContext recoveryRSContext;

    private final ThreadFactory threadFactory = new UniqueNameThreadFactory("atalanta-drop-table-pool");
    private final BlockingDeque<TableLock> tableToDropDeque;
    private final ExecutorService executorService;

    public DropTable(AtalantaDBConfiguration conf, ZookeeperHelper zooHelper, ZooKeeperMasterContext zooContext, RecoveryRSContext recoveryRSContext, RegionServerConnector regionServerConnector, ExecutorService executorService) {
        this.dataConf = conf.getDataConfiguration();
        this.zooHelper = zooHelper;
        this.zooContext = zooContext;
        this.regionServerConnector = regionServerConnector;
        this.recoveryRSContext = recoveryRSContext;
        this.executorService = executorService;
        tableToDropDeque = new LinkedBlockingDeque<>();
        Thread thread = new Thread(this::asyncDrop, "atalanta-master-drop-table");
        thread.start();
    }

    private void asyncDrop() {
        try {
            while (true) {
                do {
                    final TableLock tableLock = tableToDropDeque.take();
                    final TableName tableName = tableLock.getTableName();
                    try {
                        if (zooHelper.checkExists(ZooKeeperContextHelper.getTablePath(tableName))) {
                            if (zooContext.getTableStatus(tableName).equals(DROPPING)) {
                                drop(tableName);
                                // Release the lock in case of success
                                tableLock.unlockAndReleaseRef();
                            }
                        }
                    } catch (NoRetryException ex) {
                        // Stop making retries, release the lock
                        tableLock.unlockAndReleaseRef();
                        LOGGER.error("Error dropping table async: '{}', no retries", tableName, ex);
                    } catch (Exception ex) {
                        // Continue making async retries, requeue the table, don't release the lock
                        LOGGER.error(
                                "Error dropping table async: '{}', waiting {} ms and retry", tableName,
                                dataConf.getDropTableRetryDelayMs()
                        );
                        Thread.sleep(dataConf.getDropTableRetryDelayMs());
                        tableToDropDeque.add(tableLock);
                    }
                } while (!tableToDropDeque.isEmpty());
            }
        } catch (InterruptedException e) {
            LOGGER.info("AtalantaDB asyncDrop interrupted");
        }
    }

    // Tries to run an operation synchronously for one time, releases lock if there is success
    // Switches to asynchronous retries on error (if the error is not of type NoRetryException)
    public void runSyncThenAsyncOnError(TableLock tableLock) throws OperationException {
        final TableName tableName = tableLock.getTableName();
        try {
            drop(tableName);
            // Release the lock in case of success
            tableLock.unlockAndReleaseRef();
        } catch (NoRetryException ex) {
            // Don't go for async retries, release the lock
            tableLock.unlockAndReleaseRef();
            LOGGER.error("Error dropping table sync: '{}', no retries", tableName, ex);
            throw ex;
        } catch (OperationException ex) {
            // Switching to the async retries, _don't_ release the lock
            tableToDropDeque.add(tableLock);
            LOGGER.error("Error dropping table sync: '{}', switching to the async retries", tableName, ex);
            throw ex;
        }
    }

    private void drop(TableName tableName) {
        try {
            // Check whether operations for table are allowed
            recoveryRSContext.checkIncomeTableRequestsEnabled(tableName);
            LOGGER.info("Start dropping table: '{}'", tableName);

            // Check the table and its status
            AtomicBoolean retryMode = new AtomicBoolean(checkTableExists(tableName));

            // Plan the target region state
            RegionsDistributionStatus distribution = getWantedRegionsDistributionStatus(tableName, retryMode.get());

            // Pre-commit the regions and the table for dropping
            makePreCommits(tableName, distribution.getRegionsToPreCommitAndDrop(), retryMode.get());

            // Drop regions on region servers in parallel and wait, commit the regions and drop the table
            dropRegionsAndDeleteTable(executorService, distribution, tableName);
        } catch (NoRetryException ex) {
            throw new NoRetryException(String.format(
                    "Error dropping table: '%s': %s", tableName, ex.getMessage()), ex.getCause()
            );
        } catch (Exception ex) {
            throw new OperationException(
                    "Error dropping table: '" + tableName + "'", ex
            );
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
            case DROPPING:
                retryMode = true;
                break;
            case DISABLED:
                break;
            case CREATED:
            case ASSIGNED:
            case ENABLED:
                throw new NoRetryException("table is enabled. Disable it first");
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
            case DISABLING:
                throw new NoRetryException("table is disabling for now");
            case ASSIGNING:
                throw new NoRetryException("table regions is moving for now");
            default:
                throw new NoRetryException("unexpected status: " + status);
        }
        return retryMode;
    }

    private void makePreCommits(TableName tableName, List<RegionDistributionModel> regionsToPreCommit, boolean retryMode) throws Exception {
        List<CuratorOp> plan = new ArrayList<>();
        LOGGER.debug("Dropping table '{}': pre-commits started", tableName);
        // Pre-commit the table
        if (!retryMode) {
            plan.add(zooHelper.setDataTrans(ZooKeeperContextHelper.getTablePath(tableName), STATUS_PREFIX + DROPPING.getValue()));
        } else {
            LOGGER.debug("Dropping table '{}': retry mode detected", tableName);
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
                plan.add(zooContext.setRegionStatusesTrans(tableName, regionsOnSameRsToPreCommit, currentRS, DROPPING_REGION_STATUS));
                preCommitedRS.add(currentRS);
            }
            // Create a new region from the current one, with a new status only
            RegionModel sourceRegion = region.getRegionModel();
            RegionModel preCommitedRegion = new RegionModel(sourceRegion.getName(),
                    sourceRegion.getReplicaNum(),
                    DROPPING_REGION_STATUS,
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
        LOGGER.debug("Dropping table '{}': finish pre-commits", tableName);
    }

    private RegionsDistributionStatus getWantedRegionsDistributionStatus(TableName tableName, boolean retryMode) throws Exception {
        List<RegionDistributionModel> currentDistribution = zooContext.getRegionDistributionWithStatuses(tableName, DISABLED_REGION_STATUS, DROPPING_REGION_STATUS, DROPPED_REGION_STATUS);
        if (retryMode) {
            // If the table is being dropped again
            if (currentDistribution.isEmpty()) {
                // If there are no regions at all
                throw new Exception("regions not found");
            }
            // Check that there are no unexpected statuses
            long unexpectedStatusCount = currentDistribution.stream()
                    .filter(r -> r.getRegionModel().getStatusId() != DROPPING_REGION_STATUS)
                    .filter(r -> r.getRegionModel().getStatusId() != DROPPED_REGION_STATUS)
                    .count();
            Preconditions.checkArgument(unexpectedStatusCount == 0, "some regions of table not in DROPPING/DROPPED status");
            // Get the regions in pre-commit status
            List<RegionDistributionModel> preCommitDistribution = currentDistribution.stream()
                    .filter(r -> r.getRegionModel().getStatusId() == DROPPING_REGION_STATUS)
                    .collect(Collectors.toList());
            // All regions are committed
            if (preCommitDistribution.isEmpty()) {
                LOGGER.warn("Dropping table '{}': table has all regions commited, dropping table ...", tableName);
            }
            return new RegionsDistributionStatus.Builder()
                    .regionsToDropOnly(preCommitDistribution)
                    .build();
        } else {
            // Check that there are no unexpected statuses
            long unexpectedStatusCount = currentDistribution.stream()
                    .filter(r -> r.getRegionModel().getStatusId() != DISABLED_REGION_STATUS)
                    .count();
            Preconditions.checkArgument(unexpectedStatusCount == 0, "some regions of table not in DISABLED status");
            // If the table is dropped for the first time, get all regions
            List<RegionDistributionModel> initialDistribution = currentDistribution.stream()
                    .filter(r -> r.getRegionModel().getStatusId() == DISABLED_REGION_STATUS)
                    .collect(Collectors.toList());
            return new RegionsDistributionStatus.Builder()
                    .regionsToPreCommitAndDrop(initialDistribution)
                    .build();
        }
    }

    private synchronized void commitRegions(TableName tableName, List<RegionDistributionModel> regions) throws Exception {
        zooContext.setRegionStatus(tableName, regions, DROPPED_REGION_STATUS);
    }

    private void dropRegionsAndDeleteTable(ExecutorService executorService, RegionsDistributionStatus regionsDistributionStatus, TableName tableName) throws Exception {
        List<RegionDistributionModel> distribution = new ArrayList<>();
        distribution.addAll(regionsDistributionStatus.getRegionsToDropOnly());
        distribution.addAll(regionsDistributionStatus.getRegionsToPreCommitAndDrop());
        LOGGER.debug("Dropping table '{}': start dropping regions", tableName);
        createDropRegionsTasks(executorService, distribution, tableName);
        try {
            LOGGER.debug("Dropping table '{}': deleting table node", tableName);
            zooHelper.delete(ZooKeeperContextHelper.getTablePath(tableName));
            String basePathForTableExtParam = ZooKeeperCoreContext.getBasePathForTableExtParam(tableName);
            if (zooHelper.checkExists(basePathForTableExtParam)) {
                zooHelper.delete(basePathForTableExtParam);
            }
        } catch (Exception ex) {
            throw new Exception("unable to delete table in ZooKeeper", ex);
        }
        LOGGER.info("Done dropping table: '{}'", tableName);
    }

    /**
     * Drops regions on region servers asynchronously and commits them on success.
     *
     * @param regionsDistribution distribution of regions across region servers
     * @param tableName           table whose regions are dropped
     */
    private void createDropRegionsTasks(ExecutorService executorService, List<RegionDistributionModel> regionsDistribution, TableName tableName) throws Exception {
        List<CompletableFuture<RegionDistributionModel>> futures = new ArrayList<>();
        for (RegionDistributionModel region : regionsDistribution) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    String dropTableStr = String.format("Dropping table '%s': dropping region '%s'", tableName, region.toShortString());
                    regionServerConnector.monitor(tableName, dropTableStr, region, StringUtils.EMPTY, (stub, req, stream) ->
                            stub.get().dropRegion(req, stream)
                    );
                } catch (Throwable ex) {
                    LOGGER.error("Dropping table '{}': unable to drop region '{}'", tableName, region.toShortString(), ex);
                    throw new CompletionException(ex);
                }
                // Commit the region if the previous step succeeded
                return Thread.currentThread().isInterrupted() ? null : region;

            }, executorService));
        }
        List<RegionDistributionModel> commitRegions = FutureUtil.waitAndGet(futures);
        List<RegionDistributionModel> commitNewRegions = commitRegions.stream()
                .map(region -> {
                    RegionModel oldRegion = region.getRegionModel();
                    RegionModel newRegion = new RegionModel(oldRegion.getName(),
                            oldRegion.getReplicaNum(),
                            DROPPING_REGION_STATUS,
                            oldRegion.getBeginKey(),
                            oldRegion.getEndKey(),
                            oldRegion.getDataBlockSize(),
                            oldRegion.getCompressType(),
                            oldRegion.getRecordCompressionType(),
                            oldRegion.getDictionary(),
                            oldRegion.getRecordTargetCount());
                    return new RegionDistributionModel(newRegion, region.getRegionServerModel());
                }).collect(Collectors.toList());
        commitRegions(tableName, commitNewRegions);
    }

    /**
     * Result of checking the current region distribution.
     */
    private static final class RegionsDistributionStatus {

        // Regions to pre-commit and drop
        private final List<RegionDistributionModel> regionsToPreCommitAndDrop;

        // Regions to drop
        private final List<RegionDistributionModel> regionsToDropOnly;

        private RegionsDistributionStatus(RegionsDistributionStatus.Builder builder) {
            this.regionsToPreCommitAndDrop = builder.regionsToPreCommitAndDrop;
            this.regionsToDropOnly = builder.regionsToDropOnly;
        }

        public static final class Builder {

            private List<RegionDistributionModel> regionsToPreCommitAndDrop = new ArrayList<>();

            private List<RegionDistributionModel> regionsToDropOnly = new ArrayList<>();

            private Builder() {
            }

            public RegionsDistributionStatus.Builder regionsToPreCommitAndDrop(List<RegionDistributionModel> regionsToPreCommitAndDrop) {
                this.regionsToPreCommitAndDrop = regionsToPreCommitAndDrop;
                return this;
            }

            public RegionsDistributionStatus.Builder regionsToDropOnly(List<RegionDistributionModel> regionsToDropOnly) {
                this.regionsToDropOnly = regionsToDropOnly;
                return this;
            }

            public RegionsDistributionStatus build() {
                return new RegionsDistributionStatus(this);
            }
        }

        public List<RegionDistributionModel> getRegionsToPreCommitAndDrop() {
            return regionsToPreCommitAndDrop;
        }

        public List<RegionDistributionModel> getRegionsToDropOnly() {
            return regionsToDropOnly;
        }
    }
}
