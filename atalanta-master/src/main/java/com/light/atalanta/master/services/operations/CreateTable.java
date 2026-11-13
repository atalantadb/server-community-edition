/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations;

import com.google.common.base.Preconditions;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.NamespaceNotExistsException;
import com.light.atalanta.exceptions.NotEnoughRegionServerInstances;
import com.light.atalanta.client.exceptions.TableAlreadyExistsException;
import com.light.atalanta.exceptions.NoRetryException;
import com.light.atalanta.master.services.Master;
import com.light.atalanta.master.services.TableLock;
import com.light.atalanta.master.services.UniqueNameThreadFactory;
import com.light.atalanta.master.services.grpc.RegionServerConnector;
import com.light.atalanta.master.services.grpc.RsHealthChecker;
import com.light.atalanta.master.services.recovery.RecoveryRSContext;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.master.vo.GrpcConfiguration;
import com.light.atalanta.model.*;
import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.master.vo.DataConfiguration;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.util.FutureUtil;
import com.light.atalanta.util.ZookeeperStatus;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.DEFAULT_NAMESPACE;
import static com.light.atalanta.ATALANTAConstants.MAX_SPLIT_KEY_BYTES;
import static com.light.atalanta.ATALANTAConstants.MIN_SPLIT_KEY_BYTES;
import static com.light.atalanta.ATALANTAConstants.STATUS_PREFIX;
import static com.light.atalanta.services.ZooKeeperContextHelper.CREATED_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.CREATING_REGION_STATUS;
import static com.light.atalanta.util.ZookeeperStatus.*;

/**
 * Runs runSyncThenAsyncOnError() for a single synchronous attempt that throws on failure,
 * then switches to asynchronous retries. Asynchronous retries run in a single global thread.
 */
@SuppressWarnings("rawtypes")
public class CreateTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateTable.class);

    private final Master master;

    private final DataConfiguration dataConf;
    private final GrpcConfiguration grpcConf;

    private final ZookeeperHelper zooHelper;
    private final ZooKeeperMasterContext zooContext;

    private final RecoveryRSContext recoveryRSContext;
    private final RegionServerConnector regionServerConnector;

    private final ThreadFactory threadFactory = new UniqueNameThreadFactory("atalanta-create-table-pool");

    private final BlockingDeque<CreateTablePrm> tableToCreateDeque;
    private final ExecutorService executorService;

    public CreateTable(Master master, AtalantaDBConfiguration conf, ZookeeperHelper zooHelper, ZooKeeperMasterContext zooContext, RecoveryRSContext recoveryRSContext, RegionServerConnector regionServerConnector, ExecutorService executorService) {
        this.master = master;
        this.dataConf = conf.getDataConfiguration();
        this.grpcConf = conf.getGrpcConfiguration();
        this.zooHelper = zooHelper;
        this.zooContext = zooContext;
        this.recoveryRSContext = recoveryRSContext;
        this.regionServerConnector = regionServerConnector;
        this.executorService = executorService;
        tableToCreateDeque = new LinkedBlockingDeque<>();
        Thread thread = new Thread(this::asyncCreate, "atalanta-master-create-table");
        thread.start();
    }

    // Asynchronous retries for the operation
    private void asyncCreate() {
        try {
            while (true) {
                do {
                    CreateTablePrm tablePrm = tableToCreateDeque.take();
                    final TableName tableName = tablePrm.tableLock.getTableName();
                    try {
                        if (zooHelper.checkExists(ZooKeeperContextHelper.getTablePath(tableName))) {
                            if (zooContext.getTableStatus(tableName).equals(CREATING)) {
                                create(tablePrm);
                                // Release the lock in case of success
                                tablePrm.tableLock.unlockAndReleaseRef();
                            }
                        }
                    } catch (NoRetryException ex) {
                        // Stop making retries, release the lock
                        tablePrm.tableLock.unlockAndReleaseRef();
                        LOGGER.error("Error creating table async: '{}', no retries", tableName, ex);
                    } catch (Exception ex) {
                        // Continue making async retries, requeue the table, don't release the lock
                        LOGGER.error("Error creating table async. retry.", ex);
                        LOGGER.error(
                                "Error creating table async: '{}', waiting {} ms and retry", tableName,
                                dataConf.getCreateTableRetryDelayMs()
                        );
                        Thread.sleep(dataConf.getCreateTableRetryDelayMs());
                        tableToCreateDeque.add(tablePrm);
                    }
                } while (!tableToCreateDeque.isEmpty());
            }
        } catch (InterruptedException e) {
            LOGGER.info("AtalantaDB asyncCreate interrupted");
        }
    }

    // Tries to run an operation synchronously for one time, releases lock if there is success
    // Switches to asynchronous retries on error (if the error is not of type NoRetryException)
    public void runSyncThenAsyncOnError(TableLock tableLock, List<byte[]> splitKeys, int dataBlockSize,
                                        CompressType compressType, RecordCompressionType recordCompressionType,
                                        boolean dictionary, Long recordTargetCount) throws OperationException, NoRetryException {
        CreateTablePrm createTablePrm = new CreateTablePrm(tableLock, splitKeys, dataBlockSize, compressType,
                recordCompressionType, dictionary, recordTargetCount);
        try {
            create(createTablePrm);
            // Release the lock in case of success
            tableLock.unlockAndReleaseRef();
        } catch (NoRetryException ex) {
            // Don't go for async retries, release the lock
            tableLock.unlockAndReleaseRef();
            LOGGER.error("Error creating table sync: '{}', no retries", tableLock.getTableName(), ex);
            throw ex;
        } catch (OperationException ex) {
            // Switching to the async retries, _don't_ release the lock
            tableToCreateDeque.add(createTablePrm);
            LOGGER.error("Error creating table sync: '{}', switching to the async retries", tableLock.getTableName(), ex);
            throw ex;
        }
    }

    private void create(CreateTablePrm createTablePrm) throws NoRetryException, OperationException {
        ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers = new ConcurrentHashMap<>();
        TableName tableName = createTablePrm.tableLock.getTableName();
        try {
            // Check whether operations for table are allowed
            recoveryRSContext.checkIncomeTableRequestsEnabled(tableName);
            LOGGER.info("Start creating table: '{}'. region count: {}, Comp: {}, " +
                            "Rec Comp: {}, Dict: {}, targetCount {}",
                    tableName, createTablePrm.splitKeys.size() + 1, createTablePrm.compressType,
                    createTablePrm.recordCompressionType, createTablePrm.dictionary, createTablePrm.recordTargetCount );

            // Check whether the namespace and the table exist
            checkNamespaceExists(tableName);
            boolean retryMode = checkTableExists(tableName);

            // Plan the target region distribution: which regions to distribute, which region servers to remove, etc.
            DistributionStatus distribution = getWantedDistributionStatus(tableName,
                    createTablePrm.splitKeys, createTablePrm.dataBlockSize, createTablePrm.compressType, retryMode,
                    createTablePrm.recordCompressionType, createTablePrm.dictionary,
                    createTablePrm.recordTargetCount);

            // Start monitoring the related RS
            distribution.getRegionsToDistributeOnly().stream()
                    .map(RegionDistributionModel::getRegionServerModel)
                    .peek(it -> addPingChecker(pingCheckers, it))
                    .forEach(recoveryRSContext::addPayloaded);
            distribution.getRegionsToPreCommitAndDistribute().stream()
                    .map(RegionDistributionModel::getRegionServerModel)
                    .peek(it -> addPingChecker(pingCheckers, it))
                    .forEach(recoveryRSContext::addPayloaded);

            // Pre-commit the table and regions, remove lost RS
            makePreCommits(tableName, distribution.getRegionsToPreCommitAndDistribute(), retryMode, distribution.getDiedRegionServers());

            // Update the region distribution after pre-commits / re-pre-commits if we are retrying
            if (retryMode) {
                distribution.setAllRegions(
                        zooContext.getRegionDistributionWithStatuses(tableName, CREATING_REGION_STATUS, CREATED_REGION_STATUS)
                );
            }

            // Distribute regions to RS in parallel and wait, commit the regions and the table
            distributeRegionsAndCommitTable(executorService, distribution, tableName, pingCheckers);
        } catch (NoRetryException ex) {
            throw new NoRetryException(String.format(
                    "Error creating table: '%s': %s", tableName, ex.getMessage()), ex.getCause()
            );
        } catch (Exception ex) {
            throw new OperationException(
                    "Error creating table: '" + tableName + "'", ex
            );
        } finally {
            pingCheckers.values().forEach(RsHealthChecker::close);
        }
    }

    private void checkNamespaceExists(TableName tableName) throws Exception {
        boolean namespaceNotExists;
        try {
            namespaceNotExists = !zooHelper.checkExists(tableName.getNamespace());
        } catch (Exception ex) {
            throw new Exception("unable to check namespace exists", ex);
        }
        if (namespaceNotExists) {
            // Create the default namespace if it doesn't exist
            if (dataConf.isAutoCreateNs() || tableName.getNamespace().equals(DEFAULT_NAMESPACE)) {
                master.createNamespace(tableName.getNamespace());
            } else {
                throw new NoRetryException(new NamespaceNotExistsException(tableName.getNamespace()));
            }
        }
    }

    private boolean checkTableExists(TableName tableName) throws Exception {
        boolean tableExists = false;
        boolean retryMode = false;
        try {
            String tablePath = ZooKeeperContextHelper.getTablePath(tableName);
            if (zooHelper.checkExists(tablePath)) {
                retryMode = ZookeeperStatus.fromString(zooHelper.getNotNullString(tablePath)).equals(CREATING);
                tableExists = !retryMode;
            }
        } catch (Exception ex) {
            throw new Exception("unable to check table exists", ex);
        }
        if (tableExists) {
            throw new NoRetryException(new TableAlreadyExistsException(tableName));
        }
        return retryMode;
    }

    private void makePreCommits(TableName tableName, List<RegionDistributionModel> regionsToPreCommit, boolean retryMode, List<RegionServerModel> lostRegionServers) throws Exception {
        List<CuratorOp> plan = new ArrayList<>();
        LOGGER.debug("Creating table '{}': pre-commits started", tableName);
        // The disable and drop table operations update statuses.
        // That is not needed here, since region models are created on the fly rather than loaded from ZooKeeper.

        // Pre-commit the table
        if (!retryMode) {
            plan.add(zooHelper.createTrans(ZooKeeperContextHelper.getTablePath(tableName), STATUS_PREFIX + CREATING.getValue()));
        } else {
            LOGGER.debug("Creating table '{}': retry mode detected", tableName);
        }
        // Pre-commit the regions
        List<RegionServerModel> preCommitedRS = new ArrayList<>();
        if (!regionsToPreCommit.isEmpty()) {
            zooContext.addExtParamToPlan(plan, tableName,
                    regionsToPreCommit.stream().findAny().get().getRegionModel());
        } else {
            LOGGER.debug("Call makePreCommits with empty regionsToPreCommit. " +
                    "Table: {}, retryMode: {}, lostRegionServer count: {}",
                    tableName, retryMode, lostRegionServers.size());
        }
        for (RegionDistributionModel region : regionsToPreCommit) {
            RegionServerModel currentRS = region.getRegionServerModel();
            if (preCommitedRS.contains(currentRS)) {
                continue;
            }
            List<RegionModel> regionsOnSameRsToPreCommit = regionsToPreCommit.stream()
                    .filter(rdm -> rdm.getRegionServerModel().equals(currentRS))
                    .map(RegionDistributionModel::getRegionModel)
                    .collect(Collectors.toList());
            plan.addAll(zooContext.addNewRegionsTrans(tableName, regionsOnSameRsToPreCommit, currentRS));
            preCommitedRS.add(currentRS);
        }
        // Remove lost region servers
        for (RegionServerModel lostRegionServer : lostRegionServers) {
            try {
                LOGGER.info("Creating table '{}': deleting region server {} from ZooKeeper", tableName, lostRegionServer);
                plan.addAll(zooContext.deleteRegionServerFromTableTrans(tableName, lostRegionServer));
            } catch (Exception ex) {
                throw new Exception("unable to remove region server " + lostRegionServer + " in ZooKeeper", ex);
            }
        }
        // Run the pre-commits
        if (!plan.isEmpty()) {
            Preconditions.checkArgument(zooHelper.forOperations(plan).size() == plan.size(), "unexpected transaction error");
        }
        LOGGER.debug("Creating table '{}': finish pre-commits", tableName);
    }

    private void distributeRegionsAndCommitTable(ExecutorService executorService,
                                                 DistributionStatus distributionStatus,
                                                 TableName tableName,
                                                 ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers) throws Exception {
        List<RegionDistributionModel> distributionToAct = new ArrayList<>();
        distributionToAct.addAll(distributionStatus.getRegionsToDistributeOnly());
        distributionToAct.addAll(distributionStatus.getRegionsToPreCommitAndDistribute());

        // Map leader replicas to the core addresses of their reserves
        Map<RegionDistributionModel, String> leaderToReservedMapping = new HashMap<>();
        // For reserve replicas the key has an empty value
        Map<RegionDistributionModel, String> reservedMapping = new HashMap<>();

        // Fill the mapping depending on whether the replica is a leader or a reserve
        distributionToAct.forEach(rdm -> {
            if (rdm.getRegionModel().getReplicaNum() == 0) {
                // Save the reserve replica addresses to the map
                leaderToReservedMapping.put(rdm, zooContext.getReservedAddresses(distributionStatus.getAllRegions(), rdm.getRegionModel().getName(), dataConf.getReplicationFactor()));
            } else {
                // Write an empty string for a reserve replica
                reservedMapping.put(rdm, StringUtils.EMPTY);
            }
        });

        // Additional check: every reserve must have a leader.
        // If not, log a warning and also send assign commands
        // for those leaders to update their reserve addresses.
        // This happens when some RS disconnects while the table is being created.
        // On a CreateTable restart the master by default processes only the regions
        // that were on the lost RS, and won't update their reserve addresses by itself.
        Map<RegionDistributionModel, String> explicitLeaderToReservedMapping = new HashMap<>();
        Set<String> forceLeaderUpdates = new HashSet<>();
        for (RegionDistributionModel rdm : reservedMapping.keySet()) {
            String reservedName = rdm.getRegionModel().getName();
            // If some reserves are distributed without their leaders, we must explicitly send commands
            // to the corresponding leaders to update the replica addresses
            if (leaderToReservedMapping.keySet().stream()
                    .map(leader -> leader.getRegionModel().getName())
                    .noneMatch(reservedName::equals)) {
                LOGGER.warn("Creating table '{}': planned force leader addresses update for {}", tableName, rdm.toShortString());
                forceLeaderUpdates.add(reservedName); // reserved replica name always equals leader replica name
            }
        }
        // Go through the leaders that we must rerun and collect the reserve addresses
        distributionStatus.getAllRegions().stream()
                .filter(leader -> leader.getRegionModel().getReplicaNum() == 0)
                .filter(leader -> forceLeaderUpdates.contains(leader.getRegionModel().getName()))
                .forEach(rdm ->
                        explicitLeaderToReservedMapping.put(
                                rdm, zooContext.getReservedAddresses(distributionStatus.getAllRegions(), rdm.getRegionModel().getName(), dataConf.getReplicationFactor())
                        )
                );
        if (!explicitLeaderToReservedMapping.isEmpty()) {
            LOGGER.warn("Creating table '{}': start forced leaders update", tableName);
            createDistributeRegionsTasks(executorService, explicitLeaderToReservedMapping, tableName, pingCheckers);
        }

        // Wait for the reserve replicas to be distributed first, and only then for the leaders
        LOGGER.debug("Creating table '{}': start distributing reserved replicas", tableName);
        createDistributeRegionsTasks(executorService, reservedMapping, tableName, pingCheckers);
        LOGGER.debug("Creating table '{}': start distributing leader replicas", tableName);
        createDistributeRegionsTasks(executorService, leaderToReservedMapping, tableName, pingCheckers);

        try {
            // Commit the table
            LOGGER.debug("Creating table '{}': commiting table", tableName);
            zooHelper.setData(ZooKeeperContextHelper.getTablePath(tableName), STATUS_PREFIX + CREATED.getValue());
        } catch (Exception ex) {
            throw new Exception("unable to commit table in ZooKeeper or delete split_keys node", ex);
        }
        LOGGER.info("Done creating table: '{}'", tableName);
    }

    private synchronized void commitRegions(TableName tableName, List<RegionDistributionModel> regions) throws Exception {
        LOGGER.info("Start commit regions to ZK. size: {}", regions.size());
        zooContext.setRegionStatus(tableName, regions, CREATED_REGION_STATUS);
        LOGGER.info("Complete commit regions to ZK. size: {}", regions.size());
    }

    /**
     * Plans the distribution of regions across region servers based on the regions of dead region servers.
     */
    private List<RegionDistributionModel> planDistribution(List<RegionDistributionModel> oldDistribution, List<RegionModel> regionsToRetry) throws Exception {
        List<RegionServerModel> runningRS = zooContext.getRunningRegionServers();
        List<RegionDistributionModel> newDistribution = new ArrayList<>();
        try {
            // Assign a new region server to each region
            for (RegionModel region : regionsToRetry) {
                RegionServerModel regionServer;
                try {
                    regionServer = zooContext.getRandomRunningRegionServerWithoutRegionForCreateTable(runningRS, oldDistribution, newDistribution, region);
                } catch (NotEnoughRegionServerInstances ex) {
                    // If every region server already has this region, take one that has it
                    regionServer = zooContext.getRandomRegionServer(runningRS);
                }
                newDistribution.add(new RegionDistributionModel(region, regionServer));
            }
        } catch (Exception ex) {
            throw new Exception("unable to plan region distribution", ex);
        }
        return newDistribution;
    }

    private void createDistributeRegionsTasks(ExecutorService executorService, Map<RegionDistributionModel, String> distributionMapping, TableName tableName, ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers) throws Exception {
        List<CompletableFuture<RegionDistributionModel>> futures = new ArrayList<>();
        for (RegionDistributionModel region : distributionMapping.keySet()) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    // enableRegion == distributeRegion
                    // distributeRegion != startRegion
                    String createTableStr = String.format("Creating table '%s': distributing region '%s'", tableName, region.toShortString());
                    regionServerConnector.monitor(tableName, createTableStr, region, distributionMapping.get(region), (stub, req, stream) ->
                            stub.get().distributeRegion(req, stream)
                    );
                } catch (Throwable ex) {
                    LOGGER.error("Creating table '{}': unable to distribute region '{}'", tableName, region.toShortString(), ex);
                    throw new CompletionException(ex);
                }
                // Commit the region if the previous step succeeded and the thread was not interrupted
                return Thread.currentThread().isInterrupted() ? null : region;

            }, executorService));
        }
        List<RegionDistributionModel> commitRegions = FutureUtil.waitAndGet(futures);
        LOGGER.info("Distribute regions is complete. map size: {}", distributionMapping.size());
        RsHealthChecker.checkRsErrors(pingCheckers);
        commitRegions(tableName, commitRegions);
    }

    private DistributionStatus getWantedDistributionStatus(TableName tableName,
                                                           List<byte[]> splitKeys,
                                                           int dataBlockSize,
                                                           CompressType compressType,
                                                           boolean retryMode,
                                                           RecordCompressionType recordCompressionType,
                                                           boolean dictionary, Long recordTargetCount) throws Exception {
        /* If the table is created for the first time, build the distribution from scratch */
        if (!retryMode) {
            List<RegionDistributionModel> regions = planDistribution(splitKeys, dataBlockSize, compressType,
                    recordCompressionType, dictionary, recordTargetCount);
            return new DistributionStatus.Builder()
                    .regionsToPreCommitAndDistribute(regions)
                    .allRegions(regions)
                    .build();
        }
        /* In pre-commit mode, read what is in ZooKeeper */
        List<RegionDistributionModel> currentDistribution = zooContext.getRegionDistributionWithStatuses(tableName, CREATING_REGION_STATUS, CREATED_REGION_STATUS);
        if (currentDistribution.isEmpty()) {
            throw new Exception("no regions found");
        } else {
            // If there are regions, check whether their region servers are alive
            LOGGER.warn("Creating table '{}': found pre-commit region distribution, check region servers ...", tableName);
            Set<RegionServerModel> regionServersToCheck = new HashSet<>();
            currentDistribution.forEach(d -> regionServersToCheck.add(d.getRegionServerModel()));
            List<RegionServerModel> runningRegionServers = zooContext.getRunningRegionServers();
            if (new HashSet<>(runningRegionServers).containsAll(regionServersToCheck)) {
                // If all region servers are alive
                return makeDistributionStatusWhenAllRunning(tableName, currentDistribution);
            } else {
                // If some are dead
                return makeDistributionStatusWhenSomeDied(tableName, currentDistribution, regionServersToCheck, runningRegionServers);
            }
        }
    }

    private DistributionStatus makeDistributionStatusWhenAllRunning(TableName tableName, List<RegionDistributionModel> currentDistribution) {
        LOGGER.info("Creating table '{}': all region servers is running", tableName);
        // If all region servers are alive, return the distribution of uncommitted regions
        List<RegionDistributionModel> preCommitRegions = currentDistribution.stream()
                .filter(r -> r.getRegionModel().getStatusId() == CREATING_REGION_STATUS)
                .collect(Collectors.toList());
        if (preCommitRegions.isEmpty()) {
            LOGGER.warn("Creating table '{}': table has all regions commited, commiting table ...", tableName);
            return new DistributionStatus.Builder().build();
        } else {
            LOGGER.warn("Creating table '{}': table has uncommited regions: {}", tableName, preCommitRegions);
            return new DistributionStatus.Builder()
                    .regionsToDistributeOnly(preCommitRegions)
                    .allRegions(currentDistribution)
                    .build();
        }
    }

    private DistributionStatus makeDistributionStatusWhenSomeDied(TableName tableName, List<RegionDistributionModel> currentDistribution, Set<RegionServerModel> currentTableRegionServers, List<RegionServerModel> runningRegionServers) throws Exception {
        // If some region server is dead
        List<RegionServerModel> diedRegionServers = new ArrayList<>(currentTableRegionServers);
        diedRegionServers.removeAll(runningRegionServers);
        LOGGER.warn("Creating table '{}': region servers not running: {}, redistributing that regions ...", tableName, diedRegionServers);
        // Pre-commit and distribute the regions of dead region servers
        List<RegionModel> regionsFromDiedRegionServers = currentDistribution.stream()
                .filter(r -> diedRegionServers.contains(r.getRegionServerModel()))
                .map(r -> {
                    // Reset the status to pre-commit
                    RegionModel sourceRegion = r.getRegionModel();
                    return new RegionModel(sourceRegion.getName(), sourceRegion.getReplicaNum(), CREATING_REGION_STATUS,
                            sourceRegion.getBeginKey(), sourceRegion.getEndKey(),
                            sourceRegion.getDataBlockSize(), sourceRegion.getCompressType(),
                            sourceRegion.getRecordCompressionType(), sourceRegion.getDictionary(),
                            sourceRegion.getRecordTargetCount());
                })
                .collect(Collectors.toList());
        // Also take into account the uncommitted regions of live region servers
        List<RegionDistributionModel> regionsFromRunningRegionServersToRetry = currentDistribution.stream()
                .filter(r -> !diedRegionServers.contains(r.getRegionServerModel()))
                .filter(r -> r.getRegionModel().getStatusId() == CREATING_REGION_STATUS)
                .collect(Collectors.toList());
        return new DistributionStatus.Builder()
                .regionsToPreCommitAndDistribute(planDistribution(currentDistribution, regionsFromDiedRegionServers))
                .regionsToDistributeOnly(regionsFromRunningRegionServersToRetry)
                .diedRegionServers(diedRegionServers)
                .allRegions(currentDistribution)
                .build();
    }

    private List<RegionDistributionModel> planDistribution(List<byte[]> splitKeys,
                                                           int dataBlockSize,
                                                           CompressType compressType,
                                                           RecordCompressionType recordCompressionType,
                                                           boolean dictionary, Long recordTargetCount) throws Exception {
        List<RegionDistributionModel> distribution = new ArrayList<>();
        int replicationFactor = dataConf.getReplicationFactor();
        int regionsCount = splitKeys.size() + 1;
        // Get the list of available RS
        List<RegionServerModel> runningRS = zooContext.getRunningRegionServers();
        if (runningRS.isEmpty()) {
            throw new NoRetryException(new NotEnoughRegionServerInstances(0));
        }
        // Shuffle
        Collections.shuffle(runningRS);
        for (int currentRegionIndex = 0; currentRegionIndex < regionsCount; currentRegionIndex++) {
            String regionName = RegionModel.makeRandomRegionName();
            byte[] startKey = getRegionStart(splitKeys, currentRegionIndex);
            byte[] endKey = getRegionEnd(splitKeys, currentRegionIndex, regionsCount);
            for (int replicaNum = 0; replicaNum < replicationFactor; replicaNum++) {
                RegionModel regionToAdd = new RegionModel(regionName, replicaNum,
                        CREATING_REGION_STATUS, startKey, endKey, dataBlockSize, compressType,
                        recordCompressionType, dictionary, recordTargetCount);
                // Spread regions across all RS as evenly as possible, placing replicas on different servers where possible
                int curRs = (currentRegionIndex * replicationFactor
                        + ((replicaNum + currentRegionIndex/ runningRS.size()) % replicationFactor) ) % runningRS.size();
                distribution.add(new RegionDistributionModel(regionToAdd, runningRS.get(curRs)));
            }
        }
        return distribution;
    }

    // Run RS monitoring threads
    private void addPingChecker(ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers, RegionServerModel rs) {
        if (!pingCheckers.containsKey(rs)) {
            pingCheckers.put(rs, new RsHealthChecker(rs, grpcConf));
        }
    }

    /**
     * Returns the start key of a region by its number.
     *
     * @param splitKeys    distribution of keys across regions
     * @param regionNumber region number
     */
    private byte[] getRegionStart(List<byte[]> splitKeys, int regionNumber) {
        // If this is the last or the only region
        if (regionNumber == 0) {
            return MIN_SPLIT_KEY_BYTES;
        }
        // splitKeys has no lower bound, hence -1
        return splitKeys.get(regionNumber - 1);
    }

    /**
     * Returns the end key of a region by its number.
     *
     * @param splitKeys    distribution of keys across regions
     * @param regionNumber region number
     */
    private byte[] getRegionEnd(List<byte[]> splitKeys, int regionNumber, int regionsCount) {
        // If this is the last or the only region
        if (regionsCount == 0 || regionNumber == regionsCount - 1) {
            return MAX_SPLIT_KEY_BYTES;
        }
        return splitKeys.get(regionNumber);
    }

    /**
     * Result of checking the current region distribution.
     */
    private static final class DistributionStatus {

        // Dead region servers to remove
        private final List<RegionServerModel> diedRegionServers;

        // Regions to pre-commit and distribute
        private final List<RegionDistributionModel> regionsToPreCommitAndDistribute;

        // Regions to distribute
        private final List<RegionDistributionModel> regionsToDistributeOnly;

        // All regions
        private List<RegionDistributionModel> allRegions;

        private DistributionStatus(Builder builder) {
            this.diedRegionServers = builder.diedRegionServers;
            this.regionsToPreCommitAndDistribute = builder.regionsToPreCommitAndDistribute;
            this.regionsToDistributeOnly = builder.regionsToDistributeOnly;
            this.allRegions = builder.allRegions;
        }

        private static final class Builder {

            private List<RegionServerModel> diedRegionServers = new ArrayList<>();

            private List<RegionDistributionModel> regionsToPreCommitAndDistribute = new ArrayList<>();

            private List<RegionDistributionModel> regionsToDistributeOnly = new ArrayList<>();

            private List<RegionDistributionModel> allRegions = new ArrayList<>();

            private Builder() {
            }

            public Builder diedRegionServers(List<RegionServerModel> diedRegionServers) {
                this.diedRegionServers = diedRegionServers;
                return this;
            }

            public Builder regionsToPreCommitAndDistribute(List<RegionDistributionModel> regionsToPreCommitAndDistribute) {
                this.regionsToPreCommitAndDistribute = regionsToPreCommitAndDistribute;
                return this;
            }

            public Builder regionsToDistributeOnly(List<RegionDistributionModel> regionsToDistributeOnly) {
                this.regionsToDistributeOnly = regionsToDistributeOnly;
                return this;
            }

            public DistributionStatus.Builder allRegions(List<RegionDistributionModel> allRegions) {
                this.allRegions = allRegions;
                return this;
            }

            public DistributionStatus build() {
                return new DistributionStatus(this);
            }
        }

        public List<RegionServerModel> getDiedRegionServers() {
            return diedRegionServers;
        }

        public List<RegionDistributionModel> getRegionsToPreCommitAndDistribute() {
            return regionsToPreCommitAndDistribute;
        }

        public List<RegionDistributionModel> getRegionsToDistributeOnly() {
            return regionsToDistributeOnly;
        }

        public List<RegionDistributionModel> getAllRegions() {
            return allRegions;
        }

        public void setAllRegions(List<RegionDistributionModel> allRegions) {
            this.allRegions = allRegions;
        }
    }

    private static class CreateTablePrm {
        private final TableLock tableLock;
        private final List<byte[]> splitKeys;
        private final int dataBlockSize;
        private final CompressType compressType;
        private final RecordCompressionType recordCompressionType;
        private final boolean dictionary;
        private final Long recordTargetCount;

        private CreateTablePrm(TableLock tableLock, List<byte[]> splitKeys, int dataBlockSize, CompressType compressType, RecordCompressionType recordCompressionType, boolean dictionary, Long recordTargetCount) {
            this.tableLock = tableLock;
            this.splitKeys = splitKeys;
            this.dataBlockSize = dataBlockSize;
            this.compressType = compressType;
            this.recordCompressionType = recordCompressionType;
            this.dictionary = dictionary;
            this.recordTargetCount = recordTargetCount;
        }
    }
}
