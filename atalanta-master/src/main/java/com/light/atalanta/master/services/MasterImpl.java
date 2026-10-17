/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services;

import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.TableBlockedException;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.master.services.grpc.RegionServerConnector;
import com.light.atalanta.master.services.helpers.TableCandidateSelector;
import com.light.atalanta.master.services.operations.*;
import com.light.atalanta.master.services.operations.move.MoveRegion;
import com.light.atalanta.master.services.rebalance.RebalanceBlueprint;
import com.light.atalanta.master.services.rebalance.RegionRebalanceModel;
import com.light.atalanta.master.services.recovery.RecoveryRSContext;
import com.light.atalanta.master.services.recovery.RegionRecoveryModel;
import com.light.atalanta.master.services.runners.PreCommitsRunner;
import com.light.atalanta.master.services.runners.RegionsRebalanceRunner;
import com.light.atalanta.master.services.runners.RegionsRecoveryRunner;
import com.light.atalanta.master.services.runners.RegionsSplitRunner;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.metrics.RsJmxClient;
import com.light.atalanta.model.*;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.services.ZookeeperHelperExtended;
import io.grpc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

public class MasterImpl implements Master {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterImpl.class);

    private final AtalantaDBConfiguration conf;
    private final ZookeeperHelperExtended zookeeperHelperExtended;

    private final OperationRunner operationRunner;

    private final CreateNamespace createNamespace;
    private final CreateTable createTable;
    private final EnableTable enableTable;
    private final DisableTable disableTable;
    private final DropTable dropTable;
    private final MoveRegion moveRegion;
    private final SplitTable splitTable;
    private final ModifyTable modifyTable;
    private final RecoverTable recoverTable;
    private final SyncTableReplicas syncTableReplicas;
    private final RebalanceTableReplicas rebalanceTableReplicas;
    private final RebalanceTableRegions rebalanceTableRegions;
    private final CreateSnapshot createSnapshot;
    private final CloneSnapshot cloneSnapshot;
    private final DeleteSnapshot deleteSnapshot;
    private final StartStopRegionServer startStopRegionServer;

    private final PreCommitsRunner preCommitsRunner;
    private final RegionsRecoveryRunner regionsRecoveryRunner;
    private final RegionsSplitRunner regionsSplitRunner;
    private final RegionsRebalanceRunner regionsRebalanceRunner;
    private final RecoveryRSContext recoveryRSContext;
    private final ZooKeeperMasterContext zooContext;
    private final ZookeeperHelper zooHelper;

    private final ExecutorService rebalanceExecutor;
    private final ExecutorService syncExecutor;
    private final ExecutorService operationStoppageExecutor;

    private final RsJmxClient rsJmxClient;

    private final ThreadFactory rebalanceThreadFactory = new UniqueNameThreadFactory("atalanta-rebalance-manual-pool");
    private final ThreadFactory syncThreadFactory = new UniqueNameThreadFactory("atalanta-sync-manual-pool");
    private final ThreadFactory recoveryNotifierThreadFactory = new UniqueNameThreadFactory("atalanta-recovery-notifier-pool");

    private final LockManager lockManager;
    private final RecoveryLogger recoveryLogger;
    private final MasterStartLogger masterStartLogger;
    private final ExecutorService executorService;
    private volatile Thread startupThread;

    public MasterImpl(AtalantaDBConfiguration conf, ZookeeperHelper zooHelper, ZooKeeperMasterContext zooContext,
                      RegionServerConnector regionServerConnector, RecoveryRSContext recoveryRSContext,
                      ModifyTable modifyTable, PreCommitsRunner preCommitsRunner, RegionsRecoveryRunner regionsRecoveryRunner,
                      RegionsSplitRunner regionsSplitRunner, RegionsRebalanceRunner regionsRebalanceRunner,
                      ZookeeperHelperExtended zookeeperHelperExtended, RsJmxClient rsJmxClient,
                      RecoveryLogger recoveryLogger, MasterStartLogger masterStartLogger, LockManager lockManager,
                      EnableTable enableTable, ExecutorService executorService) throws ZKException {
        this.modifyTable = modifyTable;
        this.rsJmxClient = rsJmxClient;
        this.recoveryLogger = recoveryLogger;
        this.masterStartLogger = masterStartLogger;
        this.executorService = executorService;
        LOGGER.info("Master configuration is: {}", conf);
        this.conf = conf;
        this.createNamespace = new CreateNamespace(zooHelper);
        this.createTable = new CreateTable(this, conf, zooHelper, zooContext, recoveryRSContext, regionServerConnector, executorService);
        this.enableTable = enableTable;
        this.disableTable = new DisableTable(conf, zooHelper, zooContext, recoveryRSContext, regionServerConnector, executorService);
        this.dropTable = new DropTable(conf, zooHelper, zooContext, recoveryRSContext, regionServerConnector, executorService);
        this.moveRegion = new MoveRegion(conf, zooContext, zooHelper, regionServerConnector, recoveryRSContext, executorService);
        this.splitTable = new SplitTable(conf, zooContext, zooHelper, recoveryRSContext, regionServerConnector, this.rsJmxClient, executorService);
        this.recoverTable = new RecoverTable(conf, zooContext, zooHelper, recoveryRSContext, regionServerConnector, rsJmxClient, executorService);
        this.syncTableReplicas = new SyncTableReplicas(conf, zooContext, zooHelper, this);
        this.rebalanceTableReplicas = new RebalanceTableReplicas(zooContext, zooHelper, this);
        this.rebalanceTableRegions = new RebalanceTableRegions(zooContext, zooHelper, this);
        this.recoveryRSContext = recoveryRSContext;
        this.regionsRecoveryRunner = regionsRecoveryRunner;
        this.regionsSplitRunner = regionsSplitRunner;
        this.regionsRebalanceRunner = regionsRebalanceRunner;
        this.preCommitsRunner = preCommitsRunner;
        this.createSnapshot = new CreateSnapshot(zooHelper, zooContext, regionServerConnector);
        this.cloneSnapshot = new CloneSnapshot(zooHelper, zooContext, regionServerConnector, lockManager, conf, enableTable, this.executorService);
        this.deleteSnapshot = new DeleteSnapshot(zooHelper, zooContext, regionServerConnector);
        this.zookeeperHelperExtended = zookeeperHelperExtended;
        this.startStopRegionServer = new StartStopRegionServer(zookeeperHelperExtended, zooContext, moveRegion, conf, executorService);
        this.zooContext = zooContext;
        this.zooHelper = zooHelper;
        this.operationRunner = new OperationRunner(conf, zooContext, regionServerConnector);
        this.lockManager = lockManager;
        this.rebalanceExecutor = Executors.newFixedThreadPool(conf.getDataConfiguration().getRebalanceTableThreadsCount(), rebalanceThreadFactory);
        this.syncExecutor = Executors.newFixedThreadPool(conf.getDataConfiguration().getRegionsRecoveryTableThreadsCount(), syncThreadFactory);
        this.operationStoppageExecutor = Executors.newFixedThreadPool(conf.getDataConfiguration().getRegionsRecoveryOperationsStoppageTableThreadsCount(), recoveryNotifierThreadFactory);
    }

    @Override
    public boolean start(Server server) throws Exception {
        startupThread = Thread.currentThread();
        int allDistributionCount = zooContext.loadAllDistribution();
        masterStartLogger.totalRegions(allDistributionCount);
        // Stop all running operations on all RS for all tables
        List<RegionServerModel> runningRegionServers = zooContext.getRunningRegionServers();
        masterStartLogger.waitAllRSForAllTables(runningRegionServers.size());
        if (!runningRegionServers.isEmpty()) {
            operationRunner.waitAllRSForAllTables(conf.getDataConfiguration().getRegionsRecoveryOperationsStoppageRSThreadsCount());
        } else {
            LOGGER.info("There are no running RS to stop tasks on");
        }
        masterStartLogger.setState(MasterStartState.SCAN_RECOVERING_TABLES);
        // Scan recovery pre-commits
        Map<TableName, List<RegionServerModel>> recoveringTables = preCommitsRunner.scanRecoveringTables(this);
        masterStartLogger.tableToRecoveryCount(recoveringTables.size());
        // Add all previously lost RS to the list of RS that are not allowed to start
        recoveringTables.values().stream()
                .flatMap(Collection::stream)
                .forEach(recoveryRSContext::addRecovering);
        // Lock all tables to be recovered in advance, so that the user can't do anything with them
        ConcurrentHashMap<TableName, TableLock> tableLocks = new ConcurrentHashMap<>();
        CompletableFuture<?>[] stopOperations = new CompletableFuture[recoveringTables.size()];
        AtomicInteger idx = new AtomicInteger(0);
        masterStartLogger.setState(MasterStartState.START_LOCK_RECOVERING_TABLE);
        recoveringTables.keySet().forEach(tbl ->
                stopOperations[idx.getAndIncrement()] = CompletableFuture.runAsync(
                        () -> tableLocks.put(tbl, notifyTableRecovering(tbl)), operationStoppageExecutor

                )
        );
        CompletableFuture.allOf(stopOperations).get();
        operationStoppageExecutor.shutdownNow();
        masterStartLogger.setState(MasterStartState.COMPLETE_LOCK_RECOVERING_TABLE);
        // Start the gRPC server
        server.start();
        LOGGER.info("GRPC server started. {}", conf.getGrpcConfiguration().myName());
        masterStartLogger.setState(MasterStartState.GRPC_SERVER_STARTED);
        // Start lock monitoring
        lockManager.startLockMonitoring();
        LOGGER.info("Lock monitoring started.");
        // Run synchronous region recovery pre-commits, if any
        preCommitsRunner.syncRecoveryAction(recoveringTables.keySet(), tableLocks, this);
        LOGGER.info("Sync recovery started.");
        // Start RS health monitoring
        regionsRecoveryRunner.action(this);
        LOGGER.info("regionsRecoveryRunner started.");
        // Start region size monitoring, run splits when needed
        if (conf.getDataConfiguration().getRegionsAutoSplitEnabled()) {
            regionsSplitRunner.action(this);
            LOGGER.info("regionsSplitRunner started.");
        } else {
            LOGGER.info("Regions auto splitting was disabled in the settings");
        }
        // Start region distribution monitoring, run rebalancing when needed
        if (conf.getDataConfiguration().getRegionsAutoRebalanceEnabled()) {
            regionsRebalanceRunner.action(this);
            LOGGER.info("regionsRebalanceRunner started.");
        } else {
            LOGGER.info("Regions auto rebalancing was disabled in the settings");
        }
        // Run asynchronous pre-commits, if any.
        // PreCommitsRunner has already run for recoveringTables (after recovery), so skip them this time.
        preCommitsRunner.asyncAction(this, null, recoveringTables.keySet(), null);
        LOGGER.info("preCommitsRunner started.");
        startStopRegionServer.actionRecovery();
        LOGGER.info("startStopRegionServer started.");
        modifyTable.startMonitoring();
        return true;
    }

    /*
     * The methods below must be called from the gRPC service or the REST controller.
     */

    @Override
    public void createNamespace(String namespace) throws Exception {
        createNamespace.action(namespace);
    }

    @Override
    public void createTable(TableName tableName, List<byte[]> splitKeys, int dataBlockSize, CompressType compressType,
                            RecordCompressionType recordCompressionType, boolean dictionary, Long recordTargetCount) throws Throwable {
        String operationName = "CREATE_TABLE";
        recoveryRSContext.checkIncomeTableRequestsEnabled(tableName);
        final TableLock tableLock = lockManager.acquireRef(tableName, operationName);
        if (tableLock.tryLock(conf.getDataConfiguration().getLockWaitingTimeoutMs(), operationName)) {
            createTable.runSyncThenAsyncOnError(tableLock, splitKeys, dataBlockSize, compressType,
                    recordCompressionType, dictionary, recordTargetCount);
        } else {
            lockManager.releaseRef(tableLock);
            throw new RuntimeException("Lock waiting timeout for table: " + tableName);
        }
    }

    @Override
    public void enableTable(TableName tableName, TableLock tableLock) throws Throwable {
        String operationName = "ENABLE_TABLE";
        if (recoveryRSContext.isIncomeTableRequestsBlocked(tableName)) {
            if (tableLock != null) {
                tableLock.unlockAndReleaseRef();
            }
            throw new TableBlockedException(tableName);
        }
        if (tableLock == null) {
            tableLock = lockManager.acquireRef(tableName, operationName);
            if (!tableLock.tryLock(conf.getDataConfiguration().getLockWaitingTimeoutMs(), operationName)) {
                tableLock.releaseRef();
                throw new RuntimeException("Lock waiting timeout for table: " + tableName);
            }
        }
        enableTable.runSyncThenAsyncOnError(tableLock);
    }

    @Override
    public void disableTable(TableName tableName, TableLock tableLock) throws Throwable {
        String operationName = "DISABLE_TABLE";
        if (recoveryRSContext.isIncomeTableRequestsBlocked(tableName)) {
            if (tableLock != null) {
                tableLock.unlockAndReleaseRef();
            }
            throw new TableBlockedException(tableName);
        }
        if (tableLock == null) {
            tableLock = lockManager.acquireRef(tableName, operationName);
            if (!tableLock.tryLock(conf.getDataConfiguration().getLockWaitingTimeoutMs(), operationName)) {
                tableLock.releaseRef();
                throw new RuntimeException("Lock waiting timeout for table: " + tableName);
            }
        }
        disableTable.runSyncThenAsyncOnError(tableLock);
    }

    @Override
    public void dropTable(TableName tableName, TableLock tableLock) throws Throwable {
        String operationName = "DROP_TABLE";
        if (recoveryRSContext.isIncomeTableRequestsBlocked(tableName)) {
            if (tableLock != null) {
                tableLock.unlockAndReleaseRef();
            }
            throw new TableBlockedException(tableName);
        }
        if (tableLock == null) {
            tableLock = lockManager.acquireRef(tableName, operationName);
            if (!tableLock.tryLock(conf.getDataConfiguration().getLockWaitingTimeoutMs(), operationName)) {
                tableLock.releaseRef();
                throw new RuntimeException("Lock waiting timeout for table: " + tableName);
            }
        }
        dropTable.runSyncThenAsyncOnError(tableLock);
    }

    @Override
    public void moveRegion(TableName tableName, List<RegionRebalanceModel> moveModels, TableLock tableLock) throws Throwable {
        String operationName = "MOVE_REGION";
        if (recoveryRSContext.isIncomeTableRequestsBlocked(tableName)) {
            if (tableLock != null) {
                tableLock.unlockAndReleaseRef();
            }
            throw new TableBlockedException(tableName);
        }
        if (tableLock == null) {
            tableLock = lockManager.acquireRef(tableName, operationName);
            if (!tableLock.tryLock(conf.getDataConfiguration().getLockWaitingTimeoutMs(), operationName)) {
                tableLock.releaseRef();
                throw new RuntimeException("Lock waiting timeout for table: " + tableName);
            }
        }
        try {
            moveRegion.action(tableName, moveModels);
        } finally {
            tableLock.unlock();
            lockManager.releaseRef(tableLock);
        }
    }

    @Override
    public void splitTable(TableName tableName, List<String> regionNamesToSplit, TableLock tableLock) throws Throwable {
        String operationName = "SPLIT_TABLE";
        if (recoveryRSContext.isIncomeTableRequestsBlocked(tableName)) {
            if (tableLock != null) {
                tableLock.unlockAndReleaseRef();
            }
            throw new TableBlockedException(tableName);
        }
        if (tableLock == null) {
            tableLock = lockManager.acquireRef(tableName, operationName);
            if (!tableLock.tryLock(conf.getDataConfiguration().getLockWaitingTimeoutMs(), operationName)) {
                tableLock.releaseRef();
                throw new RuntimeException("Lock waiting timeout for table: " + tableName);
            }
        }
        try {
            splitTable.action(tableName, regionNamesToSplit);
        } finally {
            tableLock.unlock();
            lockManager.releaseRef(tableLock);
        }
    }

    @Override
    public void modifyTable(TableName tableName, CompressType compressType,
                            RecordCompressionType recordCompressionType,
                            Long recordTargetCount, Boolean dictionary) throws InterruptedException, IOException {
        String operationName = "MODIFY_TABLE";
        if (recoveryRSContext.isIncomeTableRequestsBlocked(tableName)) {
            throw new TableBlockedException(tableName);
        }
        TableLock tableLock = lockManager.acquireRef(tableName, operationName);
        if (!tableLock.tryLock(conf.getDataConfiguration().getLockWaitingTimeoutMs(), operationName)) {
            tableLock.releaseRef();
            throw new RuntimeException("Lock waiting timeout for table: " + tableName);
        }

        try {
            List<RegionDistributionModel> rdmList = zooContext.getRegionDistributionWithStatuses(tableName);
            modifyTable.action(tableName, new StorageParam(compressType, recordCompressionType, dictionary, recordTargetCount, rdmList));
        } finally {
            tableLock.unlock();
            lockManager.releaseRef(tableLock);
        }
    }

    @Override
    public void modificationCompleteNotification() {
        modifyTable.modificationCompleteNotification();
    }

    @Override
    public void rebalanceTableReplicas(List<TableName> tablesToRebalance, AtomicLong rebalancingTables) {
        synchronized (lockManager.getBulkLockMutex()) {
            String operationName = "REBALANCE_TABLE_REPLICAS";

            // Get individual table locks, no global lock is required
            Set<TableLock> tableLockSet = tablesToRebalance.stream()
                    .map(tableName -> lockManager.acquireRef(tableName, operationName))
                    .collect(Collectors.toSet());

            try {
                // 1. Acquire global read lock.
                //      It will be released once, right after parallel rebalance is done.
                // 2. Acquire table-scoped locks for each table to be processed.
                //      They will be released by each rebalancing thread
                bulkTryLock(tableLockSet, conf.getDataConfiguration().getLockWaitingTimeoutMs(), operationName);

                // A new rebalance request for the table has arrived, so reset the stop flag
                tablesToRebalance.forEach(operationRunner::removeTableFromGracefulStopReplicasRebalance);

                // Run the analysis
                RebalanceBlueprint rebalanceBlueprint;
                try {
                    rebalanceBlueprint = rebalanceTableReplicas.computeBlueprint(tablesToRebalance);
                    rebalanceBlueprint.getRebalanceModels().forEach((table, outcome) ->
                            LOGGER.info("Found new table to rebalance replicas: '{}'. Rebalance plan: {}", table, outcome)
                    );
                } catch (Exception ex) {
                    LOGGER.error("Unable to compute rebalance plan", ex);
                    return;
                }
                // Run the rebalancing if it is enabled in the configuration
                if (conf.getDataConfiguration().getRegionsRebalanceAnalyticsOnly()) {
                    LOGGER.info("Table replicas rebalancing execution is turned off in the settings");
                    return;
                }
                CountDownLatch cdl = new CountDownLatch(rebalanceBlueprint.getRebalanceModels().size());
                rebalanceBlueprint.getRebalanceModels().forEach((tableName, blueprint) -> rebalanceExecutor.execute(() -> {
                    Optional.ofNullable(rebalancingTables).ifPresent(AtomicLong::incrementAndGet);
                    try {
                        // Individual table-scoped locks will be unlocked here
                        rebalanceTable(tableName, blueprint, tableLockSet);
                    } catch (Throwable ex) {
                        LOGGER.error("Unable to rebalance table replicas {}", tableName, ex);
                    } finally {
                        Optional.ofNullable(rebalancingTables).ifPresent(AtomicLong::decrementAndGet);
                        cdl.countDown();
                    }
                }));
                try {
                    cdl.await();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    LOGGER.error("Rebalance of table replicas {} was interrupted", tablesToRebalance);
                }
            } finally {
                // Unlock locks for tables that haven't been processed
                // tableLockSet has flexible size
                tableLockSet.forEach(TableLock::unlockAndReleaseRef);
            }
        }
    }

    private void bulkTryLock(Set<TableLock> tableLockSet, int timeoutMs, String operationName) {
        Set<TableLock> unacquiredLocks = new HashSet<>(tableLockSet);
        for (TableLock tableLock : tableLockSet) {
            try {
                // This acquires both global read lock and table-scoped lock
                if (tableLock.tryLock(timeoutMs, operationName)) {
                    unacquiredLocks.remove(tableLock);
                } else {
                    // If we can't acquire any of the locks – then we should do three things:
                    // 1. Release references for all unacquired locks, including failed
                    // 2. Remove unacquired locks from tableLockSet
                    // 3. Unlock and release references for acquired locks, this will be done later in the final section
                    releaseRefAndRemoveFromSet(tableLockSet, unacquiredLocks);
                    throw new RuntimeException(
                            String.format("Timeout locking table: %s, lock: %s", tableLock.tableName, tableLock)
                    );
                }
            } catch (InterruptedException ex) {
                releaseRefAndRemoveFromSet(tableLockSet, unacquiredLocks);
                Thread.currentThread().interrupt();
                throw new RuntimeException(
                        String.format("Error locking table: %s, lock: %s", tableLock.tableName, tableLock)
                );
            }
        }
    }

    private void releaseRefAndRemoveFromSet(Set<TableLock> tableLockSet, Set<TableLock> unacquiredLocks) {
        for (TableLock tableLock : unacquiredLocks) {
            tableLock.releaseRef();
            tableLockSet.remove(tableLock);
        }
    }

    @Override
    public void stopRebalanceTableReplicas(List<TableName> tablesToStop) {
        operationRunner.gracefulStopReplicasRebalance(tablesToStop);
    }

    // Works in two steps: first balances replicas within each region, then globally within the table
    @Override
    public void rebalanceTableRegions(List<TableName> tablesToRebalance, AtomicLong rebalancingTables) {
        synchronized (lockManager.getBulkLockMutex()) {
            String operationName = "REBALANCE_TABLE_REGIONS";

            // Get individual table locks, no global lock is required
            Set<TableLock> tableLockSet = tablesToRebalance.stream()
                    .map(tableName -> lockManager.acquireRef(tableName, operationName))
                    .collect(Collectors.toSet());

            try {
                // 1. Acquire global read lock.
                //      It will be released once, right after parallel rebalance is done.
                // 2. Acquire table-scoped locks for each table to be processed.
                //      They will be released by each rebalancing thread
                bulkTryLock(tableLockSet, conf.getDataConfiguration().getLockWaitingTimeoutMs(), operationName);

                // A new rebalance request for the table has arrived, so reset the stop flag
                tablesToRebalance.forEach(operationRunner::removeTableFromGracefulStopRegionsRebalance);

                // Run the analysis
                RebalanceBlueprint regionsBlueprint;
                try {
                    // First balance replicas within each individual region
                    RebalanceBlueprint replicasBlueprint = rebalanceTableReplicas.computeBlueprint(tablesToRebalance);
                    replicasBlueprint.getRebalanceModels().forEach((table, outcome) ->
                            LOGGER.info("Found new table to rebalance replicas: '{}'. Rebalance plan: {}", table, outcome)
                    );
                    // Then balance the regions themselves
                    regionsBlueprint = rebalanceTableRegions.computeBlueprint(tablesToRebalance, replicasBlueprint);
                    regionsBlueprint.getRebalanceModels().forEach((table, outcome) ->
                            LOGGER.info("Found new table to rebalance regions: '{}'. Rebalance plan: {}", table, outcome)
                    );
                } catch (Exception ex) {
                    LOGGER.error("Unable to compute rebalance plan", ex);
                    return;
                }
                // Run the rebalancing if it is enabled in the configuration
                if (conf.getDataConfiguration().getRegionsRebalanceAnalyticsOnly()) {
                    LOGGER.info("Table regions rebalancing execution is turned off in the settings");
                    return;
                }
                CountDownLatch cdl = new CountDownLatch(regionsBlueprint.getRebalanceModels().size());
                regionsBlueprint.getRebalanceModels().forEach((tableName, rebalanceBlueprint) -> rebalanceExecutor.execute(() -> {
                    Optional.ofNullable(rebalancingTables).ifPresent(AtomicLong::incrementAndGet);
                    try {
                        // Individual table-scoped locks will be unlocked here
                        rebalanceTable(tableName, rebalanceBlueprint, tableLockSet);
                    } catch (Throwable ex) {
                        LOGGER.error("Unable to rebalance table regions {}", tableName, ex);
                    } finally {
                        Optional.ofNullable(rebalancingTables).ifPresent(AtomicLong::decrementAndGet);
                        cdl.countDown();
                    }
                }));
                try {
                    cdl.await();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    LOGGER.error("Rebalance of table regions {} was interrupted", tablesToRebalance);
                }
            } finally {
                // Unlock locks for tables that haven't been processed
                // tableLockSet has flexible size
                tableLockSet.forEach(TableLock::unlockAndReleaseRef);
            }
        }
    }

    @Override
    public void syncTableReplicas(List<TableName> tablesToSync, AtomicLong syncingTables) {
        CountDownLatch cdl = new CountDownLatch(tablesToSync.size());
        tablesToSync.forEach(tableName -> syncExecutor.execute(() -> {
            Optional.ofNullable(syncingTables).ifPresent(AtomicLong::incrementAndGet);
            try {
                syncTableReplicas(tableName);
            } catch (Throwable ex) {
                LOGGER.error("Unable to sync table replicas {}", tableName, ex);
            } finally {
                Optional.ofNullable(syncingTables).ifPresent(AtomicLong::decrementAndGet);
                cdl.countDown();
            }
        }));
        try {
            cdl.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOGGER.error("Sync table replicas process was interrupted for: {}", tablesToSync);
        }
    }

    @Override
    public void stopRebalanceTableRegions(List<TableName> tablesToStop) {
        operationRunner.gracefulStopRegionsRebalance(tablesToStop);
    }

    @Override
    public TableLock notifyTableRecovering(TableName tableName) {
        String operationName = "RECOVER_TABLE";
        LOGGER.info("Recovery notification process started for table: {}", tableName);
        TableLock tableLock = lockManager.acquireRef(tableName, "RECOVER_TABLE");
        // This line restricts client's gRPC requests to process.
        // Also, it behaves like the "stop flag" for all running operations on the master side.
        // They will see it being set to true and finish gradually.
        recoveryRSContext.blockIncomeTableRequests(tableName);
        try {
            // Acquire lock; after we got it – we can be sure that no other operation is running for the current table.
            // We have to wait for all tasks to complete for this table on master prior to sending any
            // stop signals to RS. Otherwise, we will prevent RS from executing some rollback tasks
            // and enter the deadlock.
            tableLock.lock(operationName);
            // This method waits for all tasks related to specific table to finish gradually on all region servers.
            // Retries infinitely if there is an error when working with running RS.
            operationRunner.waitAllRSForTable(tableName,
                    conf.getDataConfiguration().getRegionsRecoveryOperationsStoppageRSThreadsCount()
            );
        } catch (InterruptedException e) {
            LOGGER.error("Waiting lock for {} interrupted", tableName);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            // We can release the stoppage flag since we are holding the lock
            recoveryRSContext.unblockIncomeTableRequests(tableName);
        }
        LOGGER.info("Recovery notification process successfully finished for table: {}", tableName);
        return tableLock;
    }

    // WARNING: The caller must hold the lock
    @Override
    public void recoverTable(TableName tableName, List<RegionServerModel> lostRS, Runnable actualizeRS, ReadWriteLock lock, boolean checkLostRS) throws Exception {
        recoverTable.action(tableName, lostRS, actualizeRS, lock, checkLostRS);
    }

    // WARNING: The caller must hold the lock
    @Override
    public boolean isNeedToRecoverTable(TableName tableName) throws Exception {
        return recoverTable.isNeedToRecover(tableName);
    }

    // WARNING: The caller must hold the lock
    @Override
    public List<RegionRecoveryModel> findRecoveryModels(TableName tableName) throws Exception {
        List<RegionDistributionModel> tableRdms = zooContext.getRegionDistributionWithStatuses(tableName);
        return recoverTable.findRecoveryModels(tableName, tableRdms);
    }

    @Override
    public boolean getRsStartPermission(RegionServerModel regionServer) throws Exception {
        if (recoveryRSContext.isShuttingDown()) return false;
        LOGGER.info("Start awaiting recovery init");
        recoveryRSContext.awaitRecoveryInit();
        LOGGER.info("Finish awaiting recovery init");
        LOGGER.info("Recovering RS list: {}, blocked: {}", recoveryRSContext.getRecovering(),
                recoveryRSContext.containsRecovering(regionServer)
        );
        LOGGER.info("Moving RS list: {}, blocked: {}", recoveryRSContext.getMoving(),
                recoveryRSContext.containsMoving(regionServer)
        );
        while (recoveryRSContext.containsRecovering(regionServer) || recoveryRSContext.containsMoving(regionServer)) {
            if (recoveryRSContext.isShuttingDown()) return false;
            Thread.sleep(500);
        }
        return !recoveryRSContext.isShuttingDown();
    }

    @Override
    public List<TableName> getTableNamesToRebalance(String rebalanceEntity, boolean checkStatus) throws Exception {
        return TableCandidateSelector.select(zooContext, zooHelper, rebalanceEntity, checkStatus);
    }

    @Override
    public List<TableName> getTableNamesToSync(String syncEntity) throws Exception {
        return TableCandidateSelector.select(zooContext, zooHelper, syncEntity, false);
    }

    @Override
    public OperationRunner getOperationRunner() {
        return operationRunner;
    }

    @Override
    public MoveRegion getMoveRegionInstance() {
        return moveRegion;
    }

    @Override
    public RecoverTable getRecoverTableInstance() {
        return recoverTable;
    }

    @Override
    public RecoveryRSContext getRecoverRSContextInstance() {
        return recoveryRSContext;
    }

    @Override
    public void createSnapshot(String name, TableName tableName) {
        createSnapshot.action(name, tableName);
    }

    @Override
    public void cloneSnapshot(String name, TableName tableName) {
        cloneSnapshot.action(name, tableName);
    }

    @Override
    public void deleteSnapshot(String name) {
        deleteSnapshot.action(name);
    }

    @Override
    public void startRegionServer(String rs) {
        startStopServerSync(rs, true, "START_RS");
    }

    @Override
    public void stopRegionServer(String rs) {
        startStopServerSync(rs, false, "STOP_RS");
    }

    private void startStopServerSync(String rs, boolean isStart, String operationName) {
        synchronized (lockManager.getBulkLockMutex()) {
            boolean success;
            try {
                // Acquired global write lock
                success = lockManager.beginGlobalOperation(conf.getDataConfiguration().getLockWaitingTimeoutMs(), operationName);
            } catch (InterruptedException interruptedException) {
                throw new RuntimeException("Acquiring global write lock interrupted");
            }

            if (!success) {
                throw new RuntimeException("Timeout acquiring global write lock");
            }

            try {
                startStopRegionServer.action(rs, isStart);
            } finally {
                // Release global write lock
                lockManager.completeGlobalOperation(operationName);
            }
        }
    }

    @Override
    public boolean checkDiskSizeForSplit(TableName table, List<String> regionNamesToSplit) throws IOException {
        return splitTable.checkDiskSizeForSplit(table, regionNamesToSplit);
    }


    /* Internal methods */

    private void rebalanceTable(TableName tableName, List<RegionRebalanceModel> rebalanceModels, Set<TableLock> tableLockSet) {
        try {
            recoveryRSContext.checkIncomeTableRequestsEnabled(tableName);
            rebalanceTableReplicas.action(tableName, rebalanceModels);
        } finally {
            // We don't need to count additional reference, since it's already counted "within the set"
            TableLock tableLock = lockManager.rawGetLockWithoutRef(tableName);
            tableLockSet.remove(tableLock);
            tableLock.unlockAndReleaseRef();
        }
    }

    private void syncTableReplicas(TableName tableName) throws Throwable {
        String operationName = "SYNC_TABLE_REPLICAS";
        recoveryRSContext.checkIncomeTableRequestsEnabled(tableName);
        TableLock tableLock = lockManager.acquireRef(tableName, operationName);
        if (!tableLock.tryLock(conf.getDataConfiguration().getLockWaitingTimeoutMs(), operationName)) {
            tableLock.releaseRef();
            throw new RuntimeException("Lock waiting timeout for table: " + tableName);
        }
        try {
            syncTableReplicas.action(tableName);
        } finally {
            tableLock.unlockAndReleaseRef();
        }
    }

    @Override
    public List<RegionServerModel> getShutdownRegionServers() throws Exception {
        List<RegionServerModel> targets = new ArrayList<>();
        for (String id : zookeeperHelperExtended.getRunningRegionServerIds()) {
            RegionServerModel rs = zooContext.getRegionServerModel(id);
            // The ordinary getRunningRegionServers() silently drops unresolved entries
            if (rs == null) throw new IOException("Cannot resolve running RS: " + id);
            targets.add(rs);
        }
        return targets;
    }

    @Override
    public void stopForShutdown() throws IOException {
        recoveryRSContext.beginShutdown();
        if (startupThread != null) startupThread.interrupt();
        regionsRecoveryRunner.close();
        regionsSplitRunner.close();
        regionsRebalanceRunner.close();
        preCommitsRunner.close();
        modifyTable.stopMonitoring();
        executorService.shutdownNow();
        rebalanceExecutor.shutdownNow();
        syncExecutor.shutdownNow();
        operationStoppageExecutor.shutdownNow();
    }

    @Override
    public void close() {
        if (regionsRecoveryRunner != null) {
            regionsRecoveryRunner.close();
        }
        if (rebalanceExecutor != null) {
            rebalanceExecutor.shutdownNow();
        }
        if (syncExecutor != null) {
            syncExecutor.shutdownNow();
        }
    }
}
