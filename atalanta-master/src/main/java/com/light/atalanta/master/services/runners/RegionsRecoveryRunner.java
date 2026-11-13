/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.runners;

import com.light.atalanta.exceptions.NoRetryException;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.TableNotExistsException;
import com.light.atalanta.master.services.Master;
import com.light.atalanta.master.services.RecoveryLogger;
import com.light.atalanta.master.services.TableLock;
import com.light.atalanta.master.services.UniqueNameThreadFactory;
import com.light.atalanta.master.services.recovery.RecoveryRSContext;
import com.light.atalanta.master.services.recovery.SnapshotRecovery;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.util.ShutdownManager;
import com.light.atalanta.util.ZookeeperStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.light.atalanta.services.ZooKeeperContextHelper.DROPPED_REGION_STATUS;

/**
 * Scans ZooKeeper for failed (lost) region servers.
 * When one is found, starts replica recovery.
 */
public class RegionsRecoveryRunner implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionsRecoveryRunner.class);

    private final AtomicLong recoveringTables;
    private final ScheduledExecutorService requestsMonitoring;

    private final AtalantaDBConfiguration conf;
    private final ZooKeeperMasterContext zooContext;
    private final ZookeeperHelper zooHelper;

    private final PreCommitsRunner preCommitsRunner;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService preCommitsExecutor;
    private final RecoveryRSContext recoveryRSContext;
    private final SnapshotRecovery snapshotRecovery;

    final Object lock11 = new Object();

    private static final int actualizeRSPauseMs = 3 * 60 * 1000; // 3 min
    private final AtomicLong lastActualizeRSRunStamp = new AtomicLong(0);
    private final AtomicBoolean isExecuting = new AtomicBoolean(false);
    private final AtomicBoolean force = new AtomicBoolean(false);

    private final ThreadFactory monitoringThreadFactory = new UniqueNameThreadFactory("atalanta-recovery-monitoring-pool");
    private final ThreadFactory preCommitsThreadFactory = new UniqueNameThreadFactory("atalanta-recovery-precommit-pool");
    private final ThreadFactory statsThreadFactory = new UniqueNameThreadFactory("atalanta-recovery-stats-pool");
    private final ThreadFactory parentThreadFactory = new UniqueNameThreadFactory("atalanta-recovery-parent-pool");
    private final RecoveryLogger recoveryLogger;

    public RegionsRecoveryRunner(AtalantaDBConfiguration conf, ZooKeeperMasterContext zooContext, ZookeeperHelper zooHelper, RecoveryRSContext recoveryRSContext, PreCommitsRunner preCommitsRunner, SnapshotRecovery snapshotRecovery, RecoveryLogger recoveryLogger, ExecutorService execturService) {
        this.conf = conf;
        this.zooContext = zooContext;
        this.zooHelper = zooHelper;
        this.preCommitsRunner = preCommitsRunner;
        this.snapshotRecovery = snapshotRecovery;
        this.recoveryLogger = recoveryLogger;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(monitoringThreadFactory);
        this.preCommitsExecutor = execturService;
        this.recoveryRSContext = recoveryRSContext;
        this.recoveringTables = new AtomicLong(0L);
        this.requestsMonitoring = Executors.newSingleThreadScheduledExecutor(statsThreadFactory);
    }

    public void action(Master master) {
        initialRegionServersScan();
        startMonitoringAsync(master);
        startMonitoringSnapshotAsync(master);
        if (conf.getDataConfiguration().getOperationsMonitoringEnabled()) {
            startOperationsMonitoring();
        }
    }

    private void startOperationsMonitoring() {
        requestsMonitoring.scheduleAtFixedRate(() -> {
            long count = recoveringTables.get();
            if (count > 0) {
                LOGGER.info("Recovering tables count: [{}]", count);
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    // Scans the whole configuration and lists the RS that store regions.
    // This list may differ from the list of running RS.
    private void initialRegionServersScan() {
        LOGGER.debug("Start scanning payloaded region servers");
        try {
            zooContext.getAllDistributions().values()
                    .forEach(tableDistribution -> tableDistribution.
                            forEach(rdm -> recoveryRSContext.addPayloaded(rdm.getRegionServerModel())));
            LOGGER.debug("Payloaded region servers: {}", recoveryRSContext.getPayloaded());
        } catch (Exception ex) {
            recoveryRSContext.checkNotShuttingDown();
            ShutdownManager.exitWithError("Unable to read regions distribution", ex);
        }
    }

    // Monitors the running RS, detects when an RS from payloadedRegionServers is lost, and runs recovery
    private void startMonitoringAsync(Master master) {
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            LOGGER.trace("Recovery analyze iteration ...");
            if (recoveryRSContext.isSyncRunning()) {
                LOGGER.info("Skipping recovery check since SYNC process is running");
                return;
            }
            List<RegionServerModel> lostRS = new ArrayList<>();
            try {
                recoveryRSContext.resetRecoveryInit();
                // Determine the lost RS. The current recovery restores replicas only from these RS.
                // If an RS is lost after the recovery has started, nothing is recovered from it in the current iteration.
                // That is why lostRS may only shrink.
                lostRS = new ArrayList<>(recoveryRSContext.getPayloaded());
                lostRS.removeAll(getAliveRegionServers(zooContext));
                recoveryRSContext.clearSkippedTables();
                if (lostRS.isEmpty()) {
                    recoveryRSContext.doneRecoveryInit();
                    LOGGER.info("All payloaded RS is working");
                    if (recoveryRSContext.getSkippedLeaderStartTables().isEmpty()) {
                        return;
                    } else {
                        LOGGER.info("Found tables with skipped leader starts");
                    }
                }
                lostRS.forEach(recoveryRSContext::addRecovering);
                recoveryRSContext.doneRecoveryInit();
                long recoveryTimeoutMs = conf.getDataConfiguration().getRegionsRecoveryTimeoutMs();
                LOGGER.warn("Waiting {} ms and recover regions ...", recoveryTimeoutMs);
                Thread.sleep(recoveryTimeoutMs);
                Set<TableName> tablesToRecover = new HashSet<>();
                // Adds tables to recover based on lost RS
                tablesToRecover.addAll(findTablesToRecover(lostRS));
                // Adds tables to recover based on past unsuccessful leader restarts
                Set<TableName> skippedLeaderStartTables = recoveryRSContext.getSkippedLeaderStartTables();
                if (!skippedLeaderStartTables.isEmpty()) {
                    LOGGER.info("Found tables with skipped leader starts: {}", skippedLeaderStartTables);
                    tablesToRecover.addAll(skippedLeaderStartTables);
                }
                if (tablesToRecover.isEmpty()) {
                    // Release all lost RS that have no tables
                    LOGGER.info("There is no more tables on RS: {}", lostRS);
                    lostRS.stream()
                            .peek(recoveryRSContext::removeRecovering)
                            .forEach(recoveryRSContext::removePayloaded);
                    return;
                }
                List<TableName> sortedTablesToRecover = sortTablesByRegionCountForRS(tablesToRecover);
                recoverTables(sortedTablesToRecover, lostRS, master);
                snapshotRecovery.recovery(lostRS);
                LOGGER.info("Finish regions recovery");
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                lostRS.forEach(recoveryRSContext::removeRecovering);
                LOGGER.error("Unable to recover regions", ex);
            } catch (Exception ex) {
                lostRS.forEach(recoveryRSContext::removeRecovering);
                LOGGER.error("Unable to recover regions", ex);
            } finally {
                recoveryRSContext.doneRecoveryInit();
            }
        }, conf.getDataConfiguration().getRegionsRecoveryInitialCheckDelayMs(), conf.getDataConfiguration().getRegionsRecoveryCheckDelayMs(), TimeUnit.MILLISECONDS);
    }

    private List<TableName> sortTablesByRegionCountForRS(Set<TableName> tableNames) {
        LOGGER.info("Getting tables to recover from ZK cache");
        // Get the region distribution of all tables from the cache at once
        Map<TableName, List<RegionDistributionModel>> allDistributions = zooContext.getAllDistributions();
        // Fill the table size map
        Map<TableName, Integer> tableRegionCount = new HashMap<>();
        for (TableName tableName : tableNames) {
            List<RegionDistributionModel> tableRdms =
                    allDistributions.getOrDefault(tableName, Collections.emptyList());
            tableRegionCount.put(tableName, tableRdms.size());
        }
        // Sort by the number of regions (descending)
        List<TableName> result = tableNames.stream()
                .sorted(Comparator.comparingInt(tableRegionCount::get).reversed())
                .collect(Collectors.toList());

        // Move the last table to the head of the list:
        // actualizeRS.run processes the first table in the list last, for a reason not yet known.
        // This is a workaround.
        if (result.size() > 1) {
            TableName last = result.remove(result.size() - 1);
            result.add(0, last);
        }
        // Log the resulting order
        List<String> tableListToOutput = result.stream()
                .map(tn -> String.format("%ntable: %s, count: %d", tn, tableRegionCount.get(tn)))
                .collect(Collectors.toList());
        LOGGER.info("Sorted tables to recover: {}", tableListToOutput);
        return result;
    }

    private void startMonitoringSnapshotAsync(Master master) {
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
                    List<RegionServerModel> lostRS;
                    try {
                        lostRS = new ArrayList<>(snapshotRecovery.getSnapshotRs());
                        lostRS.removeAll(getAliveRegionServers(zooContext));
                        if (lostRS.isEmpty()) {
                            LOGGER.info("All snapshot RS is working");
                            return;
                        }
                        snapshotRecovery.recovery(lostRS);
                        LOGGER.info("Finish snapshot regions recovery");
                    } catch (Throwable ex) {
                        LOGGER.error("Unable to recover regions", ex);
                    }
                }, conf.getDataConfiguration().getRegionsRecoveryInitialCheckDelayMs(),
                conf.getDataConfiguration().getRegionsRecoveryCheckDelayMs(), TimeUnit.MILLISECONDS);
    }

    private List<TableName> findTablesToRecover(List<RegionServerModel> lostRS) {
        List<TableName> recoveryQueue = new LinkedList<>();
        zooContext.getAllDistributions().entrySet().stream()
                .filter(e -> e.getValue().stream().anyMatch(rdm ->
                        isGoodStatusForRecovering(rdm)
                        && lostRS.contains(rdm.getRegionServerModel())))
                .map(Map.Entry::getKey)
                .forEach(recoveryQueue::add);
        return recoveryQueue;
    }

    private static boolean isGoodStatusForRecovering(RegionDistributionModel rdm) {
        return rdm.getRegionModel().getStatusId() != DROPPED_REGION_STATUS;
    }

    private void recoverTables(List<TableName> tablesToRecover, List<RegionServerModel> lostRS, Master master) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            int totalRegions = zooContext.getAllDistributions().values().stream()
                    .mapToInt(v -> v.size()).sum();
            LOGGER.debug("Start recovering. Total regions in cache: {}", totalRegions);
        }
        ExecutorService executorService = Executors.newFixedThreadPool(conf.getDataConfiguration().getRegionsRecoveryTableThreadsCount(), parentThreadFactory);
        CountDownLatch latch = new CountDownLatch(tablesToRecover.size());
        ReadWriteLock lock = new ReentrantReadWriteLock();
        recoveryLogger.setRecoveryTableCount(tablesToRecover.size());
        // Update the lost/recovering RS lists
        Runnable actualizeRS = () -> {
            if (!isExecuting.get() && !force.get()) {
                long msSinceLastRun = System.currentTimeMillis() - lastActualizeRSRunStamp.get();
                boolean timeToRun = msSinceLastRun >= actualizeRSPauseMs;
                if (!timeToRun) {
                    LOGGER.warn("Skipping actualizeRS run, next run in {} ms", actualizeRSPauseMs - msSinceLastRun);
                    return;
                }
            }
            if (!isExecuting.get() || force.get()) {
                synchronized (lock11) {
                    if (!isExecuting.get() || force.get()) {
                        isExecuting.set(true);
                        try {
                            List<RegionServerModel> free;
                            if (lostRS.isEmpty()) {
                                LOGGER.info("Looks like lostRS are now alive, force: {}", force.get());
                                return;
                            }
                            long current = System.currentTimeMillis();
                            LOGGER.info("{} Waiting lock: actualizeRS operation {}", current, force.get());
                            lock.readLock().lock();
                            try {
                                LOGGER.info("{} Waiting lock done actualizeRS operation started", current);
                                Set<RegionServerModel> remainingRS = new HashSet<>();
                                tablesToRecover
                                        .forEach(tbl -> {
                                            if (remainingRS.size() != lostRS.size()) {
                                                remainingRS.addAll(findTableRS(zooContext, tbl, lostRS));
                                            }
                                        });
                                LOGGER.trace("actualizeRS: found remaining RS");
                                free = lostRS.stream()
                                        .filter(rs -> !remainingRS.contains(rs))
                                        .collect(Collectors.toList());
                            } finally {
                                lock.readLock().unlock();
                            }
                            if (!free.isEmpty()) {
                                LOGGER.info("{} Waiting lock22: actualizeRS operation", current);
                                lock.writeLock().lock();
                                try {
                                    LOGGER.info("{} Waiting lock22 done actualizeRS operation started", current);
                                    free.stream()
                                            .peek(recoveryRSContext::removeRecovering)
                                            .filter(rs -> !recoveryRSContext.getSkippedTables().contains(rs))
                                            .forEach(recoveryRSContext::removePayloaded);
                                } finally {
                                    lock.writeLock().unlock();
                                }
                            }

                            lock.readLock().lock();
                            try {
                                LOGGER.trace("actualizeRS: alive RS processed");
                                List<RegionServerModel> recoveringRS = new ArrayList<>();
                                tablesToRecover.stream()
                                        .filter(this::isTableRecovering)
                                        .forEach(tbl -> recoveringRS.addAll(findRecoveringRS(tbl, lostRS, master)));
                                LOGGER.trace("actualizeRS: found RS to recover");
                                List<RegionServerModel> suddenlyAliveRS = new ArrayList<>(lostRS);
                                suddenlyAliveRS.retainAll(getAliveRegionServers(zooContext));
                                LOGGER.trace("actualizeRS: found suddenly alive RS");
                                free = suddenlyAliveRS.stream()
                                        .filter(rs -> !recoveringRS.contains(rs))
                                        .collect(Collectors.toList());
                            } finally {
                                lock.readLock().unlock();
                            }
                            if (!free.isEmpty()) {
                                LOGGER.info("{} Waiting lock33: actualizeRS operation", current);
                                lock.writeLock().lock();
                                try {
                                    LOGGER.info("{} Waiting lock33 done actualizeRS operation started", current);
                                    free.stream()
                                            .peek(recoveryRSContext::removeRecovering)
                                            .forEach(lostRS::remove);
                                } finally {
                                    lock.writeLock().unlock();
                                }
                            }
                            lastActualizeRSRunStamp.set(System.currentTimeMillis());
                            LOGGER.info("actualizeRS operation finished");
                        } finally {
                            isExecuting.set(false);
                        }
                    }
                }
            }
        };
        try {
            recoveryLogger.startRecovery(tablesToRecover);
            for (TableName tableToRecover : tablesToRecover) {
                recoveryLogger.startRecoveryTable(tableToRecover);
                // If the table doesn't need recovery, protect its RS from removal from the payloaded list
                if (!isNeedToRecoverTable(tableToRecover, master)) {
                    LOGGER.info("No need to recover table: {} because of status, skipping", tableToRecover);
                    recoveryRSContext.addSkippedTables(getRegionServersForTable(tableToRecover));
                    latch.countDown();
                    // There may be no tables that really need recovery, so the lost/recovering lists must be updated
                    actualizeRS.run();
                } else {
                    recoverTable(tableToRecover, executorService, master, lostRS, latch, actualizeRS, lock);
                }

            }
            LOGGER.info("Awaiting while recovery will be completed");
            latch.await();
        } finally {
            force.set(true);
            try {
                actualizeRS.run();
            } finally {
                force.set(false);
            }

            recoveryRSContext.removeAllRecovering();
            LOGGER.info("Recovery was completed");
            recoveryLogger.setRecoveryTableCount(0);
            // Reject new tasks first, then wait for the pool to finish
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                    executorService.shutdownNow();
                }
            } finally {
                if (!executorService.isTerminated()) {
                    executorService.shutdownNow();
                }
            }
            if (LOGGER.isDebugEnabled()) {
                int totalRegions = zooContext.getAllDistributions().values().stream()
                        .mapToInt(v -> v.size()).sum();
                LOGGER.debug("Total regions in cache after recovering: {}", totalRegions);
            }
        }
    }

    private void recoverTable(TableName rt, ExecutorService executorService, Master master, List<RegionServerModel> lostRS, CountDownLatch latch, Runnable actualizeRS, ReadWriteLock lock1) {
        final TableName tableToRecover = rt;
        executorService.execute(() -> {
            // Recovery of some tables may not start right away, so the lost/recovering lists must be updated
            actualizeRS.run();
            LOGGER.info("Recovering regions for table: {}", tableToRecover);
            TableLock tableLock = master.notifyTableRecovering(tableToRecover);
            recoveringTables.incrementAndGet();
            try {
                master.recoverTable(tableToRecover, lostRS, actualizeRS, lock1, true);
            } catch (Exception ex) {
                LOGGER.error("Error recovering table: '{}'", tableToRecover, ex);
                recoveryRSContext.addSkippedTables(getRegionServersForTable(tableToRecover));
            } finally {
                // Update the lost/recovering lists
                recoveryRSContext.checkNotShuttingDown();
                actualizeRS.run();
                // Run the pre-commits for the processed table without releasing the lock.
                // If there are no pre-commits, PreCommitRunner releases the lock itself.
                if (Thread.currentThread().isInterrupted()) {
                    recoveryRSContext.checkNotShuttingDown();
                    ShutdownManager.exitWithError("Recovery thread was interrupted");
                }
                LOGGER.debug("Start submitting new PreCommitsRunner thread");
                try {
                    preCommitsExecutor.submit(() -> {
                        preCommitsRunner.asyncAction(master, tableToRecover, null, tableLock);
                    });
                } catch (Throwable t) {
                    recoveryRSContext.checkNotShuttingDown();
                    ShutdownManager.exitWithError("Unable to run PreCommitsRunner thread", t);
                }
                LOGGER.debug("Finish submitting new PreCommitsRunner thread");
                recoveringTables.decrementAndGet();
                latch.countDown();
                recoveryLogger.completeRecoveryTable(rt);
            }
        });
    }

    // Finds the given RS that still host regions of the table
    public static List<RegionServerModel> findTableRS(ZooKeeperMasterContext zooContext, TableName tableName, List<RegionServerModel> rs) {
        List<RegionServerModel> lostTableRS = new ArrayList<>();
        Map<String, RegionServerModel> lostRSMap = rs.stream().collect(Collectors.toMap(RegionServerModel::getName, it -> it));
        boolean done = false;
        while (!done) {
            try {
                zooContext.getRegionServerNamesForTable(tableName).stream()
                        .filter(lostRSMap::containsKey)
                        .map(lostRSMap::get)
                        .forEach(lostTableRS::add);
                if (Thread.currentThread().isInterrupted()) {
                    LOGGER.error("Process was interrupted");
                    throw new RuntimeException(new InterruptedException());
                }
                done = true;
            } catch (TableNotExistsException ex) {
                // This is the case when the table was already removed
                LOGGER.warn("Table no longer exists: '{}', skipping ...", tableName);
                done = true;
            } catch (Exception ex) {
                // TODO: Handle ZooKeeper being down
                LOGGER.error("Unable to find region servers for table: '{}', wait 5000 ms and retry ...", tableName, ex);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException interrupted) {
                    LOGGER.error("Process was interrupted", ex);
                    throw new RuntimeException(new InterruptedException());
                }
            }
        }
        return lostTableRS;
    }

    private boolean isNeedToRecoverTable(TableName tableName, Master master) {
        boolean result = false;
        boolean done = false;
        while (!done) {
            try {
                result = master.isNeedToRecoverTable(tableName);
                if (Thread.currentThread().isInterrupted()) {
                    LOGGER.error("Process was interrupted");
                    throw new RuntimeException(new InterruptedException());
                }
                done = true;
            } catch (NoRetryException ex) {
                LOGGER.error("Unable to check table recovery needs: '{}'", tableName, ex);
                done = true;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOGGER.error("Process was interrupted", ex);
                throw new RuntimeException(new InterruptedException());
            } catch (Exception ex) {
                LOGGER.error("Unable to check table recovery needs: '{}'", tableName, ex);
            }
        }
        return result;
    }

    private List<RegionServerModel> getRegionServersForTable(TableName tableName) {
        List<RegionServerModel> regionServers = new ArrayList<>();
        boolean done = false;
        while (!done) {
            try {
                regionServers = zooContext.getRegionServersForTable(tableName);
                if (Thread.currentThread().isInterrupted()) {
                    LOGGER.error("Process was interrupted");
                    throw new RuntimeException(new InterruptedException());
                }
                done = true;
            } catch (Exception ex) {
                LOGGER.error("Unable to get region servers for table: '{}'", tableName, ex);
            }
        }
        return regionServers;
    }

    public static List<RegionServerModel> findRecoveringRS(TableName tableName, List<RegionServerModel> lostRS, Master master) {
        LOGGER.info("Running findRecoveringRS operation for table: {}", tableName);
        List<RegionServerModel> recoveringRS = new ArrayList<>();
        boolean done = false;
        while (!done) {
            try {
                master.findRecoveryModels(tableName).stream()
                        .map(rm -> rm.getLostRdm().getRegionServerModel())
                        .filter(lostRS::contains)
                        .forEach(recoveringRS::add);
                if (Thread.currentThread().isInterrupted()) {
                    LOGGER.error("Process was interrupted");
                    throw new RuntimeException(new InterruptedException());
                }
                done = true;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOGGER.error("Process was interrupted", ex);
                throw new RuntimeException(new InterruptedException());
            } catch (Exception ex) {
                LOGGER.error("Unable to find recovery models for table: '{}'", tableName, ex);
            }
        }
        return recoveringRS;
    }

    public static List<RegionServerModel> findRecoveringRS(TableName tableName, Master master) {
        List<RegionServerModel> recoveringRS = new ArrayList<>();
        boolean done = false;
        while (!done) {
            try {
                master.findRecoveryModels(tableName).stream()
                        .map(rm -> rm.getLostRdm().getRegionServerModel())
                        .forEach(recoveringRS::add);
                if (Thread.currentThread().isInterrupted()) {
                    LOGGER.error("Process was interrupted");
                    throw new RuntimeException(new InterruptedException());
                }
                done = true;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOGGER.error("Process was interrupted", ex);
                throw new RuntimeException(new InterruptedException());
            } catch (Exception ex) {
                LOGGER.error("Unable to find recovery models for table: '{}'", tableName, ex);
            }
        }
        return recoveringRS;
    }

    public static List<RegionServerModel> getAliveRegionServers(ZooKeeperMasterContext zooContext) {
        List<RegionServerModel> aliveRS = new ArrayList<>();
        boolean done = false;
        while (!done) {
            try {
                aliveRS = zooContext.getAliveOrDisabledRegionServers();
                if (Thread.currentThread().isInterrupted()) {
                    LOGGER.error("Process was interrupted");
                    throw new RuntimeException(new InterruptedException());
                }
                done = true;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOGGER.error("Process was interrupted", ex);
                throw new RuntimeException(new InterruptedException());
            } catch (Exception ex) {
                LOGGER.error("Unable to get alive region servers", ex);
            }
        }
        return aliveRS;
    }

    private boolean isTableExists(TableName tableName) {
        String zkPath = ZooKeeperContextHelper.getTablePath(tableName);
        while (true) {
            try {
                return zooHelper.checkExists(zkPath);
            } catch (Exception ex) {
                recoveryRSContext.checkNotShuttingDown();
                if (ex.getCause().getClass().equals(InterruptedException.class)) {
                    ShutdownManager.exitWithError("Recovery thread was interrupted");
                }
                LOGGER.error("Unable to check table exists", ex);
            }
        }
    }

    private boolean isTableRecovering(TableName tableName) {
        ZookeeperStatus tableStatus = null;
        boolean done = false;
        while (!done) {
            try {
                String tablePath = ZooKeeperContextHelper.getTablePath(tableName);
                String statusStr = zooHelper.getNotNullString(tablePath);
                if (statusStr.isEmpty()) {
                    // Table can be dropped already
                    LOGGER.warn("Table no longer exists: '{}', skipping ...", tableName);
                    return false;
                }
                tableStatus = ZookeeperStatus.fromString(statusStr);
                done = true;
            } catch (Exception ex) {
                recoveryRSContext.checkNotShuttingDown();
                if (ex.getCause().getClass().equals(InterruptedException.class)) {
                    ShutdownManager.exitWithError("Recovery thread was interrupted");
                }
                // Table can be dropped already
                if (!isTableExists(tableName)) {
                    LOGGER.warn("Table no longer exists: '{}', skipping ...", tableName);
                    return false;
                }
                LOGGER.error("Unable to read table status", ex);
            }
        }
        return tableStatus.equals(ZookeeperStatus.RECOVERING);
    }

    @Override
    public void close() {
        requestsMonitoring.shutdownNow();
        scheduledExecutorService.shutdownNow();
    }
}
