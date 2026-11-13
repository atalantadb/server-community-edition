/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.runners;

import com.light.atalanta.TableName;
import com.light.atalanta.master.services.TableLock;
import com.light.atalanta.master.services.UniqueNameThreadFactory;
import com.light.atalanta.master.services.recovery.RecoveryRSContext;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.model.CompressType;
import com.light.atalanta.model.RecordCompressionType;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.util.ShutdownManager;
import com.light.atalanta.util.ZookeeperStatus;
import com.light.atalanta.master.services.Master;
import com.light.atalanta.services.ZookeeperHelper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.light.atalanta.ATALANTAConstants.*;
import static com.light.atalanta.master.services.runners.RegionsRecoveryRunner.findTableRS;

/**
 * WARNING: Must be called by MasterImpl only.
 * <p>
 * Scans ZooKeeper for pre-commits and runs each of them asynchronously.
 * If something fails, it is only logged.
 */
public final class PreCommitsRunner implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PreCommitsRunner.class);

    private static final String OPERATION_NAME = "PRE_COMMITS";

    private final AtomicLong runningOperationsCount;

    private final ScheduledExecutorService requestsMonitoring;

    private final ZookeeperHelper zooHelper;

    private final ExecutorService executorService;

    private final RecoveryRSContext recoveryRSContext;

    private final ZooKeeperMasterContext zooContext;

    private final AtalantaDBConfiguration conf;

    private final ThreadFactory statsThreadFactory = new UniqueNameThreadFactory("atalanta-precommit-stats-pool");
    private final ThreadFactory syncThreadFactory = new UniqueNameThreadFactory("atalanta-precommit-sync-pool");

    public PreCommitsRunner(ZookeeperHelper zooHelper, RecoveryRSContext recoveryRSContext, AtalantaDBConfiguration conf, ExecutorService executorService, ZooKeeperMasterContext zooContext) {
        this.zooHelper = zooHelper;
        this.recoveryRSContext = recoveryRSContext;
        this.executorService = executorService;
        this.zooContext = zooContext;
        this.requestsMonitoring = Executors.newSingleThreadScheduledExecutor(statsThreadFactory);
        this.conf = conf;
        this.runningOperationsCount = new AtomicLong(0L);
        if (conf.getDataConfiguration().getOperationsMonitoringEnabled()) {
            startOperationsMonitoring();
        }
    }

    private void startOperationsMonitoring() {
        requestsMonitoring.scheduleAtFixedRate(() -> {
            long count = runningOperationsCount.get();
            if (count > 0) {
                LOGGER.info("Currently running pre-commits: [{}]", count);
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    public void asyncAction(Master master, TableName tableToProcess, Set<TableName> tablesToExclude, TableLock tableLock) {
        LOGGER.info("Start check pre-commit actions");
        boolean done = false;
        try {
            final List<String> namespaces = zooHelper.watchedGetChildren();
            for (String ns : namespaces) {
                // Besides namespaces, /atalanta may contain other nodes. They are skipped here.
                if (ns.equals(ATALANTA_METADATA)
                        || ns.equals(SNAPSHOT_DIR)
                        || ns.equals(F_MASTER_NODE)
                        || ns.equals(REGION_SERVER_INFO_SUBPATH)
                        || ns.equals(REGION_SERVER_EPHEMERAL_SUBPATH)) {
                    continue;
                }
                try {
                    final List<String> tables = zooHelper.watchedGetChildren(ns);
                    for (String table : tables) {
                        if (tableToProcess != null && !tableToProcess.equals(TableName.valueOf(ns, table))) {
                            continue;
                        }
                        if (tablesToExclude != null && tablesToExclude.contains(TableName.valueOf(ns, table))) {
                            continue;
                        }
                        done = true;
                        try {
                            final String statusStr = zooHelper.getNotNullString(ns + ZK_SEPARATOR + table);
                            if (StringUtils.isEmpty(statusStr)) {
                                LOGGER.warn("Pre-commit check table '{}:{}': status has not been set", ns, table);
                                continue;
                            }
                            LOGGER.info("Status {}:{} = {}", ns, table, statusStr);
                            final ZookeeperStatus status = ZookeeperStatus.fromString(statusStr);
                            runStatusIfNeeded(master, status, TableName.valueOf(ns, table), tableLock);
                        } catch (Exception ex) {
                            if (tableLock != null && tableToProcess != null && table.equals(tableToProcess.getTableName())) {
                                tableLock.unlockAndReleaseRef(OPERATION_NAME);
                            }
                            LOGGER.error("Unable to do pre-commit action in the table level", ex);
                        }
                    }
                } catch (Exception ex) {
                    if (tableLock != null && tableToProcess != null && ns.equals(tableToProcess.getNamespace())) {
                        tableLock.unlockAndReleaseRef(OPERATION_NAME);
                    }
                    LOGGER.error("Unable to do pre-commit in the namespace level", ex);
                }
            }
            // Release the lock if the table was not found
            if (tableLock != null && !done) {
                LOGGER.debug("Table is not found: {}", tableToProcess);
                tableLock.unlockAndReleaseRef(OPERATION_NAME);
            }
        } catch (Exception e) {
            if (tableLock != null) {
                tableLock.unlockAndReleaseRef(OPERATION_NAME);
            }
            LOGGER.error("Unable to do pre-commit in the root level", e);
        }
        LOGGER.info("Async pre-commits were started");
    }

    /**
     * Returns the tables that have recovery pre-commits.
     */
    public Map<TableName, List<RegionServerModel>> scanRecoveringTables(Master master) {
        Map<TableName, List<RegionServerModel>> recoveringTables = new HashMap<>();
        LOGGER.info("Start check recovery pre-commit actions");
        try {
            final List<String> namespaces = zooHelper.watchedGetChildren();
            for (String ns : namespaces) {
                // Besides namespaces, /atalanta may contain other nodes. They are skipped here.
                if (ns.equals(ATALANTA_METADATA)
                        || ns.equals(SNAPSHOT_DIR)
                        || ns.equals(F_MASTER_NODE)
                        || ns.equals(REGION_SERVER_INFO_SUBPATH)
                        || ns.equals(REGION_SERVER_EPHEMERAL_SUBPATH)) {
                    continue;
                }
                try {
                    final List<String> tables = zooHelper.watchedGetChildren(ns);
                    for (String table : tables) {
                        try {
                            LOGGER.info("check table: {}:{}", ns, table);
                            final String statusStr = zooHelper.getNotNullString(ns + ZK_SEPARATOR + table);
                            if (ZookeeperStatus.fromString(statusStr) == ZookeeperStatus.RECOVERING) {
                                TableName tableName = TableName.valueOf(ns, table);
                                recoveringTables.put(tableName, RegionsRecoveryRunner.findRecoveringRS(tableName, master));
                            }
                        } catch (Exception ex) {
                            recoveryRSContext.checkNotShuttingDown();
                            ShutdownManager.exitWithError("Unable to scan recovery pre-commits in the table level for: '" + ns + ":" + table + "'", ex);
                        }
                    }
                } catch (Exception ex) {
                    LOGGER.error("Unable to scan recovery pre-commits in the namespace level for: '" + ns + "'", ex);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Unable to scan recovery pre-commits in the root level", ex);
        }
        return recoveringTables;
    }

    /**
     * Runs recovery for the given tables.
     */
    public void syncRecoveryAction(Set<TableName> tablesToRecover, ConcurrentHashMap<TableName, TableLock> tableLocks, Master master) throws Exception {
        LOGGER.info("Starting recovery pre-commit actions");
        ExecutorService executorService = Executors.newFixedThreadPool(conf.getDataConfiguration().getRegionsRecoveryTableThreadsCount(), syncThreadFactory);
        CountDownLatch latch = new CountDownLatch(tablesToRecover.size());
        ConcurrentHashMap<TableName, String> tablesLeft = new ConcurrentHashMap<>();
        ConcurrentHashMap<TableName, String> currentTables = new ConcurrentHashMap<>();
        tablesToRecover.forEach(tbl -> tablesLeft.put(tbl, StringUtils.EMPTY));
        try {
            // There is no need to cancel recovery of tables that haven't started yet!
            // Nor to synchronize by lostRS, since they are not used in pre-commits
            // Updates the start permissions.
            Runnable actualizeRS = () -> {
                Set<TableName> tables = new HashSet<>(currentTables.keySet());
                tables.addAll(tablesLeft.keySet());
                List<RegionServerModel> lostRS = new ArrayList<>(recoveryRSContext.getRecovering());
                Set<RegionServerModel> remainingRS = new HashSet<>();
                tables
                        .forEach(tbl -> remainingRS.addAll(findTableRS(zooContext, tbl, lostRS)));
                lostRS.stream()
                        .filter(rs -> !remainingRS.contains(rs))
                        .forEach(recoveryRSContext::removeRecovering);
                List<RegionServerModel> recoveringRS = new ArrayList<>();
                tables
                        .forEach(tbl -> recoveringRS.addAll(RegionsRecoveryRunner.findRecoveringRS(tbl, lostRS, master)));
                List<RegionServerModel> suddenlyAliveRS = new ArrayList<>(lostRS);
                suddenlyAliveRS.retainAll(RegionsRecoveryRunner.getAliveRegionServers(zooContext));
                suddenlyAliveRS.stream()
                        .filter(rs -> !recoveringRS.contains(rs))
                        .forEach(recoveryRSContext::removeRecovering);
            };
            tablesToRecover.forEach((tableName) -> executorService.execute(() -> {
                currentTables.put(tableName, StringUtils.EMPTY);
                try {
                    TableLock tableLock = tableLocks.get(tableName);
                    recoverTable(master, tableName, tableLock, actualizeRS);
                } finally {
                    tablesLeft.remove(tableName);
                    currentTables.remove(tableName);
                    // Update the recoveringRS list
                    actualizeRS.run();
                    latch.countDown();
                }
            }));
            LOGGER.info("Awaiting while recovery pre-commits will be completed");
            latch.await();
        } finally {
            // Shut down the local recovery pool
            executorService.shutdownNow();
        }
    }

    private void runStatusIfNeeded(Master master, ZookeeperStatus status, TableName tableName, TableLock tableLock) {
        switch (status) {
            case CREATING:
                CompletableFuture.runAsync(() -> createTable(master, tableName), executorService);
                break;
            case ENABLING:
                CompletableFuture.runAsync(() -> enableTable(master, tableName, tableLock), executorService);
                break;
            case DISABLING:
                CompletableFuture.runAsync(() -> disableTable(master, tableName, tableLock), executorService);
                break;
            case DROPPING:
                CompletableFuture.runAsync(() -> dropTable(master, tableName, tableLock), executorService);
                break;
            case SPLITTING:
                CompletableFuture.runAsync(() -> splitTable(master, tableName, tableLock), executorService);
                break;
            case ASSIGNING:
                CompletableFuture.runAsync(() -> moveRegion(master, tableName, tableLock), executorService);
                break;
            default:
                if (tableLock != null) {
                    tableLock.unlockAndReleaseRef(OPERATION_NAME);
                }
                LOGGER.debug("Status: {} is not pre-commit status", status);
        }
    }

    private void createTable(Master master, TableName tableName) {
        runningOperationsCount.incrementAndGet();
        try {
            // createTable reads the distribution from ZooKeeper itself, so pass an empty array
            master.createTable(tableName, Collections.emptyList(), DEFAULT_DATA_BLOCK_SIZE,
                    CompressType.none, RecordCompressionType.none, false, 0L);
        } catch (Throwable e) {
            LOGGER.error("Unable to create table: {}", tableName, e);
        } finally {
            runningOperationsCount.decrementAndGet();
        }
    }

    private void enableTable(Master master, TableName tableName, TableLock tableLock) {
        runningOperationsCount.incrementAndGet();
        try {
            master.enableTable(tableName, tableLock);
        } catch (Throwable e) {
            LOGGER.error("Unable to enable table: {}", tableName, e);
        } finally {
            runningOperationsCount.decrementAndGet();
        }
    }

    private void disableTable(Master master, TableName tableName, TableLock tableLock) {
        runningOperationsCount.incrementAndGet();
        try {
            master.disableTable(tableName, tableLock);
        } catch (Throwable e) {
            LOGGER.error("Unable to disable table: {}", tableName, e);
        } finally {
            runningOperationsCount.decrementAndGet();
        }
    }

    private void dropTable(Master master, TableName tableName, TableLock tableLock) {
        runningOperationsCount.incrementAndGet();
        try {
            master.dropTable(tableName, tableLock);
        } catch (Throwable e) {
            LOGGER.error("Unable to drop table: {}", tableName, e);
        } finally {
            runningOperationsCount.decrementAndGet();
        }
    }

    private void splitTable(Master master, TableName tableName, TableLock tableLock) {
        runningOperationsCount.incrementAndGet();
        try {
            master.splitTable(tableName, new ArrayList<>(), tableLock);
        } catch (Throwable e) {
            LOGGER.error("Unable to split table: {}", tableName, e);
        } finally {
            runningOperationsCount.decrementAndGet();
        }
    }

    private void moveRegion(Master master, TableName tableName, TableLock tableLock) {
        runningOperationsCount.incrementAndGet();
        try {
            master.moveRegion(tableName, Collections.emptyList(), tableLock);
        } catch (Throwable e) {
            LOGGER.error("Error moving table table region: '{}'", tableName, e);
        } finally {
            runningOperationsCount.decrementAndGet();
        }
    }

    private void recoverTable(Master master, TableName tableName, TableLock tableLock, Runnable actualizeRS) {
        runningOperationsCount.incrementAndGet();
        try {
            master.recoverTable(tableName, Collections.emptyList(), actualizeRS, new ReentrantReadWriteLock(), false);
        } catch (Exception ex) {
            LOGGER.error("Error recovering table: '{}'", tableName, ex);
        } finally {
            // Run the pre-commits for the processed table without releasing the lock
            recoveryRSContext.checkNotShuttingDown();
            // If there are no pre-commits, PreCommitRunner releases the lock itself
            if (Thread.currentThread().isInterrupted()) {
                recoveryRSContext.checkNotShuttingDown();
                ShutdownManager.exitWithError("PreCommitsRunner recovery thread was interrupted");
            }
            try {
                executorService.submit(() -> {
                    asyncAction(master, tableName, null, tableLock);
                });
            } catch (Throwable t) {
                recoveryRSContext.checkNotShuttingDown();
                ShutdownManager.exitWithError("Unable to run PreCommitsRunner thread", t);
            }
            LOGGER.debug("Finish submitting new PreCommitsRunner thread");
            runningOperationsCount.decrementAndGet();
        }
    }

    @Override
    public void close() throws IOException {
        requestsMonitoring.shutdownNow();
    }
}
