/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.recovery;

import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.TableBlockedException;
import com.light.atalanta.master.services.RecoveryLogger;
import com.light.atalanta.model.RegionServerModel;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
/**
 * Keeps the state of region servers during recovery.
 */
@SuppressFBWarnings("SWL_SLEEP_WITH_LOCK_HELD")
public class RecoveryRSContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryRSContext.class);
    private final RecoveryLogger recoveryLogger;

    public RecoveryRSContext(RecoveryLogger recoveryLogger) {
        this.recoveryLogger = recoveryLogger;
    }

    private final ConcurrentHashMap<TableName, Object> blockedIncomeTableRequests = new ConcurrentHashMap<>();
    private volatile boolean shuttingDown;

    public void beginShutdown() {
        shuttingDown = true;
        doneRecoveryInit();
    }

    public boolean isShuttingDown() {
        return shuttingDown;
    }

    public void checkNotShuttingDown() {
        if (shuttingDown) throw new java.util.concurrent.CancellationException("Master is shutting down");
    }

    /**
     * Method checks, whether it's allowed to start / continue tasks execution on the table.
     * If one wants to block tasks execution (or stop them gracefully),
     * it's required to call blockIncomeTableRequests().
     * Use unblockIncomeTableRequests() to unblock.
     */
    public void checkIncomeTableRequestsEnabled(TableName tableName) throws TableBlockedException {
        if (isIncomeTableRequestsBlocked(tableName)) {
            throw new TableBlockedException(tableName);
        }
    }
    public boolean isIncomeTableRequestsBlocked(TableName tableName) {
        return shuttingDown || blockedIncomeTableRequests.containsKey(tableName);
    }

    /**
     * Signals for all tasks to stop gracefully, prohibits new tasks execution.
     * Works within specified table.
     */
    public void blockIncomeTableRequests(TableName tableName) {
        blockedIncomeTableRequests.put(tableName, StringUtils.EMPTY);
    }

    /**
     * Allows tasks execution. Supposed to be called after blockIncomeTableRequests().
     */
    public void unblockIncomeTableRequests(TableName tableName) {
        blockedIncomeTableRequests.remove(tableName);
    }

    // RS whose health must be monitored
    private final ConcurrentHashMap<RegionServerModel, Object> payloadedRegionServers = new ConcurrentHashMap<>();

    // RS from which replicas are currently being recovered
    private final ConcurrentHashMap<RegionServerModel, Object> recoveringRegionServers = new ConcurrentHashMap<>();

    // RS that are currently involved in replica moves in some way
    private final ConcurrentHashMap<RegionServerModel, Set<TableName>> movingRegionServers = new ConcurrentHashMap<>();

    // Some tables were skipped on these RS during recovery
    private final ConcurrentHashMap<RegionServerModel, Object> skippedTablesRS = new ConcurrentHashMap<>();

    // Not all leaders of these tables could be started during recovery; this must be done later
    private final ConcurrentHashMap<TableName, Object> skippedLeaderStartTables = new ConcurrentHashMap<>();

    // Recovery is initializing, so DO_JOB must not be answered to region servers yet
    private final AtomicReference<CountDownLatch> recoveryInit = new AtomicReference<>(new CountDownLatch(0));

    // Reserve replicas are being synchronized with the leader
    private final AtomicBoolean isSyncRunning = new AtomicBoolean(false);

    public synchronized void resetRecoveryInit() {
        recoveryInit.getAndSet(new CountDownLatch(1)).countDown();
    }

    public void awaitRecoveryInit() throws InterruptedException {
        recoveryInit.get().await();
    }

    public synchronized void doneRecoveryInit() {
        recoveryInit.get().countDown();
    }

    // Adds a new RS to payloadedRegionServers.
    // This means that the algorithm will monitor the health of this RS.
    public synchronized void addPayloaded(RegionServerModel payloadedRegionServer) {
        if (!payloadedRegionServers.containsKey(payloadedRegionServer)) {
            LOGGER.info("Found new payloaded RS with id: {}", payloadedRegionServer);
        }
        payloadedRegionServers.putIfAbsent(payloadedRegionServer, StringUtils.EMPTY);
    }

    public synchronized Set<RegionServerModel> getPayloaded() {
        return payloadedRegionServers.keySet();
    }

    public synchronized void removePayloaded(RegionServerModel payloadedRegionServer) {
        if (payloadedRegionServers.containsKey(payloadedRegionServer)) {
            LOGGER.info("RS is no more payloaded: {}", payloadedRegionServer);
        }
        payloadedRegionServers.remove(payloadedRegionServer);
    }

    // Adds a new RS to recoveringRegionServers.
    // This means that a replica is being recovered from this RS.
    public synchronized void addRecovering(RegionServerModel recoveringRegionServer) {
        if (!this.recoveringRegionServers.containsKey(recoveringRegionServer)) {
            LOGGER.info("Found new recovering RS: {}", recoveringRegionServer);
        }
        recoveringRegionServers.putIfAbsent(recoveringRegionServer, StringUtils.EMPTY);
        recoveryLogger.setRecoveryRsCount(recoveringRegionServers.size());
    }

    public synchronized Set<RegionServerModel> getRecovering() {
        return recoveringRegionServers.keySet();
    }

    public synchronized boolean containsRecovering(RegionServerModel regionServer) {
        return recoveringRegionServers.containsKey(regionServer);
    }

    public synchronized void removeRecovering(RegionServerModel recoveringRegionServer) {
        if (recoveringRegionServers.containsKey(recoveringRegionServer)) {
            LOGGER.info("RS is no more recovering: {}", recoveringRegionServer);
        }
        recoveringRegionServers.remove(recoveringRegionServer);
        recoveryLogger.setRecoveryRsCount(recoveringRegionServers.size());
    }

    public synchronized void removeAllRecovering() {
        if (!recoveringRegionServers.isEmpty()) {
            LOGGER.warn("[WARN] Recovering RS list is not empty, cleaning... {}", getRecovering());
            recoveringRegionServers.clear();
            recoveryLogger.setRecoveryRsCount(recoveringRegionServers.size());
        }
    }

    // Adds a new RS to movingRegionServers.
    // This means that the RS is involved in a move in some way,
    // and it must not be started until it is removed from the list.
    public synchronized void addMoving(RegionServerModel rs, TableName tableName) {
        movingRegionServers.compute(rs, (k, v) -> {
            if (v == null) {
                return new HashSet<>(Collections.singleton(tableName));
            } else {
                v.add(tableName);
                return v;
            }
        });
        LOGGER.info("New moving RS request: {}. blocked by: {}", rs, movingRegionServers.get(rs));
    }

    public synchronized Set<RegionServerModel> getMoving() {
        return movingRegionServers.keySet();
    }

    public synchronized boolean containsMoving(RegionServerModel regionServer) {
        return movingRegionServers.get(regionServer) != null && !movingRegionServers.get(regionServer).isEmpty();
    }

    public synchronized void removeMoving(RegionServerModel rs, TableName tableName) {
        movingRegionServers.compute(rs, (k, v) -> {
            if (v == null) {
                LOGGER.warn("List of blocked tables is null for: {}", k);
                return null;
            } else {
                v.remove(tableName);
                return v;
            }
        });
        LOGGER.info("New remove RS from moving request: {}. blocked by: {}", rs, movingRegionServers.get(rs));
    }

    // Adds new RS with tables skipped during recovery to skippedTablesRS
    public synchronized void addSkippedTables(List<RegionServerModel> skippedTablesRS) {
        skippedTablesRS.forEach(this::addSkippedTables);
    }

    public synchronized void addSkippedTables(RegionServerModel skippedTablesRS) {
        if (!this.skippedTablesRS.containsKey(skippedTablesRS)) {
            LOGGER.info("Found new RS with recovery-skipped tables: {}", skippedTablesRS);
        }
        this.skippedTablesRS.putIfAbsent(skippedTablesRS, StringUtils.EMPTY);
    }

    public synchronized void clearSkippedTables() {
        skippedTablesRS.clear();
    }

    public synchronized Set<RegionServerModel> getSkippedTables() {
        return skippedTablesRS.keySet();
    }

    // Adds new RS with tables skipped during recovery to skippedLeaderStartTables
    public synchronized void addSkippedLeaderStartTable(TableName tableName) {
        if (!this.skippedLeaderStartTables.containsKey(tableName)) {
            LOGGER.info("Found new table with recovery-skipped leader starts: {}", tableName);
        }
        this.skippedLeaderStartTables.putIfAbsent(tableName, StringUtils.EMPTY);
    }

    public synchronized Set<TableName> getSkippedLeaderStartTables() {
        return skippedLeaderStartTables.keySet();
    }

    public synchronized boolean containsSkippedLeaderStartTables(TableName tableName) {
        return skippedLeaderStartTables.containsKey(tableName);
    }

    public synchronized void removeSkippedLeaderStartTable(TableName tableName) {
        if (skippedLeaderStartTables.containsKey(tableName)) {
            LOGGER.info("Table is not more in the list of recovery-skipped leader starts: {}", tableName);
        }
        skippedLeaderStartTables.remove(tableName);
    }


    // Signals that the synchronization of reserve replicas with the leader has started
    public synchronized void notifySyncStart() {
        LOGGER.info("Notifying that SYNC process started");
        isSyncRunning.set(true);
    }

    // Signals that the synchronization of reserve replicas with the leader has finished
    public synchronized void notifySyncFinish() {
        LOGGER.info("Notifying that SYNC process finished");
        isSyncRunning.set(false);
    }

    // Returns true if reserve replicas are being synchronized with the leader
    public synchronized boolean isSyncRunning() {
        return isSyncRunning.get();
    }
}
