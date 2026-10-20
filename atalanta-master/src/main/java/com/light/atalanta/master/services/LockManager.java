/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services;

import com.light.atalanta.TableName;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * --------------------------------
 * Lock types
 * --------------------------------
 *
 * 1. Table-scoped locks
 *     - References can be acquired with acquireRef() and release with releaseRef().
 *       Multiple clients can get a reference to a table-scoped lock using this manager.
 *       Lock manager counts lock refs and prevents it from deletion while ref count > 0.
 *       When the ref count reaches zero, the lock is being automatically removed.
 *
 * 2. Global read-write lock
 *     - Global write lock needs to be acquired for global operations like StartStopRS,
 *       with the need to make sure that activeConcurrentOperations is 0)
 *       LockManager does all necessary coordination automatically.
 *       Regular operations should use tableScopeLock.tryLock() or lock();
 *       Global operations should use beginGlobalOperation() and completeGlobalOperation().
 *       Don't confuse global operations with bulk operations.
 *
 * 3. bulkLockMutex
 *     - Mutex to be synchronized on if operation needs to lock multiple known
 *       tables at the same time to prevent deadlocks.
 *       It's for the bulk operations, not global.
 **
 * --------------------------------
 * Locking strategies
 * --------------------------------
 *
 * 1. Single table
 *     - Use table-scoped lock.
 *       When being acquired, LockManager automatically does all the work
 *       related to global synchronization.
 *
 * 2. Set of known tables – bulk operation (don't confuse with global operations)
 *     - Use a set of table-scoped locks,
 *       but remember to synchronize on bulkLockMutex to prevent deadlocks.
 *
 * 3. Global operation (doesn't allow any other operation to be run and should block even non-existent tables)
 *     - Call beginGlobalOperation() and completeGlobalOperation() within the same thread.
 *       LockManager will do all required coordination.
 *
 * --------------------------------
 * Lock patterns
 * --------------------------------
 * manager.acquireRef() -> lock.tryLock() -> success -> lock.unlock() -> manager.releaseRef()
 * manager.acquireRef() -> lock.tryLock() -> success -> lock.unlockAndReleaseRef()
 * manager.acquireRef() -> lock.tryLock() -> fail -> manager.releaseRef()
 * manager.acquireRef() -> lock.tryLock() -> fail -> lock.releaseRef()
 * manager.acquireRef() -> lock.lock() -> lock.unlock() -> manager.releaseRef()
 * manager.acquireRef() -> lock.lock() -> lock.unlockAndReleaseRef()
 * manager.beginGlobalOperation() -> manager.completeGlobalOperation()
 *
 * --------------------------------
 * Two-phase concurrency control with handoff
 * --------------------------------
 *
 * Coordination for global operations like StartStopRS is particularly complex.
 * Regular solution in such cases is to use ReadWriteLock:
 * - Regular/bulk operations should acquire the global read lock
 * - Global operations should acquire the global write lock
 *
 * However, most of the operations on master can start in one thread and complete in another.
 * That's not possible to solve with ReadWriteLock, since it doesn't allow lock ownership to be changed.
 *
 * To solve this problem, we use "ywo-phase concurrency control with handoff" approach.
 *
 * All regular operations prior to their start should do the following:
 * 1. Acquire global read lock.
 * 2. Increment activeConcurrentOperations counter
 * 3. Release global read lock <- it's an important step, since this allows us to complete operation in another thread
 *
 * Note that the steps from above are being done automatically by LockManager within tableScopedLock.tryLock() or lock().
 *
 * All global operations should do the following:
 * 1. Call manager.beginGlobalOperation() right before start
 * 2. Call manager.completeGlobalOperation() right after completion.
 *
 * --------------------------------
 * Troubleshooting
 * --------------------------------
 * 1. Right before actual lock acquisition:
 *      [ENABLE_TABLE] Locking the table: test:t94
 * 2. Right after lock is acquired:
 *      [ENABLE_TABLE] Successfully locked table: test:t94
 * 3. Right before lock is released:
 *      [ENABLE_TABLE] Unlocking the table: test:t94
 */
public class LockManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LockManager.class);

    // Used to execute operations on a lock set: get ref, create ref, cleanup ref
    private final Object lockManagementMutex = new Object();

    // Table-scoped lock ref storage
    private final Map<TableName, TableLock> locks = new HashMap<>();

    // Snapshot of a lock set for monitoring purposes
    private final Map<TableName, TableLock> lockSnapshot = new HashMap<>();

    // Needs to be synchronized on when operation does bulk table locking.
    // Prevents deadlocks.
    private final Object bulkLockMutex = new Object();

    // Synchronization for global operations.
    // Part of the "two-phase concurrency control with handoff" mechanism, see LockManager's Javadoc.
    private final ReadWriteLock globalLock = new ReentrantReadWriteLock(true);
    private final AtomicInteger activeTableOperations = new AtomicInteger(0);

    /**
     * Acquires table-scoped lock reference.
     * Lock ref should be released later with {@link LockManager#releaseRef(TableLock)} or {@link TableLock#releaseRef()}.
     */
    public TableLock acquireRef(TableName tableName, String operationName) {
        synchronized (lockManagementMutex) {
            LOGGER.info("[{}] Acquiring lock reference for table: {}", operationName, tableName);
            TableLock tableLock = locks.get(tableName);

            if (tableLock == null) {
                LOGGER.info("[{}] Creating new lock for table: {}", operationName, tableName);
                tableLock = new TableLock(
                        LOGGER, tableName, operationName,
                        globalLock, activeTableOperations,
                        lockManagementMutex, this::lockCleanup
                );
                locks.put(tableName, tableLock);
            } else {
                LOGGER.info("[{}] Using existing lock reference for table: {}", operationName, tableName);
            }

            int references = tableLock.refCount.incrementAndGet();
            LOGGER.info("[{}] Lock references count for table: {} is: {}", operationName, tableName, references);
            return tableLock;
        }
    }

    /**
     * After calling this method – lock should not be used again,
     * until it's acquired again with {@link LockManager#acquireRef}.
     */
    public void releaseRef(TableLock tableLock) {
        releaseRef(tableLock, tableLock.baseOperationName.get());
    }
    public void releaseRef(TableLock tableLock, String operationName) {
        tableLock.releaseRef(operationName);
    }

    /**
     * Cleanup action that's called by the lock on ref release.
     * Removes the lock from cache if it's not referenced and not used by any consumers.
     * Expected to be called manually.
     */
    public void lockCleanup(TableName tableName, String operationName) {
        synchronized (lockManagementMutex) {
            TableLock tableLock = locks.get(tableName);
            if (tableLock == null) {
                LOGGER.warn("[{}] Lock is already removed for table: {}", operationName, tableName);
                return;
            }
            int references = tableLock.refCount.get();
            if (references == 0) {
                LOGGER.info("[{}] No lock references found for table: {}, removing lock", operationName, tableName);
                locks.remove(tableName);
            } else {
                LOGGER.info("[{}] Don't remove lock, found references for table: {}, references: {}", operationName, tableName, references);
            }
        }
    }

    /**
     * USE WITH CAUTION.
     * Gets existing lock from cache, errors out if it does not exist, doesn't count references.
     * Should be used only to get the lock from cache with intention to delete it.
     */
    public TableLock rawGetLockWithoutRef(TableName tableName) {
        synchronized (lockManagementMutex) {
            TableLock tableLock = locks.get(tableName);
            if (tableLock == null) {
                String errorMessage = "Can't find existing lock for table: " + tableName;
                LOGGER.error(errorMessage);
                throw new RuntimeException(errorMessage);
            }
            return tableLock;
        }
    }

    public Object getBulkLockMutex() {
        return bulkLockMutex;
    }

    /**
     * Begins a global operation with priority.
     * This will block new regular operations and wait for existing ones to complete.
     */
    public boolean beginGlobalOperation(int timeoutMs, String operationName) throws InterruptedException {
        // Block until we can acquire the global write lock
        LOGGER.info("[GLOBAL_WRITE:{}] Locking global write lock", operationName);

        boolean success;
        try {
            success = globalLock.writeLock().tryLock(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            LOGGER.error("[GLOBAL_WRITE:{}] Global write locking has been interrupted", operationName);
            Thread.currentThread().interrupt();
            throw interrupted;
        }

        if (!success) {
            LOGGER.info("[GLOBAL_WRITE:{}] Global write lock waiting timeout", operationName);
            return false;
        }

        try {
            LOGGER.info("[GLOBAL_READ:{}] Successfully locked global write lock", operationName);
            LOGGER.info("[GLOBAL_READ:{}] Waiting for active operations to be zero. current number: {}",
                    operationName, activeTableOperations.get()
            );
            while (activeTableOperations.get() > 0) {
                Thread.sleep(400);
            }
            LOGGER.info("[GLOBAL_READ:{}] No more active operations found, starting global operation", operationName);
        } catch (InterruptedException interrupted) {
            LOGGER.error("[GLOBAL_WRITE:{}] Waiting for active operations process interrupted", operationName);
            completeGlobalOperation(operationName);
            Thread.currentThread().interrupt();
            throw interrupted;
        }

        return true;
    }

    public void completeGlobalOperation(String operationName) {
        LOGGER.info("[GLOBAL_WRITE:{}] Unlocking global write lock", operationName);
        globalLock.writeLock().unlock();
        LOGGER.info("[GLOBAL_READ:{}] Global write lock unlocked", operationName);
    }

    private final ThreadFactory lockMonitoringThreadFactory = (runnable) -> {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.setName("lock-monitoring-daemon");
        return thread;
    };
    private final ScheduledExecutorService lockMonitoringExecutor = Executors.newSingleThreadScheduledExecutor(
            lockMonitoringThreadFactory
    );
    private final AtalantaDBConfiguration conf;

    public LockManager(AtalantaDBConfiguration conf) {
        this.conf = conf;
    }
    public void startLockMonitoring() {
        lockMonitoringExecutor.scheduleWithFixedDelay(() -> {
            synchronized (lockManagementMutex) {
                lockSnapshot.clear();
                lockSnapshot.putAll(locks);
            }
            if (lockSnapshot.isEmpty()) {
                LOGGER.info("No active table lock references found");
                return;
            }
            AtomicInteger index = new AtomicInteger(0);
            List<String> lockStats = lockSnapshot.entrySet().stream().map(entry -> {
                TableName tableName = entry.getKey();
                TableLock tableLock = entry.getValue();
                return String.format("\t%nid: %d, table: %s, hash: %d, ref count: %d. operation: %s",
                        index.incrementAndGet(), tableName, tableLock.hashCode(), tableLock.refCount.get(), tableLock.baseOperationName.get()
                );
            }).collect(Collectors.toList());
            LOGGER.info("Active table lock references: {}", lockStats);
            lockSnapshot.clear();
        }, 0, conf.getDataConfiguration().getLockMonitoringDelayMs(), TimeUnit.MILLISECONDS);
    }
}
