/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services;

import com.light.atalanta.TableName;
import org.slf4j.Logger;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.BiConsumer;

/**
 * Lock with the ref tracking.
 * 1. The Main characteristic is that the lock can be unlocked, but the ref can still be tracked.
 * 2. If ref is released (releaseRef, unlockAndRelease) – lock shouldn't be used again
 *    until it's obtained again from {@link LockManager#acquireRef(TableName, String)}.
 * 3. Calls cleanup action on ref release, defined by LockManager.
 */
public final class TableLock {
    private final Logger logger;
    private final Object lockManagementMutex;
    private final BiConsumer<TableName, String> cleanup;

    final AtomicReference<String> baseOperationName;
    final TableName tableName;

    private final Semaphore lock;

    private final ReadWriteLock globalLock;
    private final AtomicInteger activeTableOperations;

    // How many clients are holding a lock reference
    final AtomicInteger refCount;

    TableLock(Logger logger,
              TableName tableName,
              String baseOperationName,
              ReadWriteLock globalLock,
              AtomicInteger activeTableOperations,
              Object lockManagementMutex,
              BiConsumer<TableName, String> cleanup
    ) {
        this.logger = logger;
        this.lockManagementMutex = lockManagementMutex;
        this.tableName = tableName;
        this.baseOperationName = new AtomicReference<>(baseOperationName);
        this.lock = new Semaphore(1, true);
        this.globalLock = globalLock;
        this.activeTableOperations = activeTableOperations;
        this.refCount = new AtomicInteger(0);
        this.cleanup = cleanup;
    }

    public TableName getTableName() {
        return tableName;
    }

    public void lock(String operationName) throws InterruptedException {
        // Global synchronization
        beginOperation(Integer.MAX_VALUE, operationName);

        // Table-scoped lock
        logger.info("[{}] Locking the table: {}", operationName, tableName);
        lock.acquire();
        logger.info("[{}] Successfully locked table: {}", operationName, tableName);
        baseOperationName.set(operationName);
    }

    public boolean tryLock(int timeoutMs, String operationName) throws InterruptedException {
        // Global synchronization
        if (!beginOperation(timeoutMs, operationName)) {
            return false;
        }

        // Thread-scoped lock
        logger.info("[{}] Locking the table: {}", operationName, tableName);
        boolean success;
        try {
            success = lock.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            logger.error("[{}] Locking has been interrupted for table: {}", operationName, tableName);
            completeOperation(operationName);
            Thread.currentThread().interrupt();
            throw interrupted;
        }

        if (!success) {
            logger.error("[{}] Lock waiting timeout for table: {}", operationName, tableName);
            completeOperation(operationName);
            return false;
        }

        logger.info("[{}] Successfully locked table: {}", operationName, tableName);
        baseOperationName.set(operationName);
        return true;
    }

    public void unlock() {
        unlock(baseOperationName.get());
    }
    public void unlock(String operationName) {
        // Table-scoped unlock
        logger.info("[{}] Unlocking the table: {}", operationName, tableName);
        lock.release();

        // Global synchronization
        completeOperation(operationName);
    }

    /**
     * Releases the lock ref (in some cases, unlocks the lock as well).
     * Calls cleanup action, defined by LockManager {@link LockManager#lockCleanup(TableName, String)}.
     * Current cleanup implementation – remove lock from cache if it's not referenced.
     */
    public void releaseRef() {
        releaseRef(baseOperationName.get());
    }
    public void unlockAndReleaseRef() {
        unlock();
        releaseRef();
    }
    public void unlockAndReleaseRef(String baseOperationName) {
        unlock(baseOperationName);
        releaseRef(baseOperationName);
    }
    public void releaseRef(String operationName) {
        synchronized (lockManagementMutex) {
            logger.info("[{}] Releasing lock reference for table: {}", baseOperationName, tableName);
            int references = refCount.decrementAndGet();
            logger.info("[{}] Lock references count for table: {} is: {}", operationName, tableName, references);
            // Cleanup action – remove lock from cache if it's not referenced
            cleanup.accept(tableName, baseOperationName.get());
        }
    }

    /**
     * Global lock coordination.
     * This method will wait if a global operation is in progress.
     * Part of the "two-phase concurrency control with handoff" mechanism, see LockManager's Javadoc.
     */
    private boolean beginOperation(int timeoutMs, String operationName) throws InterruptedException {
        // Block until we can acquire the global read lock
        logger.info("[GLOBAL_READ:{}] Locking global read lock for table: {}", operationName, tableName);
        boolean success;
        try {
            success = globalLock.readLock().tryLock(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            logger.error("[GLOBAL_READ:{}] Global read locking has been interrupted for table: {}", operationName, tableName);
            Thread.currentThread().interrupt();
            throw interrupted;
        }

        if (!success) {
            logger.info("[GLOBAL_READ:{}] Global read lock waiting timeout for table: {}", operationName, tableName);
            return false;
        }

        // Register this operation and release the read lock - we're now tracked in the counter
        logger.info("[GLOBAL_READ:{}] Successfully locked global read lock for table: {}", operationName, tableName);
        try {
            activeTableOperations.incrementAndGet();
            logger.info("[GLOBAL_READ:{}] Started operation tracker for table: {}", operationName, tableName);
        } finally {
            globalLock.readLock().unlock();
            logger.info("[GLOBAL_READ:{}] Unlocked global read lock for table: {}", operationName, tableName);
        }

        return true;
    }

    private void completeOperation(String operationName) {
        logger.info("[GLOBAL_READ:{}] Tracking finished for table: {}", operationName, tableName);
        activeTableOperations.decrementAndGet();
    }
}
