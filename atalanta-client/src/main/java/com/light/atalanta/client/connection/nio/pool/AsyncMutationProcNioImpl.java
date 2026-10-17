/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.pool;

import com.light.atalanta.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AsyncMutationProcNioImpl implements AsyncMutationProcNio {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncMutationProcNioImpl.class);
    private static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 60_000L;
    private final ConnectionNio connection;
    private final Thread thread;
    private final BlockingDeque<Pair<TableName, Pair<Object, CompletableFuture<Object>>>> dbOperationQueue;
    private static final AtomicInteger threadIndex = new AtomicInteger();
    private final TableName flushOpTable = TableName.valueOf("", "flushOp");

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private final AtomicReference<CompletableFuture<Object>> activeFuture = new AtomicReference<>();
    private final AtomicReference<Throwable> terminalError = new AtomicReference<>();
    private final int connectionPoolQueueSize;
    private final int connectionPoolOverflowLimut;
    private final long shutdownTimeoutMs;
    private final boolean directDispatch;

    public AsyncMutationProcNioImpl(ConnectionNio connection, int connectionPoolQueueSize) {
        this(connection, connectionPoolQueueSize, false);
    }

    public AsyncMutationProcNioImpl(ConnectionNio connection, int connectionPoolQueueSize,
                                    boolean directDispatch) {
        this(connection, connectionPoolQueueSize, DEFAULT_SHUTDOWN_TIMEOUT_MS, directDispatch);
    }

    AsyncMutationProcNioImpl(ConnectionNio connection, int connectionPoolQueueSize, long shutdownTimeoutMs) {
        this(connection, connectionPoolQueueSize, shutdownTimeoutMs, false);
    }

    AsyncMutationProcNioImpl(ConnectionNio connection, int connectionPoolQueueSize,
                             long shutdownTimeoutMs, boolean directDispatch) {
        this.connectionPoolQueueSize = connectionPoolQueueSize;
        this.connectionPoolOverflowLimut = connectionPoolQueueSize / 10;
        this.shutdownTimeoutMs = shutdownTimeoutMs;
        this.directDispatch = directDispatch;
        dbOperationQueue = new LinkedBlockingDeque<>(connectionPoolQueueSize);
        this.connection = connection;
        thread = new Thread(this::asyncProcess, "atalanta-cp-" + threadIndex.incrementAndGet() + " " + Thread.currentThread().getName());
        thread.setDaemon(true);
        thread.start();
    }

    private void asyncProcess() {
        try {
            while (true) {
                boolean isQueueOverflow = false;
                do {
                    final Pair<TableName, Pair<Object, CompletableFuture<Object>>> m = dbOperationQueue.take();
                    if (isQueueOverflow) {
                        if (dbOperationQueue.size() < connectionPoolOverflowLimut) {
                            LOGGER.info("Queue is not full. size: {}", dbOperationQueue.size());
                            isQueueOverflow = false;
                        }
                    } else if (dbOperationQueue.size() > connectionPoolQueueSize - connectionPoolOverflowLimut) {
                        LOGGER.warn("Queue size is {}, which is greater than the connection pool overflow limit of {}",
                                dbOperationQueue.size(), connectionPoolQueueSize - connectionPoolOverflowLimut);
                        isQueueOverflow = true;
                    }
                    final TableName tableName = m.getFirst();
                    final Object operations = m.getSecond().getFirst();
                    final CompletableFuture<Object> resultFuture = m.getSecond().getSecond();
                    activeFuture.set(resultFuture);
                    if (operations instanceof ShutdownOp) {
                        resultFuture.complete(null);
                        activeFuture.compareAndSet(resultFuture, null);
                    } else {
                        processingOperation(tableName, operations, resultFuture);
                        activeFuture.compareAndSet(resultFuture, null);
                    }

                } while (!dbOperationQueue.isEmpty());
            }
        } catch (Throwable error) {
            boolean expectedShutdown = error instanceof InterruptedException && isClosed.get();
            if (!expectedShutdown)
                terminalError.compareAndSet(null, error);

            failOutstandingOperations(expectedShutdown ? new IllegalStateException("NIO mutation processor is closed") : error);
            if (expectedShutdown)
                LOGGER.debug("AsyncMutationProcNioImpl interrupted");
            else
                LOGGER.error("AsyncMutationProcNioImpl terminated unexpectedly", error);

        } finally {
            threadIndex.decrementAndGet();
            try {
                connection.close();
            } catch (Throwable error) {
                terminalError.compareAndSet(null, error);
                LOGGER.error("Cannot close NIO mutation processor connection", error);
            } finally {
                Throwable failure = terminalError.get();
                if (failure == null) {
                    failure = new IllegalStateException("NIO mutation processor stopped before operation completed");
                }
                failOutstandingOperations(failure);
            }
        }
    }

    private void failOutstandingOperations(Throwable error) {
        CompletableFuture<Object> processing = activeFuture.getAndSet(null);
        if (processing != null) {
            processing.completeExceptionally(error);
        }
        Pair<TableName, Pair<Object, CompletableFuture<Object>>> queued;
        while ((queued = dbOperationQueue.poll()) != null) {
            queued.getSecond().getSecond().completeExceptionally(error);
        }
    }

    private void processingOperation(TableName tableName, Object operations, CompletableFuture<Object> resultFuture) {
        try {
            Object result = Result.EMPTY_RESULT;
            if (operations instanceof RegionLocationOp) {
                Table table = connection.getTable(tableName);
                result = table.getAllRegionLocations();
            } else if (operations instanceof List) {
                Object operation = ((List<?>) operations).iterator().next();
                if (operation instanceof DbMutation) {
                    NioTable table = (NioTable) connection.getTable(tableName);
                    result = table.submitMutations((List<DbMutation>) operations);
                } else if (operation instanceof Get) {
                    Table table = connection.getTable(tableName);
                    if (((List<?>) operations).size() == 1 ) {
                        result = new Result[1];
                        ((Result[])result)[0] = table.get((Get)operation);
                    } else {
                        result = table.get((List<Get>) operations);
                    }
                }
            } else {
                throw new UnsupportedOperationException("operation not supported: " + operations.getClass().getName());
            }
            resultFuture.complete(result);
        } catch (IOException | RuntimeException ex) {
            LOGGER.error("Error while processing DB operation", ex);
            resultFuture.completeExceptionally(ex);
        }
    }

    private final Lock lock = new ReentrantLock();


    @Override
    public <T> CompletableFuture<Object> operation(List<T> operations, TableName tableName) {
        if (directDispatch) {
            CompletableFuture<Object> future = new CompletableFuture<>();
            lock.lock();
            try {
                if (isClosed.get()) {
                    future.completeExceptionally(new IllegalStateException("NIO mutation processor is closed"));
                } else {
                    processingOperation(tableName, operations, future);
                }
            } finally {
                lock.unlock();
            }
            return future;
        }
        return enqueue(operations, tableName);
    }


    private CompletableFuture<Object> enqueue(Object operations, TableName tableName) {
        final CompletableFuture<Object> future = new CompletableFuture<>();
        lock.lock();
        try {
            if (isClosed.get()) {
                future.completeExceptionally(new IllegalStateException("NIO mutation processor is closed"));
                return future;
            }
            dbOperationQueue.put(new Pair<>(tableName, new Pair<>(operations, future)));
            if (!thread.isAlive()) {
                dbOperationQueue.forEach(op -> op.getSecond().getSecond().completeExceptionally(new IllegalStateException("NIO mutation processing thread is stopped")));
                dbOperationQueue.clear();
            }
        } catch (InterruptedException e) {
            LOGGER.info("operation has been interrupted");
            Thread.currentThread().interrupt();
            future.completeExceptionally(e);
        } finally {
            lock.unlock();
        }
        return future;
    }

    @Override
    public List<RegionLocation> getAllRegionLocations(TableName tableName) {
        RegionLocationOp rlo = new RegionLocationOp();
        CompletableFuture<Object> future = enqueue(rlo, tableName);
        try {
            return (List<RegionLocation>) future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            LOGGER.error("cannot get region locations for table {}", tableName);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResultScanner getScanner(ScanRows scan, TableName tableName) throws IOException {
        return connection.getTable(tableName).getScanner((Scan) scan);
    }

    @Override
    public void close() throws IOException {
        if (Thread.currentThread() == thread) {
            throw new IOException("NIO mutation processor cannot be closed from its processing thread");
        }

        CompletableFuture<Object> drainFuture = new CompletableFuture<>();
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(shutdownTimeoutMs);
        IOException failure = null;
        boolean closeOwner = false;
        lock.lock();
        try {
            closeOwner = isClosed.compareAndSet(false, true);
            if (closeOwner) {
                if (!thread.isAlive()) {
                    failure = new IOException("NIO mutation processing thread is already stopped", terminalError.get());
                } else {
                    try {
                        boolean scheduled = dbOperationQueue.offer(new Pair<>(flushOpTable, new Pair<>(new ShutdownOp(), drainFuture)), remainingMillis(deadlineNanos), TimeUnit.MILLISECONDS);
                        if (!scheduled) {
                            failure = timeoutFailure("scheduling shutdown marker");
                        }
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                        failure = new IOException("Interrupted while scheduling NIO mutation processor shutdown", error);
                    }
                }
            }
        } finally {
            lock.unlock();
        }

        if (!closeOwner) {
            awaitCloseCompletion();
            return;
        }

        try {
            if (failure == null) {
                try {
                    drainFuture.get(remainingMillis(deadlineNanos), TimeUnit.MILLISECONDS);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    failure = new IOException("Interrupted while draining NIO mutation processor", error);
                } catch (ExecutionException error) {
                    failure = asIOException("Cannot drain NIO mutation processor", error.getCause());
                } catch (TimeoutException error) {
                    failure = timeoutFailure("draining accepted operations");
                }
            }

            thread.interrupt();
            try {
                thread.join(remainingMillis(deadlineNanos));
                if (thread.isAlive()) {
                    failure = addFailure(failure, timeoutFailure("waiting for worker termination"));
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                failure = addFailure(failure, new IOException("Interrupted while waiting for NIO mutation processor", error));
            }

            if (terminalError.get() != null) {
                failure = addFailure(failure, asIOException("NIO mutation processor terminated with an error", terminalError.get()));
            }
        } finally {
            if (failure == null) {
                closeFuture.complete(null);
            } else {
                closeFuture.completeExceptionally(failure);
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    private void awaitCloseCompletion() throws IOException {
        try {
            closeFuture.get();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for NIO mutation processor to close", error);
        } catch (ExecutionException error) {
            throw asIOException("Cannot close NIO mutation processor", error.getCause());
        }
    }

    private static IOException asIOException(String message, Throwable error) {
        return error instanceof IOException ? (IOException) error : new IOException(message, error);
    }

    private static IOException addFailure(IOException current, IOException next) {
        if (current == null) {
            return next;
        }
        if (current != next) {
            current.addSuppressed(next);
        }
        return current;
    }

    private long remainingMillis(long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            return 1L;
        }
        return Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
    }

    private IOException timeoutFailure(String phase) {
        StringBuilder message = new StringBuilder("Timed out ")
                .append(phase)
                .append(" for worker ")
                .append(thread.getName())
                .append(", state=")
                .append(thread.getState());
        for (StackTraceElement frame : thread.getStackTrace()) {
            message.append(System.lineSeparator()).append("\tat ").append(frame);
        }
        return new IOException(message.toString());
    }

    private static class ShutdownOp {
    }

    private static class RegionLocationOp {
    }
}
