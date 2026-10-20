/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.pool;

import com.light.atalanta.*;
import com.light.atalanta.transport.messages.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Deprecated
public class AsyncMutationProcImpl implements AsyncMutationProc {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncMutationProcImpl.class);
    private final Connection connection;
    private final Thread thread;
    private final BlockingDeque<Pair<TableName, Pair<RowKey, CompletableFuture<Result>>>> dbOperationQueue;
    private static final AtomicInteger threadIndex = new AtomicInteger();
    private final AtomicReference<Exception> runtimeError = new AtomicReference<>();
    private TableName flushOpTable = TableName.valueOf("", "flushOp");

    private AtomicBoolean isClosed = new AtomicBoolean(false);

    public AsyncMutationProcImpl(Connection connection, int connectionPoolQueueSize) {
        dbOperationQueue = new LinkedBlockingDeque<>(connectionPoolQueueSize);
        this.connection = connection;
        thread = new Thread(this::asyncProcess, "atalanta-cp-" + threadIndex.incrementAndGet() + " " + Thread.currentThread().getName());
        thread.setDaemon(true);
        thread.start();
    }

    private class FlushOp implements RowKey {

        @Override
        public byte[] getRow() {
            return new byte[0];
        }
    }

    private void asyncProcess() {
        try {
            while (true) {
                do {
                    final Pair<TableName, Pair<RowKey, CompletableFuture<Result>>> m = dbOperationQueue.take();
                    final TableName tableName = m.getFirst();
                    final RowKey operation = m.getSecond().getFirst();
                    final CompletableFuture<Result> resultFuture = m.getSecond().getSecond();
                    try {
                        if (operation != null) {
                            Result result = Result.EMPTY_RESULT;
                            if (operation instanceof DbMutation) {
                                DbBufferedMutator bm = connection.getBufferedMutator(tableName);
                                if (operation instanceof Put && ((Put) operation).getType() == MessageType.PUT_AND_GET) {
                                    // Experimental feature: not ready for use
                                    throw new UnsupportedOperationException("putAndGet/mutateAndGet are experimental and not ready for use");
                                }
                                bm.mutate((DbMutation) operation);
                            } else if (operation instanceof Get) {
                                Table table = connection.getTable(tableName);
                                result = table.get((Get) operation);
                            } else { // FlushOp: nothing to do
                            }

                            resultFuture.complete(result);
                            runtimeError.set(null);

                        } else {
                            connection.getTable(tableName).close();
                            resultFuture.complete(null);
                        }
                    } catch (Exception ex) {
                        LOGGER.error("Error while processing DB operation", ex);
                        runtimeError.set(ex);
                        resultFuture.completeExceptionally(ex);
                    }

                } while (!dbOperationQueue.isEmpty());
            }
        } catch (InterruptedException e) {
            HashSet<TableName> tableClosed = new HashSet<TableName>();
            dbOperationQueue.forEach(m -> {
                final TableName tableName = m.getFirst();
                RowKey op = m.getSecond().getFirst();
                if (!(op instanceof FlushOp) && !tableClosed.contains(tableName)) {
                    try {
                        connection.getTable(tableName).close();
                        tableClosed.add(tableName);
                    } catch (IOException ex) {
                        LOGGER.error("Cannot close table '{}'", tableName, ex);
                    }
                }
                m.getSecond().getSecond().complete(null);
            });
            dbOperationQueue.clear();
            LOGGER.info("AtalantaDB asyncProcess interrupted");
        } catch (Exception ex) {
            runtimeError.set(ex);
            LOGGER.error("Cannot mutate", ex);
        } finally {
            try {
                threadIndex.decrementAndGet();
                connection.close();
            } catch (IOException ex) {
                LOGGER.error("Cannot close AtalantaDB connection", ex);
            }
            dbOperationQueue.forEach(op -> op.getSecond().getSecond().complete(null));
            dbOperationQueue.clear();
        }
    }

    @Override
    public CompletableFuture<Result> mutate(DbMutation mutation, TableName tableName) {
        Objects.requireNonNull(mutation);
        return operationInternal(mutation, tableName);
    }

    private CompletableFuture<Result> operationInternal(RowKey operation, TableName tableName) {
        Exception previous = runtimeError.getAndSet(null);
        if (previous != null) {
            throw new RuntimeException(previous);
        }

        final CompletableFuture<Result> future = new CompletableFuture<>();
        try {
            dbOperationQueue.put(new Pair<>(tableName, new Pair<>(operation, future)));
            if (!thread.isAlive()) {
                dbOperationQueue.forEach(op -> op.getSecond().getSecond().complete(null));
                dbOperationQueue.clear();
            }
        } catch (InterruptedException e) {
            LOGGER.info("operation has been interrupted");
            Thread.currentThread().interrupt();
        }
        return future;
    }

    public CompletableFuture<Result> close(TableName tableName) {
        if (thread.isAlive() && !thread.isInterrupted()) {
            return operationInternal(null, tableName);
        } else {
            return null;
        }
    }

    @Override
    public CompletableFuture<Result> get(GetRows get, TableName tableName) throws IOException {
        return operationInternal(get, tableName);
    }

    @Override
    public void waitAllOperation() throws IOException {
        Exception previous = runtimeError.getAndSet(null);
        if (previous != null) {
            throw new IOException(previous);
        }

        if (isClosed.get()) {
            return;
        }

        try {
            final CompletableFuture<Result> future = new CompletableFuture<>();
            dbOperationQueue.put(new Pair<>(flushOpTable, new Pair<>(new FlushOp(), future)));
            if (thread.isAlive()) {
                future.get();
            } else {
                dbOperationQueue.clear();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("interrupted!");
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResultScanner getScanner(ScanRows scan, TableName tableName) throws IOException {
        Exception previous = runtimeError.getAndSet(null);
        if (previous != null) {
            throw new IOException(previous);
        }
        return connection.getTable(tableName).getScanner((Scan) scan);
    }

    @Override
    public void close() throws IOException {
        if (isClosed.get()) {
            return;
        }
        Exception previous = runtimeError.getAndSet(null);
        if (previous != null) {
            throw new IOException(previous);
        }
        waitAllOperation();
        isClosed.set(true);
        thread.interrupt();
    }
}
