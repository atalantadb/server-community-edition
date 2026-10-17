/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.tool;

import com.light.atalanta.Connection;
import com.light.atalanta.Put;
import com.light.atalanta.Table;
import com.light.atalanta.TableName;
import com.light.atalanta.factory.ObjectFactory;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Writes a deterministic configurable-size workload. */
public final class DataWriter {

    public static final long DEFAULT_RECORD_COUNT = 10_000_000L;
    public static final int PUTS_PER_REGION = 100;

    public WriteResult write(Path configurationPath,
                             DataToolConfig toolConfig,
                             PrintStream output) throws Exception {
        ObjectFactory factory = DatabaseInitializer.createObjectFactory(
                toolConfig.getConnectionType());
        SharedConnection sharedConnection = new SharedConnection(factory, configurationPath);
        AtomicLong nextRecord = new AtomicLong();
        AtomicLong recordsWritten = new AtomicLong();
        long totalRecords = toolConfig.getRecordCount();
        ExecutorService executor = Executors.newFixedThreadPool(toolConfig.getWriterThreadCount());
        List<Future<?>> workers = new ArrayList<>(toolConfig.getWriterThreadCount());

        output.println(String.format(Locale.US, "%,d records will be written using ",
                totalRecords)
                + toolConfig.getWriterThreadCount() + " threads.");
        if (toolConfig.isBatchWritesEnabled()) {
            long batchCount = calculateBatchCount(totalRecords, toolConfig.getBatchSize());
            output.println("Batch mode: " + toolConfig.getBatchSize()
                    + " records per request, " + batchCount + " requests in total.");
        } else {
            output.println("Single-operation mode is enabled.");
        }
        long started = System.nanoTime();

        // Keep one reference until every submitted worker has finished. This
        // prevents a fast worker from closing the shared connection before a
        // slower worker has acquired it.
        sharedConnection.acquire();
        try {
            for (int worker = 0; worker < toolConfig.getWriterThreadCount(); worker++) {
                workers.add(executor.submit(() -> runWorker(
                        sharedConnection, factory, toolConfig,
                        totalRecords, nextRecord, recordsWritten)));
            }
            waitForWorkers(workers);
        } finally {
            executor.shutdownNow();
            sharedConnection.release();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }

        if (recordsWritten.get() != totalRecords) {
            throw new IOException("Expected to write " + totalRecords
                    + " records, but wrote " + recordsWritten.get() + '.');
        }
        long elapsedNanos = System.nanoTime() - started;
        return new WriteResult(recordsWritten.get(), elapsedNanos);
    }

    static long calculateBatchCount(long totalRecords, int batchSize) {
        if (totalRecords < 0 || batchSize <= 0) {
            throw new IllegalArgumentException("Record count and batch size must be valid.");
        }
        return (totalRecords + batchSize - 1L) / batchSize;
    }

    private void runWorker(SharedConnection sharedConnection,
                           ObjectFactory factory,
                           DataToolConfig toolConfig,
                           long totalRecords,
                           AtomicLong nextRecord,
                           AtomicLong recordsWritten) {
        boolean acquired = false;
        try {
            Connection connection = sharedConnection.acquire();
            acquired = true;
            TableName tableName = factory.tableName(DataToolSchema.NAMESPACE, DataToolSchema.TABLE);
            Table table = connection.getTable(tableName);
            if (toolConfig.isBatchWritesEnabled()) {
                writeBatches(table, factory, toolConfig.getBatchSize(), totalRecords,
                        nextRecord, recordsWritten);
            } else {
                writeSingleRecords(table, factory, totalRecords, nextRecord, recordsWritten);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (acquired) {
                try {
                    sharedConnection.release();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void writeBatches(Table table,
                              ObjectFactory factory,
                              int batchSize,
                              long totalRecords,
                              AtomicLong nextRecord,
                              AtomicLong recordsWritten) throws IOException {
        while (true) {
            long firstRecord = nextRecord.getAndAdd(batchSize);
            if (firstRecord >= totalRecords) {
                return;
            }
            int currentBatchSize = (int) Math.min(batchSize, totalRecords - firstRecord);
            List<Put> puts = new ArrayList<>(currentBatchSize);
            for (int offset = 0; offset < currentBatchSize; offset++) {
                puts.add(RecordFactory.createPut(factory, firstRecord + offset));
            }
            table.put(puts);
            recordsWritten.addAndGet(currentBatchSize);
        }
    }

    private void writeSingleRecords(Table table,
                                    ObjectFactory factory,
                                    long totalRecords,
                                    AtomicLong nextRecord,
                                    AtomicLong recordsWritten) throws IOException {
        while (true) {
            long recordNumber = nextRecord.getAndIncrement();
            if (recordNumber >= totalRecords) {
                return;
            }
            table.put(RecordFactory.createPut(factory, recordNumber));
            recordsWritten.incrementAndGet();
        }
    }

    private void waitForWorkers(List<Future<?>> workers) throws Exception {
        for (Future<?> worker : workers) {
            try {
                worker.get();
            } catch (ExecutionException e) {
                for (Future<?> remaining : workers) {
                    remaining.cancel(true);
                }
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException && cause.getCause() != null) {
                    cause = cause.getCause();
                }
                if (cause instanceof Exception) {
                    throw (Exception) cause;
                }
                throw new RuntimeException(cause);
            }
        }
    }

    /** Write-stage metrics. */
    public static final class WriteResult {
        private final long recordsWritten;
        private final long elapsedNanos;

        WriteResult(long recordsWritten, long elapsedNanos) {
            this.recordsWritten = recordsWritten;
            this.elapsedNanos = elapsedNanos;
        }

        public long getRecordsWritten() {
            return recordsWritten;
        }

        public long getElapsedMillis() {
            return TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
        }

        public double getOperationsPerSecond() {
            return recordsWritten * 1_000_000_000.0 / Math.max(1L, elapsedNanos);
        }
    }
}
