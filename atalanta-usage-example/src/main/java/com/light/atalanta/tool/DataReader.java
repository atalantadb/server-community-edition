/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.tool;

import com.light.atalanta.Connection;
import com.light.atalanta.Get;
import com.light.atalanta.Result;
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

/** Reads and verifies all deterministic records written by {@link DataWriter}. */
public final class DataReader {

    public ReadResult read(Path configurationPath,
                           DataToolConfig toolConfig,
                           PrintStream output) throws Exception {
        ObjectFactory factory = DatabaseInitializer.createObjectFactory(
                toolConfig.getConnectionType());
        SharedConnection sharedConnection = new SharedConnection(factory, configurationPath);
        AtomicLong nextRecord = new AtomicLong();
        AtomicLong recordsRead = new AtomicLong();
        long totalRecords = toolConfig.getRecordCount();
        ExecutorService executor = Executors.newFixedThreadPool(toolConfig.getWriterThreadCount());
        List<Future<?>> workers = new ArrayList<>(toolConfig.getWriterThreadCount());

        output.println(String.format(Locale.US, "Reading %,d records using ", totalRecords)
                + toolConfig.getWriterThreadCount() + " threads.");
        if (toolConfig.isBatchWritesEnabled()) {
            long batchCount = DataWriter.calculateBatchCount(
                    totalRecords, toolConfig.getBatchSize());
            output.println("Batch read mode: " + toolConfig.getBatchSize()
                    + " records per request, " + batchCount + " requests in total.");
        } else {
            output.println("Single-operation read mode is enabled.");
        }
        long started = System.nanoTime();

        sharedConnection.acquire();
        try {
            for (int worker = 0; worker < toolConfig.getWriterThreadCount(); worker++) {
                workers.add(executor.submit(() -> runWorker(
                        sharedConnection, factory, toolConfig, totalRecords,
                        nextRecord, recordsRead)));
            }
            waitForWorkers(workers);
        } finally {
            executor.shutdownNow();
            sharedConnection.release();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }

        if (recordsRead.get() != totalRecords) {
            throw new IOException("Expected to read " + totalRecords
                    + " records, but read " + recordsRead.get() + '.');
        }
        long elapsedNanos = System.nanoTime() - started;
        return new ReadResult(recordsRead.get(), elapsedNanos);
    }

    private void runWorker(SharedConnection sharedConnection,
                           ObjectFactory factory,
                           DataToolConfig toolConfig,
                           long totalRecords,
                           AtomicLong nextRecord,
                           AtomicLong recordsRead) {
        boolean acquired = false;
        try {
            Connection connection = sharedConnection.acquire();
            acquired = true;
            TableName tableName = factory.tableName(DataToolSchema.NAMESPACE, DataToolSchema.TABLE);
            Table table = connection.getTable(tableName);
            if (toolConfig.isBatchWritesEnabled()) {
                readBatches(table, factory, toolConfig.getBatchSize(), totalRecords,
                        nextRecord, recordsRead);
            } else {
                readSingleRecords(table, factory, totalRecords, nextRecord, recordsRead);
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

    private void readBatches(Table table,
                             ObjectFactory factory,
                             int batchSize,
                             long totalRecords,
                             AtomicLong nextRecord,
                             AtomicLong recordsRead) throws IOException {
        while (true) {
            long firstRecord = nextRecord.getAndAdd(batchSize);
            if (firstRecord >= totalRecords) {
                return;
            }
            int currentBatchSize = (int) Math.min(
                    batchSize, totalRecords - firstRecord);
            List<Get> gets = new ArrayList<>(currentBatchSize);
            for (int offset = 0; offset < currentBatchSize; offset++) {
                gets.add(RecordFactory.createGet(factory, firstRecord + offset));
            }
            Result[] results = table.get(gets);
            validateBatch(results, firstRecord, currentBatchSize);
            recordsRead.addAndGet(currentBatchSize);
        }
    }

    private void readSingleRecords(Table table,
                                   ObjectFactory factory,
                                   long totalRecords,
                                   AtomicLong nextRecord,
                                   AtomicLong recordsRead) throws IOException {
        while (true) {
            long recordNumber = nextRecord.getAndIncrement();
            if (recordNumber >= totalRecords) {
                return;
            }
            Result result = table.get(RecordFactory.createGet(factory, recordNumber));
            if (result == null || !Boolean.TRUE.equals(result.getExists())) {
                throw new IOException("Record was not found: " + recordNumber);
            }
            recordsRead.incrementAndGet();
        }
    }

    static void validateBatch(Result[] results,
                              long firstRecord,
                              int expectedSize) throws IOException {
        if (results == null || results.length != expectedSize) {
            int actualSize = results == null ? 0 : results.length;
            throw new IOException("Batch starting at record " + firstRecord
                    + " returned " + actualSize + " results; expected " + expectedSize + '.');
        }
        for (int offset = 0; offset < results.length; offset++) {
            if (results[offset] == null || !Boolean.TRUE.equals(results[offset].getExists())) {
                throw new IOException("Record was not found: " + (firstRecord + offset));
            }
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

    /** Read-stage metrics. */
    public static final class ReadResult {
        private final long recordsRead;
        private final long elapsedNanos;

        ReadResult(long recordsRead, long elapsedNanos) {
            this.recordsRead = recordsRead;
            this.elapsedNanos = elapsedNanos;
        }

        public long getRecordsRead() {
            return recordsRead;
        }

        public long getElapsedMillis() {
            return TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
        }

        public double getOperationsPerSecond() {
            return recordsRead * 1_000_000_000.0 / Math.max(1L, elapsedNanos);
        }
    }
}
