/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.tool;

import com.light.atalanta.Connection;
import com.light.atalanta.DbCellScanner;
import com.light.atalanta.Result;
import com.light.atalanta.ResultScanner;
import com.light.atalanta.Scan;
import com.light.atalanta.Table;
import com.light.atalanta.TableName;
import com.light.atalanta.factory.ObjectFactory;
import com.light.atalanta.util.UniformSplit;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Performs a full table scan over non-overlapping key ranges in parallel. */
public final class DataScanner {

    public ScanResult scan(Path configurationPath,
                           DataToolConfig toolConfig,
                           PrintStream output) throws Exception {
        ObjectFactory factory = DatabaseInitializer.createObjectFactory(
                toolConfig.getConnectionType());
        SharedConnection sharedConnection = new SharedConnection(factory, configurationPath);
        List<ScanRange> ranges = createRanges(toolConfig.getWriterThreadCount());
        AtomicLong recordsScanned = new AtomicLong();
        ExecutorService executor = Executors.newFixedThreadPool(ranges.size());
        List<Future<?>> workers = new ArrayList<>(ranges.size());

        output.println(String.format(Locale.US,
                "Scanning %,d records in parallel using %d non-overlapping key ranges.",
                toolConfig.getRecordCount(), ranges.size()));
        long started = System.nanoTime();

        // Keep the shared connection open until all range workers finish
        sharedConnection.acquire();
        try {
            for (ScanRange range : ranges) {
                workers.add(executor.submit(() -> scanRange(
                        sharedConnection, factory, range, recordsScanned)));
            }
            waitForWorkers(workers);
        } finally {
            executor.shutdownNow();
            sharedConnection.release();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }

        long elapsedNanos = System.nanoTime() - started;
        if (recordsScanned.get() != toolConfig.getRecordCount()) {
            throw new IOException("Expected to scan " + toolConfig.getRecordCount()
                    + " records, but scanned " + recordsScanned.get() + '.');
        }
        return new ScanResult(recordsScanned.get(), elapsedNanos);
    }

    static List<ScanRange> createRanges(int rangeCount) {
        if (rangeCount <= 0) {
            throw new IllegalArgumentException("Range count must be greater than zero.");
        }
        UniformSplit uniformSplit = new UniformSplit();
        byte[][] splitKeys = rangeCount == 1
                ? new byte[0][]
                : uniformSplit.split(rangeCount);
        List<ScanRange> ranges = new ArrayList<>(rangeCount);
        byte[] startKey = uniformSplit.firstRow();
        for (byte[] splitKey : splitKeys) {
            ranges.add(new ScanRange(startKey, splitKey));
            startKey = splitKey;
        }
        ranges.add(new ScanRange(startKey, uniformSplit.lastRow()));
        return ranges;
    }

    private void scanRange(SharedConnection sharedConnection,
                           ObjectFactory factory,
                           ScanRange range,
                           AtomicLong recordsScanned) {
        boolean acquired = false;
        try {
            Connection connection = sharedConnection.acquire();
            acquired = true;
            TableName tableName = factory.tableName(DataToolSchema.NAMESPACE, DataToolSchema.TABLE);
            Table table = connection.getTable(tableName);
            Scan scan = factory.scan()
                    .withStartRow(range.getStartKey())
                    .withStopRow(range.getStopKey());
            scan.setCaching(1);

            long rangeRecordCount = 0;
            try (ResultScanner scanner = table.getScanner(scan)) {
                Result result;
                while ((result = scanner.next()) != null) {
                    consumeCells(result);
                    rangeRecordCount++;
                }
            }
            recordsScanned.addAndGet(rangeRecordCount);
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

    static int consumeCells(Result result) throws IOException {
        if (result == null) {
            throw new IOException("Scanner returned a null result.");
        }
        int cellsScanned = 0;
        DbCellScanner cellScanner = result.cellScanner();
        while (cellScanner.advance()) {
            cellScanner.current();
            cellsScanned++;
        }
        return cellsScanned;
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

    /** Inclusive start and exclusive stop key for one parallel scan worker. */
    static final class ScanRange {
        private final byte[] startKey;
        private final byte[] stopKey;

        ScanRange(byte[] startKey, byte[] stopKey) {
            this.startKey = Arrays.copyOf(startKey, startKey.length);
            this.stopKey = Arrays.copyOf(stopKey, stopKey.length);
        }

        byte[] getStartKey() {
            return Arrays.copyOf(startKey, startKey.length);
        }

        byte[] getStopKey() {
            return Arrays.copyOf(stopKey, stopKey.length);
        }
    }

    /** Scan-stage metrics. */
    public static final class ScanResult {
        private final long recordsScanned;
        private final long elapsedNanos;

        ScanResult(long recordsScanned, long elapsedNanos) {
            this.recordsScanned = recordsScanned;
            this.elapsedNanos = elapsedNanos;
        }

        public long getRecordsScanned() {
            return recordsScanned;
        }

        public long getElapsedMillis() {
            return TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
        }

        public double getOperationsPerSecond() {
            return recordsScanned * 1_000_000_000.0 / Math.max(1L, elapsedNanos);
        }
    }
}
