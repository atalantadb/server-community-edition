/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.light.atalanta.*;
import com.light.atalanta.api.DbTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Deprecated
public class RowCounter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RowCounter.class);

    public static long rowCount(Connection connection, TableName tableName) throws IOException {
        final AtomicLong counter = new AtomicLong(0L);
        final String parentThreadName = Thread.currentThread().getName();
        final List<RegionLocation> regions = connection.getRegionLocator(tableName).getAllRegionLocations();
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        final DbTable t = connection.getTable(tableName);
        final Map<RegionServer, List<DbRegionLocation>> map = regions
                .stream()
                .collect(Collectors.groupingBy(DbRegionLocation::getRegionServer));
        final List<ExecutorService> executors = new ArrayList<>();

        final AtomicInteger taskCounter = new AtomicInteger(1);
        map.keySet().forEach(rsm -> {
            final String threadNameTemplate = parentThreadName + "-" + tableName + ":" + rsm.getHost() + ":" + rsm.getCoreClientPort() + "-counter-%d";
            final ExecutorService service = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(threadNameTemplate).build());
            executors.add(service);
            final List<DbRegionLocation> locations = map.get(rsm);
            locations.forEach(l -> {
                CompletableFuture<Void> f = CompletableFuture.runAsync(() -> {
                    final int taskNumber = taskCounter.getAndIncrement();
                    LOGGER.debug("Task {} started", taskNumber);
                    final DbRegion regionCompactInfo = l.getRegionModel();
                    LOGGER.info("Start counting records for {}", regionCompactInfo);
                    try (ResultScanner rs = t.getScanner(new Scan(regionCompactInfo.getStartKey(), regionCompactInfo.getEndKey()))) {
                        final AtomicLong rowCounts = new AtomicLong(0L);
                        while (rs.next() != null) {
                            rowCounts.incrementAndGet();
                        }
                        LOGGER.info("End counting records for {}: {}", regionCompactInfo, rowCounts.get());
                        counter.addAndGet(rowCounts.get());
                    } catch (IOException e) {
                        LOGGER.error("Row Counter can't complete task for region: {}", regionCompactInfo.getName(), e);
                        throw new RuntimeException(e);
                    }
                    LOGGER.debug("Task {} completed", taskNumber);
                }, service);
                futures.add(f);
            });
        });
        LOGGER.debug("There are {} tasks for execution", taskCounter.get());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        LOGGER.debug("Counting successfully completed");
        executors.forEach(es -> {
            LOGGER.debug("Start closing {}", es.toString());
            es.shutdown();
            try {
                es.awaitTermination(1, TimeUnit.SECONDS);
                LOGGER.debug("Thread closed: {}", Thread.currentThread().getName());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        return counter.get();
    }
}