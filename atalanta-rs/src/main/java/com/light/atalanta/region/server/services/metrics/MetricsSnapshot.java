/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.metrics;

import com.light.atalanta.TableName;
import com.light.atalanta.metrics.RegionMetrics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A completed metrics update. No mutable input objects are retained. */
final class MetricsSnapshot {
    static final Values ZERO = new Values(0, 0, 0, 0, 0, 0);

    record Values(long keys, long memUsageBytes, long countGet, long countPut, long countDel, long size) {
        static Values copyOf(RegionMetrics metrics) {
            return new Values(metrics.getKeys(), metrics.getMemUsageBytes(), metrics.getCountGet(),
                    metrics.getCountPut(), metrics.getCountDel(), metrics.getSize());
        }

        Values withSize(long newSize) {
            return new Values(keys, memUsageBytes, countGet, countPut, countDel, newSize);
        }
    }

    record Region(String name, int replicaNum, TableName table, Values values) {
        static Region copyOf(RegionMetrics metrics) {
            return new Region(metrics.getRegionName(), metrics.getReplicaNum(), null, Values.copyOf(metrics));
        }
    }

    private final Map<String, Region> regions;
    private final Map<TableName, Values> tables;
    private final Map<String, Values> namespaces;
    private final Values server;

    private MetricsSnapshot(Map<String, Region> regions, Map<TableName, Values> tables,
                            Map<String, Values> namespaces, Values server) {
        this.regions = Map.copyOf(regions);
        this.tables = Map.copyOf(tables);
        this.namespaces = Map.copyOf(namespaces);
        this.server = server;
    }

    static MetricsSnapshot empty() {
        return new MetricsSnapshot(Map.of(), Map.of(), Map.of(), ZERO);
    }

    static MetricsSnapshot aggregate(List<Region> preparedRegions) {
        Map<String, Region> regions = new HashMap<>();
        for (Region region : preparedRegions) {
            // One contribution per region, even if the input contains a duplicate
            regions.put(region.name(), region);
        }
        Map<TableName, Totals> tables = new HashMap<>();
        Map<String, Totals> namespaces = new HashMap<>();
        Totals server = new Totals();
        for (Region region : regions.values()) {
            TableName table = region.table();
            if (table != null) {
                tables.computeIfAbsent(table, ignored -> new Totals()).add(region.values());
                namespaces.computeIfAbsent(table.getNamespace(), ignored -> new Totals()).add(region.values());
                server.add(region.values());
            }
        }
        Map<TableName, Values> tableValues = new HashMap<>();
        tables.forEach((table, totals) -> tableValues.put(table, totals.freeze()));
        Map<String, Values> namespaceValues = new HashMap<>();
        namespaces.forEach((namespace, totals) -> namespaceValues.put(namespace, totals.freeze()));
        return new MetricsSnapshot(regions, tableValues, namespaceValues, server.freeze());
    }

    Map<String, Region> regions() {
        return regions;
    }

    Map<TableName, Values> tables() {
        return tables;
    }

    Map<String, Values> namespaces() {
        return namespaces;
    }

    Values region(String name) {
        Region region = regions.get(name);
        return region == null ? ZERO : region.values();
    }

    Values table(TableName table) {
        return tables.getOrDefault(table, ZERO);
    }

    Values namespace(String namespace) {
        return namespaces.getOrDefault(namespace, ZERO);
    }

    Values server() {
        return server;
    }

    /** These accumulators are owned exclusively by the aggregating thread. */
    private static final class Totals {
        private long keys;
        private long memUsageBytes;
        private long countGet;
        private long countPut;
        private long countDel;
        private long size;

        void add(Values values) {
            keys += values.keys();
            memUsageBytes += values.memUsageBytes();
            countGet += values.countGet();
            countPut += values.countPut();
            countDel += values.countDel();
            size += values.size();
        }

        Values freeze() {
            return new Values(keys, memUsageBytes, countGet, countPut, countDel, size);
        }
    }
}
