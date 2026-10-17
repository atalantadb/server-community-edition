/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.services;

import com.light.atalanta.metrics.RegionMetrics;
import com.light.atalanta.metrics.RsJmxClient;
import com.light.atalanta.model.RegionServerModel;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ToDoubleFunction;

@Service
public class PrometheusService {

    public static final RegionMetrics EMPTY_METRICS = new RegionMetrics();
    private final MeterRegistry meterRegistry;

    private final RsJmxClient rsJmxClient;

    private final ZookeeperDaoService zookeeperDaoService;

    private long updateTime = 0;

    private final Map<String, Meter> metersSize = new ConcurrentHashMap<>();

    private final Map<String, Meter> metersKeys = new ConcurrentHashMap<>();

    private final Map<String, Meter> metersMemUsageBytes = new ConcurrentHashMap<>();

    private final Map<String, Meter> metersCountGet = new ConcurrentHashMap<>();

    private final Map<String, Meter> metersCountPut = new ConcurrentHashMap<>();

    private final Map<String, Meter> metersCountDel = new ConcurrentHashMap<>();

    private final Map<String, RegionMetrics> regionMetrics = new ConcurrentHashMap<>();

    public PrometheusService(MeterRegistry meterRegistry,
                             RsJmxClient rsJmxClient,
                             ZookeeperDaoService zookeeperDaoService) {
        this.meterRegistry = meterRegistry;
        this.rsJmxClient = rsJmxClient;
        this.zookeeperDaoService = zookeeperDaoService;
    }

    @PostConstruct
    public void init() {
        FunctionCounter.builder("jmx.metrics", this, this::updateJmxMetrics)    // 2 - create a counter using the fluent API
                .description("jmx metrics update time")
                .register(meterRegistry);
    }

    private double updateJmxMetrics(PrometheusService prometheusService) {
        refreshPrometheusMetrics();
        return (double) System.currentTimeMillis();
    }

    private synchronized void refreshPrometheusMetrics() {
        long curTime = System.currentTimeMillis();
        if (curTime - updateTime > RsJmxClient.METRICS_TTL_MS) {
            Set<String> regionAlive = new HashSet<>();
            zookeeperDaoService.getAllRegionServers()
                    .forEach(rs -> refreshPrometheusMetricsByRs(rs, rsJmxClient.getRsJmxMetrics(rs).getRegionsMetrics(), regionAlive));
            clearNotUsedRegion(regionAlive);
            updateTime = System.currentTimeMillis();
        }
    }

    private void refreshPrometheusMetricsByRs(RegionServerModel rs, Map<String, Map<Integer, RegionMetrics>> rsJmxMetrics, Set<String> regionAlive) {
        rsJmxMetrics.entrySet().forEach(
                e -> refreshPrometheusMetricsByRegion(rs, e, regionAlive));
    }

    private void clearNotUsedRegion(Set<String> regionAlive) {
        for (Iterator<Map.Entry<String, Meter>> it = metersSize.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, Meter> entry = it.next();
            String key = entry.getKey();
            if (!regionAlive.contains(key)) {
                meterRegistry.remove(entry.getValue());
                it.remove();
                meterRegistry.remove(metersKeys.remove(key));
                meterRegistry.remove(metersMemUsageBytes.remove(key));
                meterRegistry.remove(metersCountGet.remove(key));
                meterRegistry.remove(metersCountPut.remove(key));
                meterRegistry.remove(metersCountDel.remove(key));
                regionMetrics.remove(key);
            }
        }
    }

    private void refreshPrometheusMetricsByRegion(RegionServerModel rs, Map.Entry<String, Map<Integer, RegionMetrics>> regions, Set<String> regionAlive) {
        regions.getValue().entrySet().forEach(
                replicas -> refreshPrometheusMetricsByReplica(rs, regions.getKey(), replicas, regionAlive));
    }

    private void refreshPrometheusMetricsByReplica(RegionServerModel rs, String key, Map.Entry<Integer, RegionMetrics> replicas, Set<String> regionAlive) {
        String regKey = key + replicas.getKey();
        regionAlive.add(regKey);
        regionMetrics.put(regKey, replicas.getValue());
        if (metersSize.get(regKey) == null) {
            metersSize.put(regKey, createMeter(rs, regKey, replicas.getValue(), replicas.getKey(), this::getRegionSize, "size"));
            metersKeys.put(regKey, createMeter(rs, regKey, replicas.getValue(), replicas.getKey(),this::getRegionKeys, "keys"));
            metersMemUsageBytes.put(regKey, createMeter(rs, regKey, replicas.getValue(), replicas.getKey(), this::getRegionMem, "mem"));
            metersCountGet.put(regKey, createMeter(rs, regKey, replicas.getValue(), replicas.getKey(), this::getRegionCountGet, "get"));
            metersCountPut.put(regKey, createMeter(rs, regKey, replicas.getValue(), replicas.getKey(), this::getRegionCountPut, "put"));
            metersCountDel.put(regKey, createMeter(rs, regKey, replicas.getValue(), replicas.getKey(), this::getRegionCountDel, "del"));
        }
    }

    private FunctionCounter createMeter(RegionServerModel rs, String regKey, RegionMetrics regionMetrics, Integer replicaNum, ToDoubleFunction<String> func, String name) {
        return FunctionCounter.builder("region." + name, regKey, func)    // 2 - create a counter using the fluent API
                .tag("name", rs.getName())
                .tag("host", rs.getHost())
                .tag("coreClientPort", String.valueOf(rs.getCoreClientPort()))
                .tag("javaPort", String.valueOf(rs.getJavaPort()))
                .tag("region", regionMetrics.getRegionName())
                .tag("replicaNum", String.valueOf(replicaNum))
                .tag("ns", regionMetrics.getNs())
                .tag("table", regionMetrics.getTable())
                .description("region " + name)
                .register(meterRegistry);
    }

    private double getRegionSize(String regKey) {
        return regionMetrics.getOrDefault(regKey, EMPTY_METRICS).getSize();
    }

    private double getRegionKeys(String regKey) {
        return regionMetrics.getOrDefault(regKey, EMPTY_METRICS).getKeys();
    }

    private double getRegionMem(String regKey) {
        return regionMetrics.getOrDefault(regKey, EMPTY_METRICS).getMemUsageBytes();
    }

    private double getRegionCountGet(String regKey) {
        return regionMetrics.getOrDefault(regKey, EMPTY_METRICS).getCountGet();
    }

    private double getRegionCountPut(String regKey) {
        return regionMetrics.getOrDefault(regKey, EMPTY_METRICS).getCountPut();
    }

    private double getRegionCountDel(String regKey) {
        return regionMetrics.getOrDefault(regKey, EMPTY_METRICS).getCountDel();
    }

}
