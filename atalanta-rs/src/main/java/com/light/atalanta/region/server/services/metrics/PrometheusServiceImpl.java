/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.metrics;

import com.light.atalanta.TableName;
import com.light.atalanta.metrics.RegionMetrics;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.region.server.services.RegionServerInfoSupplier;
import com.light.atalanta.region.server.services.ReplicaMapperProcessor;
import com.light.atalanta.region.server.utils.RsUtils;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import com.light.atalanta.services.ZooKeeperCoreContext;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileStore;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToDoubleFunction;

import static com.light.atalanta.util.BannerPrinter.*;

public class PrometheusServiceImpl implements PrometheusService, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusServiceImpl.class);
    private static final int MAX_METRICS_DEQUE_SIZE = 10;
    private static final int SIZE_WORKERS = 4;
    private static final Set<String> REGION_METERS = Set.of(
            "region.size", "region.keys", "region.mem", "region.get", "region.put", "region.del");
    private final MeterRegistry meterRegistry;
    private final RegionServerInfoSupplier serverInfo;
    private final ZooKeeperCoreContext zooKeeperCoreContext;
    private final ReplicaMapperProcessor replicaMapperProcessor;
    public static final RegionMetrics EMPTY_METRICS = new RegionMetrics();
    private final Map<String, Meter> metersSize = new ConcurrentHashMap<>();
    private final Map<String, Meter> metersKeys = new ConcurrentHashMap<>();
    private final Map<String, Meter> metersMemUsageBytes = new ConcurrentHashMap<>();
    private final Map<String, Meter> metersCountGet = new ConcurrentHashMap<>();
    private final Map<String, Meter> metersCountPut = new ConcurrentHashMap<>();
    private final Map<String, Meter> metersCountDel = new ConcurrentHashMap<>();
    private final Map<TableName, Meter> metersSizeByTable = new ConcurrentHashMap<>();
    private final Map<TableName, Meter> metersKeysByTable = new ConcurrentHashMap<>();
    private final Map<TableName, Meter> metersMemUsageBytesByTable = new ConcurrentHashMap<>();
    private final Map<TableName, Meter> metersCountGetByTable = new ConcurrentHashMap<>();
    private final Map<TableName, Meter> metersCountPutByTable = new ConcurrentHashMap<>();
    private final Map<TableName, Meter> metersCountDelByTable = new ConcurrentHashMap<>();
    private final Map<String, Meter> metersSizeByNs = new ConcurrentHashMap<>();
    private final Map<String, Meter> metersKeysByNs = new ConcurrentHashMap<>();
    private final Map<String, Meter> metersMemUsageBytesByNs = new ConcurrentHashMap<>();
    private final Map<String, Meter> metersCountGetByNs = new ConcurrentHashMap<>();
    private final Map<String, Meter> metersCountPutByNs = new ConcurrentHashMap<>();
    private final Map<String, Meter> metersCountDelByNs = new ConcurrentHashMap<>();
    private volatile MetricsSnapshot currentSnapshot = MetricsSnapshot.empty();
    private final BlockingDeque<List<MetricsSnapshot.Region>> metricsDeque =
            new LinkedBlockingDeque<>(MAX_METRICS_DEQUE_SIZE);
    private final ExecutorService sizeExecutor;
    private final Thread processingThread;
    private final Gauge versionInfo;
    private final RegionServerConfiguration conf;

    public PrometheusServiceImpl(MeterRegistry meterRegistry, RegionServerInfoSupplier serverInfo,
                                 ZooKeeperCoreContext zooKeeperCoreContext,
                                 ReplicaMapperProcessor replicaMapperProcessor, RegionServerConfiguration conf) {
        this.meterRegistry = meterRegistry;
        this.serverInfo = serverInfo;
        this.zooKeeperCoreContext = zooKeeperCoreContext;
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.conf = conf;
        versionInfo = buildVersionInfo(meterRegistry);
        buildDiskInfoMetrics();
        buildRsMetrics();
        AtomicInteger workerNumber = new AtomicInteger();
        sizeExecutor = Executors.newFixedThreadPool(SIZE_WORKERS, task -> {
            Thread thread = new Thread(task, "atalanta-metrics-size-" + workerNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        processingThread = new Thread(this::asyncMetricsProcess, "atalanta-metrics-process");
        processingThread.setDaemon(true);
        processingThread.start();
    }

    private void asyncMetricsProcess() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                processingMetrics();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            sizeExecutor.shutdownNow();
        }
    }

    @Override
    public void close() {
        processingThread.interrupt();
        sizeExecutor.shutdownNow();
    }

    @Override
    public void addRegionMetrics(List<RegionMetrics> regionMetrics) {
        // JMX retains the original RegionMetrics objects. Capture values before queuing,
        // and never mutate the caller's list or its objects in the background worker.
        List<MetricsSnapshot.Region> samples = new ArrayList<>(regionMetrics.size());
        for (RegionMetrics metrics : regionMetrics) {
            if (StringUtils.isNotEmpty(metrics.getRegionName())) {
                samples.add(MetricsSnapshot.Region.copyOf(metrics));
            }
        }
        boolean res = metricsDeque.offer(samples);
        if (!res) {
            LOGGER.warn("The metrics deque is full. Skip metrics");
        }
    }

    private void processingMetrics() throws InterruptedException {
        LOGGER.info("Wait metrics...");
        List<MetricsSnapshot.Region> samples = metricsDeque.take();
        long started = System.nanoTime();
        LOGGER.info("Start metrics processing. Size: {}", samples.size());
        try {
            RegionServerModel rs = serverInfo.getMe("default");
            List<MetricsSnapshot.Region> prepared = prepareRegions(samples);
            long preparedAt = System.nanoTime();
            MetricsSnapshot nextSnapshot = MetricsSnapshot.aggregate(prepared);
            long aggregatedAt = System.nanoTime();
            for (MetricsSnapshot.Region region : nextSnapshot.regions().values()) {
                if (region.table() != null) {
                    initPrometheusMeter(rs, region, region.table());
                }
            }
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Metrics processing stopped");
            }
            // Publish only fully prepared values. Readers retain the previous snapshot
            // while disk reads, aggregation and meter registration are in progress.
            currentSnapshot = nextSnapshot;
            clearMetrics(nextSnapshot.regions().keySet());
            long completedAt = System.nanoTime();
            LOGGER.info("Complete metrics processing. Size: {}; prepare={} ms; aggregate={} ms; "
                            + "publish={} ms; total={} ms; queued={}", samples.size(),
                    TimeUnit.NANOSECONDS.toMillis(preparedAt - started),
                    TimeUnit.NANOSECONDS.toMillis(aggregatedAt - preparedAt),
                    TimeUnit.NANOSECONDS.toMillis(completedAt - aggregatedAt),
                    TimeUnit.NANOSECONDS.toMillis(completedAt - started), metricsDeque.size());
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            LOGGER.warn("Cannot add region metrics; keeping the previous snapshot. Size: " + samples.size(), ex);
        }
    }

    private List<MetricsSnapshot.Region> prepareRegions(List<MetricsSnapshot.Region> samples)
            throws InterruptedException, ExecutionException {
        int workers = Math.min(SIZE_WORKERS, samples.size());
        List<Callable<List<MetricsSnapshot.Region>>> tasks = new ArrayList<>(workers);
        for (int worker = 0; worker < workers; worker++) {
            final int from = (int) ((long) samples.size() * worker / workers);
            final int to = (int) ((long) samples.size() * (worker + 1) / workers);
            tasks.add(() -> {
                List<MetricsSnapshot.Region> prepared = new ArrayList<>(to - from);
                for (int index = from; index < to; index++) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException("Region size collection stopped");
                    }
                    MetricsSnapshot.Region sample = samples.get(index);
                    TableName table = replicaMapperProcessor.getReplicaTableName(sample.name());
                    MetricsSnapshot.Values values = sample.values();
                    if (table != null) {
                        values = values.withSize(replicaMapperProcessor.getSize(sample.name()));
                    }
                    prepared.add(new MetricsSnapshot.Region(sample.name(), sample.replicaNum(), table, values));
                }
                return prepared;
            });
        }
        // At most SIZE_WORKERS tasks/futures, regardless of the number of regions.
        // invokeAll waits for all workers, including when one of them fails.
        List<Future<List<MetricsSnapshot.Region>>> results = sizeExecutor.invokeAll(tasks);
        List<MetricsSnapshot.Region> prepared = new ArrayList<>(samples.size());
        for (Future<List<MetricsSnapshot.Region>> result : results) {
            prepared.addAll(result.get());
        }
        return prepared;
    }

    MetricsSnapshot snapshot() {
        return currentSnapshot;
    }

    private void clearMetrics(Set<String> regionNames) {
        Set<String> aliveRegions = new HashSet<>(regionNames);
        // Inspect the registry itself: older cleanup may have already discarded
        // tracking-map entries while leaving their meters registered
        for (Meter meter : meterRegistry.getMeters()) {
            Meter.Id id = meter.getId();
            String region = id.getTag("region");
            if (REGION_METERS.contains(id.getName()) && region != null && !aliveRegions.contains(region)) {
                meterRegistry.remove(meter);
            }
        }
        metersSize.keySet().retainAll(aliveRegions);
        metersKeys.keySet().retainAll(aliveRegions);
        metersMemUsageBytes.keySet().retainAll(aliveRegions);
        metersCountGet.keySet().retainAll(aliveRegions);
        metersCountPut.keySet().retainAll(aliveRegions);
        metersCountDel.keySet().retainAll(aliveRegions);
    }

    private void initPrometheusMeter(RegionServerModel rs, MetricsSnapshot.Region rm,
                                     TableName tn) {
        try {
            final String regionName = rm.name();
            if (metersSize.get(regionName) == null) {
                metersSize.put(regionName, createMeter(rs, regionName, rm, this::getRegionSize, "size", tn));
                metersKeys.put(regionName, createMeter(rs, regionName, rm, this::getRegionKeys, "keys", tn));
                metersMemUsageBytes.put(regionName, createMeter(rs, regionName, rm, this::getRegionMem, "mem", tn));
                metersCountGet.put(regionName, createMeter(rs, regionName, rm, this::getRegionCountGet, "get", tn));
                metersCountPut.put(regionName, createMeter(rs, regionName, rm, this::getRegionCountPut, "put", tn));
                metersCountDel.put(regionName, createMeter(rs, regionName, rm, this::getRegionCountDel, "del", tn));
            }
            if (metersSizeByTable.get(tn) == null && conf.getDataConfiguration().isTableMetricsEnabled()) {
                metersSizeByTable.put(tn, createMeterByTable(rs, tn, this::getTableSize, "size"));
                metersKeysByTable.put(tn, createMeterByTable(rs, tn, this::getTableKeys, "keys"));
                metersMemUsageBytesByTable.put(tn, createMeterByTable(rs, tn, this::getTableMem, "mem"));
                metersCountGetByTable.put(tn, createMeterByTable(rs, tn, this::getTableCountGet, "get"));
                metersCountPutByTable.put(tn, createMeterByTable(rs, tn, this::getTableCountPut, "put"));
                metersCountDelByTable.put(tn, createMeterByTable(rs, tn, this::getTableCountDel, "del"));
            }
            String ns = tn.getNamespace();
            if (metersSizeByNs.get(ns) == null) {
                metersSizeByNs.put(ns, createMeterByNs(rs, ns, this::getNsSize, "size"));
                metersKeysByNs.put(ns, createMeterByNs(rs, ns, this::getNsKeys, "keys"));
                metersMemUsageBytesByNs.put(ns, createMeterByNs(rs, ns, this::getNsMem, "mem"));
                metersCountGetByNs.put(ns, createMeterByNs(rs, ns, this::getNsCountGet, "get"));
                metersCountPutByNs.put(ns, createMeterByNs(rs, ns, this::getNsCountPut, "put"));
                metersCountDelByNs.put(ns, createMeterByNs(rs, ns, this::getNsCountDel, "del"));
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private FunctionCounter createMeter(RegionServerModel rs, String regionName,
                                        MetricsSnapshot.Region regionMetrics, ToDoubleFunction<String> func, String name,
                                        TableName tn) throws IOException {
        return FunctionCounter.builder("region." + name, regionName, func)    // 2 - create a counter using the fluent API
                .tag("name", rs.getName())
                .tag("host", rs.getHost())
                .tag("coreClientPort", String.valueOf(rs.getCoreClientPort()))
                .tag("javaPort", String.valueOf(rs.getJavaPort()))
                .tag("region", regionMetrics.name())
                .tag("replicaNum", String.valueOf(regionMetrics.replicaNum()))
                .tag("ns", tn.getNamespace())
                .tag("table", tn.getTableName())
                .description("region " + name)
                .register(meterRegistry);
    }

    private FunctionCounter createMeterByTable(RegionServerModel rs, TableName tn, ToDoubleFunction<TableName> func, String name) throws IOException {
        return FunctionCounter.builder("table." + name, tn, func)    // 2 - create a counter using the fluent API
                .tag("name", rs.getName())
                .tag("host", rs.getHost())
                .tag("coreClientPort", String.valueOf(rs.getCoreClientPort()))
                .tag("javaPort", String.valueOf(rs.getJavaPort()))
                .tag("ns", tn.getNamespace())
                .tag("table", tn.getTableName())
                .description("table " + name)
                .register(meterRegistry);
    }

    private FunctionCounter createMeterByNs(RegionServerModel rs, String ns, ToDoubleFunction<String> func, String name) throws IOException {
        return FunctionCounter.builder("ns." + name, ns, func)    // 2 - create a counter using the fluent API
                .tag("name", rs.getName())
                .tag("host", rs.getHost())
                .tag("coreClientPort", String.valueOf(rs.getCoreClientPort()))
                .tag("javaPort", String.valueOf(rs.getJavaPort()))
                .tag("ns", ns)
                .description("table " + name)
                .register(meterRegistry);
    }


    private double getRegionSize(String regKey) {
        return Math.max(currentSnapshot.region(regKey).size(), 0);
    }

    private double getTableSize(TableName tableName) {
        return Math.max(currentSnapshot.table(tableName).size(), 0);
    }

    private double getNsSize(String ns) {
        return Math.max(currentSnapshot.namespace(ns).size(), 0);
    }

    private double getRegionKeys(String regKey) {
        return Math.max(currentSnapshot.region(regKey).keys(), 0);
    }

    private double getTableKeys(TableName tableName) {
        return Math.max(currentSnapshot.table(tableName).keys(), 0);
    }

    private double getNsKeys(String ns) {
        return Math.max(currentSnapshot.namespace(ns).keys(), 0);
    }

    private double getRegionMem(String regKey) {
        return Math.max(currentSnapshot.region(regKey).memUsageBytes(), 0);
    }

    private double getTableMem(TableName tableName) {
        return Math.max(currentSnapshot.table(tableName).memUsageBytes(), 0);
    }

    private double getNsMem(String ns) {
        return Math.max(currentSnapshot.namespace(ns).memUsageBytes(), 0);
    }

    private double getRegionCountGet(String regKey) {
        return Math.max(currentSnapshot.region(regKey).countGet(), 0);
    }

    private double getTableCountGet(TableName tableName) {
        return Math.max(currentSnapshot.table(tableName).countGet(), 0);
    }

    private double getNsCountGet(String ns) {
        return Math.max(currentSnapshot.namespace(ns).countGet(), 0);
    }

    private double getRegionCountPut(String regKey) {
        return Math.max(currentSnapshot.region(regKey).countPut(), 0);
    }

    private double getTableCountPut(TableName tableName) {
        return Math.max(currentSnapshot.table(tableName).countPut(), 0);
    }

    private double getNsCountPut(String ns) {
        return Math.max(currentSnapshot.namespace(ns).countPut(), 0);
    }

    private double getRegionCountDel(String regKey) {
        return Math.max(currentSnapshot.region(regKey).countDel(), 0);
    }

    private double getTableCountDel(TableName tableName) {
        return Math.max(currentSnapshot.table(tableName).countDel(), 0);
    }

    private double getNsCountDel(String ns) {
        return Math.max(currentSnapshot.namespace(ns).countDel(), 0);
    }

    private static Gauge buildVersionInfo(MeterRegistry meterRegistry) {
        long v = 0L;
        try {
            v = Long.parseLong(getBuild().replaceAll("[^0-9]", ""));
        } catch (NumberFormatException ex) {
            //ignore
        }
        final long val = v;
        return Gauge.builder("rs_version", () -> val)
                .tag("build", getBuild())
                .tag("date", getBuildDate())
                .tag("commit", getCommit())
                .register(meterRegistry);
    }

    private void buildDiskInfoMetrics() {
        try {
            Set<FileStore> fileStores = RsUtils.getStorages(conf.getRsDirs());
            RegionServerModel rs = serverInfo.getMe("default");
            for (FileStore fileStore : fileStores) {
                Gauge.builder("rs_disk_usable", () -> {
                            try {
                                return fileStore.getUsableSpace();
                            } catch (IOException e) {
                                LOGGER.error("cannot add disk_usable  metrics", e);
                                return 0;
                            }
                        })
                        .tag("name", rs.getName())
                        .tag("host", rs.getHost())
                        .tag("disk", fileStore.name())
                        .register(meterRegistry);

                Gauge.builder("rs_disk_total_size", () -> {
                            try {
                                return fileStore.getTotalSpace();
                            } catch (IOException e) {
                                LOGGER.error("cannot add disk_usable  metrics", e);
                                return 0;
                            }
                        })
                        .tag("name", rs.getName())
                        .tag("host", rs.getHost())
                        .tag("disk", fileStore.name())
                        .register(meterRegistry);
            }
        } catch (Exception ex) {
            LOGGER.error("cannot add disk metrics", ex);
        }

    }

    private void buildRsMetrics() {
        try {
            RegionServerModel rs = serverInfo.getMe("default");
            createMeterForRs(rs, (s) -> Math.max(currentSnapshot.server().size(), 0), "size");
            createMeterForRs(rs, (s) -> Math.max(currentSnapshot.server().keys(), 0), "keys");
            createMeterForRs(rs, (s) -> Math.max(currentSnapshot.server().memUsageBytes(), 0), "mem");
            createMeterForRs(rs, (s) -> Math.max(currentSnapshot.server().countGet(), 0), "get");
            createMeterForRs(rs, (s) -> Math.max(currentSnapshot.server().countPut(), 0), "put");
            createMeterForRs(rs, (s) -> Math.max(currentSnapshot.server().countDel(), 0), "del");
        } catch (Exception e) {
            LOGGER.error("Cannot add RS aggregate metrics", e);
        }
    }

    private FunctionCounter createMeterForRs(RegionServerModel rs, ToDoubleFunction<String> func, String name) throws IOException {
        return FunctionCounter.builder("rs_aggregate", rs.getName(), func)    // 2 - create a counter using the fluent API
                .tag("name", rs.getName())
                .tag("host", rs.getHost())
                .tag("coreClientPort", String.valueOf(rs.getCoreClientPort()))
                .tag("javaPort", String.valueOf(rs.getJavaPort()))
                .description("table " + name)
                .register(meterRegistry);
    }


}
