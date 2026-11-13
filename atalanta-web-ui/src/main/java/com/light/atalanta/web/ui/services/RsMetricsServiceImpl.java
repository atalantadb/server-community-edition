/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.services;

import com.google.protobuf.Empty;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.grpc.RegionServerServiceGrpc;
import com.light.atalanta.metrics.RegionMetrics;
import com.light.atalanta.metrics.RegionServerDiskInfo;
import com.light.atalanta.metrics.RegionServerMetrics;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.web.ui.model.RegionServerModelExt;
import com.light.atalanta.web.ui.model.VersionInfoExt;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.internal.DnsNameResolverProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import prometheus.types.Counter;
import prometheus.types.Gauge;
import prometheus.types.Metric;
import prometheus.types.MetricFamily;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.light.atalanta.web.ui.WebUiUtils.parseMetrics;
import static com.light.atalanta.web.ui.services.PrometheusService.EMPTY_METRICS;


@Service
public class RsMetricsServiceImpl implements RsMetricsService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RsMetricsServiceImpl.class.getName());
    @Value("${ping.rs.delay:5000}")
    private long pingRsDelay;
    @Value("${metrics.grpc.max-inbound-message-size:33554432}")
    private int maxMetricsMessageSize = 32 * 1024 * 1024;
    @Value("${metrics.grpc.request-timeout-ms:10000}")
    private long metricsRequestTimeoutMs = 10_000;
    private static final RegionServerMetrics EMPTY_RS_METRICS = new RegionServerMetrics();
    public static final Map<Integer, RegionMetrics> EMPTY_MAP_REGION_METRICS = new ConcurrentHashMap<>();
    private Map<RegionServerModel, RegionServerMetrics> regionServerMetrics = new ConcurrentHashMap<>();
    private final ScheduledExecutorService progressExecutor = Executors.newSingleThreadScheduledExecutor();
    private ZookeeperDaoService zookeeperDaoService;
    private RsMetrics rsCurMetrics = new RsMetrics();

    @Autowired
    public void setZookeeperDaoService(@Lazy ZookeeperDaoService zookeeperDaoService) {
        this.zookeeperDaoService = zookeeperDaoService;
    }

    public RsMetricsServiceImpl() {

    }

    @PostConstruct
    public void startPingRsThread() {
        if (maxMetricsMessageSize <= 0 || metricsRequestTimeoutMs <= 0) {
            throw new IllegalArgumentException("Metrics gRPC message size and request timeout must be positive");
        }
        progressExecutor.scheduleWithFixedDelay(this::pingRs, 0, pingRsDelay, TimeUnit.MILLISECONDS);
    }

    @Override
    public long getRegionCount() {
        return rsCurMetrics.regionCount;
    }

    @Override
    public long getRegionCountFollower1() {
        return rsCurMetrics.regionCountFollower1;
    }

    @Override
    public long getRegionCountFollower2() {
        return rsCurMetrics.regionCountFollower2;
    }

    @Override
    public long getSizeTotal() {
        return rsCurMetrics.sizeTotal;
    }

    @Override
    public long getKeysTotal() {
        return rsCurMetrics.keysTotal;
    }

    @Override
    public long getMemUsageBytesTotal() {
        return rsCurMetrics.memUsageBytesTotal;
    }

    @Override
    public long getCountGetTotal() {
        return rsCurMetrics.countGetTotal;
    }

    @Override
    public long getCountPutTotal() {
        return rsCurMetrics.countPutTotal;
    }

    @Override
    public long getCountDelTotal() {
        return rsCurMetrics.countDelTotal;
    }

    private void pingRs() {
        try {
            List<RegionServerModelExt> rsList = zookeeperDaoService.getRegionServerModelExts();
            rsList.parallelStream().forEach(rs -> updateRsStat(rs));
            updateAggregates();
        } catch (Exception e) {
            LOGGER.warn("cannot get RS stat. {}", e.getMessage());
        }
    }

    private void updateAggregates() {
        List<RegionServerModelExt> allRegionServers = zookeeperDaoService.getAllRegionServers();
        rsCurMetrics.regionCount = calcTotalRegionCount(allRegionServers);
        rsCurMetrics.regionCountFollower1 = calcRegionCountFollower1(allRegionServers);
        rsCurMetrics.regionCountFollower2 = calcRegionCountFollower2(allRegionServers);
        rsCurMetrics.sizeTotal = calcSizeTotal(allRegionServers);
        rsCurMetrics.keysTotal = calcKeysTotal(allRegionServers);
        rsCurMetrics.memUsageBytesTotal = calcMemUsageBytesTotal(allRegionServers);
        rsCurMetrics.countGetTotal = calcCountGetTotal(allRegionServers);
        rsCurMetrics.countPutTotal = calcCountPutTotal(allRegionServers);
        rsCurMetrics.countDelTotal = calcCountDelTotal(allRegionServers);
    }

    private long calcTotalRegionCount(List<RegionServerModelExt> allRegionServers) {
        return allRegionServers.parallelStream()
                .map(RegionServerModel::getRegionServerMetrics)
                .mapToLong(RegionServerMetrics::getRegionCount).sum();
    }

    private long calcRegionCountFollower1(List<RegionServerModelExt> allRegionServers) {
        return allRegionServers.parallelStream()
                .map(RegionServerModel::getRegionServerMetrics)
                .mapToLong(RegionServerMetrics::getRegionCountFollower1).sum();
    }

    private long calcRegionCountFollower2(List<RegionServerModelExt> allRegionServers) {
        return allRegionServers.parallelStream()
                .map(RegionServerModel::getRegionServerMetrics)
                .mapToLong(RegionServerMetrics::getRegionCountFollower2).sum();
    }

    private long calcSizeTotal(List<RegionServerModelExt> allRegionServers) {
        return allRegionServers.parallelStream()
                .map(RegionServerModel::getRegionServerMetrics)
                .mapToLong(RegionServerMetrics::getSizeTotal).sum();
    }

    private long calcKeysTotal(List<RegionServerModelExt> allRegionServers) {
        return allRegionServers.parallelStream()
                .map(RegionServerModel::getRegionServerMetrics)
                .mapToLong(RegionServerMetrics::getKeysTotal).sum();
    }

    private long calcMemUsageBytesTotal(List<RegionServerModelExt> allRegionServers) {
        return allRegionServers.parallelStream()
                .map(RegionServerModel::getRegionServerMetrics)
                .mapToLong(RegionServerMetrics::getMemUsageBytesTotal).sum();
    }

    private long calcCountGetTotal(List<RegionServerModelExt> allRegionServers) {
        return allRegionServers.parallelStream()
                .map(RegionServerModel::getRegionServerMetrics)
                .mapToLong(RegionServerMetrics::getCountGetTotal).sum();
    }

    private long calcCountPutTotal(List<RegionServerModelExt> allRegionServers) {
        return allRegionServers.parallelStream()
                .map(RegionServerModel::getRegionServerMetrics)
                .mapToLong(RegionServerMetrics::getCountPutTotal).sum();
    }

    private long calcCountDelTotal(List<RegionServerModelExt> allRegionServers) {
        return allRegionServers.parallelStream()
                .map(RegionServerModel::getRegionServerMetrics)
                .mapToLong(RegionServerMetrics::getCountDelTotal).sum();
    }

    void updateRsStat(RegionServerModelExt rsExt) {
        RegionServerModel rs = new RegionServerModel(rsExt.getName(), rsExt.getHost(), rsExt.getJavaPort(),
                rsExt.getCoreClientPort(), rsExt.getJmxPort());
        ManagedChannel channel = null;
        try {
            if (!rsExt.getRsStatus().equals("RUNNING")) {
                regionServerMetrics.put(rsExt, EMPTY_RS_METRICS);
                return;
            }
            channel = createMetricsChannel(rs);
            RegionServerServiceGrpc.RegionServerServiceBlockingStub stub = RegionServerServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(metricsRequestTimeoutMs, TimeUnit.MILLISECONDS);
            RegionServerProto.RsMetrics grpcRes = stub.getMetrics(Empty.getDefaultInstance());
            LOGGER.trace("metrics res: {}", grpcRes);
            Map<String, MetricFamily> rsMetricsMap = parseMetrics(grpcRes.getScrape())
                    .parallelStream()
                    .collect(Collectors.toMap(m -> m.getName(), m -> m));
            addRsMetrics(rs, rsMetricsMap);
        } catch (Exception ex) {
            LOGGER.error("metrics error!", ex);
            regionServerMetrics.remove(rs);
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }

    ManagedChannel createMetricsChannel(RegionServerModel rs) {
        return ManagedChannelBuilder.forAddress(rs.getHost(), rs.getJavaPort())
                .usePlaintext()
                .nameResolverFactory(new DnsNameResolverProvider())
                .maxInboundMessageSize(maxMetricsMessageSize)
                .build();
    }

    private void addRsMetrics(RegionServerModel rs, Map<String, MetricFamily> rsMetricsMap) {
        RegionServerMetrics metrics = new RegionServerMetrics();
        metrics.setSizeTotal(calcTotal(rsMetricsMap, "size"));
        metrics.setKeysTotal(calcTotal(rsMetricsMap, "keys"));
        metrics.setMemUsageBytesTotal(calcTotal(rsMetricsMap, "mem"));
        metrics.setCountGetTotal(calcTotal(rsMetricsMap, "get"));
        metrics.setCountPutTotal(calcTotal(rsMetricsMap, "put"));
        metrics.setCountDelTotal(calcTotal(rsMetricsMap, "del"));
        metrics.setRegionCount(calcRegionCount(rsMetricsMap));
        metrics.setRegionCountFollower1(calcRegionCountFollower(rsMetricsMap, "1"));
        metrics.setRegionCountFollower2(calcRegionCountFollower(rsMetricsMap, "2"));
        metrics.getRsDisks().putAll(calcRsDisk(rsMetricsMap));
        metrics.getRegionsMetrics().putAll(calcRegionMetrics(rsMetricsMap));
        MetricFamily ver = rsMetricsMap.get("rs_version");
        metrics.setVersionInfo(new VersionInfoExt(ver));
        regionServerMetrics.put(rs, metrics);
    }

    private Map<String, Map<Integer, RegionMetrics>> calcRegionMetrics(Map<String, MetricFamily> rsMetricsMap) {
        MetricFamily metricKeys = rsMetricsMap.get("region_keys_total");
        MetricFamily metricSize = rsMetricsMap.get("region_size_total");
        MetricFamily metricMem = rsMetricsMap.get("region_mem_total");
        MetricFamily metricGet = rsMetricsMap.get("region_get_total");
        MetricFamily metricPut = rsMetricsMap.get("region_put_total");
        MetricFamily metricDel = rsMetricsMap.get("region_del_total");

        final Map<String, Map<Integer, RegionMetrics>> res = new ConcurrentHashMap<>();

        if (metricKeys != null) {
            metricKeys.getMetrics().parallelStream().filter(m -> m instanceof Counter)
                    .forEach(m -> {
                        RegionMetrics rm = getRegionMetrics(res, m);
                        rm.setKeys((long) ((Counter) m).getValue());
                    });
        }
        if (metricSize != null) {
            metricSize.getMetrics().parallelStream().filter(m -> m instanceof Counter)
                    .forEach(m -> {
                        RegionMetrics rm = getRegionMetrics(res, m);
                        rm.setSize((long) ((Counter) m).getValue());
                    });
        }
        if (metricMem != null) {
            metricMem.getMetrics().parallelStream().filter(m -> m instanceof Counter)
                    .forEach(m -> {
                        RegionMetrics rm = getRegionMetrics(res, m);
                        rm.setMemUsageBytes((long) ((Counter) m).getValue());
                    });
        }
        if (metricGet != null) {
            metricGet.getMetrics().parallelStream().filter(m -> m instanceof Counter)
                    .forEach(m -> {
                        RegionMetrics rm = getRegionMetrics(res, m);
                        rm.setCountGet((long) ((Counter) m).getValue());
                    });
        }
        if (metricPut != null) {
            metricPut.getMetrics().parallelStream()
                    .filter(m -> m instanceof Counter)
                    .forEach(m -> {
                        RegionMetrics rm = getRegionMetrics(res, m);
                        rm.setCountPut((long) ((Counter) m).getValue());
                    });
        }
        if (metricDel != null) {
            metricDel.getMetrics().parallelStream()
                    .filter(m -> m instanceof Counter)
                    .forEach(m -> {
                        RegionMetrics rm = getRegionMetrics(res, m);
                        rm.setCountDel((long) ((Counter) m).getValue());
                    });
        }

        return res;
    }

    private RegionMetrics getRegionMetrics(Map<String, Map<Integer, RegionMetrics>> res, Metric m) {
        Map<Integer, RegionMetrics> metricsMap = res.computeIfAbsent(getRegionName(m), k -> new ConcurrentHashMap<>());
        RegionMetrics rm = metricsMap.computeIfAbsent(getReplicaNum(m), s -> new RegionMetrics());
        return rm;
    }

    private Integer getReplicaNum(Metric m) {
        return Integer.valueOf(m.getLabels().getOrDefault("replicaNum", "-1"));
    }

    private static String getRegionName(Metric m) {
        return m.getLabels().get("region").split("_")[0];
    }

    private Map<String, RegionServerDiskInfo> calcRsDisk(Map<String, MetricFamily> rsMetricsMap) {
        MetricFamily diskTotalMetric = rsMetricsMap.get("rs_disk_total_size");
        MetricFamily diskUsableMetric = rsMetricsMap.get("rs_disk_usable");
        if (diskTotalMetric == null || diskUsableMetric == null) {
            return new ConcurrentHashMap<>();
        }
        Map<String, RegionServerDiskInfo> res = diskTotalMetric.getMetrics().parallelStream()
                .filter(m -> m instanceof Gauge)
                .collect(Collectors.toMap(m -> m.getLabels().get("disk"),
                        m -> toRegionServerDiskInfo((Gauge) m)));
        diskUsableMetric.getMetrics().parallelStream()
                .filter(m -> m instanceof Gauge)
                .forEach(m -> addDiskUsableInfo((Gauge) m, res));
        return res;
    }

    private void addDiskUsableInfo(Gauge m, Map<String, RegionServerDiskInfo> res) {
        RegionServerDiskInfo diskInfo = res.get(m.getLabels().get("disk"));
        if (diskInfo != null) {
            diskInfo.setUsable((long) m.getValue());
        }
    }

    private RegionServerDiskInfo toRegionServerDiskInfo(Gauge m) {
        RegionServerDiskInfo res = new RegionServerDiskInfo();
        res.setTotalSize((long) m.getValue());
        return res;
    }

    private long calcRegionCountFollower(Map<String, MetricFamily> rsMetricsMap, String replicaNum) {
        MetricFamily metricFamily = rsMetricsMap.get("region_keys_total");
        if (metricFamily == null) {
            return 0;
        }
        return metricFamily.getMetrics().parallelStream()
                .filter(m -> m.getLabels().getOrDefault("replicaNum", "").equals(replicaNum))
                .count();
    }

    private long calcRegionCount(Map<String, MetricFamily> rsMetricsMap) {
        MetricFamily metricFamily = rsMetricsMap.get("region_keys_total");
        if (metricFamily == null) {
            return 0;
        }
        return metricFamily.getMetrics().size();
    }

    private long calcTotal(Map<String, MetricFamily> rsMetricsMap, String keys) {
        MetricFamily metricFamily = rsMetricsMap.get("region_" + keys + "_total");
        if (metricFamily == null) {
            return 0;
        }
        return metricFamily.getMetrics().parallelStream()
                .filter(m -> m instanceof Counter)
                .mapToLong(m -> (long) ((Counter) m).getValue())
                .sum();
    }

    @Override
    public RegionServerMetrics getRsMetrics(RegionServerModel r) {
        RegionServerModel rs = (r instanceof RegionServerModelExt) ? new RegionServerModel(r.getName(), r.getHost(), r.getJavaPort(),
                r.getCoreClientPort(), r.getJmxPort()) : r;
        return regionServerMetrics.getOrDefault(rs, EMPTY_RS_METRICS);
    }

    @Override
    public RegionMetrics getRsMetrics(RegionServerModel rs, String name, int replicaNum) {
        RegionServerMetrics rsm = getRsMetrics(rs);
        return rsm.getRegionsMetrics()
                .getOrDefault(name, EMPTY_MAP_REGION_METRICS)
                .getOrDefault(replicaNum, EMPTY_METRICS);

    }

    private static class RsMetrics {
        private long regionCount;
        private long regionCountFollower1;
        private long regionCountFollower2;
        private long sizeTotal;
        private long keysTotal;
        private long memUsageBytesTotal;
        private long countGetTotal;
        private long countPutTotal;
        private long countDelTotal;
    }

}
