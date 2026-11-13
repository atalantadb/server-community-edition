/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.metrics;

import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.services.ZookeeperHelperExtended;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.light.atalanta.util.BannerPrinter.*;

public class MasetrPrometheusServiceImpl implements MasterPrometheusService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasetrPrometheusServiceImpl.class);
    public static final int RS_MONITORING_DELAY = 10;
    private final MeterRegistry meterRegistry;
    private final ZookeeperHelperExtended zookeeperHelperExtended;
    private final ZooKeeperCoreContext zooContext;

    private final Gauge versionInfo;
    private final Map<String, Long> rsStatusMap = new ConcurrentHashMap<>();
    private final Map<String, Gauge> rsGaugeMap = new ConcurrentHashMap<>();
    Map<String, Long> statusMap = Map.of("STOPPED", 0L,
            "INITIALIZING", 1L,
            "RUNNING", 2L,
            "DISABLING", 3L,
            "DISABLED", 4L);

    public MasetrPrometheusServiceImpl(MeterRegistry meterRegistry, ZookeeperHelperExtended zookeeperHelperExtended, ZooKeeperCoreContext zooContext) {
        this.meterRegistry = meterRegistry;
        versionInfo = buildVersionInfo(meterRegistry);
        this.zookeeperHelperExtended = zookeeperHelperExtended;
        this.zooContext = zooContext;
        ScheduledExecutorService checkService = Executors.newSingleThreadScheduledExecutor();
        checkService.scheduleWithFixedDelay(this::rsStatusMonitoring, 0, RS_MONITORING_DELAY, TimeUnit.SECONDS);
    }

    private void rsStatusMonitoring() {
        try {
            for (String rsId : zookeeperHelperExtended.getAllRegionServerInfoIds()) {
                RegionServerModel regionServerModel = zooContext.getRegionServerModel(rsId);
                setRsStatusMetrics(regionServerModel, getRegionServerStatus(rsId));
            }
        } catch (Exception e) {
            LOGGER.error("Cannot collect rs status metrics", e);
        }
    }

    private String getRegionServerStatus(String rsId)  {
        try {
            return zookeeperHelperExtended.getRegionServerStatus(rsId);
        } catch (Exception e) {
            return "STOPPED";
        }
    }

    private void setRsStatusMetrics(RegionServerModel rs, String regionServerStatus) {
        rsStatusMap.put(rs.getName(), getRsStatusId(regionServerStatus));
        rsGaugeMap.computeIfAbsent(rs.getName(), (k) ->
                Gauge.builder("rsInfo", () -> rsStatusMap.getOrDefault(rs.getName(), 0L))
                        .tag("rsId", rs.getName())
                        .tag("host", rs.getHost())
                        .tag("javaPort", String.valueOf(rs.getJavaPort()))
                        .tag("corePort", String.valueOf(rs.getCoreClientPort()))
                        .register(meterRegistry));
    }

    private Long getRsStatusId(String regionServerStatus) {
        return statusMap.getOrDefault(regionServerStatus, 5L);
    }

    private static Gauge buildVersionInfo(MeterRegistry meterRegistry) {
        Long v = 0L;
        try {
            v = Long.valueOf(getBuild().replaceAll("[^0-9]", ""));
        } catch (NumberFormatException ex) {
            //ignore
        }
        final Long val = v;
        return Gauge.builder("master_version", () -> val)
                .tag("build", getBuild())
                .tag("date", getBuildDate())
                .tag("commit", getCommit())
                .register(meterRegistry);
    }

}
