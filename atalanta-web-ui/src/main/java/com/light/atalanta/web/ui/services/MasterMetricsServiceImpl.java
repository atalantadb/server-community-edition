/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.services;

import com.google.protobuf.Empty;
import com.light.atalanta.grpc.MasterMonitoringGrpc;
import com.light.atalanta.grpc.MasterProto;
import com.light.atalanta.grpc.MasterServiceGrpc;
import com.light.atalanta.metrics.VersionInfo;
import com.light.atalanta.web.ui.WebUiUtils;
import com.light.atalanta.web.ui.model.VersionInfoExt;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.internal.DnsNameResolverProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import prometheus.types.MetricFamily;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class MasterMetricsServiceImpl implements MasterMetricsService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterMetricsServiceImpl.class.getName());
    private final ZookeeperDaoService zookeeperDaoService;
    private MasterProto.PingResponse pingResponse = null;
    private MasterProto.InfoResponse infoResponse = null;
    private final ScheduledExecutorService progressExecutor = Executors.newSingleThreadScheduledExecutor();
    private MasterProto.RecoveryMonitoringInfo recoveryMonitoringInfo = null;
    private MasterProto.MasterStartMonitoringInfo masterStartMonitoringInfo;
    private Map<String, MetricFamily> metrics;
    private VersionInfo versionInfo = new VersionInfo();
    @Value("${ping.master.delay:5000}")
    private long pingMasterDelay;

    public MasterMetricsServiceImpl(ZookeeperDaoService zookeeperDaoService) {
        this.zookeeperDaoService = zookeeperDaoService;
    }
    @PostConstruct
    public void startPingMasterThread() {
        progressExecutor.scheduleWithFixedDelay(this::pingLeaderMaster, 0, pingMasterDelay, TimeUnit.MILLISECONDS);
    }

    private void pingLeaderMaster() {
        ManagedChannel chMasterApi = null;
        ManagedChannel chMasterInfo = null;
        try {
            String masterUrl = zookeeperDaoService.getMasterUrl();
            LOGGER.debug("Try get info from master: {}", masterUrl);
            chMasterApi = getMasterGrpcChannelApi(masterUrl);
            chMasterInfo = getMasterGrpcChannelInfo(masterUrl);
            pingResponse = pingFMaster(chMasterApi);
            infoResponse = infoFMaster(chMasterApi);
            metrics = metricsFMaster(chMasterInfo);
            updateVersionInfo();
            recoveryMonitoringInfo = recoveryInfoFMaster(chMasterInfo);
            masterStartMonitoringInfo = masterStartFMaster(chMasterInfo);
        } catch (Exception e) {
            LOGGER.debug("No master url!");
        } finally {
            if(chMasterApi != null) {
                chMasterApi.shutdownNow();
            }
            if(chMasterInfo != null) {
                chMasterInfo.shutdownNow();
            }
        }
    }

    @Override
    public MasterProto.PingResponse getMasterLeaderPingResponse() {
        return pingResponse;
    }

    @Override
    public MasterProto.InfoResponse info() {
        return infoResponse;
    }

    @Override
    public MasterProto.RecoveryMonitoringInfo recoveryInfo() {
        return recoveryMonitoringInfo;
    }

    @Override
    public MasterProto.MasterStartMonitoringInfo getMasterStartInfo() {
        return masterStartMonitoringInfo;
    }

    @Override
    public VersionInfo getMasterVersionInfo() {
        return versionInfo;
    }


    private MasterProto.PingResponse pingFMaster(ManagedChannel channel) {

        try {
            MasterServiceGrpc.MasterServiceBlockingStub stub = MasterServiceGrpc.newBlockingStub(channel);
            MasterProto.PingResponse grpcRes = stub.ping(Empty.getDefaultInstance());
            LOGGER.trace("Ping res: {}", grpcRes);
            return grpcRes;
        } catch (Exception ex) {
            LOGGER.error("ping error!", ex);
            return null;
        }
    }

    private static ManagedChannel getMasterGrpcChannelApi(String masterUrl) {
        String[] split = masterUrl.split(":");
        String host = split[0];
        int port = Integer.parseInt(split[1]);
        return ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .nameResolverFactory(new DnsNameResolverProvider())
                .build();
    }

    private static ManagedChannel getMasterGrpcChannelInfo(String masterUrl) {
        String[] split = masterUrl.split(":");
        String host = split[0];
        int port = Integer.parseInt(split[1]) + 1;
        return ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .nameResolverFactory(new DnsNameResolverProvider())
                .build();
    }

    private MasterProto.InfoResponse infoFMaster(ManagedChannel channel) {
        try {
            MasterServiceGrpc.MasterServiceBlockingStub stub = MasterServiceGrpc.newBlockingStub(channel);
            MasterProto.InfoResponse grpcRes = stub.info(Empty.getDefaultInstance());
            LOGGER.trace("info res: {}", grpcRes);
            return grpcRes;
        } catch (Exception ex) {
            LOGGER.error("info error!", ex);
            return null;
        }
    }

    private Map<String, MetricFamily> metricsFMaster(ManagedChannel channel) {
        try {
            MasterMonitoringGrpc.MasterMonitoringBlockingStub stub = MasterMonitoringGrpc.newBlockingStub(channel);
            MasterProto.MasterMetrics grpcRes = stub.getMetrics(Empty.getDefaultInstance());
            LOGGER.trace("metrics res: {}", grpcRes);
            return WebUiUtils.parseMetrics(grpcRes.getScrape())
                    .stream()
                    .collect(Collectors.toMap(MetricFamily::getName, m -> m));
        } catch (Exception ex) {
            LOGGER.error("metrics error!", ex);
            return new ConcurrentHashMap<>();
        }
    }

    private void updateVersionInfo() {
        MetricFamily ver = metrics.get("master_version");
        versionInfo = new VersionInfoExt(ver);
    }

    private MasterProto.RecoveryMonitoringInfo recoveryInfoFMaster(ManagedChannel channel) {
        try {
            MasterMonitoringGrpc.MasterMonitoringBlockingStub stub = MasterMonitoringGrpc.newBlockingStub(channel);
            MasterProto.RecoveryMonitoringInfo grpcRes = stub.recoveryInfo(Empty.getDefaultInstance());
            LOGGER.trace("recovery res: {}", grpcRes);
            return grpcRes;
        } catch (Exception ex) {
            LOGGER.error("recovery info error!", ex);
            return null;
        }
    }

    private MasterProto.MasterStartMonitoringInfo masterStartFMaster(ManagedChannel channel) {
        try {
            MasterMonitoringGrpc.MasterMonitoringBlockingStub stub = MasterMonitoringGrpc.newBlockingStub(channel);
            MasterProto.MasterStartMonitoringInfo grpcRes = stub.masterStartInfo(Empty.getDefaultInstance());
            LOGGER.trace("master start info res: {}", grpcRes);
            return grpcRes;
        } catch (Exception ex) {
            LOGGER.error("master start error!", ex);
            return null;
        }
    }

}
