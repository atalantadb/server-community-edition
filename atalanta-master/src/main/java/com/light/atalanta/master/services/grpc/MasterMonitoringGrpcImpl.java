/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.grpc;

import com.google.protobuf.Empty;
import com.light.atalanta.grpc.MasterMonitoringGrpc;
import com.light.atalanta.grpc.MasterProto;
import com.light.atalanta.master.services.MasterStartLogger;
import com.light.atalanta.master.services.RecoveryLogger;
import com.light.atalanta.util.ProtoConverters;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static com.light.atalanta.master.services.grpc.MasterServiceGrpcImpl.buildMsg;

public class MasterMonitoringGrpcImpl extends MasterMonitoringGrpc.MasterMonitoringImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterMonitoringGrpcImpl.class);
    private final RecoveryLogger recoveryLogger;
    private final MasterStartLogger masterStartLogger;
    private final MeterRegistry meterRegistry;
    private long scapeUpdateTime = 0L;
    private String scrape = StringUtils.EMPTY;

    public MasterMonitoringGrpcImpl(RecoveryLogger recoveryLogger, MasterStartLogger masterStartLogger, MeterRegistry meterRegistry) {
        this.recoveryLogger = recoveryLogger;
        this.masterStartLogger = masterStartLogger;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void recoveryInfo(Empty request, StreamObserver<MasterProto.RecoveryMonitoringInfo> responseObserver) {
        try {
            responseObserver.onNext(buildRecoveryResponse());
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            LOGGER.error("Cannot send recovery info", ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        }
    }

    @Override
    public void masterStartInfo(Empty request, StreamObserver<MasterProto.MasterStartMonitoringInfo> responseObserver) {
        try {
            responseObserver.onNext(buildMasterStartResponse());
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            LOGGER.error("Cannot send start master info", ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        }
    }

    @Override
    public void getMetrics(Empty request, StreamObserver<MasterProto.MasterMetrics> responseObserver) {
        try {
            responseObserver.onNext(buildMasterMetricsResponse());
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            LOGGER.error("Cannot send master metrics", ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        }
    }

    private MasterProto.MasterMetrics buildMasterMetricsResponse() {
        return MasterProto.MasterMetrics.newBuilder()
                .setScrape(getScrape())
                .build();
    }

    synchronized private String getScrape() {
        long curTime = System.currentTimeMillis();
        if (curTime - scapeUpdateTime > 100) {
            scrape = ((PrometheusMeterRegistry) meterRegistry).scrape();
            scapeUpdateTime = curTime;
        }
        return scrape;
    }

    private MasterProto.RecoveryMonitoringInfo buildRecoveryResponse() {
        return MasterProto.RecoveryMonitoringInfo.newBuilder()
                .setRecoveryTableCount(recoveryLogger.getRecoveryTableCount())
                .addAllRecoveryInfoMsg(toProtoInfoList())
                .build();
    }

    private List<MasterProto.FMasterInfoMsg> toProtoInfoList() {
        return recoveryLogger.getInfo().entrySet().stream()
                .map(e -> buildMsg(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    private MasterProto.MasterStartMonitoringInfo buildMasterStartResponse() {
        return MasterProto.MasterStartMonitoringInfo.newBuilder()
                .addAllMasterStartInfoMsg(toProtoMasterStartInfoList())
                .build();
    }

    private List<MasterProto.FMasterInfoMsg> toProtoMasterStartInfoList() {
        return masterStartLogger.getInfo().entrySet().stream()
                .map(e -> buildMsg(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }



}
