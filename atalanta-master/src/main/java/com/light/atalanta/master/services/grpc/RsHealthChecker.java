/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.grpc;

import com.google.common.base.Preconditions;
import com.google.protobuf.Empty;
import com.light.atalanta.grpc.RegionServerServiceGrpc;
import com.light.atalanta.master.exceptions.UnavailableRsException;
import com.light.atalanta.master.services.UniqueNameThreadFactory;
import com.light.atalanta.master.vo.GrpcConfiguration;
import com.light.atalanta.model.RegionServerModel;
import io.grpc.ManagedChannel;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class RsHealthChecker implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RsHealthChecker.class);

    private final List<RegionServerServiceGrpc.RegionServerServiceBlockingStub> stubs;
    private final RegionServerModel regionServer;
    private final AtomicReference<Exception> exContainer;
    private final ScheduledExecutorService executorService;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final GrpcConfiguration grpcConf;

    private final ThreadFactory healthCheckerThreadFactory = new UniqueNameThreadFactory("atalanta-rs-health-checker-pool");

    public RsHealthChecker(RegionServerModel regionServer, GrpcConfiguration grpcConf) {
        this.regionServer = regionServer;
        this.grpcConf = grpcConf;
        this.executorService = Executors.newSingleThreadScheduledExecutor(healthCheckerThreadFactory);
        this.exContainer = new AtomicReference<>();
        this.stubs = new ArrayList<>();
        start();
    }

    public void start() {
        RegionServerServiceGrpc.RegionServerServiceBlockingStub stub = getBlockingStub(regionServer);
        stubs.add(stub);
        int hashCode = hashCode();
        executorService.scheduleWithFixedDelay(() -> {
            if (exContainer.get() != null) {
                return;
            }
            try {
                Empty result = stub.ping(Empty.getDefaultInstance());
                Preconditions.checkNotNull(result, "Received empty result from: " + regionServer + ". Id: " + hashCode);
            } catch (Exception ex) {
                if (!Thread.currentThread().isInterrupted() && !closed.get()) {
                    LOGGER.error("Ping to region server: {} is NOT success. Id: {}", regionServer, hashCode, ex);
                    exContainer.set(ex);
                    executorService.shutdownNow();
                } else {
                    LOGGER.warn("Ping to region server: {} was interrupted. Id: {}", regionServer, hashCode);
                }
            }
        }, 0, 150, TimeUnit.MILLISECONDS);
    }

    public Optional<Exception> getError() {
        return Optional.ofNullable(exContainer.get());
    }

    // The caller is expected to close these channels after use!
    private RegionServerServiceGrpc.RegionServerServiceBlockingStub getBlockingStub(RegionServerModel regionServer) {
        ManagedChannel channel = NettyChannelBuilder.forAddress(regionServer.getHost(), regionServer.getJavaPort())
                .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, grpcConf.getRsReconnectTimeoutMs())
                .usePlaintext()
                .nameResolverFactory(new DnsNameResolverProvider())
                .build();
        return RegionServerServiceGrpc.newBlockingStub(channel);
    }

    public static void checkRsErrors(ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers) throws UnavailableRsException {
        AtomicBoolean error = new AtomicBoolean(false);
        for (Map.Entry<RegionServerModel, RsHealthChecker> pingChecker : pingCheckers.entrySet()) {
            Optional<Exception> exception = pingChecker.getValue().getError();
            if (exception.isPresent()) {
                LOGGER.error("Found error while pinging region server: {}", pingChecker.getKey(), exception.get());
                error.set(true);
                pingCheckers.values().forEach(RsHealthChecker::close);
                break;
            }
        }
        if (error.get()) {
            throw new UnavailableRsException("Found errors while pinging regions on RS. Please, check the logs");
        }
    }

    @Override
    public void close() {
        closed.set(true);
        stubs.forEach(stub -> ((ManagedChannel) stub.getChannel()).shutdownNow());
        executorService.shutdownNow();
    }
}
