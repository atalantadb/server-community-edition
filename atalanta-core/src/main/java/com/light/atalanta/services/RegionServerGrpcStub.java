/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.services;

import com.light.atalanta.grpc.RegionServerServiceGrpc;
import com.light.atalanta.model.RegionServerModel;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Creates gRPC client for a specified region server.
 * WARN: it's strongly recommended to use {@link RegionServerConnector} instead of this.
 * WARN: one should close gRPC channel after making use of gRPC stubs by calling close() method.
 */
public class RegionServerGrpcStub {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionServerGrpcStub.class);

    private static final int CONNECTION_POOL_SIZE = 10;

    private static final ConcurrentMap<RegionServerModel, ConcurrentMap<Integer, Sync>> syncPool = new ConcurrentHashMap<>();
    private static final ConcurrentMap<RegionServerModel, ConcurrentMap<Integer, Async>> asyncPool = new ConcurrentHashMap<>();

    private static final AtomicInteger syncIndex = new AtomicInteger(0);
    private static final AtomicInteger asyncIndex = new AtomicInteger(0);

    private static final Set<ConnectivityState> ALLOWED_CHANNEL_STATES = new HashSet<>(Arrays.asList(
            ConnectivityState.CONNECTING, ConnectivityState.IDLE, ConnectivityState.READY)
    );

    public static Sync sync(RegionServerModel regionServer, int reconnectMs) {
        int nextIdx = syncIndex.getAndUpdate(i -> (i + 1) % CONNECTION_POOL_SIZE);
        return syncPool.computeIfAbsent(regionServer, rs ->
                IntStream.range(0, CONNECTION_POOL_SIZE).boxed()
                        .collect(Collectors.toConcurrentMap(
                                idx -> idx, idx -> new Sync(rs, reconnectMs, idx)
                        ))
        ).get(nextIdx);
    }

    public static Async async(RegionServerModel regionServer, int reconnectMs) {
        int nextIdx = asyncIndex.getAndUpdate(i -> (i + 1) % CONNECTION_POOL_SIZE);
        return asyncPool.computeIfAbsent(regionServer, rs ->
                IntStream.range(0, CONNECTION_POOL_SIZE).boxed()
                        .collect(Collectors.toConcurrentMap(
                                idx -> idx, idx -> new Async(rs, reconnectMs, idx)
                        ))
        ).get(nextIdx);
    }

    // Waits for the result, then returns
    public static class Sync {
        private final Object mutex = new Object();
        private RegionServerServiceGrpc.RegionServerServiceBlockingStub blockingStub;
        private final RegionServerModel regionServer;
        private final int reconnectMs;
        private final int index;

        // reconnectMs is a timeout for connection establishment and closure
        private Sync(RegionServerModel regionServer, int reconnectMs, int index) {
            LOGGER.trace("Sync gRPC channel [{}] for '{}': initializing", index, regionServer);
            this.reconnectMs = reconnectMs;
            this.regionServer = regionServer;
            this.index = index;
            connect(regionServer);
        }

        public RegionServerServiceGrpc.RegionServerServiceBlockingStub get() {
            if (isNeedToReconnect()) {
                synchronized (mutex) {
                    if (isNeedToReconnect()) {
                        LOGGER.debug("Sync gRPC channel [{}] for '{}': reconnecting", index, regionServer);
                        closeConnection();
                        connect(regionServer);
                    }
                }
            }
            return blockingStub;
        }

        private boolean isNeedToReconnect() {
            ManagedChannel channel = ((ManagedChannel) blockingStub.getChannel());
            ConnectivityState current = channel.getState(true);
            return !ALLOWED_CHANNEL_STATES.contains(current);
        }

        private void connect(RegionServerModel regionServer) {
            ManagedChannel channel = NettyChannelBuilder.forAddress(regionServer.getHost(), regionServer.getJavaPort())
                    .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, reconnectMs)
                    .usePlaintext()
                    .nameResolverFactory(new DnsNameResolverProvider())
                    .build();
            blockingStub = RegionServerServiceGrpc.newBlockingStub(channel);
        }

        private void closeConnection() {
            try {
                ManagedChannel channel = ((ManagedChannel) blockingStub.getChannel());
                channel.shutdownNow();
                channel.awaitTermination(reconnectMs, TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                LOGGER.warn("Unable to close sync gRPC channel [{}] properly", index, ex);
            }
        }
    }

    // Doesn't wait for the result
    public static class Async {
        private final Object mutex = new Object();
        private RegionServerServiceGrpc.RegionServerServiceStub stub;
        private final RegionServerModel regionServer;
        private final int reconnectMs;
        private final int index;

        // reconnectMs is a timeout for connection establishment and closure
        private Async(RegionServerModel regionServer, int reconnectMs, int index) {
            LOGGER.trace("Async gRPC channel [{}] for '{}': initializing", index, regionServer);
            this.reconnectMs = reconnectMs;
            this.regionServer = regionServer;
            this.index = index;
            connect(regionServer);
        }

        public RegionServerServiceGrpc.RegionServerServiceStub get() {
            if (isNeedToReconnect()) {
                synchronized (mutex) {
                    if (isNeedToReconnect()) {
                        LOGGER.debug("Async gRPC channel [{}] for '{}': reconnecting", index, regionServer);
                        closeConnection();
                        connect(regionServer);
                    }
                }
            }
            return stub;
        }

        private boolean isNeedToReconnect() {
            ManagedChannel channel = ((ManagedChannel) stub.getChannel());
            ConnectivityState current = channel.getState(true);
            return !ALLOWED_CHANNEL_STATES.contains(current);
        }

        private void connect(RegionServerModel regionServer) {
            ManagedChannel channel = NettyChannelBuilder.forAddress(regionServer.getHost(), regionServer.getJavaPort())
                    .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, reconnectMs)
                    .usePlaintext()
                    .nameResolverFactory(new DnsNameResolverProvider())
                    .build();
            stub = RegionServerServiceGrpc.newStub(channel);
        }

        private void closeConnection() {
            try {
                ManagedChannel channel = ((ManagedChannel) stub.getChannel());
                channel.shutdownNow();
                channel.awaitTermination(reconnectMs, TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                LOGGER.warn("Unable to close async gRPC channel [{}] properly", index, ex);
            }
        }
    }
}
