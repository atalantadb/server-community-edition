/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services;

import com.google.protobuf.Empty;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.region.server.vo.MasterConfiguration;
import com.light.atalanta.services.ZookeeperHelperExtended;
import com.light.atalanta.util.ProtoConverters;
import io.grpc.ManagedChannel;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static com.light.atalanta.ATALANTAConstants.HOSTNAME_SEPARATOR;
import static com.light.atalanta.grpc.MasterServiceGrpc.MasterServiceBlockingStub;
import static com.light.atalanta.grpc.MasterServiceGrpc.newBlockingStub;

public class MasterConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterConnector.class);

    private final ZookeeperHelperExtended zookeeperHelperExtended;
    private final int retryLimit;
    private final int connectionRequestTimeoutMs;

    private MasterServiceBlockingStub master;

    public MasterConnector(MasterConfiguration masterConf, ZookeeperHelperExtended zookeeperHelperExtended) {
        this.zookeeperHelperExtended = zookeeperHelperExtended;
        this.retryLimit = masterConf.getConnectionRetryLimit();
        this.connectionRequestTimeoutMs = masterConf.getConnectionRequestTimeoutMs();
    }

    private void connect() throws Exception {
        final String masterHostPort = zookeeperHelperExtended.getMasterHostPort();
        final String host = masterHostPort.split(HOSTNAME_SEPARATOR)[0];
        final int port = Integer.parseInt(masterHostPort.split(HOSTNAME_SEPARATOR)[1]);
        ManagedChannel channel = NettyChannelBuilder.forAddress(host, port)
                .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionRequestTimeoutMs)
                .usePlaintext()
                .nameResolverFactory(new DnsNameResolverProvider())
                .build();
        master = newBlockingStub(channel);
    }

    public boolean getRsStartPermission(RegionServerModel regionServer) throws Exception {
        LOGGER.info("Requesting start permission from master ...");
        Exception lastException = null;
        for (int i = 0; i < retryLimit; i++) {
            try {
                // The master address may not be published in ZooKeeper yet during startup
                if (master == null) {
                    connect();
                }
                return master.getRsStartPermission(ProtoConverters.toProtoRegionServer(regionServer)).getValue();
            } catch (Exception e) {
                if (master != null) {
                    if (master.getChannel() instanceof ManagedChannel channel) {
                        channel.shutdownNow();
                    }
                    master = null;
                }
                if (e instanceof InterruptedException || Thread.currentThread().isInterrupted()) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
                lastException = e;
                if (i + 1 < retryLimit) {
                    LOGGER.error("Error requesting RS start permission. Attempt: {}. Waiting {} ms ...", (i + 1), connectionRequestTimeoutMs, e);
                    try {
                        Thread.sleep(connectionRequestTimeoutMs);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw interrupted;
                    }
                }
            }
        }

        throw Optional.ofNullable(lastException).orElse(new RuntimeException());
    }

    public void modificationCompleteNotification() {
        master.modificationCompleteNotification(Empty.getDefaultInstance());
    }
}
