/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.tools;

import com.google.protobuf.Empty;
import com.light.atalanta.Configuration;
import com.light.atalanta.Connection;
import com.light.atalanta.ConnectionThreadLocal;
import com.light.atalanta.client.factories.AtalantaDbNioClientObjectFactory;
import com.light.atalanta.grpc.RegionServerServiceGrpc;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.services.ZookeeperHelperExtended;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.light.atalanta.ATALANTAConstants.REGION_SERVER_RUNNING;
import static com.light.atalanta.config.AtalantaProperties.*;

/** Read-only CE Docker startup probe, run inside the existing shell image. */
public final class ClusterReadiness {
    private ClusterReadiness() {
    }

    public static void main(String[] args) {
        int status = 0;
        try {
            if (args.length != 1 || !args[0].matches("[1-9][0-9]{0,3}")) {
                throw new IllegalArgumentException("Usage: ClusterReadiness <minimum-running-region-servers>");
            }
            Configuration conf = Configuration.create();
            conf.addResource(new File("config/atalanta-site.xml"));
            conf.set(ATALANTA_CONNECTION_POOL_SIZE, "1");
            conf.set(ATALANTA_CLIENT_NIO_SELECTOR_LOOP_SIZE, "1");
            conf.set(ATALANTA_CONNECTION_RETRY_COUNT, "1");
            conf.set(ATALANTA_CONNECTION_RETRY_DELAY_MS, "100");
            conf.set(ATALANTA_CONNECTION_POOL_DISCONNECT_TIMEOUT_MS, "1000");
            conf.set(ATALANTA_MASTER_DISCONNECTION_TIMEOUT_MS, "1000");
            try (Connection connection = new AtalantaDbNioClientObjectFactory().connection(conf)) {
                ConnectionThreadLocal client = (ConnectionThreadLocal) connection.getAdmin().getConnection();
                String mode = client.getMasterStub().withDeadlineAfter(3, TimeUnit.SECONDS)
                        .ping(Empty.getDefaultInstance()).getMode();
                requireLeader(mode);
                List<RegionServerModel> servers = readyServers(client.getZooKeeperCoreContext(),
                        new ZookeeperHelperExtended(client.getZooHelper()), Integer.parseInt(args[0]));
                for (RegionServerModel server : servers) {
                    ManagedChannel channel = NettyChannelBuilder.forAddress(server.getHost(), server.getJavaPort())
                            .usePlaintext().build();
                    try {
                        RegionServerServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS)
                                .ping(Empty.getDefaultInstance());
                    } finally {
                        channel.shutdownNow();
                    }
                }
                System.out.println("Database ready: active master and " + servers.size() + " running RegionServer(s).");
            }
        } catch (Exception ex) {
            System.err.println("Database is not ready: " + ex);
            status = 1;
        }
        // Do not leave client/Netty background threads behind after this one-shot probe.
        System.exit(status);
    }

    static void requireLeader(String mode) {
        if (!"LEADER".equals(mode)) {
            throw new IllegalStateException("Master is not active: " + mode);
        }
    }

    static List<RegionServerModel> readyServers(ZooKeeperCoreContext context,
                                               ZookeeperHelperExtended helper, int minimum) throws Exception {
        List<RegionServerModel> servers = new ArrayList<>();
        // The legacy getRunningRegionServers() also includes DISABLING; do not accept those.
        for (RegionServerModel server : context.getRunningRegionServers()) {
            if (REGION_SERVER_RUNNING.equals(helper.getRegionServerStatus(server.getName()))) {
                servers.add(server);
            }
        }
        if (servers.size() < minimum) {
            throw new IllegalStateException("Expected at least " + minimum + " RUNNING RegionServer(s), found " + servers.size());
        }
        return servers;
    }
}
