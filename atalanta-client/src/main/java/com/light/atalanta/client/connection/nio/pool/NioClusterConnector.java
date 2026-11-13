/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.pool;

import com.light.atalanta.DbConfiguration;
import com.light.atalanta.RepeatableExecutor;
import com.light.atalanta.client.connection.nio.transport.NioSelectorLoop;
import com.light.atalanta.client.connection.nio.transport.NioSelectorLoopGroup;
import com.light.atalanta.client.connection.nio.transport.TRSClientConnectionImpl;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.transport.TRSClientConnection;
import com.light.atalanta.transport.TransportConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NioClusterConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(NioClusterConnector.class);
    private final Map<String, RegionServerModel> topology;
    private final DbConfiguration conf;
    private final ZooKeeperCoreContext zooKeeperCoreContext;
    private final NioSelectorLoopGroup selectorLoopGroup;

    public NioClusterConnector(ZooKeeperCoreContext zooKeeperCoreContext, DbConfiguration conf,
                               NioSelectorLoopGroup selectorLoopGroup) {
        this.conf = conf;
        this.selectorLoopGroup = selectorLoopGroup;
        this.topology = new HashMap<>();
        this.zooKeeperCoreContext = zooKeeperCoreContext;
        initTopology();
    }

    public TRSClientConnection getConnectionToCore(String regionServerName, ConnectionNio connectionNio) throws IOException {
        LOGGER.info("Creating new connection to " + regionServerName);
        RegionServerModel rsm = topology.get(regionServerName);
        if (rsm == null) {
            LOGGER.warn("Cluster topology not actual. Get new cluster topology for current cluster configuration");
            initTopology();
            rsm = topology.get(regionServerName);
            if (rsm == null) {
                throw new RuntimeException("Bad cluster topology. RS not found: " + regionServerName);
            }
        }
        TransportConfiguration transportConfiguration = TransportConfiguration.fromConfiguration(conf);
        NioSelectorLoop selectorLoop = selectorLoopGroup.next();
        LOGGER.info("Selected NIO loop for rs: {}, connectionNio: {}, group size: {}",
                regionServerName, connectionNio, selectorLoopGroup.size());
        return new TRSClientConnectionImpl(transportConfiguration, rsm.getHost(), rsm.getCoreClientPort(),
                selectorLoop, connectionNio);
    }

    private void initTopology() {
        LOGGER.debug("Get cluster topology by active RS");
        new RepeatableExecutor()
                .withMaxRetries(5)
                .withStaticTimeout(5_000)
                .execute(() -> {
                    try {
                        zooKeeperCoreContext.getRunningRegionServers().forEach(
                                regionServerModel -> topology.put(regionServerModel.getName(), regionServerModel));
                        return null;
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                });
        LOGGER.debug("Cluster topology updated");
    }
}
