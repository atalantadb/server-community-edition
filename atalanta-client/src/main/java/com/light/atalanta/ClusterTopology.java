/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.transport.TRSConnectionStub;
import com.light.atalanta.transport.TransportConfiguration;
import com.light.atalanta.transport.TransportConnectionFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ClusterTopology {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTopology.class);
    public static final int DEFAULT_CORE_TIMEOUT = 60_000;

    private final ZooKeeperCoreContext zooKeeperCoreContext;
    private final DbConfiguration conf;
    private final Map<String, RegionServerModel> topology;

    public ClusterTopology(ZooKeeperCoreContext zooKeeperCoreContext, DbConfiguration conf) {
        this.conf = conf;
        this.topology = new HashMap<>();
        this.zooKeeperCoreContext = zooKeeperCoreContext;
        initTopology();
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

    public TRSConnectionStub getNewConnectionToCore(String regionServerName) {
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
        return TransportConnectionFactory.createConnectionStub(transportConfiguration,
                rsm.getHost(),
                rsm.getCoreClientPort());
    }

}
