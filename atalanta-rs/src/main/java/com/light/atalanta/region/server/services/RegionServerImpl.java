/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services;

import com.google.common.base.Preconditions;
import com.light.atalanta.region.server.RegionServerApplication;
import com.light.atalanta.region.server.scheduled.CoreHealthChecker;
import com.light.atalanta.region.server.scheduled.ZkHealthChecker;
import com.light.atalanta.region.server.services.grpc.RegionServerGrpc;
import com.light.atalanta.region.server.services.operations.progressed.ModifyRegion;
import com.light.atalanta.region.server.services.rebalance.DisksRebalanceProcessor;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import com.light.atalanta.services.ZookeeperHelperExtended;
import com.light.atalanta.util.ShutdownManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.Closeable;

import static com.light.atalanta.ATALANTAConstants.REGION_SERVER_INITIALIZING;
import static com.light.atalanta.ATALANTAConstants.REGION_SERVER_RUNNING;

public class RegionServerImpl implements RegionServer, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionServerImpl.class);

    private final RegionServerInfoSupplier serverInfo;
    private final ZookeeperHelperExtended zookeeperHelperExtended;
    private final FileSystem fileSystem;
    private final RegionServerGrpc grpcServer;
    private final CoreManager coreManager;
    private final ZkHealthChecker zkHealthChecker;
    private final CoreHealthChecker coreHealthChecker;
    private final RegionServerConfiguration regionServerConfiguration;
    private final ReplicaMapperProcessor replicaMapperProcessor;
    private final MasterConnector masterConnector;
    private final DisksRebalanceProcessor disksRebalanceProcessor;
    private final String profile;
    private final ModifyRegion modifyRegion;

    public RegionServerImpl(RegionServerInfoSupplier serverInfo,
                            ZookeeperHelperExtended zookeeperHelperExtended,
                            FileSystem fileSystem,
                            RegionServerGrpc grpcServer,
                            CoreManager coreManager,
                            RegionServerConfiguration regionServerConfiguration,
                            ReplicaMapperProcessor replicaMapperProcessor,
                            ZkHealthChecker zkHealthChecker,
                            CoreHealthChecker coreHealthChecker,
                            MasterConnector masterConnector,
                            DisksRebalanceProcessor disksRebalanceProcessor, String profile, ModifyRegion modifyRegion) {
        this.disksRebalanceProcessor = disksRebalanceProcessor;
        this.modifyRegion = modifyRegion;
        LOGGER.info("Region server configuration is: {}", regionServerConfiguration);
        Preconditions.checkArgument(!regionServerConfiguration.toString().contains("null"), "not all parameters filled in");
        this.serverInfo = serverInfo;
        this.zookeeperHelperExtended = zookeeperHelperExtended;
        this.fileSystem = fileSystem;
        this.grpcServer = grpcServer;
        this.profile = profile;
        this.coreManager = coreManager;
        this.regionServerConfiguration = regionServerConfiguration;
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.zkHealthChecker = zkHealthChecker;
        this.coreHealthChecker = coreHealthChecker;
        this.masterConnector = masterConnector;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                zookeeperHelperExtended.unregisterRs(serverInfo.regionServerId());
            } catch (Exception ex) {
                LOGGER.error("Unable to unregister RS", ex);
            }
        }));
    }

    @PostConstruct
    @Override
    public boolean start() throws Exception {
        LOGGER.info("Region server start complete. Time: {}", System.currentTimeMillis() - RegionServerApplication.getBeginTime());
        LOGGER.info("Starting Region server with id: {}", serverInfo.regionServerId());
        checkSplitBackupDir();
        fileSystem.removeTmp();
        // Initial RS registration
        zookeeperHelperExtended.registerRs(serverInfo.regionServerId(), serverInfo.hostPorts(), REGION_SERVER_INITIALIZING);
        return startRegisteredRs();
    }

    private boolean startRegisteredRs() throws Exception {
        // Ask the master for permission to start
        if (!masterConnector.getRsStartPermission(serverInfo.getMe(profile))) {
            ShutdownManager.exitWithError("Received start denied message from master");
        }
        disksRebalanceProcessor.rebalance();
        replicaMapperProcessor.initMapping();
        coreManager.initialAssignAndProcessOrphan();
        grpcServer.start();
        // Final RS registration
        zookeeperHelperExtended.updateRsStatus(serverInfo.regionServerId(), REGION_SERVER_RUNNING);
        zkHealthChecker.startAsync();
        coreHealthChecker.startAsync();
        modifyRegion.startMonitoring();
        return true;
    }

    private void checkSplitBackupDir() {
        if (regionServerConfiguration.getDataConfiguration().getSplitSourceBackupEnabled()) {
            String dir = regionServerConfiguration.getDataConfiguration().getSplitSourceBackupDir();
            if (!fileSystem.isPathExist(dir)) {
                ShutdownManager.exitWithError("The flag splitSourceBackupEnabled is enabled, but splitSourceBackupDir is not exist. splitSourceBackupDir: " + dir);
            }
        }
    }


    @Override
    public void close() {
        if (grpcServer != null) {
            try {
                grpcServer.close();
            } catch (Exception ex) {
                LOGGER.error("Unable to close gRPC server properly", ex);
            }
        }
        try {
            zkHealthChecker.stopAsync();
            zkHealthChecker.awaitTerminated();
            coreHealthChecker.stopAsync();
            coreHealthChecker.awaitTerminated();
        } catch (Exception ex) {
            LOGGER.error("Unable to properly stop health checkers", ex);
        }
    }
}
