/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.scheduled;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.light.atalanta.region.server.services.DefaultDisposableService;
import com.light.atalanta.region.server.services.RegionServerInfoSupplier;
import com.light.atalanta.region.server.vo.HealthCheckerConfiguration;
import com.light.atalanta.services.ZookeeperHelperExtended;
import com.light.atalanta.util.ShutdownManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.light.atalanta.ATALANTAConstants.HOSTNAME_SEPARATOR;
import static com.light.atalanta.ATALANTAConstants.REGION_SERVER_DISABLED;
import static com.light.atalanta.ATALANTAConstants.REGION_SERVER_DISABLING;
import static com.light.atalanta.ATALANTAConstants.REGION_SERVER_RUNNING;

public class ZkHealthChecker extends AbstractScheduledService implements DefaultDisposableService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkHealthChecker.class);

    private final RegionServerInfoSupplier serverInfo;
    private final ZookeeperHelperExtended zkHelper;
    private final HealthCheckerConfiguration delays;
    private final String profile;

    public ZkHealthChecker(RegionServerInfoSupplier serverInfo, ZookeeperHelperExtended zkHelper, HealthCheckerConfiguration delays, String profile) {
        this.serverInfo = serverInfo;
        this.zkHelper = zkHelper;
        this.delays = delays;
        this.profile = profile;
    }

    @Override
    protected void runOneIteration() throws Exception {
        LOGGER.trace("Start comparing ZK state with RS-information.");
        String zkRsInfo = "";
        try {
            zkRsInfo = zkHelper.getRegionServerInfo(serverInfo.regionServerId());
        } catch (Exception ex) {
            ShutdownManager.exitWithError("Error getting RS info from ZK", ex);
        }
        if (!zkRsInfo.startsWith(serverInfo.regionServerHost(profile) + HOSTNAME_SEPARATOR + serverInfo.clientPort())) {
            ShutdownManager.exitWithError("Information in ZK: '" + zkRsInfo
                    + "' is not equals with Region Server info: '" + serverInfo.hostPorts()
                    + "'. Stopping RS");
        }
        try {
            zkRsInfo = zkHelper.getRegionServerStatus(serverInfo.regionServerId());
        } catch (Exception ex) {
            ShutdownManager.exitWithError("Error getting RS status from ZK", ex);
        }
        if ( !(zkRsInfo.equals(REGION_SERVER_RUNNING)
                || zkRsInfo.equals(REGION_SERVER_DISABLING)
                || zkRsInfo.equals(REGION_SERVER_DISABLED))) {
            ShutdownManager.exitWithError("Unexpected RS status in ZK: " + zkRsInfo);
        }
        LOGGER.trace("Health is OK");
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(delays.getInitialDelaySecs(), delays.getDelaySecs(), TimeUnit.SECONDS);
    }
}
