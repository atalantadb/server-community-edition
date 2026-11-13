/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.zookeeper;

import com.light.atalanta.master.services.leader.election.LeaderStateManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FMasterConnectionStateListener implements ConnectionStateListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(FMasterConnectionStateListener.class);

    private LeaderStateManager leaderStateManager = null;

    private final long gracePeriodMs = 3000;

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        LOGGER.info("Connection state changed to: {}", newState);
        switch (newState) {
            case SUSPENDED:
                LOGGER.warn("Connection to ZooKeeper is SUSPENDED. Starting grace period timer of {} ms...", gracePeriodMs);
                break;
            case RECONNECTED:
                LOGGER.info("Connection RECONNECTED. Leader remains active.");
                break;
            case LOST:
                LOGGER.error("Connection LOST. Switching to follower mode immediately.");
                leaderStateManager.toFollowerMode();
                break;
            default:
                LOGGER.debug("Unhandled connection state: {}", newState);
        }
    }

    public void setLeaderStateManager(LeaderStateManager leaderStateManager) {
        this.leaderStateManager = leaderStateManager;
    }

}
