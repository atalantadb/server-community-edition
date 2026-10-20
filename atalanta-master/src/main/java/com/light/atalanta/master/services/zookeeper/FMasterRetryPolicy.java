/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.zookeeper;

import com.light.atalanta.master.enums.LeaderState;
import com.light.atalanta.master.services.leader.election.LeaderStateManager;
import com.light.atalanta.master.vo.ZooKeeperConfiguration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FMasterRetryPolicy implements RetryPolicy {
    private static final Logger LOGGER = LoggerFactory.getLogger(FMasterRetryPolicy.class);

    private LeaderStateManager leaderStateManager = null;
    private final RetryPolicy delegateRetryPolicy;
    private final long gracePeriodMs;

    public FMasterRetryPolicy(ZooKeeperConfiguration zooConf) {
        this.delegateRetryPolicy = new RetryNTimes(zooConf.getConnectionRetryTimes(), zooConf.getConnectionRetrySleepMs());
        this.gracePeriodMs = 3000;
    }

    public void setLeaderStateManager(LeaderStateManager leaderStateManager) {
        this.leaderStateManager = leaderStateManager;
    }

    @Override
    public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper) {
        LOGGER.info("Zookeeper connection retry: retryCount: {}, elapsedTimeMs: {}", retryCount, elapsedTimeMs);
        if (elapsedTimeMs < gracePeriodMs) {
            // Allow a retry until the grace period expires
            return delegateRetryPolicy.allowRetry(retryCount, elapsedTimeMs, sleeper);
        } else {
            if (leaderStateManager != null
                    && (leaderStateManager.getState() == LeaderState.LEADER
                        || leaderStateManager.getState() == LeaderState.LEADER_RECONNECTING)) {
                LOGGER.error("Grace period expired (elapsedTimeMs: {} ms). Switching to follower mode.", elapsedTimeMs);
                leaderStateManager.toFollowerMode();
            }
            return false;
        }
    }

    @Override
    public boolean allowRetry(Throwable exception) {
        LOGGER.info("Zookeeper allow retry with exception: {}", exception.getMessage());
        if (leaderStateManager != null
                && (leaderStateManager.getState() == LeaderState.LEADER
                     || leaderStateManager.getState() == LeaderState.LEADER_RECONNECTING)) {
            LOGGER.error("Exception occurred during reconnection, switching to follower mode.", exception);
            leaderStateManager.toFollowerMode();
        }
        return false;
    }
}
