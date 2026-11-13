/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.leader.election.impl;

import com.light.atalanta.master.services.leader.election.Follower;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FollowerImpl implements Follower {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderCandidateImpl.class.getName());

    @Override
    public boolean start() {
        LOGGER.info("Switch to follower mode");
        return true;
    }

    @Override
    public void stop() {
        LOGGER.info("Try to stop follower mode");
    }
}
