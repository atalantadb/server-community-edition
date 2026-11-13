/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.enums;

/**
 * FMaster state machine.
 */
public enum LeaderState {
    /**
     * Starting. Possible transitions: LEADER_CANDIDATE, FOLLOWER.
     */
    INSTANCE_STARTED,

    /**
     * Trying to become the leader. Possible transitions: LEADER, FOLLOWER.
     */
    LEADER_CANDIDATE,

    /**
     * Leader. Possible transitions: LEADER_RECONNECTING.
     */
    LEADER,

    /**
     * Follower. Possible transitions: LEADER_CANDIDATE.
     */
    FOLLOWER,

    /**
     * Leader reconnecting. Possible transitions: LEADER.
     */
    LEADER_RECONNECTING;

}
