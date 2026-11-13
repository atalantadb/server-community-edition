/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services;

public enum MasterStartState {
    LEADER_CANDIDATE(null, 1, "Leader candidate."),
    NEW_LEADER(LEADER_CANDIDATE, 2, "New leader has been elected"),
    TRY_TO_START_MASTER(NEW_LEADER, 3, "Try to start new leader"),
    TOTAL_REGION_FROM_ZK(TRY_TO_START_MASTER, 4, "Region count has been calculated"),
    WAIT_ALL_RS_FOR_ALL_TABLES(TOTAL_REGION_FROM_ZK, 5, "Wait RS"),
    SCAN_RECOVERING_TABLES(WAIT_ALL_RS_FOR_ALL_TABLES, 6, "Scan recovering tables"),
    TABLE_TO_RECOVERY_COUNT(SCAN_RECOVERING_TABLES, 7, "Recovering tables count was calculated"),
    START_LOCK_RECOVERING_TABLE(TABLE_TO_RECOVERY_COUNT, 8, "Start lock recovering table"),
    COMPLETE_LOCK_RECOVERING_TABLE(START_LOCK_RECOVERING_TABLE, 9, "Lock table for recovering successful"),
    GRPC_SERVER_STARTED(COMPLETE_LOCK_RECOVERING_TABLE, 10, "Start Master GRPC server"),
    STARTED(GRPC_SERVER_STARTED, 11, "Master started successful"),
    ALREADY_LEADER(STARTED, 12, "Master already started"),
    LEADER_AFTER_RECONNECTING(STARTED, 13, "Master reconnecting successful"),
    NO_NEED_TO_BE_LEADER(null, 0, "No need to be leader");

    private final MasterStartState prev;

    public MasterStartState getNext() {
        return next;
    }

    private MasterStartState next = null;
    private final String stateName;

    public int getStateNumber() {
        return stateNumber;
    }

    private final int stateNumber;

    MasterStartState(MasterStartState prev, int stateNumber, String stateName) {
        this.prev = prev;
        if (prev != null) {
            prev.next = this;
        }
        this.stateName = stateName;
        this.stateNumber = stateNumber;
    }

    public MasterStartState getPrev() {
        return prev;
    }

    public String getStateName() {
        return stateName;
    }

}
