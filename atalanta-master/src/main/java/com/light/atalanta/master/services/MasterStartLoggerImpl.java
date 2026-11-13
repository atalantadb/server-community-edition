/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import static com.light.atalanta.master.services.MasterStartState.*;

public class MasterStartLoggerImpl implements MasterStartLogger {

    private MasterStartState state;
    private Map<MasterStartState, LocalDateTime> times = new ConcurrentHashMap<>();
    private int tableToRecoveryCount = 0;
    private int allDistributionCount = 0;
    private int runningRegionServersSize = 0;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS yyyy-MM-dd");

    @Override
    public Map<String, Object> getInfo() {
        TreeMap<String, Object> res = new TreeMap<>();
        return state == null ? res : addMasterStartInfo(res, state);
    }

    private Map<String, Object> addMasterStartInfo(Map<String, Object> res, MasterStartState state) {
        LocalDateTime dateTime = times.get(state);
        String time = "Time: " + (dateTime == null ? "Unknown" : FORMATTER.format(dateTime));
        String stage = String.format("Stage %02d/11 ", state.getStateNumber());
        LocalDateTime finishTime = state.getNext() == null ? null : times.get(state.getNext());
        long duration = (finishTime == null || dateTime == null) ? -1 :
                ChronoUnit.MILLIS.between(dateTime, finishTime);
        String timeWithDuration = time + (duration < 0 ? " ..." : (" Duration, ms: " + duration));
        if (state.getPrev() == null) {
            if (state == LEADER_CANDIDATE) {
                res.put(stage + state.getStateName(), timeWithDuration);
            } else if (state == NO_NEED_TO_BE_LEADER) {
                res.put(state.getStateName(), time );
            }
            return res;
        } else {
            res = addMasterStartInfo(res, state.getPrev());
            String value = timeWithDuration;
            if (state == TABLE_TO_RECOVERY_COUNT) {
                value = "Table to recovery: " + tableToRecoveryCount + value;
            } else if (state == WAIT_ALL_RS_FOR_ALL_TABLES) {
                value = "Count of running RS: " + runningRegionServersSize + " " + value;
            } else if (state == TOTAL_REGION_FROM_ZK) {
                value = "Available regions count: " + allDistributionCount + " " + value;
            }
            res.put(stage + state.getStateName(), value);
            return res;
        }
    }

    @Override
    public void setState(MasterStartState state) {
        this.state = state;
        updateTime();
    }

    @Override
    public void tableToRecoveryCount(int size) {
        setState(MasterStartState.TABLE_TO_RECOVERY_COUNT);
        tableToRecoveryCount = size;
    }

    @Override
    public void totalRegions(int allDistributionCount) {
        setState(MasterStartState.TOTAL_REGION_FROM_ZK);
        this.allDistributionCount = allDistributionCount;
    }

    @Override
    public void waitAllRSForAllTables(int size) {
        setState(MasterStartState.WAIT_ALL_RS_FOR_ALL_TABLES);
        runningRegionServersSize = size;
    }

    private void updateTime() {
        times.put(state, LocalDateTime.now());
    }
}
