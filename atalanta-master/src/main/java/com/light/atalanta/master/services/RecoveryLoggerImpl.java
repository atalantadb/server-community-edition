/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services;

import com.light.atalanta.TableName;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.light.atalanta.master.services.grpc.MasterServiceGrpcImpl.MAX_JOIN_STR;

public class RecoveryLoggerImpl implements RecoveryLogger  {

    private Map<String, Object> infoMap = new ConcurrentHashMap<>();
    private long recoveryTableCount = 0;
    private long recoveryRsCount = 0;
    private final Set<String> recoveredTables = ConcurrentHashMap.newKeySet();

    @Override
    public Map<String, Object> getInfo() {
        infoMap.put("Recovery RS count", String.valueOf(recoveryRsCount));
        infoMap.put("Table to recovery count", String.valueOf(recoveryTableCount));
        infoMap.put("Table in recovering count", recoveredTables.size());
        String tableInRecovery = String.join(",", recoveredTables);
        if (tableInRecovery.length() > MAX_JOIN_STR) {
            tableInRecovery = tableInRecovery.substring(0, MAX_JOIN_STR) + "...";
        }
        infoMap.put("recover tables: ", tableInRecovery);
        return infoMap;
    }

    @Override
    public long getRecoveryTableCount() {
        return recoveryTableCount;
    }

    @Override
    public void setRecoveryTableCount(long recoveryTableCount) {
        this.recoveryTableCount = recoveryTableCount;
    }

    @Override
    public long getRecoveryRsCount() {
        return recoveryRsCount;
    }

    @Override
    public void setRecoveryRsCount(long recoveryRsCount) {
        this.recoveryRsCount = recoveryRsCount;
    }

    @Override
    public void startRecovery(List<TableName> tablesToRecover) {
       recoveryTableCount = tablesToRecover.size();
    }

    @Override
    public void startRecoveryTable(TableName tableToRecover) {
        recoveredTables.add(tableToRecover.toString());
    }

    @Override
    public void completeRecoveryTable(TableName rt) {
        recoveredTables.remove(rt.toString());
    }
}
