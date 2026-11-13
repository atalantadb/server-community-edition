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

public interface RecoveryLogger {
    Map<String, Object> getInfo();
    long getRecoveryTableCount();
    void setRecoveryTableCount(long recoveryTableCount);
    long getRecoveryRsCount();
    void setRecoveryRsCount(long recoveryRsCount);
    void startRecovery(List<TableName> tablesToRecover);
    void startRecoveryTable(TableName tableToRecover);
    void completeRecoveryTable(TableName rt);
}
