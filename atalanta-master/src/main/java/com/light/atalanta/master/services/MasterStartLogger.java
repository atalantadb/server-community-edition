/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services;

import java.util.Map;

public interface MasterStartLogger {
    Map<String, Object> getInfo();

    void setState(MasterStartState masterStartState);

    void tableToRecoveryCount(int size);

    void totalRegions(int allDistributionCount);

    void waitAllRSForAllTables(int size);
}
