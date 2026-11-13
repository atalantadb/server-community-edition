/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import java.util.NavigableMap;

public interface GetRows extends RowKey {
    GetRows addColumn(byte[] defaultColumnFamilyBytes, byte[] bytes);

    GetRows setCheckExistenceOnly(boolean b);

    GetRows setTimestamp(long timestamp);

    NavigableMap<byte[], NavigableMap<byte[], Long>> getFamilyMap();

    default GetRows readAllVersions() {
        return this;
    }
}
