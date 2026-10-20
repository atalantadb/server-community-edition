/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.pool;

import com.light.atalanta.NameOfTable;
import com.light.atalanta.transport.values.RegionInfo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;

public class TablesKeyMappingStorage {

    private static Map<String, ConcurrentNavigableMap<byte[], RegionInfo>> keyMaps = new ConcurrentHashMap<>();

    private TablesKeyMappingStorage() {
    }

    public static void put(NameOfTable tableName, ConcurrentNavigableMap<byte[], RegionInfo> keyMap) {
        keyMaps.put(tableName.getNameAsString(), keyMap);
    }

    public static ConcurrentNavigableMap<byte[], RegionInfo> get(NameOfTable tableName) {
        return keyMaps.get(tableName.getNameAsString());
    }
}
