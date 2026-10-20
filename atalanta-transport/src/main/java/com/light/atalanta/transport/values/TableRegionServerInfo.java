/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.values;

import com.light.atalanta.NameOfTable;
import com.light.atalanta.transport.RegionPosition;
import com.light.atalanta.collections.CopyOnWriteArrayMap;
import com.light.atalanta.util.BytesComparator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

public class TableRegionServerInfo {

    private final NameOfTable tableName;
    private final List<RegionInfo> regions;
    private final ConcurrentNavigableMap<byte[], RegionInfo> keyMapping;

    public TableRegionServerInfo(NameOfTable tableName, List<RegionInfo> regions) {
        this.tableName = tableName;
        this.regions = regions;
        this.keyMapping = new CopyOnWriteArrayMap<>(BytesComparator.BYTES_COMPARATOR);
        for (RegionInfo info : this.regions) {
            keyMapping.put(info.getStartKey(), info);
        }
    }

    public String getNamespace() {
        return tableName.getNamespace();
    }

    public String getTable() {
        return tableName.getTableName();
    }

    public NameOfTable getTableName() {
        return tableName;
    }

    public List<RegionInfo> getRegions() {
        return regions;
    }

    public int getRegionPosition(byte[] rowkey) {
        Map.Entry<byte[], RegionInfo> entry = keyMapping.floorEntry(rowkey);
        if (entry == null) {
            return RegionPosition.UNSET.getPosition();
        }
        return entry.getValue().getPosition();
    }

    @Override
    public String toString() {
        return "TableRegionServerInfo{" +
                "tableName=" + tableName +
                ", regions=" + regions +
                '}';
    }
}
