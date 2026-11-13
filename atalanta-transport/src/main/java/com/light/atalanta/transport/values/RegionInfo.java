/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.values;

import com.light.atalanta.NameOfTable;
import com.light.atalanta.transport.RegionPosition;
import com.light.atalanta.util.Bytes;

import java.util.Arrays;
import java.util.Objects;

public class RegionInfo {
    private final NameOfTable tableName;
    private final RegionName regionName;
    private final RegionPosition position;

    private final byte[] startKey;
    private final byte[] endKey;

    public RegionInfo(NameOfTable tableName, RegionName regionName, RegionPosition position, byte[] startKey, byte[] endKey) {
        this.tableName = tableName;
        this.regionName = regionName;
        this.position = position;
        this.startKey = Bytes.cloneBytesArray(startKey);
        this.endKey = Bytes.cloneBytesArray(endKey);
    }

    public NameOfTable getTableName() {
        return tableName;
    }

    public RegionName getRegionName() {
        return regionName;
    }

    public int getPosition() {
        return position.getPosition();
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegionInfo that = (RegionInfo) o;
        return tableName.equals(that.tableName) && regionName.equals(that.regionName) && position.equals(that.position) && Arrays.equals(startKey, that.startKey) && Arrays.equals(endKey, that.endKey);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(tableName, regionName, position);
        result = 31 * result + Arrays.hashCode(startKey);
        result = 31 * result + Arrays.hashCode(endKey);
        return result;
    }

    @Override
    public String toString() {
        return "RegionInfo{" +
                "tableName=" + tableName +
                ", regionName=" + regionName +
                ", position=" + position +
                ", startKey=" + Arrays.toString(startKey) +
                ", endKey=" + Arrays.toString(endKey) +
                '}';
    }
}
