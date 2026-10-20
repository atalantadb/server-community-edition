/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.values;


import com.light.atalanta.NameOfTable;
import com.light.atalanta.TableName;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.light.atalanta.transport.values.RegionName.REGION_NAME_LENGTH;

public class TableMapObject implements SerializedObject {
    private final NameOfTable tableName;
    private final List<RegionName> regions;

    public TableMapObject(NameOfTable tableName, List<String> regions) {
        Objects.requireNonNull(tableName);
        this.tableName = tableName;
        validateRegionsList(regions);
        this.regions = regions.stream().map(RegionName::new).collect(Collectors.toList());
    }

    public TableMapObject(String namespace, String tableName, List<String> regions) {
        this(TableName.valueOf(namespace, tableName), regions);
    }

    public TableMapObject(byte[] fromBytes) {
        String str = new String(fromBytes);
        String[] splitLine = str.split(":");
        Objects.requireNonNull(splitLine[0]);
        Objects.requireNonNull(splitLine[1]);
        this.tableName = TableName.valueOf(splitLine[0], splitLine[1]);

        List<String> regList = Arrays.asList(splitLine[2].split(","));
        validateRegionsList(regList);

        this.regions = regList.stream().map(RegionName::new).collect(Collectors.toList());
    }

    private void validateRegionsList(List<String> regions) {
        Objects.requireNonNull(regions);
        if (regions.size() == 0) {
            throw new IllegalArgumentException("Regions list is empty");
        }
    }


    public NameOfTable getTableName() {
        return tableName;
    }

    public String getNamespace() {
        return tableName.getNamespace();
    }

    public String getTable() {
        return tableName.getTableName();
    }

    public List<RegionName> getRegionNames() {
        return regions;
    }

    public List<String> getRegionNamesAsString() {
        return regions.stream().map(RegionName::getName).collect(Collectors.toList());
    }

    @Override
    public ByteBuffer serializeObject(ByteBuffer bb) throws IOException {
        bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.put(toString().getBytes(StandardCharsets.UTF_8));
        return bb;
    }

    @Override
    public int getSizeInBytes() {
        int regionCount = regions.size();
        return tableName.toString().getBytes(StandardCharsets.UTF_8).length // table
                + 1 // ":"
                + (REGION_NAME_LENGTH + 2) * regionCount // region_name + "_0"
                + regionCount - 1; // count of "," - 1
    }

    @Override
    public String toString() {
        return tableName + ":" + String.join(",", getRegionNamesAsString());
    }
}
