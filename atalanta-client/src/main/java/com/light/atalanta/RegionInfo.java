/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

public class RegionInfo implements DbRegionInfo {
    private final TableName tableName;
    private final String name;

    public RegionInfo(TableName tableName, String name) {
        this.tableName = tableName;
        this.name = name;
    }

    @Override
    public TableName getTable() {
        return tableName;
    }

    @Override
    public String getRegionName() {
        return name;
    }

    @Override
    public byte[] getStartKey() {
        return new byte[0];
    }

    @Override
    public byte[] getEndKey() {
        return new byte[0];
    }
}
