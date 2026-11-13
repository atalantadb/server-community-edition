/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.api.DbSnapshotDescription;

public class SnapshotDescription implements DbSnapshotDescription {

    private final String name;
    private final TableName tableName;

    protected SnapshotDescription() {
        name = null;
        tableName = null;
    }

    public SnapshotDescription(TableName tableName, String snapshotName) {
        name = snapshotName;
        this.tableName = tableName;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public TableName getTableName() {
        return tableName;
    }
}
