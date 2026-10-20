/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.api.DbTableDescriptor;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class TableDescriptor implements DbTableDescriptor {

    private final TableName tableName;
    private final Set<DbColumnDescriptor> columnDescriptors = new HashSet<>();

    protected TableDescriptor() {
        tableName = null;
    }

    public TableDescriptor(TableName tableName) {
        this.tableName = tableName;
    }

    @Override
    public TableName getTableName() {
        return tableName;
    }

    @Override
    public DbTableDescriptor addFamily(DbColumnDescriptor columnDescriptor) {
        this.columnDescriptors.add(columnDescriptor);
        return this;
    }

    @Override
    public Collection<DbColumnDescriptor> getFamilies() {
        return columnDescriptors;
    }
}
