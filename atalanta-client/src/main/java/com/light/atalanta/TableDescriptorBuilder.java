/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;


public class TableDescriptorBuilder  {

    private TableName tableName;

    private DbColumnFamilyDescriptor columnFamilyDescriptor;

    protected TableDescriptorBuilder() {

    }
    public TableDescriptorBuilder(TableName tableName) {
        this.tableName = tableName;
    }

    public static TableDescriptorBuilder newBuilder(TableName tableName) {
        return new TableDescriptorBuilder(tableName);
    }

    public TableDescriptorBuilder setColumnFamily(DbColumnFamilyDescriptor columnFamilyDescriptor) {
        this.columnFamilyDescriptor = columnFamilyDescriptor;
        return this;
    }

    public TableDescriptor build() {
        TableDescriptor res = new TableDescriptor(tableName);
        res.addFamily(new ColumnDescriptor(columnFamilyDescriptor.getName()));
        return res;
    }
}
