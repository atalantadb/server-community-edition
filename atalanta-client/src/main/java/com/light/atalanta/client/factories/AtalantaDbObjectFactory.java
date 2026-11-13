/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.factories;

import com.light.atalanta.*;
import com.light.atalanta.factory.ObjectFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static com.light.atalanta.config.AtalantaProperties.ATALANTA_CONNECTION_RETRY_COUNT;
import static com.light.atalanta.config.AtalantaProperties.ATALANTA_CONNECTION_RETRY_DELAY_MS;

public class AtalantaDbObjectFactory implements ObjectFactory {

    @Override
    public TableName tableName(String name) {
        return TableName.fromString(name);
    }

    @Override
    public TableName tableName(String namespace, String tableName) {
        return TableName.valueOf(namespace, tableName);
    }

    @Override
    public Connection connection(Configuration configuration) throws IOException {
        return ConnectionFactoryThreadLocal.createConnection(configuration);
    }

    private ConnectionThreadLocal connectionInternal(Configuration configuration) {
        try {
            return ConnectionFactoryThreadLocal.createConnection(configuration);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Configuration configuration() {
        return (Configuration) Configuration.create();
    }

    @Override
    public Configuration configuration(Configuration anotherConfiguration) {
        return (Configuration) Configuration.copyConfiguration(anotherConfiguration);
    }

    @Override
    public Result emptyResult() {
        return Result.EMPTY_RESULT;
    }

    @Override
    public Put put(byte[] rowKey) {
        return new Put(rowKey);
    }

    @Override
    public Put put(byte[] rowKey, Long ts) {
        return new Put(rowKey, ts);
    }

    @Override
    public Scan scan() {
        return new Scan();
    }

    @Override
    public DbSingleColumnValueFilter singleColumnValueFilter(byte[] family, byte[] qualifier, DbCompareFilter.CompareOp compareOp, byte[] value) {
        return new SingleColumnValueFilter(family, qualifier, compareOp, value);
    }

    @Override
    public Get get(byte[] key) {
        return new Get(key);
    }

    @Override
    public Delete delete(byte[] key) {
        return new Delete(key);
    }

    @Override
    public Delete delete(byte[] key, long ts) {
        return new Delete(key, ts);
    }

    @Override
    public Append append(byte[] key) {
        return new Put(key);
    }

    @Override
    public TableDescriptor tableDescriptor(TableName tableName) {
        return new TableDescriptor(tableName);
    }

    @Override
    public ColumnDescriptor columnDescriptor(byte[] familyName) {
        return new ColumnDescriptor(familyName);
    }

    @Override
    public ColumnDescriptor columnDescriptor(String familyName) {
        return new ColumnDescriptor(familyName.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public NamespaceDescriptor namespaceDescriptor(String namespace) {
        return new NamespaceDescriptor(namespace);
    }

    @Override
    public SnapshotDescription snapshotDescription(TableName tableName, String snapshotName) {
        return new SnapshotDescription(tableName, snapshotName);
    }

    @Override
    public RowMutations rowMutations(byte[] rowKey) {
        return new RowMutations(rowKey);
    }


}
