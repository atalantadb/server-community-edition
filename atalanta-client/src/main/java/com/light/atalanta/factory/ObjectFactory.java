/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.factory;

import com.light.atalanta.*;
import org.apache.commons.lang3.NotImplementedException;

import java.io.IOException;

public interface ObjectFactory {

    TableName tableName(String name);

    TableName tableName(String namespace, String tableName);

    Connection connection(Configuration configuration) throws IOException;

    Configuration configuration();
    Configuration configuration(Configuration configuration);

    Result emptyResult();

    Put put(byte[] rowKey);

    Put put(byte[] rowKey, Long ts);

    Scan scan();

    DbSingleColumnValueFilter singleColumnValueFilter(byte[] family, byte[] qualifier, DbCompareFilter.CompareOp compareOp, byte[] value);

    Get get(byte[] key);

    Delete delete(byte[] key);

    Delete delete(byte[] key, long ts);

    Append append(byte[] key);

    TableDescriptor tableDescriptor(TableName tableName);

    DbColumnDescriptor columnDescriptor(byte[] familyName);

    DbColumnDescriptor columnDescriptor(String familyName);

    NamespaceDescriptor namespaceDescriptor(String namespace);

    SnapshotDescription snapshotDescription(TableName tableName, String snapshotName);

    RowMutations rowMutations(byte[] rowKey);
}
