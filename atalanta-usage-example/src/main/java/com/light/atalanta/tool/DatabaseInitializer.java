/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.tool;

import com.light.atalanta.Configuration;
import com.light.atalanta.Connection;
import com.light.atalanta.DbColumnDescriptor;
import com.light.atalanta.TableName;
import com.light.atalanta.api.DbAdmin;
import com.light.atalanta.api.DbTableDescriptor;
import com.light.atalanta.client.factories.AtalantaDbNioClientObjectFactory;
import com.light.atalanta.client.factories.AtalantaDbObjectFactory;
import com.light.atalanta.factory.ObjectFactory;
import com.light.atalanta.model.CompressType;
import com.light.atalanta.model.RecordCompressionType;
import com.light.atalanta.util.UniformSplit;
import java.io.IOException;
import java.nio.file.Path;

/** Opens the selected client connection and creates the workload table. */
public final class DatabaseInitializer {

    public Result initialize(Path configurationPath, DataToolConfig toolConfig) throws IOException {
        ObjectFactory factory = createObjectFactory(toolConfig.getConnectionType());
        Configuration clientConfig = factory.configuration();
        clientConfig.addResource(configurationPath.toFile());

        try (Connection connection = factory.connection(clientConfig);
             DbAdmin admin = connection.getAdmin()) {
            boolean namespaceCreated = createNamespaceIfMissing(admin);
            boolean tableCreated = createTableIfMissing(admin, factory, toolConfig);
            return new Result(namespaceCreated, tableCreated);
        }
    }

    static ObjectFactory createObjectFactory(ConnectionType connectionType) {
        if (connectionType == ConnectionType.NIO) {
            return new AtalantaDbNioClientObjectFactory();
        }
        return new AtalantaDbObjectFactory();
    }

    private boolean createNamespaceIfMissing(DbAdmin admin) throws IOException {
        if (admin.isNamespaceExists(DataToolSchema.NAMESPACE)) {
            return false;
        }
        admin.createNamespace(DataToolSchema.NAMESPACE);
        return true;
    }

    private boolean createTableIfMissing(DbAdmin admin,
                                         ObjectFactory factory,
                                         DataToolConfig toolConfig) throws IOException {
        TableName tableName = factory.tableName(DataToolSchema.NAMESPACE, DataToolSchema.TABLE);
        if (admin.tableExists(tableName)) {
            return false;
        }

        DbTableDescriptor tableDescriptor = factory.tableDescriptor(tableName);
        tableDescriptor.addFamily(createColumnDescriptor(factory, toolConfig));
        admin.createTable(tableDescriptor, createSplitKeys(toolConfig.getRegionCount()));
        return true;
    }

    static DbColumnDescriptor createColumnDescriptor(ObjectFactory factory,
                                                      DataToolConfig toolConfig) {
        DbColumnDescriptor column = factory.columnDescriptor(DataToolSchema.COLUMN_FAMILY);
        column.setBlocksize(2);
        column.setCompressionType(CompressType.none);
        column.setRecordCompressionType(toolConfig.isRecordCompressionEnabled()
                ? RecordCompressionType.lz
                : RecordCompressionType.none);
        column.setDictionary(toolConfig.isQualifierDictionaryEnabled(
                DataToolSchema.totalQualifierBytes()));
        return column;
    }

    static byte[][] createSplitKeys(int regionCount) {
        if (regionCount == 1) {
            return new byte[0][0];
        }
        return new UniformSplit().split(regionCount);
    }

    /** Result of the idempotent namespace/table initialization. */
    public static final class Result {
        private final boolean namespaceCreated;
        private final boolean tableCreated;

        Result(boolean namespaceCreated, boolean tableCreated) {
            this.namespaceCreated = namespaceCreated;
            this.tableCreated = tableCreated;
        }

        public boolean isNamespaceCreated() {
            return namespaceCreated;
        }

        public boolean isTableCreated() {
            return tableCreated;
        }
    }
}
