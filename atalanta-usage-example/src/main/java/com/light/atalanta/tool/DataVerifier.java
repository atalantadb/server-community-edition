/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.tool;

import com.light.atalanta.Configuration;
import com.light.atalanta.Connection;
import com.light.atalanta.Table;
import com.light.atalanta.TableName;
import com.light.atalanta.factory.ObjectFactory;
import java.io.IOException;
import java.nio.file.Path;

/** Performs table-wide verification operations. */
public final class DataVerifier {

    public long count(Path configurationPath, DataToolConfig toolConfig) throws IOException {
        ObjectFactory factory = DatabaseInitializer.createObjectFactory(
                toolConfig.getConnectionType());
        Configuration clientConfig = factory.configuration();
        clientConfig.addResource(configurationPath.toFile());
        TableName tableName = factory.tableName(DataToolSchema.NAMESPACE, DataToolSchema.TABLE);

        try (Connection connection = factory.connection(clientConfig)) {
            Table table = connection.getTable(tableName);
            return table.count();
        }
    }
}
