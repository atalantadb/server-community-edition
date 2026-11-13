/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.factories;

import com.light.atalanta.*;
import com.light.atalanta.client.connection.nio.pool.ConnectionPoolNio;

import java.io.IOException;

import static com.light.atalanta.config.AtalantaProperties.*;


 // Creates independent NIO connections.
 // Applications should keep one connection for their whole lifetime instead of calling the factory repeatedly.
 // The legacy property atalanta.connection.pool.reusable has no effect on NIO.


public class AtalantaDbNioClientObjectFactory extends AtalantaDbObjectFactory {
    @Override
    public Connection connection(Configuration configuration) {
        return getConnection(configuration);
    }

    ConnectionPoolNio getConnection(DbConfiguration configuration) {
        if (isHBaseFilter(configuration)) {
            Scan.setupNewAPI();
        }
        RepeatableExecutor repeatableExecutor = new RepeatableExecutor();
        return repeatableExecutor
                .withMaxRetries(configuration.getInt(ATALANTA_CONNECTION_RETRY_COUNT))
                .withStaticTimeout(configuration.getInt(ATALANTA_CONNECTION_RETRY_DELAY_MS))
                .execute(() -> connectionInternal((Configuration) configuration));
    }

    private static ConnectionPoolNio connectionInternal(Configuration configuration) {
        try {
            return new ConnectionPoolNio(configuration);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isHBaseFilter(DbConfiguration configuration) {
        return configuration.getBoolean(ATALANTA_HBASE_SCAN_FILTER);
    }

}
