/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.factories;

import com.light.atalanta.*;
import com.light.atalanta.client.connection.nio.pool.ConnectionPoolNio;
import com.light.atalanta.client.connection.nio.pool.reusable.ReusableConnectionPool;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.light.atalanta.config.AtalantaProperties.*;


 // Creates independent NIO connections.
 // Applications should keep one connection for their whole lifetime instead of calling the factory repeatedly.
 // The legacy property atalanta.connection.pool.reusable has no effect on NIO.


public class AtalantaDbReusableClientObjectFactory extends AtalantaDbObjectFactory {

    private static volatile Map<DbConfiguration, Connection> reusableConnectionMap = new ConcurrentHashMap<>();

    @Override
    public Connection connection(Configuration configuration) {
            return getReusableConnection(configuration);
    }

    private Connection getReusableConnection(DbConfiguration configuration) {
        if (reusableConnectionMap.get(configuration) == null || reusableConnectionMap.get(configuration).isClosed()) {
            synchronized (AtalantaDbReusableClientObjectFactory.class) {
                if (reusableConnectionMap.get(configuration) == null || reusableConnectionMap.get(configuration).isClosed()) {
                    reusableConnectionMap.put(configuration, getConnection(configuration));
                }
            }
        }
        ReusableConnectionPool res = (ReusableConnectionPool)reusableConnectionMap.get(configuration);
        res.incrementRefCounter();
        return res;
    }

    private ConnectionPoolNio getConnection(DbConfiguration configuration) {
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
            return new ReusableConnectionPool(configuration);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isHBaseFilter(DbConfiguration configuration) {
        return configuration.getBoolean(ATALANTA_HBASE_SCAN_FILTER);
    }

}
