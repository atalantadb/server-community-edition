/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.factories;

import com.light.atalanta.*;
import com.light.atalanta.client.connection.pool.ConnectionPool;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.light.atalanta.config.AtalantaProperties.*;

@Deprecated
public class AtalantaDbConnectionPoolObjectFactory extends AtalantaDbObjectFactory {

    private static volatile Map<DbConfiguration, Connection> reusableConnectionMap = new ConcurrentHashMap<>();

    @Override
    public Connection connection(Configuration configuration) {
        if (configuration.getBoolean(ATALANTA_CONNECTION_POOL_REUSABLE)) {
            return getReusableConnection(configuration);
        }
        return getConnection(configuration);
    }

    private Connection getReusableConnection(DbConfiguration configuration) {
        if (reusableConnectionMap.get(configuration) == null || reusableConnectionMap.get(configuration).isClosed()) {
            synchronized (AtalantaDbConnectionPoolObjectFactory.class) {
                if (reusableConnectionMap.get(configuration) == null || reusableConnectionMap.get(configuration).isClosed()) {
                    reusableConnectionMap.put(configuration, getConnection(configuration));
                }
            }
        }
        return reusableConnectionMap.get(configuration);
    }

    private ConnectionPool getConnection(DbConfiguration configuration) {
        if (isHBaseFilter(configuration)) {
            Scan.setupNewAPI();
        }
        RepeatableExecutor repeatableExecutor = new RepeatableExecutor();
        return repeatableExecutor
                .withMaxRetries(configuration.getInt(ATALANTA_CONNECTION_RETRY_COUNT))
                .withStaticTimeout(configuration.getInt(ATALANTA_CONNECTION_RETRY_DELAY_MS))
                .execute(() -> connectionInternal((Configuration) configuration));
    }

    private static ConnectionPool connectionInternal(Configuration configuration) {
        try {
            return new ConnectionPool(configuration);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isHBaseFilter(DbConfiguration configuration) {
        return configuration.getBoolean(ATALANTA_HBASE_SCAN_FILTER);
    }

}
