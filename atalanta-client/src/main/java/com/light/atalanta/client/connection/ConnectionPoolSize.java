/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection;

import com.light.atalanta.Configuration;

import static com.light.atalanta.config.AtalantaProperties.ATALANTA_CONNECTION_POOL_SIZE;

public final class ConnectionPoolSize {
    private ConnectionPoolSize() {
    }

    public static int resolve(Configuration configuration) {
        int poolSize = configuration.getInt(ATALANTA_CONNECTION_POOL_SIZE);
        if (poolSize == 0) {
            poolSize = Runtime.getRuntime().availableProcessors() * 2;
            configuration.set(ATALANTA_CONNECTION_POOL_SIZE, poolSize);
        }
        return poolSize;
    }
}
