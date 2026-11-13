/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import java.io.IOException;

import static com.light.atalanta.config.AtalantaProperties.ATALANTA_HBASE_SCAN_FILTER;

public final class ConnectionFactoryThreadLocal {

    public static ConnectionThreadLocal createConnection(DbConfiguration conf) throws IOException {
        if (isHBaseFilter(conf)) {
            Scan.setupNewAPI();
        }
        return new ConnectionThreadLocal(conf);
    }

    private ConnectionFactoryThreadLocal() {
    }

    private static boolean isHBaseFilter(DbConfiguration configuration) {
        return configuration.getBoolean(ATALANTA_HBASE_SCAN_FILTER);
    }
}
