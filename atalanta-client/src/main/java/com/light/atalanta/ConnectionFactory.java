/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.client.factories.AtalantaDbConnectionPoolObjectFactory;

import java.io.IOException;

import static com.light.atalanta.config.AtalantaProperties.ATALANTA_HBASE_SCAN_FILTER;

public class ConnectionFactory {

    private static final AtalantaDbConnectionPoolObjectFactory factory = new AtalantaDbConnectionPoolObjectFactory();

    public static Connection createConnection(Configuration configuration) throws IOException {
        if (isHBaseFilter(configuration)) {
            Scan.setupNewAPI();
        }
        return factory.connection(configuration);
    }

    private static boolean isHBaseFilter(Configuration configuration) {
        return configuration.getBoolean(ATALANTA_HBASE_SCAN_FILTER);
    }
}
