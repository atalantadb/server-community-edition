/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.pool;

import com.light.atalanta.Configuration;
import com.light.atalanta.Connection;
import com.light.atalanta.client.factories.AtalantaDbConnectionPoolObjectFactory;
import com.light.atalanta.client.factories.AtalantaDbObjectFactory;
import com.light.atalanta.factory.ObjectFactory;

import java.io.IOException;

@Deprecated
public class ATALANTAStaticConnection {
    private Connection connection;

    private ATALANTAStaticConnection() {
    }

    public synchronized void initConnection(Configuration configuration) throws IOException {
        initConnection(configuration, false);
    }

    public synchronized void initConnection(Configuration configuration, boolean isPooled) throws IOException {
        if (this.connection != null) {
            return;
        }
        ObjectFactory objectFactory = isPooled ? new AtalantaDbConnectionPoolObjectFactory() : new AtalantaDbObjectFactory();
        this.connection = objectFactory.connection((Configuration) configuration);
    }

    public Connection getConnection() {
        return this.connection;
    }

    public static ATALANTAStaticConnection getInstance() {
        return SingletonHolder.INSTANCE;
    }
    private static class SingletonHolder {
        private static final ATALANTAStaticConnection INSTANCE = new ATALANTAStaticConnection();
    }


}
