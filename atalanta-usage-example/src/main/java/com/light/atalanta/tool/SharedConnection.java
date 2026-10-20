/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.tool;

import com.light.atalanta.Configuration;
import com.light.atalanta.Connection;
import com.light.atalanta.factory.ObjectFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * One process-wide connection shared by all writer workers. The lifecycle
 * follows the reference-counting pattern used by AtalantaClient.
 */
final class SharedConnection {

    private final AtomicInteger threadCount = new AtomicInteger();
    private final ObjectFactory factory;
    private final Path configurationPath;
    private Connection connection;

    SharedConnection(ObjectFactory factory, Path configurationPath) {
        this.factory = factory;
        this.configurationPath = configurationPath;
    }

    Connection acquire() throws IOException {
        threadCount.getAndIncrement();
        synchronized (threadCount) {
            if (connection == null) {
                try {
                    Configuration configuration = factory.configuration();
                    configuration.addResource(configurationPath.toFile());
                    connection = factory.connection(configuration);
                } catch (IOException | RuntimeException e) {
                    threadCount.decrementAndGet();
                    throw e;
                }
            }
            return connection;
        }
    }

    void release() throws IOException {
        int remaining = threadCount.decrementAndGet();
        if (remaining < 0) {
            throw new IllegalStateException("Connection reference count became negative.");
        }
        if (remaining == 0) {
            synchronized (threadCount) {
                if (threadCount.get() == 0 && connection != null) {
                    Connection connectionToClose = connection;
                    connection = null;
                    connectionToClose.close();
                }
            }
        }
    }

    int getReferenceCount() {
        return threadCount.get();
    }
}
