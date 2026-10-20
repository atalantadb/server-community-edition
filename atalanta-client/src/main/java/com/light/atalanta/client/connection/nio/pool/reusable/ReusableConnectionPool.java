/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.pool.reusable;

import com.light.atalanta.Configuration;
import com.light.atalanta.client.connection.nio.pool.ConnectionPoolNio;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

public class ReusableConnectionPool extends ConnectionPoolNio {
    private final ReentrantLock lock = new ReentrantLock();
    private int refCounter = 1;
    private boolean closed = false;

    public ReusableConnectionPool(Configuration configuration) throws IOException {
        super(configuration);
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (closed) {
                return;
            }

            refCounter--;
            if (refCounter <= 0) {
                closed = true;
                super.close();
            }
        } finally {
            lock.unlock();
        }
    }

    public void incrementRefCounter() {
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("Connection pool is already closed");
            }
            refCounter++;
        } finally {
            lock.unlock();
        }
    }
}
