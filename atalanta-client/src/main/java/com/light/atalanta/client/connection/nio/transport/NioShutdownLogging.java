/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.transport;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BooleanSupplier;

/** Pool-local logging hints only. Does not control transport or request lifecycle. */
public final class NioShutdownLogging {
    private long registeredConnections;
    private long confirmedThrough;
    private final Set<Scope> active = new HashSet<>();

    synchronized BooleanSupplier registerConnection() {
        long id = ++registeredConnections;
        return () -> isExpectedShutdown(id);
    }

    public synchronized Scope begin() {
        Scope scope = new Scope(registeredConnections);
        active.add(scope);
        return scope;
    }

    private synchronized boolean isExpectedShutdown(long id) {
        if (id <= confirmedThrough) return true;
        for (Scope scope : active) {
            if (id <= scope.lastConnection) return true;
        }
        return false;
    }

    public final class Scope implements AutoCloseable {
        private final long lastConnection;
        private boolean confirmed;

        private Scope(long lastConnection) {
            this.lastConnection = lastConnection;
        }

        public void confirm() {
            synchronized (NioShutdownLogging.this) {
                confirmed = true;
            }
        }

        @Override public void close() {
            synchronized (NioShutdownLogging.this) {
                if (active.remove(this) && confirmed) {
                    // Keep late EOFs quiet only for connections existing before this request.
                    // No channel references are retained, and newly registered channels are unaffected.
                    confirmedThrough = Math.max(confirmedThrough, lastConnection);
                }
            }
        }
    }
}
