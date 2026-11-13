/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.transport;

import com.light.atalanta.NameOfTable;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.BooleanSupplier;

/**
 * Per-channel connection state.
 */
class ChannelContext {
    final Queue<PendingRequest> pendingRequests = new ConcurrentLinkedQueue<>();
    final Consumer<NameOfTable> invalidRegionMapHandler;
    final Consumer<Throwable> terminalConnectionFailureHandler;
    BooleanSupplier expectedShutdown = () -> false;

    boolean isExpectedIdleShutdown() {
        return expectedShutdown.getAsBoolean() && pendingRequests.isEmpty();
    }

    final ByteBuffer netReadBuffer;

    ByteBuffer accumulator = ByteBuffer.allocate(16384);

    ChannelContext(Consumer<NameOfTable> invalidRegionMapHandler,
                   Consumer<Throwable> terminalConnectionFailureHandler) {
        this.invalidRegionMapHandler = invalidRegionMapHandler;
        this.terminalConnectionFailureHandler = terminalConnectionFailureHandler;
        
            this.netReadBuffer = ByteBuffer.allocate(8192);
    }

}
