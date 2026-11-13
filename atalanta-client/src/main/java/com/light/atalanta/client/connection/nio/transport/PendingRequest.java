/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.transport;

import com.light.atalanta.NameOfTable;

import java.util.concurrent.CompletableFuture;

final class PendingRequest {
    final CompletableFuture<byte[]> future;
    final NameOfTable tableName;
    final boolean regionMapRefresh;
    final long deadlineNanos;

    PendingRequest(CompletableFuture<byte[]> future, NameOfTable tableName, boolean regionMapRefresh,
                   long deadlineNanos) {
        this.future = future;
        this.tableName = tableName;
        this.regionMapRefresh = regionMapRefresh;
        this.deadlineNanos = deadlineNanos;
    }
}
