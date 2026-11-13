/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.transport;

import com.light.atalanta.NameOfTable;
import com.light.atalanta.Pair;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;
import com.light.atalanta.transport.exceptions.UnavailableServerException;
import com.light.atalanta.transport.messages.BulkPutOperation;
import com.light.atalanta.transport.messages.Operation;
import com.light.atalanta.transport.messages.PutOperation;
import com.light.atalanta.transport.messages.TransportMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface TRSClientAsyncConnection {
    CompletableFuture<byte[]> sendBytes(ByteBuffer bb, int length) throws IOException, NotActualRegionMappingException, UnavailableServerException;

    CompletableFuture<byte[]> sendMessage0(TransportMessage message, NameOfTable tableName) throws IOException;

    List<Pair<List<PutOperation>, CompletableFuture<byte[]>>> sendBulkPutAsync(BulkPutOperation bulkPut,
                                                                                NameOfTable tableName) throws IOException;
    void flushBulkPut(List<CompletableFuture<byte[]>> responses) throws NotActualRegionMappingException;

    CompletableFuture<Void> refreshRegionMap(NameOfTable tableName);

    int getRegionPosition(NameOfTable tableName, byte[] rowKey);
}
