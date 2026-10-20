/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.buffers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

public interface ByteArrayBuffer<T> {
    int MAX_BUFFER_SIZE = 64*1024;

    ByteBuffer serializeMessage(T message) throws IOException;

    default byte[] serialize(T message) throws IOException {
        return serializeMessage(message).array();
    }

    default byte[] serializeMessage(T message, Supplier<Integer> s) throws IOException {
        int size = s.get();
        ByteBuffer bb = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        return bb.put(serialize(message), 0, size).array();
    }
}
