/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.decoding;

import com.light.atalanta.transport.messages.TransportMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class NoOpCompressor implements Compressor {
    @Override
    public byte[] compressBytes(byte[] bytes) throws IOException {
        return bytes;
    }

    @Override
    public byte[] compressMessage(TransportMessage message) throws IOException {
        ByteBuffer buffer = message.serializeMessage(ByteBuffer.allocate(message.getMessageSize())
                .order(ByteOrder.LITTLE_ENDIAN));
        return buffer.position() == buffer.array().length
                ? buffer.array()
                : Arrays.copyOfRange(buffer.array(), buffer.arrayOffset(),
                buffer.arrayOffset() + buffer.position());
    }
}
