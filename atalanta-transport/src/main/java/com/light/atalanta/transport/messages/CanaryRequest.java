/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.transport.buffers.ByteArrayBuffer;
import com.light.atalanta.transport.buffers.TransportBufferFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CanaryRequest implements TransportMessage {
    private static final String REQUEST_MESSAGE = "ping";
    private static final byte[] REQUEST_MESSAGE_BYTES = REQUEST_MESSAGE.getBytes(UTF_8);

    @Override
    public ByteBuffer serializeMessage(ByteBuffer byteBuffer) throws IOException {
        byteBuffer.put(REQUEST_MESSAGE_BYTES);
        return byteBuffer;
    }

    @Override
    public int getMessageSize() {
        return REQUEST_MESSAGE_BYTES.length;
    }

    @Override
    public MessageType getMessageType() {
        throw new IllegalStateException("Method is not implemented.");
    }

    @Override
    public ByteArrayBuffer<TransportMessage> getBuffer() {
        return TransportBufferFactory.getTransportBuffer();
    }
}
