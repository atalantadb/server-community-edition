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

public interface TransportMessage {


    // Writes the message at the current buffer position.
    // Implementations must return the same instance and must not clear or rewind the buffer.
    ByteBuffer serializeMessage(ByteBuffer byteBuffer) throws IOException;

    int getMessageSize();

    MessageType getMessageType();

    default ByteArrayBuffer<TransportMessage> getBuffer() {
        return TransportBufferFactory.getDefaultTransportBuffer();
    }

}
