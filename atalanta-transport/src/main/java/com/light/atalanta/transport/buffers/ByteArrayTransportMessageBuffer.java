/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.buffers;

import com.light.atalanta.transport.messages.TransportMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ByteArrayTransportMessageBuffer implements ByteArrayBuffer<TransportMessage> {

    @Override
    public ByteBuffer serializeMessage(TransportMessage message) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(message.getMessageSize()).order(ByteOrder.LITTLE_ENDIAN);
        return message.serializeMessage(bb);
    }

}
