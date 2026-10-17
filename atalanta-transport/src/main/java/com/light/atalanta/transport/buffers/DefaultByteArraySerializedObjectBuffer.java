/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.buffers;

import com.light.atalanta.transport.values.SerializedObject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DefaultByteArraySerializedObjectBuffer implements ByteArrayBuffer<SerializedObject> {
    @Override
    public ByteBuffer serializeMessage(SerializedObject message) throws IOException {
        return message.serializeObject(ByteBuffer.allocate(message.getSizeInBytes())
                .order(ByteOrder.LITTLE_ENDIAN));
    }
}
