/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.buffers;

import com.light.atalanta.transport.values.QualifierValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ByteArrayQualifierValueBuffer implements ByteArrayBuffer<QualifierValue> {
    private final ByteBuffer CURRENT_BUFFER = ByteBuffer.allocate(MAX_BUFFER_SIZE)
            .order(ByteOrder.LITTLE_ENDIAN);

    @Override
    public ByteBuffer serializeMessage(QualifierValue message) throws IOException {
        ByteBuffer bb = CURRENT_BUFFER.order(ByteOrder.LITTLE_ENDIAN);
        bb.position(0);
        return message.serializeObject(bb);
    }
}
