/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.transport.values.KeyValues;

import java.io.IOException;
import java.nio.ByteBuffer;

public class EmptyResultOperation extends ResultOperation {

    public static final byte[] EMPTY_OPERATION_BYTES = new byte[]{5, 0, 0, 0, 0};
    public static final ResultOperation EMPTY_OPERATION = new EmptyResultOperation();

    private static final KeyValues[] EMPTY_KEY_VALUES = new KeyValues[0];

    private EmptyResultOperation() {
        super();
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer byteBuffer) throws IOException {
        byteBuffer.put(EMPTY_OPERATION_BYTES);
        return byteBuffer;
    }

    @Override
    public int getMessageSize() {
        return 5;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.EMPTY_MESSAGE;
    }

    @Override
    public KeyValues[] getValues() {
        return EMPTY_KEY_VALUES;
    }
}
