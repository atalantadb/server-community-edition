/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.NumbersByteHelper;

import java.io.IOException;
import java.nio.ByteBuffer;

public class EmptyAuthRequest implements TransportMessage {
    private static final int EMPTY_MESSAGE_SIZE = 5;
    private static final MessageType MESSAGE_TYPE = MessageType.AUTH_NONE;

    private static final EmptyAuthRequest INSTANCE = new EmptyAuthRequest();

    private EmptyAuthRequest() {
    }

    public static EmptyAuthRequest getInstance() {
        return INSTANCE;
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer bb) throws IOException {
        bb.put(NumbersByteHelper.toBytes_int(EMPTY_MESSAGE_SIZE));
        bb.put(MESSAGE_TYPE.getTypeByte());

        return bb;
    }

    @Override
    public int getMessageSize() {
        return EMPTY_MESSAGE_SIZE;
    }

    @Override
    public MessageType getMessageType() {
        return MESSAGE_TYPE;
    }
}

