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
import java.util.Objects;

public class ClientAuthResultResponse implements TransportMessage {
    public static final int LENGTH_MESSAGE_SIZE = 4;
    public static final int LENGTH_AUTH_TYPE = 1;
    public static final int LENGTH_AUTH_RESULT = 1;

    private final int msgSize;
    private final MessageType authType;
    private final AuthResult result;

    ClientAuthResultResponse(AuthResult result, MessageType authType) {
        this.msgSize = LENGTH_MESSAGE_SIZE + LENGTH_AUTH_TYPE + LENGTH_AUTH_RESULT;
        this.result = result;
        this.authType = authType;
    }

    public ClientAuthResultResponse(byte[] fromBytes) {
        this.msgSize = OperationHelper.getMessageSize(fromBytes);
        this.authType = MessageType.fromByte(fromBytes[4]);
        this.result = AuthResult.fromByte(fromBytes[5]);
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer bb) throws IOException {
        bb.put(NumbersByteHelper.toBytes_int(msgSize));
        bb.put(authType.getTypeByte());
        bb.put(result.getCode());

        return bb;
    }

    @Override
    public int getMessageSize() {
        return msgSize;
    }

    @Override
    public MessageType getMessageType() {
        return authType;
    }

    public AuthResult getResult(){
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientAuthResultResponse that = (ClientAuthResultResponse) o;
        return msgSize == that.msgSize && authType == that.authType && result == that.result;
    }

    @Override
    public int hashCode() {
        return Objects.hash(msgSize, authType, result);
    }
}
