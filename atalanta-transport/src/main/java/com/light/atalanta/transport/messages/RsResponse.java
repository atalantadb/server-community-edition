/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.NumbersByteHelper;
import com.light.atalanta.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class RsResponse implements TransportMessage {
    public static final byte ANSWER_OK = 1;
    public static final byte ANSWER_FAIL = 0;

    public static final int LENGTH_MESSAGE_SIZE = 4;
    public static final int LENGTH_MESSAGE_TYPE = 1;
    public static final int LENGTH_ANSWER = 1;

    private final int msgSize;
    private final MessageType msgType;
    private final byte answer;

    public RsResponse(byte[] fromBytes) {
        this.msgSize = OperationHelper.getMessageSize(fromBytes);
        this.msgType = OperationHelper.getTransportMessageType(fromBytes);
        this.answer = fromBytes[5];
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer byteBuffer) throws IOException {
        byteBuffer.put(NumbersByteHelper.toBytes_int(msgSize));
        byteBuffer.put(msgType.getTypeByte());
        byteBuffer.put(answer);
        return byteBuffer;
    }

    @Override
    public int getMessageSize() {
        return msgSize;
    }

    @Override
    public MessageType getMessageType() {
        return msgType;
    }

    public boolean isPositiveAnswer() {
        return answer == ANSWER_OK;
    }

    public boolean isNegativeAnswer() {
        return !isPositiveAnswer();
    }

    @Override
    public String toString() {
        return "RsResponse{" +
                "msgSize=" + msgSize +
                ", msgType=" + msgType +
                ", answer=" + answer +
                '}';
    }
}
