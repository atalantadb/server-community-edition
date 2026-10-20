/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.NumbersByteHelper;
import com.light.atalanta.exceptions.AtalantaTransportRuntimeException;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;
import com.light.atalanta.transport.exceptions.BadDataException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.light.atalanta.transport.messages.RsResponse.LENGTH_MESSAGE_TYPE;

public class BulkResponseOperation implements TransportMessage {
    public static final int LENGTH_MESSAGE_SIZE = 4;

    private final int msgSize;
    private final List<ResultOperation> resultOperations;

    public BulkResponseOperation(byte[] fromBytes) throws NotActualRegionMappingException {
        BulkHelper.checkBulkResult(fromBytes);
        this.msgSize = OperationHelper.getMessageSize(fromBytes);
        if (msgSize != fromBytes.length || msgSize < LENGTH_MESSAGE_SIZE + LENGTH_MESSAGE_TYPE) {
            throw new BadDataException("Bulk result size " + msgSize
                    + " does not match array length " + fromBytes.length);
        }
        int pos = 5;
        int resultCount = 0;
        while (pos < msgSize) {
            int nextSize = getNestedResultSize(fromBytes, pos);
            pos = pos + nextSize;
            resultCount++;
        }

        ResultOperation[] results = new ResultOperation[resultCount];
        pos = 5;
        for (int i = 0; i < resultCount; i++) {
            int nextSize = getNestedResultSize(fromBytes, pos);
            results[i] = generateOperation(fromBytes, pos, nextSize);
            pos += nextSize;
        }
        this.resultOperations = Arrays.asList(results);
    }

    private int getNestedResultSize(byte[] fromBytes, int pos) {
        if (msgSize - pos < LENGTH_MESSAGE_SIZE) {
            throw new BadDataException("Incomplete result size at bulk offset " + pos);
        }
        int nextSize = OperationHelper.getMessageSize(fromBytes, pos);
        if (nextSize < LENGTH_MESSAGE_SIZE + LENGTH_MESSAGE_TYPE || nextSize > msgSize - pos) {
            throw new BadDataException("Invalid nested result size " + nextSize
                    + " at bulk offset " + pos + ", remaining=" + (msgSize - pos));
        }
        return nextSize;
    }

    private ResultOperation generateOperation(byte[] fromBytes, int offset, int length) {
        MessageType type = MessageType.fromByte(fromBytes[offset + LENGTH_MESSAGE_SIZE]);
        if (type == MessageType.GET)
              return ResultOperation.EMPTY;

        if (length > LENGTH_MESSAGE_SIZE + LENGTH_MESSAGE_TYPE) {
            if (length > 13 && fromBytes[offset + 13] == 1) {
                return new BigResultOperation(fromBytes, offset, length);
            } else {
                return new ResultOperation(fromBytes, offset, length);
            }
        } else {
            return EmptyResultOperation.EMPTY_OPERATION;
        }
    }

    public List<ResultOperation> getResultOperations() {
        return resultOperations;
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer byteBuffer) throws IOException {
        if (resultOperations.isEmpty()) {
            throw new AtalantaTransportRuntimeException("Gets are empty");
        }
        byteBuffer.put(NumbersByteHelper.toBytes_int(getMessageSize()));
        byteBuffer.put(getMessageType().getTypeByte());
        for (Operation resultOperation : resultOperations) {
            byteBuffer.put(resultOperation.serializeMessage(ByteBuffer.allocate(resultOperation.getMessageSize())).array());
        }
        return byteBuffer;
    }

    @Override
    public int getMessageSize() {
        return msgSize;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.BULK_RESULT_OPERATION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BulkResponseOperation that = (BulkResponseOperation) o;
        return msgSize == that.msgSize && Objects.equals(resultOperations, that.resultOperations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msgSize, resultOperations);
    }
}
