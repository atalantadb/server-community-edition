/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.NumbersByteHelper;
import com.light.atalanta.exceptions.AtalantaTransportRuntimeException;
import com.light.atalanta.transport.RegionPosition;
import com.light.atalanta.transport.buffers.ByteArrayBuffer;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BulkPutOperation implements BulkOperation, TablePositionedTransportMessage {
    public static final int LENGTH_MESSAGE_SIZE = 4;
    public static final int LENGTH_MESSAGE_TYPE = 1;
    public static final int LENGTH_REGION_POSITION = 4;

    public static final int TRANSPORT_MESSAGE_HEADER_LENGTH = LENGTH_MESSAGE_SIZE + LENGTH_MESSAGE_TYPE + LENGTH_REGION_POSITION;
    private final RegionPosition regionPosition;
    private int tablePosition;
    private final List<PutOperation> puts;
    private final List<PutOperation> operationsView;
    private int messageSize;

    public BulkPutOperation(int regionPosition, int tablePosition) {
        this.regionPosition = new RegionPosition(regionPosition);
        this.tablePosition = tablePosition;
        this.puts = new ArrayList<>();
        this.operationsView = Collections.unmodifiableList(puts);
        this.messageSize = TRANSPORT_MESSAGE_HEADER_LENGTH;
    }

    public BulkPutOperation(byte[] fromBytes) throws NotActualRegionMappingException {
        BulkHelper.checkBulkPut(fromBytes);
        this.regionPosition = new RegionPosition(NumbersByteHelper.fromBytes_int(fromBytes, 5));
        this.tablePosition = BulkHelper.NOT_USE_TABLE_POSITION;
        this.puts = new ArrayList<>();
        this.operationsView = Collections.unmodifiableList(puts);
        this.messageSize = TRANSPORT_MESSAGE_HEADER_LENGTH;
        int pos = 9;
        while (pos < fromBytes.length) {
            int nextSize = OperationHelper.getMessageSize(fromBytes, pos);
            byte[] put = new byte[nextSize];
            System.arraycopy(fromBytes, pos, put, 0, nextSize);
            puts.add(new PutOperation(put, null));
            messageSize += nextSize;
            pos = pos + nextSize;
        }
    }

    public BulkPutOperation(int regionPosition, int tablePosition, List<PutOperation> puts) {
        this.regionPosition = new RegionPosition(regionPosition);
        this.tablePosition = tablePosition;
        if (puts == null) {
            throw new IllegalArgumentException("Gets could not be empty");
        }
        this.puts = new ArrayList<>(puts);
        this.operationsView = Collections.unmodifiableList(this.puts);
        this.messageSize = TRANSPORT_MESSAGE_HEADER_LENGTH;
        for (PutOperation put : this.puts) {
            this.messageSize += put.getMessageSize();
        }
    }

    public void addPutOperation(PutOperation putOperation) {
        this.puts.add(putOperation);
        this.messageSize += putOperation.getMessageSize();
    }

    public List<? extends Operation> getOperations() {
        return operationsView;
    }

    @Override
    public ByteArrayBuffer<TransportMessage> getBuffer() {
        return message -> {
            BulkPutOperation bulkPut = (BulkPutOperation) message;
            return bulkPut.serializeMessage(ByteBuffer.allocate(bulkPut.getSerializedMessageSize()));
        };
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer byteBuffer) throws IOException {
        if (puts.isEmpty()) {
            throw new AtalantaTransportRuntimeException("Puts are empty");
        }
        int messageSize = getSerializedMessageSize();
        ByteBuffer bb = byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(messageSize);
        bb.put(getMessageType().getTypeByte());
        bb.putInt(regionPosition.getPosition());
        if (tablePosition >= 0) {
            bb.putInt(tablePosition);
        }
        for (PutOperation put : puts) {
            put.serializeMessage(bb);
        }
        return bb;
    }

    private int getSerializedMessageSize() {
        return getMessageSize() + (tablePosition >= 0 ? Integer.BYTES : 0);
    }

    @Override
    public int getMessageSize() {
        return messageSize;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.BULK_PUT_OPERATION;
    }

    @Override
    public RegionPosition getRegionPosition() {
        return regionPosition;
    }

    public int getTablePosition() {
        return tablePosition;
    }

    public void setTablePosition(int tablePosition) {
        this.tablePosition = tablePosition;
    }

}
