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
import java.util.Objects;

public class BulkGetOperation implements BulkOperation, TablePositionedTransportMessage {
    public static final int LENGTH_MESSAGE_SIZE = 4;
    public static final int LENGTH_MESSAGE_TYPE = 1;
    public static final int LENGTH_REGION_POSITION = 4;

    public static final int TRANSPORT_MESSAGE_HEADER_LENGTH = LENGTH_MESSAGE_SIZE + LENGTH_MESSAGE_TYPE + LENGTH_REGION_POSITION;
    private final RegionPosition regionPosition;
    private int tablePosition;
    private final List<GetOperation> gets;
    private final List<GetOperation> operationsView;
    private int messageSize;

    public BulkGetOperation(int regionPosition, int tablePosition) {
        this.regionPosition = new RegionPosition(regionPosition);
        this.tablePosition = tablePosition;
        this.gets = new ArrayList<>();
        this.operationsView = Collections.unmodifiableList(gets);
        this.messageSize = TRANSPORT_MESSAGE_HEADER_LENGTH;
    }

    public BulkGetOperation(byte[] fromBytes, int tablePosition) throws NotActualRegionMappingException {
        this.tablePosition = tablePosition;
        BulkHelper.checkBulkGet(fromBytes);
        this.regionPosition = new RegionPosition(NumbersByteHelper.fromBytes_int(fromBytes, 5));
        this.gets = new ArrayList<>();
        this.operationsView = Collections.unmodifiableList(gets);
        this.messageSize = TRANSPORT_MESSAGE_HEADER_LENGTH;
        int pos = TRANSPORT_MESSAGE_HEADER_LENGTH + (tablePosition >= 0 ? Integer.BYTES : 0);
        while (pos < fromBytes.length) {
            int nextSize = OperationHelper.getMessageSize(fromBytes, pos);
            byte[] get = new byte[nextSize];
            System.arraycopy(fromBytes, pos, get, 0, nextSize);
            gets.add(new GetOperation(get, null));
            messageSize = Math.addExact(messageSize, nextSize);
            pos = pos + nextSize;
        }
    }

    public BulkGetOperation(int regionPosition, int tablePosition, List<GetOperation> gets) {
        this.regionPosition = new RegionPosition(regionPosition);
        this.tablePosition = tablePosition;
        if (gets == null) {
            throw new IllegalArgumentException("Gets could not be empty");
        }
        this.gets = new ArrayList<>(gets);
        this.operationsView = Collections.unmodifiableList(this.gets);
        this.messageSize = TRANSPORT_MESSAGE_HEADER_LENGTH;
        for (GetOperation get : this.gets) {
            messageSize = Math.addExact(messageSize, get.getMessageSize());
        }
    }

    public void addGetOperation(GetOperation getOperation) {
        this.gets.add(getOperation);
        messageSize = Math.addExact(messageSize, getOperation.getMessageSize());
    }

    public List<GetOperation> getOperations() {
        return operationsView;
    }

    @Override
    public ByteArrayBuffer<TransportMessage> getBuffer() {
        return message -> {
            BulkGetOperation bulkGet = (BulkGetOperation) message;
            return bulkGet.serializeMessage(ByteBuffer.allocate(bulkGet.getSerializedMessageSize())
                    .order(ByteOrder.LITTLE_ENDIAN));
        };
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer byteBuffer) throws IOException {
        if (gets.isEmpty()) {
            throw new AtalantaTransportRuntimeException("Gets are empty");
        }
        int serializedMessageSize = getSerializedMessageSize();
        ByteBuffer bb = byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(serializedMessageSize);
        bb.put(getMessageType().getTypeByte());
        bb.putInt(regionPosition.getPosition());
        if (tablePosition >= 0) {
            bb.putInt(tablePosition);
        }
        for (GetOperation get : gets) {
            get.serializeMessage(bb);
        }
        return bb;
    }

    private int getSerializedMessageSize() {
        return tablePosition >= 0 ? Math.addExact(messageSize, Integer.BYTES) : messageSize;
    }

    @Override
    public int getMessageSize() {
        return messageSize;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.BULK_GET_OPERATION;
    }

    @Override
    public RegionPosition getRegionPosition() {
        return regionPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BulkGetOperation that = (BulkGetOperation) o;
        return Objects.equals(regionPosition, that.regionPosition) && Objects.equals(gets, that.gets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(regionPosition, gets);
    }

    @Override
    public void setTablePosition(int tablePosition) {
        this.tablePosition = tablePosition;
    }

    @Override
    public int getTablePosition() {
        return tablePosition;
    }
}
