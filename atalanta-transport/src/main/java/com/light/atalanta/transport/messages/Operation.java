/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.RowKey;
import com.light.atalanta.NumbersByteHelper;
import com.light.atalanta.transport.RegionPosition;
import com.light.atalanta.transport.buffers.ByteArrayBuffer;
import com.light.atalanta.transport.buffers.TransportBufferFactory;
import com.light.atalanta.transport.values.KeyValues;
import com.light.atalanta.transport.values.KeyValuesHelper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

import static com.light.atalanta.transport.messages.BulkHelper.NOT_USE_TABLE_POSITION;
import static com.light.atalanta.transport.values.KeyValues.KEY_VALUES_OBJECT_HEADER_LENGTH;

public abstract class Operation implements PositionedTransportMessage, TablePositionedTransportMessage {
    public static final int LENGTH_MESSAGE_SIZE = 4;
    public static final int LENGTH_MESSAGE_TYPE = 1;
    public static final int LENGTH_MESSAGE_REGION_POS = 4;
    public static final int LENGTH_MESSAGE_CHUNK_NO = 2;
    public static final int LENGTH_MESSAGE_CHUNK_TOTAL = 2;
    public static final int LENGTH_MESSAGE_FLAG_INFO = 1;
    public static final int LENGTH_MESSAGE_FLAG_RESERVE = 2;

    public static final int TRANSPORT_MESSAGE_HEADER_LENGTH = LENGTH_MESSAGE_SIZE +
            LENGTH_MESSAGE_TYPE +
            LENGTH_MESSAGE_REGION_POS +
            LENGTH_MESSAGE_CHUNK_NO +
            LENGTH_MESSAGE_CHUNK_TOTAL +
            LENGTH_MESSAGE_FLAG_INFO +
            LENGTH_MESSAGE_FLAG_RESERVE;

    int msgSize;
    MessageType msgType;
    RegionPosition regionPos;

    @Override
    public int getTablePosition() {
        return tablePosition;
    }

    private int tablePosition = NOT_USE_TABLE_POSITION;
    short chunkNo;
    short chunkTotal;
    byte flagInfo;
    short flagReserve;

    KeyValues[] values;
    private final RowKey rowKey;

    Operation() {
        this(MessageType.EMPTY_MESSAGE, null);
    }

    Operation(MessageType msgType, RowKey rowKey) {
        this.rowKey = rowKey;
        this.msgSize = 0;
        this.msgType = msgType;
        this.regionPos = RegionPosition.UNSET;
        this.chunkNo = 0;
        this.chunkTotal = 0;
        this.flagInfo = 0;
        this.flagReserve = 0;
        this.values = null;
    }

    public Operation(MessageType type, KeyValues[] values, RowKey rowKey) {
        this.rowKey = rowKey;
        int valuesSize = 0;
        for (KeyValues kv : values) {
            if (kv == null) {
                throw new NullPointerException();
            }
            valuesSize += kv.getSizeInBytes();
        }

        this.msgSize = TRANSPORT_MESSAGE_HEADER_LENGTH + valuesSize;
        this.msgType = type;
        this.regionPos = RegionPosition.UNSET;
        this.chunkNo = 0;
        this.chunkTotal = 1;
        this.flagInfo = 0;
        this.flagReserve = 0;

        this.values = values;
    }

    public Operation(byte[] fromBytes, RowKey rowKey) {
        this(fromBytes, 0, fromBytes.length, rowKey);
    }

    protected Operation(byte[] fromBytes, int offset, int length, RowKey rowKey) {
        if (offset < 0 || length < TRANSPORT_MESSAGE_HEADER_LENGTH
                || offset > fromBytes.length - length) {
            throw new IllegalArgumentException("Invalid operation range: offset=" + offset
                    + ", length=" + length + ", arrayLength=" + fromBytes.length);
        }
        this.msgSize = OperationHelper.getMessageSize(fromBytes, offset);
        if (msgSize != length) {
            throw new IllegalArgumentException("Operation size " + msgSize
                    + " does not match range length " + length);
        }
        this.msgType = MessageType.fromByte(fromBytes[offset + LENGTH_MESSAGE_SIZE]);
        this.regionPos = new RegionPosition(NumbersByteHelper.fromBytes_int(fromBytes,
                offset + LENGTH_MESSAGE_SIZE + LENGTH_MESSAGE_TYPE));
        this.rowKey = rowKey;
        this.chunkNo = 0;
        this.chunkTotal = 1;
        this.flagInfo = 0;
        this.flagReserve = 0;

        int pos = offset + TRANSPORT_MESSAGE_HEADER_LENGTH;
        int end = offset + length;
        int valueCount = 0;
        while (pos < end) {
            pos += KeyValuesHelper.getSerializedSize(fromBytes, pos, end);
            valueCount++;
        }

        this.values = new KeyValues[valueCount];
        pos = offset + TRANSPORT_MESSAGE_HEADER_LENGTH;
        for (int i = 0; i < valueCount; i++) {
            int valueSize = KeyValuesHelper.getSerializedSize(fromBytes, pos, end);
            this.values[i] = new KeyValues(fromBytes, pos, valueSize);
            pos += valueSize;
        }
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer bb) throws IOException {
        ByteBuffer res = bb.order(ByteOrder.LITTLE_ENDIAN);
        res.putInt(getMessageSize());
        res.put(msgType.getTypeByte());
        res.putInt(regionPos.getPosition());
        if (tablePosition >= 0) {
            res.putInt(tablePosition);
        } else {
            res.putShort(chunkNo);
            res.putShort(chunkTotal);
        }

        res.put(flagInfo);
        res.putShort(flagReserve);

        for (KeyValues kv : values) {
            kv.serializeObject(res);
        }
        return res;
    }

    @Override
    public int getMessageSize() {
        return msgSize;
    }

    @Override
    public MessageType getMessageType() {
        return msgType;
    }

    public short getChunkNo() {
        return chunkNo;
    }

    public short getChunkTotal() {
        return chunkTotal;
    }

    public byte getFlagInfo() {
        return flagInfo;
    }

    public short getFlagReserve() {
        return flagReserve;
    }

    @Override
    public RegionPosition getRegionPosition() {
        return regionPos;
    }

    public void setRegionPosition(int position) {
        this.regionPos = new RegionPosition(position);
    }

    public byte[] getRowkey() {
        return values != null && values.length > 0 ? values[0].getRowKey() : null;
    }

    public KeyValues[] getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "Operation:" + getMessageSize() + ":" + getMessageType().getTypeByte() + ":" + getRegionPosition().getPosition() + ":" + getChunkNo() + ":" + getChunkTotal() + ":" + getFlagInfo() + ":" + getFlagReserve() + ":"
                + Arrays.toString(getValues());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Operation operation = (Operation) o;
        return msgSize == operation.msgSize && chunkNo == operation.chunkNo && chunkTotal == operation.chunkTotal && flagInfo == operation.flagInfo && flagReserve == operation.flagReserve && msgType == operation.msgType && regionPos.equals(operation.regionPos) && Arrays.equals(values, operation.values);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(msgSize, msgType, chunkNo, chunkTotal, flagInfo, flagReserve, regionPos);
        result = 31 * result + Arrays.hashCode(values);
        return result;
    }

    @Override
    public ByteArrayBuffer<TransportMessage> getBuffer() {
        return TransportBufferFactory.getTransportBuffer();
    }

    public void setTablePosition(int tablePosition) {
        this.tablePosition = tablePosition;
    }

    public RowKey getRowKey() {
        return rowKey;
    }
}
