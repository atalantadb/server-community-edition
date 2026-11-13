/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.values;

import com.light.atalanta.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class KeyValues implements SerializedObject {

    public static final int LENGTH_ROWKEY = 4;
    public static final int LENGTH_CF_NAME = 4;
    public static final int LENGTH_VALUES = 4;

    public static final int KEY_VALUES_OBJECT_HEADER_LENGTH = LENGTH_ROWKEY + LENGTH_CF_NAME + LENGTH_VALUES;

    private final byte[] rowKey;
    private final byte[] cfName;
    private final QualifierValue[] values;
    private final int sizeInBytes;

    public KeyValues(byte[] rowKey, byte[] cfName, List<QualifierValue> values) {
        checkRowkey(rowKey);
        this.rowKey = Bytes.cloneBytesArray(rowKey);
        this.cfName = cfName == null ? new byte[]{} : Bytes.cloneBytesArray(cfName);
        this.values = values == null ? new QualifierValue[0] : values.toArray(new QualifierValue[0]);
        this.sizeInBytes = calculateSizeInBytes();
    }

    public KeyValues(byte[] rowKey, byte[] cfName, QualifierValue value) {
        this(rowKey, cfName, value == null ? null : new QualifierValue[]{value});
    }

    public KeyValues(byte[] rowKey, byte[] cfName, QualifierValue[] values) {
        checkRowkey(rowKey);
        this.rowKey = rowKey;
        this.cfName = cfName == null ? new byte[]{} : cfName;
        this.values = values == null ? new QualifierValue[0] : values;
        this.sizeInBytes = calculateSizeInBytes();
    }


    public KeyValues(byte[] rowKey, byte[] cfName, Map<String, byte[]> values) {
        checkRowkey(rowKey);
        this.rowKey = rowKey;
        this.cfName = cfName;
        this.values = new QualifierValue[values.size()];

        int i = 0;
        for (Map.Entry<String, byte[]> e : values.entrySet()) {
            this.values[i] = new QualifierValue(e.getKey().getBytes(StandardCharsets.UTF_8), e.getValue());
            i++;
        }
        this.sizeInBytes = calculateSizeInBytes();
    }

    private static void checkRowkey(byte[] bytes) {
        Objects.requireNonNull(bytes, "Rowkey cant be null");
        if (bytes.length < 1) {
            throw new IllegalArgumentException("Rowkey cant be empty");
        }
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public byte[] getCfName() {
        return cfName;
    }

    public QualifierValue[] getValues() {
        return values;
    }

    @Override
    public ByteBuffer serializeObject(ByteBuffer bb) throws IOException {
        bb.order(ByteOrder.LITTLE_ENDIAN);
        int objectSize = getSizeInBytes();

        // rowKey length
        bb.putInt(rowKey.length);
        // cf name length
        bb.putInt(cfName.length);
        // payload length
        bb.putInt(objectSize - KEY_VALUES_OBJECT_HEADER_LENGTH - rowKey.length - cfName.length);

        // rowKey bytes
        bb.put(rowKey);
        // cf name bytes
        bb.put(cfName);

        for (QualifierValue qv : values) {
            qv.serializeObject(bb);
        }
        return bb;
    }

    @Override
    public int getSizeInBytes() {
        return sizeInBytes;
    }

    private int calculateSizeInBytes() {
        int kvVolume = 0;
        for (QualifierValue qualifierValue : values) {
            kvVolume += qualifierValue.getSizeInBytes();
        }
        return KEY_VALUES_OBJECT_HEADER_LENGTH + rowKey.length + cfName.length + kvVolume;
    }

    public KeyValues(byte[] restoreFrom) {
        this(restoreFrom, 0, restoreFrom.length);
    }

    public KeyValues(byte[] restoreFrom, int offset, int length) {
        if (offset < 0 || length < KEY_VALUES_OBJECT_HEADER_LENGTH
                || offset > restoreFrom.length - length) {
            throw new IllegalArgumentException("Invalid KeyValues range: offset=" + offset
                    + ", length=" + length + ", arrayLength=" + restoreFrom.length);
        }
        final int rowKeyLength = KeyValuesHelper.getRowKeyLength(restoreFrom, offset);
        final int cfNameLength = KeyValuesHelper.getCFLength(restoreFrom, offset);
        final int valuesLength = KeyValuesHelper.getValuesSize(restoreFrom, offset);
        if (rowKeyLength < 0 || cfNameLength < 0 || valuesLength < 0) {
            throw new IllegalArgumentException("Negative KeyValues field length");
        }
        long expectedLength = (long) KEY_VALUES_OBJECT_HEADER_LENGTH
                + rowKeyLength + cfNameLength + valuesLength;
        if (expectedLength != length) {
            throw new IllegalArgumentException("KeyValues size " + expectedLength
                    + " does not match range length " + length);
        }

        byte[] rowKey = new byte[rowKeyLength];
        byte[] cfName = new byte[cfNameLength];

        int payloadOffset = offset + KEY_VALUES_OBJECT_HEADER_LENGTH;
        System.arraycopy(restoreFrom, payloadOffset, rowKey, 0, rowKeyLength);
        System.arraycopy(restoreFrom, payloadOffset + rowKeyLength, cfName, 0, cfNameLength);

        int pos = payloadOffset + rowKeyLength + cfNameLength;
        int end = offset + length;
        int valueCount = 0;
        while (pos < end) {
            pos += QualifierValueHelper.getSerializedSize(restoreFrom, pos, end);
            valueCount++;
        }

        QualifierValue[] kvPairs = new QualifierValue[valueCount];
        pos = payloadOffset + rowKeyLength + cfNameLength;
        for (int i = 0; i < valueCount; i++) {
            int valueSize = QualifierValueHelper.getSerializedSize(restoreFrom, pos, end);
            kvPairs[i] = new QualifierValue(restoreFrom, pos, valueSize);
            pos += valueSize;
        }

        this.rowKey = rowKey;
        this.cfName = cfName;
        this.values = kvPairs;
        this.sizeInBytes = length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyValues keyValues = (KeyValues) o;
        return Arrays.equals(rowKey, keyValues.rowKey) && Arrays.equals(cfName, keyValues.cfName) && Arrays.equals(values, keyValues.values);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(rowKey);
        result = 31 * result + Arrays.hashCode(cfName);
        result = 31 * result + Arrays.hashCode(values);
        return result;
    }

    @Override
    public String toString() {
        return "KeyValues[" + Arrays.toString(rowKey) + ',' + Arrays.toString(cfName) + ',' + Arrays.toString(values) + ']';
    }
}
