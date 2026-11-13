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
import java.util.Arrays;
import java.util.Objects;

import static com.light.atalanta.ATALANTAConstants.EMPTY_BYTES;

public class QualifierValue implements SerializedObject {
    public static final int LENGTH_TIMESTAMP = 8;
    public static final int LENGTH_QUALIFIER = 4;
    public static final int LENGTH_VALUE = 4;

    public static final int QUALIFIER_VALUE_HEADER_LENGTH = LENGTH_TIMESTAMP + LENGTH_QUALIFIER + LENGTH_VALUE;

    final long timestamp;

    final byte[] qualifier;
    final byte[] value;

    public QualifierValue(long timestamp, byte[] qualifier, byte[] value) {
        this(timestamp, qualifier, value, true);
    }

    private QualifierValue(long timestamp, byte[] qualifier, byte[] value, boolean cloneValue) {
        Objects.requireNonNull(qualifier, "Qualifier cant be null");

        this.timestamp = timestamp;
        this.qualifier = Bytes.cloneBytesArray(qualifier);
        this.value = value == null ? EMPTY_BYTES : cloneValue ? Bytes.cloneBytesArray(value) : value;
    }

    /**
     * Creates a qualifier value from a value array already owned by the caller.
     * The qualifier is still copied because it can be a key in the caller's map.
     */
    public static QualifierValue withSharedValue(long timestamp, byte[] qualifier, byte[] value) {
        return new QualifierValue(timestamp, qualifier, value, false);
    }

    public QualifierValue(byte[] qualifier, byte[] value) {
        this(-1L, qualifier, value);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public ByteBuffer serializeObject(ByteBuffer bb) throws IOException {
        bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.putLong(timestamp);
        bb.putInt(qualifier.length);
        bb.putInt(value.length);

        bb.put(qualifier);
        bb.put(value);
        return bb;
    }

    @Override
    public int getSizeInBytes() {
        return QUALIFIER_VALUE_HEADER_LENGTH //  kv-header length
                + qualifier.length  // qualifier
                + value.length;     // value
    }

    public QualifierValue(byte[] restoreFrom) {
        this(restoreFrom, 0, serializedSize(restoreFrom));
    }

    private static int serializedSize(byte[] restoreFrom) {
        if (restoreFrom.length < QUALIFIER_VALUE_HEADER_LENGTH) {
            throw new IllegalArgumentException("QualifierValue header is truncated");
        }
        int qualifierLength = QualifierValueHelper.getQualifierSize(restoreFrom);
        int valueLength = QualifierValueHelper.getValueSize(restoreFrom);
        if (qualifierLength < 0 || valueLength < 0) {
            throw new IllegalArgumentException("Negative QualifierValue field length");
        }
        long size = (long) QUALIFIER_VALUE_HEADER_LENGTH + qualifierLength + valueLength;
        if (size > restoreFrom.length) {
            throw new IllegalArgumentException("QualifierValue exceeds source array");
        }
        return (int) size;
    }

    public QualifierValue(byte[] restoreFrom, int offset, int length) {
        if (offset < 0 || length < QUALIFIER_VALUE_HEADER_LENGTH
                || offset > restoreFrom.length - length) {
            throw new IllegalArgumentException("Invalid QualifierValue range: offset=" + offset
                    + ", length=" + length + ", arrayLength=" + restoreFrom.length);
        }

        final long timestamp = QualifierValueHelper.getTimestampFromByteArray(restoreFrom, offset);

        final int qualifierLength = QualifierValueHelper.getQualifierSizeFromByteArray(restoreFrom, offset);
        final int valueLength = QualifierValueHelper.getValueSizeFromByteArray(restoreFrom, offset);
        if (qualifierLength < 0 || valueLength < 0) {
            throw new IllegalArgumentException("Negative QualifierValue field length");
        }
        long expectedLength = (long) QUALIFIER_VALUE_HEADER_LENGTH + qualifierLength + valueLength;
        if (expectedLength != length) {
            throw new IllegalArgumentException("QualifierValue size " + expectedLength
                    + " does not match range length " + length);
        }

        byte[] q = new byte[qualifierLength];
        byte[] v = new byte[valueLength];

        int payloadOffset = offset + QUALIFIER_VALUE_HEADER_LENGTH;
        System.arraycopy(restoreFrom, payloadOffset, q, 0, qualifierLength);
        System.arraycopy(restoreFrom, payloadOffset + qualifierLength, v, 0, valueLength);

        this.timestamp = timestamp;
        this.qualifier = q;
        this.value = v;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QualifierValue qualifierValue = (QualifierValue) o;
        return timestamp == qualifierValue.timestamp && Arrays.equals(qualifier, qualifierValue.qualifier) && Arrays.equals(value, qualifierValue.value);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(timestamp);
        result = 31 * result + Arrays.hashCode(qualifier);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "QualifierValue[" + timestamp + ',' + Arrays.toString(qualifier) + ',' + Arrays.toString(value) + ']';
    }
}
