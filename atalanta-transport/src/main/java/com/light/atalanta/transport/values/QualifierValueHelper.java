/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.values;

import com.light.atalanta.NumbersByteHelper;

public class QualifierValueHelper {

    public static long getTimestamp(byte[] kvBytes) {
        return getTimestampFromByteArray(kvBytes, 0);
    }

    /**
     * @param kvBytes byte representation of {@link QualifierValue}
     * @return {@link QualifierValue} qualifier size
     */
    public static int getQualifierSize(byte[] kvBytes) {
        return getQualifierSizeFromByteArray(kvBytes, 0);
    }

    /**
     * @param kvBytes byte representation of {@link QualifierValue}
     * @return {@link QualifierValue} value size
     */
    public static int getValueSize(byte[] kvBytes) {
        return getValueSizeFromByteArray(kvBytes, 0);
    }

    /**
     * @param payload payload of {@link QualifierValue} array or {@link KeyValues} object
     * @param pos     position of current {@link QualifierValue} in {@link KeyValues} payload bytes
     * @return timestamp of {@link QualifierValue}
     */
    public static long getTimestampFromByteArray(byte[] payload, int pos) {
        return NumbersByteHelper.fromBytes_long(payload, pos);
    }

    /**
     * @param payload payload of {@link QualifierValue} array or {@link KeyValues} object
     * @param pos     position of current {@link QualifierValue} in {@link KeyValues} payload bytes
     * @return qualifier size
     */
    public static int getQualifierSizeFromByteArray(byte[] payload, int pos) {
        return NumbersByteHelper.fromBytes_int(payload, pos + QualifierValue.LENGTH_TIMESTAMP);
    }

    /**
     * @param payload payload of {@link QualifierValue} array or {@link KeyValues} object
     * @param pos     position of current {@link QualifierValue} in {@link KeyValues} payload bytes
     * @return qualifier size
     */
    public static int getValueSizeFromByteArray(byte[] payload, int pos) {
        return NumbersByteHelper.fromBytes_int(payload,pos + QualifierValue.LENGTH_TIMESTAMP + QualifierValue.LENGTH_QUALIFIER);
    }

    /**
     * @param payload payload of {@link QualifierValue} array or {@link KeyValues} object
     * @param pos     position of current {@link QualifierValue} in {@link KeyValues} payload bytes
     * @return {@link QualifierValue} object
     */
    public static QualifierValue getKeyValueFromByteArray(byte[] payload, int pos) {
        return getKeyValueFromByteArray(payload, pos, payload.length);
    }

    public static QualifierValue getKeyValueFromByteArray(byte[] payload, int pos, int limit) {
        int size = getSerializedSize(payload, pos, limit);
        return new QualifierValue(payload, pos, size);
    }

    public static int getSerializedSize(byte[] payload, int pos, int limit) {
        if (pos < 0 || limit < pos || limit > payload.length
                || limit - pos < QualifierValue.QUALIFIER_VALUE_HEADER_LENGTH) {
            throw new IllegalArgumentException("Invalid QualifierValue boundary");
        }
        int qualifierSize = QualifierValueHelper.getQualifierSizeFromByteArray(payload, pos);
        int valueSize = QualifierValueHelper.getValueSizeFromByteArray(payload, pos);
        if (qualifierSize < 0 || valueSize < 0) {
            throw new IllegalArgumentException("Negative QualifierValue field length");
        }
        long size = (long) QualifierValue.QUALIFIER_VALUE_HEADER_LENGTH + qualifierSize + valueSize;
        if (size > limit - pos) {
            throw new IllegalArgumentException("QualifierValue exceeds containing boundary");
        }
        return (int) size;
    }

}
