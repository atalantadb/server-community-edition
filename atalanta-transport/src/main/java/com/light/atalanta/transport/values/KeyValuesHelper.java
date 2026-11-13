/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.values;

import com.light.atalanta.NumbersByteHelper;

public class KeyValuesHelper {

    public static int getRowKeyLength(byte[] keyValuesBytes) {
        return getRowKeyLength(keyValuesBytes, 0);
    }

    public static int getCFLength(byte[] keyValuesBytes) {
        return getCFLength(keyValuesBytes, 0);
    }

    public static int getValuesSize(byte[] keyValuesBytes) {
        return getValuesSize(keyValuesBytes, 0);
    }

    public static int getRowKeyLength(byte[] keyValuesBytes, int pos) {
        return NumbersByteHelper.fromBytes_int(keyValuesBytes, pos);
    }

    public static int getCFLength(byte[] keyValuesBytes, int pos) {
        return NumbersByteHelper.fromBytes_int(keyValuesBytes, pos + KeyValues.LENGTH_ROWKEY);
    }

    public static int getValuesSize(byte[] keyValuesBytes, int pos) {
        return NumbersByteHelper.fromBytes_int(keyValuesBytes, pos + KeyValues.LENGTH_ROWKEY + KeyValues.LENGTH_CF_NAME);
    }

    public static KeyValues getKeyValuesFromByteArray(byte[] payload, int pos) {
        return getKeyValuesFromByteArray(payload, pos, payload.length);
    }

    public static KeyValues getKeyValuesFromByteArray(byte[] payload, int pos, int limit) {
        int size = getSerializedSize(payload, pos, limit);
        return new KeyValues(payload, pos, size);
    }

    public static int getSerializedSize(byte[] payload, int pos, int limit) {
        if (pos < 0 || limit < pos || limit > payload.length
                || limit - pos < KeyValues.KEY_VALUES_OBJECT_HEADER_LENGTH) {
            throw new IllegalArgumentException("Invalid KeyValues boundary");
        }
        int rowKeyLength = KeyValuesHelper.getRowKeyLength(payload, pos);
        int cfLength = KeyValuesHelper.getCFLength(payload, pos);
        int valueSize = KeyValuesHelper.getValuesSize(payload, pos);
        if (rowKeyLength < 0 || cfLength < 0 || valueSize < 0) {
            throw new IllegalArgumentException("Negative KeyValues field length");
        }
        long size = (long) KeyValues.KEY_VALUES_OBJECT_HEADER_LENGTH
                + rowKeyLength + cfLength + valueSize;
        if (size > limit - pos) {
            throw new IllegalArgumentException("KeyValues exceeds containing boundary");
        }
        return (int) size;
    }
}
