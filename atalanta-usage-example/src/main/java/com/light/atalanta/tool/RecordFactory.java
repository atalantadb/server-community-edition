/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.tool;

import com.light.atalanta.Put;
import com.light.atalanta.Get;
import com.light.atalanta.factory.ObjectFactory;
import com.light.atalanta.util.XxHash32KeySalter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Creates deterministic records whose keys can be reconstructed for reads. */
public final class RecordFactory {

    static final int VALUE_SIZE = 16;

    private static final byte[] COLUMN_FAMILY =
            DataToolSchema.COLUMN_FAMILY.getBytes(StandardCharsets.UTF_8);
    private static final byte[][] QUALIFIERS = createQualifiers();

    private RecordFactory() {
    }

    public static Put createPut(ObjectFactory factory, long recordNumber) {
        Put put = factory.put(createRowKey(recordNumber));
        for (int field = 0; field < QUALIFIERS.length; field++) {
            put.addColumn(COLUMN_FAMILY, QUALIFIERS[field], createValue(recordNumber, field));
        }
        return put;
    }

    public static Get createGet(ObjectFactory factory, long recordNumber) {
        return factory.get(createRowKey(recordNumber));
    }

    public static byte[] createRowKey(long recordNumber) {
        byte[] key = String.format("user%012d", recordNumber).getBytes(StandardCharsets.UTF_8);
        return XxHash32KeySalter.saltKey(key);
    }

    private static byte[] createValue(long recordNumber, int field) {
        return ByteBuffer.allocate(VALUE_SIZE)
                .putLong(recordNumber)
                .putInt(field)
                .putInt(~field)
                .array();
    }

    private static byte[][] createQualifiers() {
        byte[][] qualifiers = new byte[DataToolSchema.QUALIFIER_COUNT][];
        for (int index = 0; index < qualifiers.length; index++) {
            qualifiers[index] = DataToolSchema.qualifier(index).getBytes(StandardCharsets.UTF_8);
        }
        return Arrays.copyOf(qualifiers, qualifiers.length);
    }
}
