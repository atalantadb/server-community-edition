/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.values;

import com.light.atalanta.NumbersByteHelper;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DeletedQualifierValue extends QualifierValue {
    private static final int FLAG_DELETED = 0x80000000;

    public DeletedQualifierValue(long timestamp, byte[] qualifier) {
        super(timestamp, qualifier, null);
    }

    @Override
    public ByteBuffer serializeObject(ByteBuffer bb) throws IOException {
        bb.put(NumbersByteHelper.toBytes_long(timestamp));
        bb.put(NumbersByteHelper.toBytes_int(qualifier.length + FLAG_DELETED));
        bb.put(NumbersByteHelper.toBytes_int(value.length));

        bb.put(qualifier);
        bb.put(value);
        return bb;
    }
}
