/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.transport.values.KeyValues;

public class ResultOperation extends Operation {
    public static final ResultOperation EMPTY = new ResultOperation(new KeyValues[0]);

    ResultOperation() {
        super();
    }

    public ResultOperation(KeyValues[] value) {
        super(MessageType.RESULT, value, null);
    }

    public ResultOperation(KeyValues value) {
        super(MessageType.RESULT, new KeyValues[]{value}, null);
    }

    public ResultOperation(byte[] fromBytes) {
        super(fromBytes, null);
    }

    public ResultOperation(byte[] fromBytes, int offset, int length) {
        super(fromBytes, offset, length, null);
    }

    public boolean isEmpty() {
        if (values == null) {
            return true;
        }

        for (KeyValues kv : values) {
            if (kv.getValues() != null && kv.getValues().length > 0) {
                return false;
            }
        }
        return true;
    }
}
