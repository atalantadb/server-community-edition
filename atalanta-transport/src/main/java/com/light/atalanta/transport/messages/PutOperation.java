/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.RowKey;
import com.light.atalanta.transport.values.KeyValues;

public class PutOperation extends Operation {

    public PutOperation(KeyValues[] value, MessageType type, RowKey rowKey) {
        super(type, value, rowKey);
    }

    public PutOperation(KeyValues[] value, RowKey rowKey) {
        super(MessageType.PUT, value, rowKey);
    }

    public PutOperation(KeyValues value, MessageType type, RowKey rowKey) {
        super(type, new KeyValues[]{value}, rowKey);
    }

    public PutOperation(KeyValues value, RowKey rowKey) {
        super(MessageType.PUT, new KeyValues[]{value}, rowKey);
    }

    public PutOperation(byte[] fromByte, RowKey rowKey) {
        super(fromByte, rowKey);
    }
}
