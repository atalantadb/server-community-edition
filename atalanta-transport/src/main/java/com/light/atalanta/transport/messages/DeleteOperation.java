/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.RowKey;
import com.light.atalanta.transport.values.KeyValues;

public class DeleteOperation extends Operation {
    public DeleteOperation(KeyValues[] value, RowKey rowKey) {
        super(MessageType.DELETE, value, rowKey);
    }

    public DeleteOperation(KeyValues value, RowKey rowKey) {
        super(MessageType.DELETE, new KeyValues[]{value}, rowKey);
    }

    public DeleteOperation(byte[] fromBytes, RowKey rowKey) {
        super(fromBytes, rowKey);
    }
}
