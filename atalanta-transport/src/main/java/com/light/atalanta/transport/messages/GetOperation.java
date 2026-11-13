/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.RowKey;
import com.light.atalanta.transport.values.KeyValues;

public class GetOperation extends Operation {

    private int sourceIndex = -1;

    public GetOperation(KeyValues[] value, RowKey rowKey) {
        super(MessageType.GET, value, rowKey);
    }

    public GetOperation(KeyValues value, RowKey rowKey) {
        super(MessageType.GET, new KeyValues[]{value}, rowKey);
    }

    public GetOperation(byte[] fromBytes, RowKey rowKey) {
        super(fromBytes, rowKey);
    }

    public int getSourceIndex() {
        return sourceIndex;
    }

    public void setSourceIndex(int sourceIndex) {
        if (sourceIndex < 0) {
            throw new IllegalArgumentException("Source index must be non-negative");
        }
        this.sourceIndex = sourceIndex;
    }
}
