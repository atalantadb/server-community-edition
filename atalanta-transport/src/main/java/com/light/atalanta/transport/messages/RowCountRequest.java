/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.transport.RegionPosition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.light.atalanta.transport.messages.BulkHelper.NOT_USE_TABLE_POSITION;

public class RowCountRequest implements PositionedTransportMessage, TablePositionedTransportMessage {
    public static final int LENGTH_MESSAGE_SIZE = 4;
    public static final int LENGTH_MESSAGE_TYPE = 1;
    public static final int LENGTH_MESSAGE_REGION_POS = 4;

    public static final int MESSAGE_SIZE = LENGTH_MESSAGE_SIZE + LENGTH_MESSAGE_TYPE + LENGTH_MESSAGE_REGION_POS;

    private final RegionPosition regionPosition;

    private int tablePosition = NOT_USE_TABLE_POSITION;

    public RowCountRequest(final int regionPosition) {
        this.regionPosition = new RegionPosition(regionPosition);
    }

    @Override
    public RegionPosition getRegionPosition() {
        return regionPosition;
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer bb) throws IOException {
        ByteBuffer res = bb.order(ByteOrder.LITTLE_ENDIAN);
        int messageSize = getMessageSize();
        res.putInt(messageSize);
        res.put(getMessageType().getTypeByte());
        res.putInt(regionPosition.getPosition());
        if (tablePosition >= 0) {
            res.putInt(tablePosition);
        }
        return res;
    }

    @Override
    public int getMessageSize() {
        return MESSAGE_SIZE + (tablePosition != NOT_USE_TABLE_POSITION ? 4 : 0);
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.ROW_COUNT;
    }

    @Override
    public void setTablePosition(int tablePosition) {
        this.tablePosition = tablePosition;
    }

    @Override
    public int getTablePosition() {
        return tablePosition;
    }
}
