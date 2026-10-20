/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.NumbersByteHelper;
import com.light.atalanta.transport.RegionPosition;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RowCountResponse implements PositionedTransportMessage {
    public static final int LENGTH_MESSAGE_SIZE = 4;
    public static final int LENGTH_MESSAGE_TYPE = 1;
    public static final int LENGTH_MESSAGE_REGION_POS = 4;
    public static final int LENGTH_MESSAGE_COUNT_POS = 8;

    public static final int MESSAGE_SIZE = LENGTH_MESSAGE_SIZE + LENGTH_MESSAGE_TYPE + LENGTH_MESSAGE_REGION_POS + LENGTH_MESSAGE_COUNT_POS;

    private final RegionPosition regionPosition;
    private final long counts;

    public RowCountResponse(final int regionPosition, final long counts) {
        this.regionPosition = new RegionPosition(regionPosition);
        this.counts = counts;
    }

    public RowCountResponse(final RegionPosition regionPosition, final long counts) {
        this.regionPosition = regionPosition;
        this.counts = counts;
    }

    public RowCountResponse(byte[] fromBytes) {
        byte[] regionPos = new byte[LENGTH_MESSAGE_REGION_POS];
        System.arraycopy(fromBytes, (LENGTH_MESSAGE_SIZE + LENGTH_MESSAGE_TYPE ), regionPos, 0, LENGTH_MESSAGE_REGION_POS);


        byte[] countBytes = new byte[LENGTH_MESSAGE_COUNT_POS];
        System.arraycopy(fromBytes, (LENGTH_MESSAGE_SIZE + LENGTH_MESSAGE_TYPE + LENGTH_MESSAGE_REGION_POS ), countBytes, 0, LENGTH_MESSAGE_COUNT_POS);

        this.regionPosition = new RegionPosition(NumbersByteHelper.fromBytes_int(regionPos, 0));
        this.counts = NumbersByteHelper.fromBytes_long(countBytes, 0);
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer byteBuffer) throws IOException {
        byteBuffer.put(NumbersByteHelper.toBytes_int(MESSAGE_SIZE));
        byteBuffer.put(getMessageType().getTypeByte());
        byteBuffer.put(NumbersByteHelper.toBytes_int(regionPosition.getPosition()));
        byteBuffer.put(NumbersByteHelper.toBytes_long(counts));
        return byteBuffer;
    }

    @Override
    public int getMessageSize() {
        return MESSAGE_SIZE;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.ROW_COUNT;
    }

    @Override
    public RegionPosition getRegionPosition() {
        return regionPosition;
    }

    public long getCounts() {
        return counts;
    }
}
