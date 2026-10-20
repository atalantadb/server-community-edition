/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.NumbersByteHelper;
import com.light.atalanta.transport.buffers.TransportBufferFactory;
import com.light.atalanta.transport.values.TableMapObject;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TableRegionMapRequest implements TransportMessage {
    public static final byte LAST_BYTE = (byte) 0;

    public static final int LENGTH_REGION_MAP_REQUEST = 4;
    public static final int LENGTH_REGION_MAP_MESSAGE_TYPE = 1;
    public static final int LENGTH_REGION_MAP_EMPTY_BYTE = 1;

    public static final int REGION_MAP_REQUEST_HEADER_LENGTH = LENGTH_REGION_MAP_REQUEST + LENGTH_REGION_MAP_MESSAGE_TYPE + LENGTH_REGION_MAP_EMPTY_BYTE;

    private final TableMapObject regionMapRequestParams;

    public TableRegionMapRequest(TableMapObject params) {
        this.regionMapRequestParams = params;
    }

    @Override
    public int getMessageSize() {
        return REGION_MAP_REQUEST_HEADER_LENGTH + regionMapRequestParams.getSizeInBytes();
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer bb) throws IOException {
        int size = getMessageSize();
        bb.put(NumbersByteHelper.toBytes_int(size));
        bb.put(getMessageType().getTypeByte());

        bb.put(TransportBufferFactory.getDefaultSerializedObjectBuffer().serialize(regionMapRequestParams));

        bb.put(LAST_BYTE);
        return bb;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.GET_REGION_MAP;
    }

    public TableMapObject getRegionMapRequestParams() {
        return regionMapRequestParams;
    }
}
