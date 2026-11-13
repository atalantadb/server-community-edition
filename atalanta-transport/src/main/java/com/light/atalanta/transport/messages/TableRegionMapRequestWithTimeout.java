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

public class TableRegionMapRequestWithTimeout extends TableRegionMapRequest {
    public static final int LENGTH_REGION_MAP_TIMEOUT = 4;

    public static final int REGION_MAP_WITH_TIMEOUT_REQUEST_HEADER_LENGTH = REGION_MAP_REQUEST_HEADER_LENGTH + LENGTH_REGION_MAP_TIMEOUT;

    private final int timeout;

    public TableRegionMapRequestWithTimeout(TableMapObject params, int timeout) {
        super(params);
        this.timeout = timeout;
    }

    @Override
    public int getMessageSize() {
        return REGION_MAP_WITH_TIMEOUT_REQUEST_HEADER_LENGTH + getRegionMapRequestParams().getSizeInBytes();
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer bb) throws IOException {
        int size = getMessageSize();
        bb.put(NumbersByteHelper.toBytes_int(size));
        bb.put(getMessageType().getTypeByte());
        bb.put(NumbersByteHelper.toBytes_int(timeout));

        bb.put(TransportBufferFactory.getDefaultSerializedObjectBuffer().serialize(getRegionMapRequestParams()));

        bb.put(LAST_BYTE);
        return bb;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.GET_REGION_MAP_WITH_TIMEOUT;
    }
}
