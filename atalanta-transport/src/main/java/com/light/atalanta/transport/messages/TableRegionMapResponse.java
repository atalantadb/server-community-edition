/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.NumbersByteHelper;
import com.light.atalanta.transport.buffers.TransportBufferFactory;
import com.light.atalanta.transport.values.RegionInfoResponseObject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class TableRegionMapResponse implements TransportMessage {
    public static final int LENGTH_REGION_MAP_RESPONSE = 4;
    public static final int LENGTH_REGION_MAP_RESPONSE_TYPE = 1;

    public static final int REGION_MAP_RESPONSE_HEADER_LENGTH = LENGTH_REGION_MAP_RESPONSE + LENGTH_REGION_MAP_RESPONSE_TYPE;

    private final MessageType type;
    private final RegionInfoResponseObject[] regionInfoResponsParams;

    public TableRegionMapResponse(RegionInfoResponseObject[] regionInfoResponseParams, MessageType type) {
        this.type = type;
        this.regionInfoResponsParams = regionInfoResponseParams;
    }

    public TableRegionMapResponse(byte[] fromBytes) {
        byte[] paramBytes = new byte[fromBytes.length - REGION_MAP_RESPONSE_HEADER_LENGTH];
        System.arraycopy(fromBytes, REGION_MAP_RESPONSE_HEADER_LENGTH, paramBytes, 0, fromBytes.length - REGION_MAP_RESPONSE_HEADER_LENGTH);
        this.type = MessageType.fromByte(fromBytes[4]);

        if (fromBytes.length <= REGION_MAP_RESPONSE_HEADER_LENGTH || type == MessageType.INVALID_REGION_MAP_ERROR) {
            this.regionInfoResponsParams = null;
        } else {
            this.regionInfoResponsParams = parseRegionsInfo(paramBytes);
        }
    }

    private RegionInfoResponseObject[] parseRegionsInfo(byte[] fromBytes) {
        if (fromBytes == null || fromBytes.length == 0 || fromBytes.length % RegionInfoResponseObject.REGION_INFO_OBJECT_SIZE != 0) {
            throw new IllegalArgumentException("Incorrect byte`s representation of RegionInfo");
        }

        int count = fromBytes.length / RegionInfoResponseObject.REGION_INFO_OBJECT_SIZE;
        RegionInfoResponseObject[] info = new RegionInfoResponseObject[count];

        int pos = 0;
        for (int i = 0; i < count; i++) {
            byte[] riBytes = new byte[RegionInfoResponseObject.REGION_INFO_OBJECT_SIZE];
            System.arraycopy(fromBytes, pos, riBytes, 0, RegionInfoResponseObject.REGION_INFO_OBJECT_SIZE);
            info[i] = new RegionInfoResponseObject(riBytes);
            pos += RegionInfoResponseObject.REGION_INFO_OBJECT_SIZE;
        }
        return info;
    }

    public RegionInfoResponseObject[] getRegionInfos() {
        return regionInfoResponsParams;
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer bb) throws IOException {
        int size = getMessageSize();
        bb.put(NumbersByteHelper.toBytes_int(size));
        bb.put(getMessageType().getTypeByte());

        for (RegionInfoResponseObject ri : regionInfoResponsParams) {
            bb.put(TransportBufferFactory.getDefaultSerializedObjectBuffer().serialize(ri));
        }
        return bb;
    }

    @Override
    public MessageType getMessageType() {
        return type;
    }

    @Override
    public int getMessageSize() {
        int regionsSize = Arrays.stream(regionInfoResponsParams).map(RegionInfoResponseObject::getSizeInBytes).mapToInt(value -> value). sum();
        return REGION_MAP_RESPONSE_HEADER_LENGTH + regionsSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableRegionMapResponse that = (TableRegionMapResponse) o;
        return type == that.type && Arrays.equals(regionInfoResponsParams, that.regionInfoResponsParams);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(type);
        result = 31 * result + Arrays.hashCode(regionInfoResponsParams);
        return result;
    }

    @Override
    public String toString() {
        return "RegionMapResponse[" + type + ',' + Arrays.toString(regionInfoResponsParams) + ']';
    }
}
