/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.values;

import com.light.atalanta.NumbersByteHelper;
import com.light.atalanta.util.Bytes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

public class RegionInfoResponseObject implements SerializedObject {
    public static final int REGION_POSITION_LENGTH = 4;
    public static final int REGION_START_KEY_LENGTH = 8;
    public static final int REGION_END_KEY_LENGTH = 8;

    public static final int REGION_INFO_OBJECT_SIZE = REGION_POSITION_LENGTH + REGION_START_KEY_LENGTH + REGION_END_KEY_LENGTH;

    private final int regionPosition;

    private final byte[] startKey;
    private final byte[] endKey;

    public RegionInfoResponseObject(byte[] regionPos, byte[] startKey, byte[] endKey) {
        validate(regionPos, "regionPosition", REGION_POSITION_LENGTH);
        this.regionPosition = NumbersByteHelper.fromBytes_int(regionPos, 0);
        validate(startKey, "startKey", REGION_START_KEY_LENGTH);
        this.startKey = Bytes.cloneBytesArray(startKey);
        validate(endKey, "endKey", REGION_END_KEY_LENGTH);
        this.endKey = Bytes.cloneBytesArray(endKey);
    }

    public RegionInfoResponseObject(byte[] fromBytes) {
        if (fromBytes == null || fromBytes.length != REGION_INFO_OBJECT_SIZE) {
            throw new IllegalArgumentException("Incorrect byte`s representation of RegionInfo");
        }
        byte[] rPos = new byte[REGION_POSITION_LENGTH];
        System.arraycopy(fromBytes, 0, rPos, 0, REGION_POSITION_LENGTH);

        byte[] sBytes = new byte[REGION_START_KEY_LENGTH];
        System.arraycopy(fromBytes, REGION_POSITION_LENGTH, sBytes, 0, REGION_START_KEY_LENGTH);

        byte[] eBytes = new byte[REGION_END_KEY_LENGTH];
        System.arraycopy(fromBytes, REGION_POSITION_LENGTH + REGION_START_KEY_LENGTH, eBytes, 0, REGION_END_KEY_LENGTH);

        validate(rPos, "regionPosition", REGION_POSITION_LENGTH);
        this.regionPosition = NumbersByteHelper.fromBytes_int(rPos, 0);
        validate(sBytes, "startKey", REGION_START_KEY_LENGTH);
        this.startKey = sBytes;
        validate(eBytes, "endKey", REGION_END_KEY_LENGTH);
        this.endKey = eBytes;
    }

    private void validate(byte[] bytes, String fieldName, int expectedLength) {
        if (bytes.length != expectedLength) {
            throw new IllegalArgumentException(fieldName + " length must be " + expectedLength);
        }
    }

    public int getRegionPosition() {
        return regionPosition;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public String getStartKeyStr() {
        return Base64.getEncoder().encodeToString(startKey);
    }

    public String getEndKeyStr() {
        return Base64.getEncoder().encodeToString(endKey);
    }

    @Override
    public ByteBuffer serializeObject(ByteBuffer bb) {
        bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.put(NumbersByteHelper.toBytes_int(regionPosition));
        bb.put(startKey);
        bb.put(endKey);
        return bb;
    }

    @Override
    public int getSizeInBytes() {
        return REGION_INFO_OBJECT_SIZE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegionInfoResponseObject that = (RegionInfoResponseObject) o;
        return regionPosition == that.regionPosition && Arrays.equals(startKey, that.startKey) && Arrays.equals(endKey, that.endKey);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(regionPosition);
        result = 31 * result + Arrays.hashCode(startKey);
        result = 31 * result + Arrays.hashCode(endKey);
        return result;
    }

    @Override
    public String toString() {
        return "RegionInfoResponse[" + regionPosition + ',' + getStartKeyStr() + ',' + getEndKeyStr() + ']';
    }
}
