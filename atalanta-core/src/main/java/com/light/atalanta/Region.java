/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.util.Bytes;

import java.util.Arrays;

public class Region implements DbRegion {

    private final String name;
    private final byte[] startKey;
    private final byte[] endKey;

    public Region(String name, byte[] startKey, byte[] endKey) {
        this.name = name;
        this.startKey = Bytes.cloneBytesArray(startKey);
        this.endKey = Bytes.cloneBytesArray(endKey);
    }

    public String getName() {
        return name;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    @Override
    public String toString() {
        return "CompactRegionModel{" +
                "name='" + name + '\'' +
                ", startKey=" + Arrays.toString(startKey) +
                ", endKey=" + Arrays.toString(endKey) +
                '}';
    }
}
