/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.api;

import com.light.atalanta.DbRegion;
import com.light.atalanta.Pair;
import com.light.atalanta.RegionLocation;

import java.io.IOException;
import java.util.List;

public interface DbRegionLocator extends AutoCloseable {
    List<RegionLocation> getAllRegionLocations() throws IOException;

    default Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
        List<RegionLocation> regions = getAllRegionLocations();
        byte[][] startKeys = new byte[regions.size()][];
        byte[][] endKeys = new byte[regions.size()][];
        DbRegion region;
        for (int i = 0; i < regions.size(); i++) {
            region = regions.get(i).getRegionModel();
            startKeys[i] = region.getStartKey();
            endKeys[i] = region.getEndKey();
        }
        return new Pair<>(startKeys, endKeys);
    }

    // HBase API compatibility
    default byte[][] getStartKeys() throws IOException {
        return getStartEndKeys().getFirst();
    }

    // HBase API compatibility
    default byte[][] getEndKeys() throws IOException {
        return getStartEndKeys().getSecond();
    }
}
