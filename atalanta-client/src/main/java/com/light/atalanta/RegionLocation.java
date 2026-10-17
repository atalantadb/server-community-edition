/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.model.RegionServerModel;

public class RegionLocation implements DbRegionLocation {

    private final DbRegion region;
    private final RegionServer regionServer;

    protected RegionLocation() {
        region = null;
        regionServer = null;
    }

    public RegionLocation(Region regionCompactInfo, RegionServerModel regionServerModel) {
        this.region = regionCompactInfo;
        this.regionServer = regionServerModel;
    }

    @Override
    public DbRegion getRegionModel() {
        return region;
    }

    @Override
    public RegionServer getRegionServer() {
        return regionServer;
    }

    @Override
    public int compareTo(DbRegionLocation o) {
        return regionServer.compareTo(o.getRegionServer());
    }
}
