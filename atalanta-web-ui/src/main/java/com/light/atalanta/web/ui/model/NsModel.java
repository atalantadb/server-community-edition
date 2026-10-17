/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.model;

import com.light.atalanta.metrics.RegionMetrics;

public class NsModel {

    private String name;

    private int tableCount;

    private RegionMetrics regionMetrics = new RegionMetrics();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTableCount(int tableCount) {
        this.tableCount = tableCount;
    }

    public int getTableCount() {
        return tableCount;
    }

    public RegionMetrics getRegionMetrics() {
        return regionMetrics;
    }

    public void setRegionMetrics(RegionMetrics regionMetrics) {
        this.regionMetrics = regionMetrics;
    }

    @Override
    public String toString() {
        return "NsModel{"
                + "name='" + name + '\''
                + ", tableCount=" + tableCount
                + '}';
    }
}
