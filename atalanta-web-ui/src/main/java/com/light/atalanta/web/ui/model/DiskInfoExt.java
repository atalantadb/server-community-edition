/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.model;

public class DiskInfoExt {

    private long regionCount;

    private long sizeRegionTotal;

    public long getRegionCount() {
        return regionCount;
    }

    public void setRegionCount(long regionCount) {
        this.regionCount = regionCount;
    }

    public long getSizeRegionTotal() {
        return sizeRegionTotal;
    }

    public void setSizeRegionTotal(long sizeRegionTotal) {
        this.sizeRegionTotal = sizeRegionTotal;
    }
}
