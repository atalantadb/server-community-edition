/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.model;

import com.google.common.base.Preconditions;

/**
 * A region bound to a region server. The binding may be actual or planned.
 */
public class RegionDistributionModel {

    private final RegionModel regionModel;

    private final RegionServerModel regionServerModel;

    private final String toString;

    private final String toShortString;

    private final String toHumanString;

    public RegionDistributionModel(RegionModel regionModel, RegionServerModel regionServerModel) {
        Preconditions.checkNotNull(regionModel, "RegionModel for RegionDistributionModel can't be null");
        Preconditions.checkNotNull(regionServerModel, "RegionServerModel for RegionDistributionModel can't be null");
        this.regionModel = regionModel;
        this.regionServerModel = regionServerModel;
        this.toString = String.join(":", regionModel.toString(), regionServerModel.toString());
        this.toShortString = String.join(":", regionModel.toShortString(), regionServerModel.getLocationString());
        this.toHumanString = String.format("region: %s, server: %s", regionModel.toHumanString(), regionServerModel);
    }

    public RegionModel getRegionModel() {
        return regionModel;
    }

    public RegionServerModel getRegionServerModel() {
        return regionServerModel;
    }

    @Override
    public String toString() {
        return toString;
    }

    public String toShortString() {
        return toShortString;
    }

    /**
     * Representation for the shell.
     */
    public String toHumanString() {
        return toHumanString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RegionDistributionModel that = (RegionDistributionModel) o;

        if (!regionModel.equals(that.regionModel)) {
            return false;
        }
        return regionServerModel.equals(that.regionServerModel);
    }

    @Override
    public int hashCode() {
        int result = regionModel.hashCode();
        result = 31 * result + regionServerModel.hashCode();
        return result;
    }
}
