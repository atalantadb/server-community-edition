/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.rebalance;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * A planned region move within rebalancing.
 * Note that equals() and hashCode() are based on regionId.
 */
public class RegionRebalanceModel {
    // Id of the region to move
    private final String regionId;

    // Id of the source RS. It may change by the time the rebalancing
    // actually runs, so it is used
    // for logging only.
    private final String rsSrc;

    // Id of the target RS
    private String rsDst;

    private final String copyFromRs;

    public RegionRebalanceModel(String regionId, String rsSrc, String rsDst) {
        this(regionId, rsSrc, rsDst, StringUtils.EMPTY);
    }
    public RegionRebalanceModel(String regionId, String rsSrc, String rsDst, String copyFromRs) {
        this.regionId = regionId;
        this.rsSrc = rsSrc;
        this.rsDst = rsDst;
        this.copyFromRs = copyFromRs;
    }

    public String getRegionId() {
        return regionId;
    }

    public String getRsSrc() {
        return rsSrc;
    }

    public String getRsDst() {
        return rsDst;
    }

    public String getCopyFromRs() {
        return copyFromRs;
    }

    // Updating rsDst avoids multiple moves:
    // rs1 -> rs2 -> rs3 is reduced to rs1 -> rs3
    public void updateRsDst(String rsDst) {
        this.rsDst = rsDst;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegionRebalanceModel that = (RegionRebalanceModel) o;
        return Objects.equals(regionId, that.regionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(regionId);
    }

    @Override
    public String toString() {
        return String.format("[%s from %s to %s]", regionId, rsSrc, rsDst);
    }
}
