/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.rebalance;

import com.light.atalanta.TableName;
import com.light.atalanta.model.RegionDistributionModel;

import java.util.List;
import java.util.Map;

/**
 * Result of the rebalance analysis. Contains two things:
 * 1. The list of region moves to run to reach the balanced state.
 * 2. The resulting projection of regions.
 */
public class RebalanceBlueprint {
    private final Map<TableName, List<RegionRebalanceModel>> rebalanceModels;

    private final Map<TableName, List<RegionDistributionModel>> projection;

    public RebalanceBlueprint(Map<TableName, List<RegionRebalanceModel>> rebalanceModels, Map<TableName, List<RegionDistributionModel>> projection) {
        this.rebalanceModels = rebalanceModels;
        this.projection = projection;
    }

    public Map<TableName, List<RegionRebalanceModel>> getRebalanceModels() {
        return rebalanceModels;
    }

    public Map<TableName, List<RegionDistributionModel>> getProjection() {
        return projection;
    }
}
