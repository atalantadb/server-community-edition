/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.services;

import com.light.atalanta.metrics.RegionMetrics;
import com.light.atalanta.metrics.RegionServerMetrics;
import com.light.atalanta.model.RegionServerModel;


public interface RsMetricsService {

    RegionServerMetrics getRsMetrics(RegionServerModel r);

    RegionMetrics getRsMetrics(RegionServerModel rs, String name, int replicaNum);

    long getRegionCount();

    long getRegionCountFollower1();

    long getRegionCountFollower2();

    long getSizeTotal();

    long getKeysTotal();

    long getMemUsageBytesTotal();

    long getCountGetTotal();

    long getCountPutTotal();

    long getCountDelTotal();

}
