/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.metrics;

import com.light.atalanta.model.RegionServerModel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface RsJmxClient {

    Long METRICS_TTL_MS = 1000L;

    RegionMetrics getRsJmxMetrics(RegionServerModel rs, String regionName, Integer replicaId);

    RegionServerMetrics getRsJmxMetrics(RegionServerModel rs);

    public CompletableFuture<Object> refresh(RegionServerModel rs);

    RegionMetrics getRsJmxMetricsSync(RegionServerModel regionServerModel, String name, Integer replicaNum);

    void clearMetrics();
}
