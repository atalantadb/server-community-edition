/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.model;

import com.light.atalanta.metrics.VersionInfo;
import prometheus.types.Metric;
import prometheus.types.MetricFamily;

public class VersionInfoExt extends VersionInfo {

    public VersionInfoExt(MetricFamily ver) {
        if (ver == null && ver.getMetrics().isEmpty()) {
            setBuild(UNKNOWN);
            setDate(UNKNOWN);
            setCommit(UNKNOWN);
            return;
        }
        Metric metric = ver.getMetrics().get(0);
        setBuild(metric.getLabels().getOrDefault("build", UNKNOWN));
        setDate(metric.getLabels().getOrDefault("date", UNKNOWN));
        setCommit(metric.getLabels().getOrDefault("commit", UNKNOWN));
    }

    public VersionInfoExt() {
        setBuild(UNKNOWN);
        setDate(UNKNOWN);
        setCommit(UNKNOWN);
    }
}
