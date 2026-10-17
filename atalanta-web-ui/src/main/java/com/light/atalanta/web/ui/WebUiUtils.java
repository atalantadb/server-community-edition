/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui;


import prometheus.PrometheusMetricsProcessor;
import prometheus.text.TextPrometheusMetricsProcessor;
import prometheus.types.MetricFamily;
import prometheus.walkers.CollectorPrometheusMetricsWalker;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class WebUiUtils {
    public static final String F1 = "%.1f";
    public static final String F3 = "%.3f";

    private WebUiUtils() {
    }

    public static String toHumanReadebleDiskSize(long diskSize) {
        return toHumanReadebleDiskSizeInternal(diskSize, F1);
    }

    public static String toHumanReadebleDiskSize3(long diskSize) {
        return toHumanReadebleDiskSizeInternal(diskSize, F3);
    }


    private static String toHumanReadebleDiskSizeInternal(long diskSize, String format) {
        if (diskSize < 100_000) {
            return String.format("%d B", diskSize);
        }
        if (diskSize < 1024 * 1024) {
            return String.format(format + " KB", diskSize / 1024.0);
        }
        if (diskSize < 1024 * 1024 * 1024) {
            return String.format(format + " MB", diskSize / 1024.0 / 1024.0);
        }
        if (diskSize < 1024 * 1024 * 1024 * 1024L) {
            return String.format(format + " GB", diskSize / 1024.0 / 1024.0 / 1024);
        }
        return String.format(format + " TB", diskSize / 1024.0 / 1024.0 / 1024 / 1024);
    }

    public static List<MetricFamily> parseMetrics(String scrape) {
        InputStream inputStream = new ByteArrayInputStream(scrape.getBytes(UTF_8));
        CollectorPrometheusMetricsWalker walker = new CollectorPrometheusMetricsWalker();
        PrometheusMetricsProcessor<MetricFamily> processor = new TextPrometheusMetricsProcessor(inputStream, walker);
        processor.walk();
        return walker.getAllMetricFamilies();
    }
}