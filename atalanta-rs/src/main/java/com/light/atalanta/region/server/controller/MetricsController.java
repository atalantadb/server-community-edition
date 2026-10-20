/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.controller;

import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/actuator")
public class MetricsController {
    private final MeterRegistry meterRegistry;
    private final RegionServerConfiguration conf;

    public MetricsController(MeterRegistry meterRegistry, RegionServerConfiguration conf) {
        this.meterRegistry = meterRegistry;
        this.conf = conf;
    }

    @GetMapping(value = "/prometheus", produces = "text/plain;version=0.0.4;charset=utf-8")
    public String getSingleMetric(
            @RequestParam(value = "withRegionMetrics", required = false, defaultValue = "false") boolean withRegionMetrics) {
        PrometheusMeterRegistry registry = (PrometheusMeterRegistry) meterRegistry;
        String contentType = "text/plain;version=0.0.4;charset=utf-8";
        if (withRegionMetrics || conf.getDataConfiguration().isRegionMetricsEnabled()) {
            return registry.scrape(contentType);
        } else {
            return List.of(registry.scrape().split(System.lineSeparator())).stream()
                    .filter(s -> !s.contains("region_"))
                    .collect(Collectors.joining(System.lineSeparator()));
        }
    }
}
