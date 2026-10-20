/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.config;

import com.light.atalanta.master.services.metrics.MasetrPrometheusServiceImpl;
import com.light.atalanta.master.services.metrics.MasterPrometheusService;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.services.ZookeeperHelperExtended;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    @Bean
    public MasterPrometheusService prometheusService(MeterRegistry meterRegistry,
                                                     ZookeeperHelperExtended zookeeperHelperExtended,
                                                     ZooKeeperCoreContext zooContext) {
        return new MasetrPrometheusServiceImpl(meterRegistry, zookeeperHelperExtended, zooContext);
    }
}
