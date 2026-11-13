/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.config;

import com.light.atalanta.metrics.RsJmxClient;
import com.light.atalanta.metrics.RsJmxClientImpl;
import com.light.atalanta.services.*;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WebUiConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebUiConfig.class.getName());

    @Value("${zooKeeperConfiguration.connectionString}")
    private String zkConnectionString;

    @Value("${zooKeeperConfiguration.connectionBasePath}")
    private String connectionBasePath;

    @Value("${zooKeeperConfiguration.connectionRetryTimes}")
    private int connectionRetryTimes;

    @Value("${zooKeeperConfiguration.connectionRetrySleepMs}")
    private int connectionRetrySleepMs;

    @Value("${zooKeeperConfiguration.sessionTimeoutMs}")
    private int sessionTimeoutMs;

    @Bean
    public CuratorFramework curatorFramework(ZookeeperHelper zookeeperHelper) {
        return zookeeperHelper.getCurator();
    }

    @Bean
    public ZookeeperHelper zookeeperHelper() throws Exception {
        ZkConfig zkConfig = new ZkConfig(
                zkConnectionString
        );
        return new ZookeeperHelperImpl(zkConfig,
                connectionBasePath,
                connectionRetryTimes,
                connectionRetrySleepMs,
                sessionTimeoutMs);
    }

    @Bean
    ZookeeperHelperExtended zookeeperHelperExtended(ZookeeperHelper zooHelper) throws Exception {
        return new ZookeeperHelperExtended(zooHelper);
    }

    @Bean
    public ZooKeeperCoreContext zooKeeperCoreContext(
            ZookeeperHelper zooHelper,
            ZookeeperHelperExtended zooHelperExt) {
        return new ZooKeeperCoreContext(zooHelper, zooHelperExt);
    }

    @Bean
    public RsJmxClient rsJmxClient() {
        return new RsJmxClientImpl();
    }

}
