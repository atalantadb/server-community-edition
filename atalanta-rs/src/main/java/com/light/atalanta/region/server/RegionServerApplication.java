/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server;

import com.light.atalanta.region.server.config.RegionServerConfig;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(excludeName = {
    "org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration",
    "org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration",
    "org.springframework.boot.actuate.autoconfigure.security.servlet.ManagementWebSecurityAutoConfiguration"
})
public class RegionServerApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionServerApplication.class.getName());

    public static long getBeginTime() {
        return beginTime;
    }

    private static long beginTime;

    public static void main(String[] args) {
        PropertyConfigurator.configure("config/log4j.properties");
        beginTime = System.currentTimeMillis();
        LOGGER.info("Starting ...");
        // Start Spring Boot
        SpringApplication.run(RegionServerApplication.class, args);
    }

    private static Class<?>[] getConfClasses() {
        return new Class<?>[] {RegionServerConfig.class};
    }
}
