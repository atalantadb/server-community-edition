/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui;

import io.grpc.LoadBalancerRegistry;
import io.grpc.internal.PickFirstLoadBalancerProvider;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.*;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.core.io.support.PropertySourceFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.StreamSupport;

@SpringBootApplication(exclude = {
        org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration.class,
        org.springframework.boot.actuate.autoconfigure.security.servlet.ManagementWebSecurityAutoConfiguration.class}
)
@ConfigurationProperties(prefix = "yaml")
@PropertySource(value = "file:${atalanta.config}", factory = AtalantaWebUiApplication.YamlPropertySourceFactory.class)
public class AtalantaWebUiApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtalantaWebUiApplication.class);

    public static void main(String[] args) {
        configureLogging(new File("config/log4j.properties"));
        LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());

        SpringApplication.run(AtalantaWebUiApplication.class, args);
    }

    static void configureLogging(File externalConfiguration) {
        if (externalConfiguration.isFile()) {
            PropertyConfigurator.configure(externalConfiguration.getPath());
            return;
        }
        URL bundledConfiguration = AtalantaWebUiApplication.class.getResource("/config/log4j.properties");
        if (bundledConfiguration == null) {
            throw new IllegalStateException("Bundled Web UI logging configuration is missing: config/log4j.properties");
        }
        PropertyConfigurator.configure(bundledConfiguration);
    }

    public void suppressWarningCheckstyleHideutilityclassconstructorDummy() {
        throw new UnsupportedOperationException();
    }

    @EventListener
    public void handleContextRefresh(ContextRefreshedEvent event) {
        final Environment env = event.getApplicationContext().getEnvironment();
        LOGGER.info("====== Environment and configuration ======");
        LOGGER.info("Active profiles: {}", Arrays.toString(env.getActiveProfiles()));
        final MutablePropertySources sources = ((AbstractEnvironment) env).getPropertySources();
        StreamSupport.stream(sources.spliterator(), false)
                .filter(ps -> ps instanceof EnumerablePropertySource)
                .map(ps -> ((EnumerablePropertySource) ps).getPropertyNames())
                .flatMap(Arrays::stream)
                .distinct()
                .filter(prop -> !(prop.contains("credentials") || prop.contains("password")))
                .forEach(prop -> LOGGER.info("{}: {}", prop, env.getProperty(prop)));
        LOGGER.info("===========================================");
    }

    public static class YamlPropertySourceFactory implements PropertySourceFactory {

        @Override
        public org.springframework.core.env.PropertySource<?> createPropertySource(String name, EncodedResource encodedResource)
                throws IOException {
            YamlPropertiesFactoryBean factory = new YamlPropertiesFactoryBean();
            Resource resource = encodedResource.getResource();
            factory.setResources(resource);
            Properties properties = factory.getObject();
            String filename = resource.getFilename();
            if (filename != null) {
                return new PropertiesPropertySource(filename, properties);
            } else {
                throw new RuntimeException("encodedResource.getResource() is null");
            }
        }
    }
}

