/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import com.light.atalanta.region.server.scheduled.CoreHealthChecker;
import com.light.atalanta.region.server.scheduled.ZkHealthChecker;
import com.light.atalanta.region.server.services.CoreManager;
import com.light.atalanta.region.server.services.CoreManagerImpl;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.region.server.services.FileSystemImpl;
import com.light.atalanta.region.server.services.MasterConnector;
import com.light.atalanta.region.server.services.RegionServer;
import com.light.atalanta.region.server.services.RegionServerImpl;
import com.light.atalanta.region.server.services.RegionServerInfoSupplier;
import com.light.atalanta.region.server.services.ReplicaMapperProcessor;
import com.light.atalanta.region.server.services.grpc.RegionServerGrpc;
import com.light.atalanta.region.server.services.metrics.JmxRsMetrics;
import com.light.atalanta.region.server.services.metrics.JmxRsMetricsImpl;
import com.light.atalanta.region.server.services.metrics.PrometheusService;
import com.light.atalanta.region.server.services.metrics.PrometheusServiceImpl;
import com.light.atalanta.region.server.services.operations.progressed.ModifyRegion;
import com.light.atalanta.region.server.services.rebalance.DiskMappingService;
import com.light.atalanta.region.server.services.rebalance.DiskMappingServiceImpl;
import com.light.atalanta.region.server.services.rebalance.DisksRebalancePlanner;
import com.light.atalanta.region.server.services.rebalance.DisksRebalancePlannerImpl;
import com.light.atalanta.region.server.services.rebalance.DisksRebalanceProcessor;
import com.light.atalanta.region.server.services.rebalance.DisksRebalanceProcessorImpl;
import com.light.atalanta.region.server.vo.GrpcConfiguration;
import com.light.atalanta.region.server.vo.HealthCheckerConfiguration;
import com.light.atalanta.region.server.vo.MasterConfiguration;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import com.light.atalanta.region.server.vo.ZooKeeperConfiguration;
import com.light.atalanta.services.*;
import com.light.atalanta.util.BannerPrinter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jmx.export.MBeanExporter;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static com.light.atalanta.ATALANTAConstants.COM_LIGHT_ATALANTA_METRICS;

@Configuration
public class RegionServerConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegionServerConfig.class);

    @Value("${spring.profiles.active:default}")
    private String profile;

    static {
        BannerPrinter.print();
    }

    @Bean
    public RegionServer regionServer(RegionServerInfoSupplier serverInfo,
                                     ZookeeperHelperExtended zookeeperHelperExtended, FileSystem fileSystem,
                                     RegionServerGrpc grpcServer, CoreManager coreManager,
                                     RegionServerConfiguration regionServerConfiguration,
                                     ReplicaMapperProcessor regionMapperProcessor,
                                     ZkHealthChecker zkHealthChecker, CoreHealthChecker coreHealthChecker,
                                     MasterConnector masterConnector, DisksRebalanceProcessor disksRebalanceProcessor,
                                     ModifyRegion modifyRegion) {
        return new RegionServerImpl(serverInfo, zookeeperHelperExtended, fileSystem, grpcServer,
                coreManager, regionServerConfiguration, regionMapperProcessor, zkHealthChecker,
                coreHealthChecker, masterConnector, disksRebalanceProcessor, profile, modifyRegion);
    }

    @Bean
    public ModifyRegion modifyRegion(ZooKeeperCoreContext zooKeeperCoreContext, ZookeeperHelper zooHelper,
                                     RegionServerConfiguration conf,
                                     RegionServerInfoSupplier regionServerInfoSupplier,
                                     ReplicaMapperProcessor replicaMapperProcessor, FileSystem fileSystem,
                                     MasterConnector masterConnector) {
        return new ModifyRegion(zooKeeperCoreContext, zooHelper, conf, regionServerInfoSupplier, replicaMapperProcessor,
                fileSystem, masterConnector);
    }

    @Bean
    public RegionServerConfiguration regionServerConfiguration() throws IOException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        File configFile = new File("config/region-server-configuration.yml");
        Preconditions.checkArgument(configFile.exists(), "YAML configuration doesn't exist!");
        RegionServerConfiguration res = yamlMapper.readValue(configFile, RegionServerConfiguration.class);
        res.getGrpcConfiguration().init(profile);
        return res;
    }

    @Bean
    public ZooKeeperConfiguration zooKeeperConfiguration(RegionServerConfiguration conf) {
        return conf.getZooKeeperConfiguration();
    }

    @Bean
    public GrpcConfiguration grpcConfiguration(RegionServerConfiguration conf) {
        GrpcConfiguration grpcConfiguration = conf.getGrpcConfiguration();
        // TODO: Move this initialization to the root configuration
        grpcConfiguration.init(profile);
        return grpcConfiguration;
    }

    @Bean
    public MasterConfiguration masterConfiguration(RegionServerConfiguration conf) {
        return conf.getMasterConfiguration();
    }

    @Bean
    public HealthCheckerConfiguration healthCheckerConfiguration(RegionServerConfiguration conf) {
        return conf.getHealthCheckerConfiguration();
    }

    @Bean
    public ZooKeeperCoreContext zooKeeperCoreContext(ZookeeperHelper zookeeperHelper, ZookeeperHelperExtended zooHelperExtended) {
        return new ZooKeeperCoreContext(zookeeperHelper, zooHelperExtended);
    }

    @Bean
    public ZookeeperHelper zooHelper(ZooKeeperConfiguration zooConf) throws Exception {
        ZkConfig zkConfig = new ZkConfig(
                zooConf.getConnectionString()
        );
        return new ZookeeperHelperImpl(zkConfig, zooConf.getConnectionBasePath(),
                zooConf.getConnectionRetryTimes(), zooConf.getConnectionRetrySleepMs(), zooConf.getSessionTimeoutMs());
    }

    @Bean
    public ZookeeperHelperExtended zooHelperExtended(ZookeeperHelper zooHelper) throws Exception {
        return new ZookeeperHelperExtended(zooHelper);
    }

    @Bean
    public RegionServerGrpc regionServerGrpc(RegionServerConfiguration regionServerConfiguration,
                                             ReplicaMapperProcessor replicaMapperProcessor, FileSystem fileSystem,
                                             RegionServerConnector regionServerConnector,
                                             RegionServerInfoSupplier regionServerInfoSupplier,
                                             ZookeeperHelper zookeeperHelper, ZooKeeperCoreContext zooKeeperCoreContext,
                                             ModifyRegion modifyRegion, MeterRegistry meterRegistry, CoreHealthChecker coreHealthChecker) {
        return new RegionServerGrpc(regionServerConfiguration, replicaMapperProcessor, fileSystem,
                regionServerConnector, regionServerInfoSupplier, zookeeperHelper, modifyRegion, zooKeeperCoreContext,
                profile, meterRegistry, coreHealthChecker);
    }

    @Bean(destroyMethod = "destroy")
    public ZkHealthChecker healthChecker(RegionServerInfoSupplier serverInfo,
                                         ZookeeperHelperExtended zookeeperHelperExtended,
                                         HealthCheckerConfiguration healthCheckerConf) {
        return new ZkHealthChecker(serverInfo, zookeeperHelperExtended, healthCheckerConf, profile);
    }

    @Bean(destroyMethod = "destroy")
    public CoreHealthChecker coreHealthChecker(RegionServerConfiguration conf,
                                               ZookeeperHelperExtended zookeeperHelperExtended,
                                               RegionServerInfoSupplier serverInfo,
                                               JmxRsMetrics jmxRsMetrics) {
        return new CoreHealthChecker(serverInfo, zookeeperHelperExtended, conf, jmxRsMetrics);
    }

    @Bean
    public ReplicaMapperProcessor replicaMapperProcessor(RegionServerConfiguration regionServerConfiguration, FileSystem fileSystem) {
        return new ReplicaMapperProcessor(regionServerConfiguration, fileSystem);
    }

    @Bean
    public RegionServerInfoSupplier infoSupplier(ZookeeperHelperExtended zookeeperHelperExtended,
                                                 RegionServerConfiguration rsConf) throws Exception {
        return new RegionServerInfoSupplier(zookeeperHelperExtended, rsConf, profile);
    }

    @Bean
    public RegionServerConnector regionServerConnector(RegionServerConfiguration conf) {
        return new RegionServerConnector(
                conf.getGrpcConfiguration().getRegionServerConnectTimeoutMs(),
                conf.getGrpcConfiguration().getReceiveRegionTimeoutMs(),
                conf.getMasterConfiguration().getProgressCalculatorIntervalMs(), (tn) -> false
        );
    }

    @Bean
    public MasterConnector masterConnector(MasterConfiguration masterConf, ZookeeperHelperExtended zookeeperHelperExtended) {
        return new MasterConnector(masterConf, zookeeperHelperExtended);
    }

    @Bean
    public FileSystem fileSystem(RegionServerConfiguration conf) {
        return new FileSystemImpl(conf);
    }

    @Bean
    public CoreManager coreManager(RegionServerConfiguration coreConf, ZooKeeperCoreContext zooContext,
                                   ReplicaMapperProcessor replicaMapperProcessor, ZookeeperHelper zooHelper) {
        return new CoreManagerImpl(coreConf, zooContext, replicaMapperProcessor, zooHelper);
    }

    @Bean
    public MBeanExporter mBeanExporter(JmxRsMetrics jmxRsMetrics) {
        MBeanExporter res = new MBeanExporter();
        Map<String, Object> beans = new HashMap<>();
        beans.put(COM_LIGHT_ATALANTA_METRICS + "RegionServer", jmxRsMetrics);
        res.setBeans(beans);
        return res;
    }

    @Bean
    public JmxRsMetrics jmxRsMetrics(ReplicaMapperProcessor replicaMapperProcessor, RegionServerConfiguration conf,
                                     PrometheusService prometheusService) {
        return new JmxRsMetricsImpl(replicaMapperProcessor, prometheusService, conf.getRsDirs());
    }

    @Bean
    public PrometheusService prometheusService(MeterRegistry meterRegistry,
                                               RegionServerInfoSupplier serverInfo,
                                               ZooKeeperCoreContext zooKeeperCoreContext,
                                               ReplicaMapperProcessor replicaMapperProcessor,
                                               RegionServerConfiguration conf) {
            return new PrometheusServiceImpl(meterRegistry, serverInfo, zooKeeperCoreContext, replicaMapperProcessor, conf);
    }

    @Bean
    public DisksRebalanceProcessor disksRebalanceProcessor(RegionServerConfiguration conf,
                                                          DiskMappingService diskMappingService,
                                                          DisksRebalancePlanner disksRebalancePlanner) {
        return new DisksRebalanceProcessorImpl(conf, diskMappingService, disksRebalancePlanner);
    }

    @Bean
    public DisksRebalancePlanner disksRebalancePlanner() {
        return new DisksRebalancePlannerImpl();
    }

    @Bean
    public DiskMappingService diskMappingService(FileSystem fileSystem, ReplicaMapperProcessor replicaMapperProcessor) {
        return new DiskMappingServiceImpl(fileSystem, replicaMapperProcessor);
    }
}
