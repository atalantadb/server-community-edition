/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.light.atalanta.Environment;
import com.light.atalanta.exceptions.FMasterRuntimeException;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.master.services.*;
import com.light.atalanta.master.services.grpc.MasterInterceptor;
import com.light.atalanta.master.services.grpc.RegionServerConnector;
import com.light.atalanta.master.services.leader.election.InstanceStarter;
import com.light.atalanta.master.services.leader.election.Leader;
import com.light.atalanta.master.services.leader.election.LeaderCandidate;
import com.light.atalanta.master.services.leader.election.LeaderStateManager;
import com.light.atalanta.master.services.leader.election.impl.InstanceStarterImpl;
import com.light.atalanta.master.services.leader.election.impl.LeaderCandidateImpl;
import com.light.atalanta.master.services.leader.election.impl.LeaderImpl;
import com.light.atalanta.master.services.leader.election.impl.LeaderStateManagerImpl;
import com.light.atalanta.master.services.metrics.JmxMasterMetrics;
import com.light.atalanta.master.services.metrics.JmxMasterMetricsImpl;
import com.light.atalanta.master.services.operations.EnableTable;
import com.light.atalanta.master.services.operations.ModifyTable;
import com.light.atalanta.master.services.recovery.RecoveryRSContext;
import com.light.atalanta.master.services.recovery.SnapshotRecovery;
import com.light.atalanta.master.services.runners.PreCommitsRunner;
import com.light.atalanta.master.services.runners.RegionsRebalanceRunner;
import com.light.atalanta.master.services.runners.RegionsRecoveryRunner;
import com.light.atalanta.master.services.runners.RegionsSplitRunner;
import com.light.atalanta.master.services.zookeeper.FMasterConnectionStateListener;
import com.light.atalanta.master.services.zookeeper.FMasterRetryPolicy;
import com.light.atalanta.master.services.zookeeper.FMasterUncaughtExceptionHandler;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.master.vo.*;
import com.light.atalanta.metrics.RsJmxClient;
import com.light.atalanta.metrics.RsJmxClientImpl;
import com.light.atalanta.services.ZkConfig;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.services.ZookeeperHelperExtended;
import com.light.atalanta.services.ZookeeperHelperImpl;
import com.light.atalanta.util.BannerPrinter;
import io.grpc.ServerInterceptor;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jmx.export.MBeanExporter;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.regex.Pattern;

import static com.light.atalanta.ATALANTAConstants.COM_LIGHT_ATALANTA_METRICS;

@Configuration
@SuppressWarnings("unused")
public class MasterConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterConfig.class);

    @Value("${spring.profiles.active:default}")
    private String profile;

    static {
        BannerPrinter.print();
    }

    public static String formatCurDate() {
        SimpleDateFormat df = new SimpleDateFormat("dd.MM.yyyy'T'HH:mm:ssX");
        return df.format(new Date());
    }

    @Bean
    public Master master(AtalantaDBConfiguration fastDBConfiguration, ZookeeperHelper zooHelper,
                         PreCommitsRunner preCommitsRunner, RegionsRecoveryRunner regionsRecoveryRunner,
                         RegionsSplitRunner regionsSplitRunner, ZooKeeperMasterContext zooContext,
                         RegionServerConnector regionServerConnector, RecoveryRSContext recoveryRSContext,
                         RegionsRebalanceRunner regionsRebalanceRunner, ZookeeperHelperExtended zookeeperHelperExtended,
                         RsJmxClient rsJmxClient, RecoveryLogger recoveryLogger, MasterStartLogger masterStartLogger,
                         ModifyTable modifyTable, LockManager lockManager, EnableTable enableTable,
                         ExecutorService executorService) throws ZKException {
        return new MasterImpl(fastDBConfiguration, zooHelper, zooContext, regionServerConnector, recoveryRSContext,
                modifyTable, preCommitsRunner, regionsRecoveryRunner, regionsSplitRunner, regionsRebalanceRunner,
                zookeeperHelperExtended, rsJmxClient, recoveryLogger, masterStartLogger, lockManager, enableTable,
                executorService);
    }

    @Bean
    public ModifyTable modifyTable(AtalantaDBConfiguration conf, ZooKeeperMasterContext zooContext,
                                   ZookeeperHelper zooHelper, LockManager lockManager, EnableTable enableTable) {
       return new ModifyTable(conf, zooContext, zooHelper, enableTable, lockManager);
    }

    @Bean
    public LockManager lockManager(AtalantaDBConfiguration conf) {
        return new LockManager(conf);
    }

    @Bean
    public EnableTable enableTable(AtalantaDBConfiguration conf, ZookeeperHelper zooHelper,
                                   ZooKeeperMasterContext zooContext, RecoveryRSContext recoveryRSContext,
                                   RegionServerConnector regionServerConnector,
                                   ExecutorService executorService){
        return new EnableTable(conf, zooHelper, zooContext, recoveryRSContext, regionServerConnector, executorService);
    }

    @Bean
    public PreCommitsRunner preCommitsRunner(ZookeeperHelper zooHelper, RecoveryRSContext recoveryRSContext,
                                             AtalantaDBConfiguration conf, ExecutorService executorService,
                                             ZooKeeperMasterContext zooContext) {
        return new PreCommitsRunner(zooHelper, recoveryRSContext, conf, executorService, zooContext);
    }

    @Bean
    public RegionsRecoveryRunner regionsRecoveryRunner(AtalantaDBConfiguration conf, ZooKeeperMasterContext zooContext,
                                                       ZookeeperHelper zooHelper, RecoveryRSContext recoveryRSContext,
                                                       PreCommitsRunner preCommitsRunner,
                                                       SnapshotRecovery snapshotRecovery, RecoveryLogger recoveryLogger,
                                                       ExecutorService execturService) {
        return new RegionsRecoveryRunner(conf, zooContext, zooHelper, recoveryRSContext, preCommitsRunner,
                snapshotRecovery, recoveryLogger, execturService);
    }

    @Bean
    public RegionsSplitRunner regionsSplitRunner(AtalantaDBConfiguration conf, ZooKeeperMasterContext zooContext,
                                                 ZookeeperHelper zooHelper, RsJmxClient rsJmxClient,
                                                 RecoveryRSContext recoveryRSContext) {
        return new RegionsSplitRunner(conf, zooContext, zooHelper, rsJmxClient, recoveryRSContext);
    }

    @Bean
    public RegionsRebalanceRunner regionsRebalanceRunner(AtalantaDBConfiguration conf,
                                                         RecoveryRSContext recoveryRSContext) {
        return new RegionsRebalanceRunner(conf, recoveryRSContext);
    }

    @Bean
    public AtalantaDBConfiguration fastDBConfiguration() throws IOException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        String atalantaConfig = System.getProperty("atalanta.config", "classpath:master-configuration.yml");
        LOGGER.info("ATALANTA master config file: {}", atalantaConfig);
        File configFile = ResourceUtils.getFile(atalantaConfig);
        Preconditions.checkArgument(configFile.exists(), "YAML configuration doesn't exist! path: " + atalantaConfig);
        return yamlMapper.readValue(configFile, AtalantaDBConfiguration.class);
    }

    @Bean
    public ZooKeeperConfiguration zooKeeperConfiguration(AtalantaDBConfiguration conf) {
        return conf.getZooKeeperConfiguration();
    }

    @Bean
    public GrpcConfiguration grpcConfiguration(AtalantaDBConfiguration conf) {
        final GrpcConfiguration grpcConfiguration = conf.getGrpcConfiguration();
        grpcConfiguration.init(profile);
        return grpcConfiguration;
    }

    @Bean
    public DataConfiguration dataConfiguration(AtalantaDBConfiguration conf) {
        return conf.getDataConfiguration();
    }

    @Bean
    public CuratorFramework curatorFramework(ZookeeperHelper zookeeperHelper) {
        return zookeeperHelper.getCurator();
    }

    @Bean
    public RetryPolicy retryPolicy(ZooKeeperConfiguration zooConf) {
        return new FMasterRetryPolicy(zooConf);
    }

    @Bean
    ConnectionStateListener connectionStateListener() {
        return new FMasterConnectionStateListener();
    }


    @Bean
    public TestConfiguration testConfiguration(AtalantaDBConfiguration conf) {
        return conf.getTestConfiguration();
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
    public ZookeeperHelperExtended zookeeperHelperExtended(ZookeeperHelper zooHelper) throws Exception {
        return new ZookeeperHelperExtended(zooHelper);
    }

    @Bean(destroyMethod = "shutdown")
    public ExecutorService cachedThreadPoolExecutor(Thread.UncaughtExceptionHandler handler) {
        final ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("master-operation-pool-%d")
                .setUncaughtExceptionHandler(handler)
                .setDaemon(true)
                .build();
        int maximumPoolSize = Runtime.getRuntime().availableProcessors() * 20;
        return new ThreadPoolExecutor(0, maximumPoolSize, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(), factory, (runnable, executor) -> {
            if (executor.isShutdown()) {
                throw new RejectedExecutionException("Executor is shut down, task rejected: " + runnable);
            }
            // Run the nested task in the current thread to avoid a starvation deadlock
            runnable.run();
        });
    }

    @Bean
    public Thread.UncaughtExceptionHandler loggingExceptionHandler() {
        return (t, e) -> LOGGER.error("Uncaught exception in thread: {}", t, e);
    }

    @Bean
    public ZooKeeperMasterContext zooContext(ZookeeperHelper zooHelper, ZookeeperHelperExtended zookeeperHelperExtended, AtalantaDBConfiguration fastDBConfiguration) {
        return new ZooKeeperMasterContext(zooHelper, zookeeperHelperExtended, fastDBConfiguration);
    }

    @Bean
    public LeaderStateManager leaderStateManager(LeaderCandidate leaderCandidate,
                                                 RetryPolicy retryPolicy,
                                                 CuratorFramework curatorFramework,
                                                 ConnectionStateListener connectionStateListener,
                                                 RecoveryLogger recoveryLogger,
                                                 MasterStartLogger masterStartLogger,
                                                 AtalantaDBConfiguration fastDBConfiguration,
                                                 ServerInterceptor masterInterceptor,
                                                 MeterRegistry meterRegistry) {
        LeaderStateManagerImpl leaderStateManager = new LeaderStateManagerImpl(leaderCandidate, curatorFramework,
                recoveryLogger, masterStartLogger, fastDBConfiguration, masterInterceptor, meterRegistry);
        // Avoids a circular dependency
        ((FMasterRetryPolicy) retryPolicy).setLeaderStateManager(leaderStateManager);
        ((FMasterConnectionStateListener) connectionStateListener).setLeaderStateManager(leaderStateManager);
        curatorFramework.getConnectionStateListenable().addListener(connectionStateListener);
        return leaderStateManager;
    }

    @Bean
    public InstanceStarter instanceStarter(ZooKeeperConfiguration zooKeeperConfiguration,
                                           GrpcConfiguration grpcConf) {
        return new InstanceStarterImpl(zooKeeperConfiguration, grpcConf);
    }

    @Bean
    public LeaderCandidate leaderCandidate(GrpcConfiguration grpcConf, ZookeeperHelper zooHelper) {
        return new LeaderCandidateImpl(grpcConf, zooHelper);
    }

    @Bean
    public Leader leader(AtalantaDBConfiguration fastDBConfiguration, LeaderStateManager leaderStateManager, Master master,
                         MasterInterceptor masterInterceptor,
                         RecoveryLogger recoveryLogger, MasterStartLogger masterStartLogger) {
        return new LeaderImpl(fastDBConfiguration, master, leaderStateManager,
                masterInterceptor, masterStartLogger);
    }

    @Bean
    public RecoveryLogger recoveryLogger() {
        return new RecoveryLoggerImpl();
    }

    @Bean
    public MasterStartLogger masterStartLogger() {
        return new MasterStartLoggerImpl();
    }

    @Bean
    public MasterInterceptor masterInterceptor() {
        return new MasterInterceptor();
    }

    @Bean
    public RegionServerConnector regionServerConnector(AtalantaDBConfiguration conf, RecoveryRSContext recoveryRSContext) {
        return new RegionServerConnector(conf, recoveryRSContext);
    }

    @Bean
    public RecoveryRSContext payloadedRegionServersHolder(RecoveryLogger recoveryLogger) {
        return new RecoveryRSContext(recoveryLogger);
    }

    @Bean
    public SnapshotRecovery snapshotRecovery(ZookeeperHelper zookeeperHelper, ZooKeeperMasterContext zooKeeperMasterContext, RegionServerConnector regionServerConnector) {
        return new SnapshotRecovery(zookeeperHelper, zooKeeperMasterContext,regionServerConnector);
    }

    @Bean
    public MBeanExporter mBeanExporter(JmxMasterMetrics jmxRsMetrics) {
        MBeanExporter res = new MBeanExporter();
        Map<String, Object> beans = new HashMap<>();
        beans.put(COM_LIGHT_ATALANTA_METRICS + "AtalantaMaster", jmxRsMetrics);
        res.setBeans(beans);
        return res;
    }

    @Bean
    public JmxMasterMetrics jmxMasterMetrics() {
        return new JmxMasterMetricsImpl();
    }

    @Bean
    public RsJmxClient rsJmxClient(AtalantaDBConfiguration conf) {
        return new RsJmxClientImpl();
    }

    private ThreadFactory buildThreadFactory() {
        return new ThreadFactoryBuilder()
                .setNameFormat("FMaster-curator-%d")
                .setDaemon(false)
                .setUncaughtExceptionHandler(new FMasterUncaughtExceptionHandler())
                .build();
    }
}
