/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.grpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.light.atalanta.region.server.exceptions.FRegionServerUncaughtExceptionHandler;
import com.light.atalanta.region.server.scheduled.CoreHealthChecker;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.region.server.services.RegionServerInfoSupplier;
import com.light.atalanta.region.server.services.ReplicaMapperProcessor;
import com.light.atalanta.region.server.services.operations.progressed.ModifyRegion;
import com.light.atalanta.region.server.utils.UniqueNameThreadFactory;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import com.light.atalanta.services.RegionServerConnector;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.util.ShutdownManager;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallExecutorSupplier;
import io.grpc.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class RegionServerGrpc implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionServerGrpc.class.getName());

    private final RegionServerConfiguration regionServerConfiguration;
    private final RegionServerConnector regionServerConnector;
    private final ReplicaMapperProcessor replicaMapperProcessor;
    private final FileSystem fileSystem;
    private final RegionServerInfoSupplier regionServerInfoSupplier;
    private final ZookeeperHelper zookeeperHelper;
    private final ModifyRegion modifyRegion;

    private ExecutorService executorService = null;

    private Server server;
    private final ZooKeeperCoreContext zooKeeperCoreContext;

    private final ThreadFactory internalThreadFactory = new UniqueNameThreadFactory("atalanta-grpc-internal-pool");
    private final String profile;
    private final MeterRegistry meterRegistry;
    private final RegionServerShutdown shutdown;
    private volatile boolean gracefulExit;
    private ExecutorService requestExecutor;
    private ExecutorService controlExecutor;

    public RegionServerGrpc(RegionServerConfiguration regionServerConfiguration,
                            ReplicaMapperProcessor replicaMapperProcessor, FileSystem fileSystem,
                            RegionServerConnector regionServerConnector,
                            RegionServerInfoSupplier regionServerInfoSupplier, ZookeeperHelper zookeeperHelper,
                            ModifyRegion modifyRegion, ZooKeeperCoreContext zooKeeperCoreContext, String profile,
                            MeterRegistry meterRegistry, CoreHealthChecker coreHealthChecker) {
        this.regionServerConfiguration = regionServerConfiguration;
        this.regionServerConnector = regionServerConnector;
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.fileSystem = fileSystem;
        this.regionServerInfoSupplier = regionServerInfoSupplier;
        this.zookeeperHelper = zookeeperHelper;
        this.modifyRegion = modifyRegion;
        this.zooKeeperCoreContext = zooKeeperCoreContext;
        this.profile = profile;
        this.meterRegistry = meterRegistry;
        this.shutdown = new RegionServerShutdown(regionServerConfiguration, coreHealthChecker, this::finishShutdown);
    }

    public void start() {
        executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setNameFormat("atalanta-rs-grpc-%d")
                .setUncaughtExceptionHandler(new FRegionServerUncaughtExceptionHandler())
                .build());
        executorService.submit(() -> {
            try {
                BindableService service = new RegionServerServiceGrpcImpl(regionServerConfiguration,
                        regionServerInfoSupplier, zookeeperHelper, zooKeeperCoreContext, replicaMapperProcessor,
                        fileSystem, regionServerConnector, modifyRegion,
                        regionServerConfiguration.getGrpcConfiguration().getProfile(), meterRegistry, shutdown);
                requestExecutor = Executors.newFixedThreadPool(regionServerConfiguration.getGrpcConfiguration().getThreadPoolSize(), internalThreadFactory);
                controlExecutor = Executors.newCachedThreadPool(new UniqueNameThreadFactory("atalanta-rs-grpc-control"));
                server = ServerBuilder
                        .forPort(regionServerConfiguration.getGrpcConfiguration().getRegionServerPort())
                        .addService(service)
                        .executor(controlExecutor)
                        .callExecutor(new ServerCallExecutorSupplier() {
                            @Override public <ReqT, RespT> Executor getExecutor(ServerCall<ReqT, RespT> call, Metadata headers) {
                                return RegionServerShutdown.isControlMethod(call.getMethodDescriptor().getFullMethodName())
                                        ? controlExecutor : requestExecutor;
                            }
                        })
                        .intercept(shutdown)
                        .build();
                server.start();
                LOGGER.info("gRPC server started. {}", regionServerConfiguration.getGrpcConfiguration().myName());
            } catch (Exception e) {
                LOGGER.error("gRPC server error! Region server will be shutdown!", e);
                server = null;
            }
            try {
                if (server != null) {
                    server.awaitTermination();
                }
            } catch (InterruptedException e) {
                LOGGER.info("gRPC server interrupted. {}", regionServerConfiguration.getGrpcConfiguration().myName());
            }
            LOGGER.info("gRPC server stopped. {}", regionServerConfiguration.getGrpcConfiguration().myName());
            if (!gracefulExit) ShutdownManager.exitWithError("gRPC server stopped");
        });
    }

    private void finishShutdown() {
        gracefulExit = true;
        try {
            // onCompleted only queues the response; drain the transport before terminating JVM
            server.shutdown();
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.warn("gRPC drain timed out; cancelling remaining calls before RS exit");
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.shutdownNow();
        } finally {
            LOGGER.info("Core stopped, gRPC shutdown finished; terminating RS");
            Runtime.getRuntime().halt(0);
        }
    }

    @Override
    public synchronized void close() {
        gracefulExit = true;
        shutdown.close();
        LOGGER.info("Try to stop gRPC server. {}", regionServerConfiguration.getGrpcConfiguration().myName());
        if (server != null && executorService != null) {
            server.shutdownNow();
            executorService.shutdownNow();
        }
        if (requestExecutor != null) requestExecutor.shutdownNow();
        if (controlExecutor != null) controlExecutor.shutdownNow();
    }
}
