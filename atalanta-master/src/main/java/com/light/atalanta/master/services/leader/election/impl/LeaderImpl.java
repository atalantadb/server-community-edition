/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.leader.election.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.light.atalanta.master.services.*;
import com.light.atalanta.master.services.grpc.MasterInterceptor;
import com.light.atalanta.master.services.grpc.MasterServiceGrpcImpl;
import com.light.atalanta.master.services.grpc.MasterShutdown;
import com.light.atalanta.master.services.leader.election.Leader;
import com.light.atalanta.master.services.leader.election.LeaderStateManager;
import com.light.atalanta.master.services.zookeeper.FMasterUncaughtExceptionHandler;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCallExecutorSupplier;
import io.grpc.ServerCall;
import io.grpc.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.light.atalanta.master.services.leader.election.impl.LeaderStateManagerImpl.ER_GRPC_SERVER;

public class LeaderImpl implements Leader {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderImpl.class.getName());
    private final AtalantaDBConfiguration fastDBConfiguration;
    private final Master master;
    private final LeaderStateManager leaderStateManager;
    private Server server;
    private ExecutorService executorService = null;
    private ExecutorService requestExecutor;
    private ExecutorService controlExecutor;
    private MasterShutdown shutdown;
    private final MasterInterceptor masterInterceptor;
    private final ThreadFactory internalThreadFactory = new UniqueNameThreadFactory("atalanta-grpc-internal-pool");
    private final MasterStartLogger masterStartLogger;


    public LeaderImpl(AtalantaDBConfiguration fastDBConfiguration, Master master, LeaderStateManager leaderStateManager,
                      MasterInterceptor masterInterceptor,
                      MasterStartLogger masterStartLogger) {
        this.fastDBConfiguration = fastDBConfiguration;
        this.master = master;
        this.leaderStateManager = leaderStateManager;
        this.masterInterceptor = masterInterceptor;
        this.masterStartLogger = masterStartLogger;
    }

    @Override
    public synchronized void start() {
        LOGGER.info("Try to start GRPC server. {}", fastDBConfiguration.getGrpcConfiguration().myName());
        executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setNameFormat("atalanta-master-grpc-%d")
                .setUncaughtExceptionHandler(new FMasterUncaughtExceptionHandler())
                .build());
        executorService.submit(() -> {
            try {
                shutdown = new MasterShutdown(master, masterInterceptor,
                        fastDBConfiguration.getGrpcConfiguration().getShutdownRsTimeoutMs(), this::finishShutdown);
                BindableService service = new MasterServiceGrpcImpl(master, leaderStateManager, fastDBConfiguration, masterInterceptor, shutdown);
                requestExecutor = Executors.newFixedThreadPool(fastDBConfiguration.getGrpcConfiguration().getThreadPoolSize(), internalThreadFactory);
                controlExecutor = Executors.newCachedThreadPool(new UniqueNameThreadFactory("atalanta-master-grpc-control"));
                server = ServerBuilder
                        .forPort(fastDBConfiguration.getGrpcConfiguration().getMasterPort())
                        .addService(service)
                        .executor(controlExecutor)
                        .callExecutor(new ServerCallExecutorSupplier() {
                            @Override public <ReqT, RespT> Executor getExecutor(ServerCall<ReqT, RespT> call, Metadata headers) {
                                return MasterInterceptor.isControlMethod(call.getMethodDescriptor().getFullMethodName())
                                        || masterInterceptor.isShuttingDown() ? controlExecutor : requestExecutor;
                            }
                        })
                        .intercept(masterInterceptor)
                        .build();
                masterStartLogger.setState(MasterStartState.TRY_TO_START_MASTER);
                master.start(server);
            } catch (Exception e) {
                if (masterInterceptor.isShuttingDown()) {
                    LOGGER.info("Master startup interrupted by shutdown_rs", e);
                    return;
                }
                LOGGER.error("Master start error", e);
                server = null;
                leaderStateManager.shutdown(ER_GRPC_SERVER);
            }
            try {
                LOGGER.info("Master GRPC server started. {}", fastDBConfiguration.getGrpcConfiguration().myName());
                masterStartLogger.setState(MasterStartState.STARTED);
                server.awaitTermination();
            } catch (InterruptedException e) {
                LOGGER.info("GRPC server interrupted. {}", fastDBConfiguration.getGrpcConfiguration().myName());
            }
            LOGGER.info("GRPC server stopped. {}", fastDBConfiguration.getGrpcConfiguration().myName());
        });
    }


    private void finishShutdown(boolean success) {
        try {
            server.shutdown();
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.warn("gRPC drain timed out during shutdown_rs");
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.shutdownNow();
        } finally {
            leaderStateManager.shutdown(success ? 0 : 1);
        }
    }

    @Override
    public synchronized void close() {
        if (shutdown != null) shutdown.close();
        LOGGER.info("Try to stop GRPC server. {}", fastDBConfiguration.getGrpcConfiguration().myName());
        if (server != null && executorService != null) {
            server.shutdownNow();
            executorService.shutdownNow();
        }
        if (requestExecutor != null) requestExecutor.shutdownNow();
        if (controlExecutor != null) controlExecutor.shutdownNow();
    }

}
