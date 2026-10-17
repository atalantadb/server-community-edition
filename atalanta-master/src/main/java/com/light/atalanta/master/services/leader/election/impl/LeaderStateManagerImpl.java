/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.leader.election.impl;

import com.light.atalanta.master.Application;
import com.light.atalanta.master.enums.LeaderState;
import com.light.atalanta.master.services.MasterStartLogger;
import com.light.atalanta.master.services.MasterStartState;
import com.light.atalanta.master.services.RecoveryLogger;
import com.light.atalanta.master.services.UniqueNameThreadFactory;
import com.light.atalanta.master.services.grpc.MasterMonitoringGrpcImpl;
import com.light.atalanta.master.services.leader.election.InstanceStarter;
import com.light.atalanta.master.services.leader.election.Leader;
import com.light.atalanta.master.services.leader.election.LeaderCandidate;
import com.light.atalanta.master.services.leader.election.LeaderStateManager;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.master.vo.GrpcConfiguration;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.BindException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.light.atalanta.ATALANTAConstants.F_MASTER_NODE;
import static com.light.atalanta.ATALANTAConstants.ZK_SEPARATOR;
import static com.light.atalanta.master.services.leader.election.impl.LeaderCandidateImpl.GRACE_PERIOD_MS;
import static java.nio.charset.StandardCharsets.UTF_8;

public class LeaderStateManagerImpl implements LeaderStateManager, LeaderLatchListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderStateManagerImpl.class.getName());
    /**
     * Exit code when the leader becomes a follower.
     */
    public static final int ER_TO_FOLLOWER = 1;
    /**
     * Exit code when the gRPC server fails to start.
     */
    public static final int ER_GRPC_SERVER = 2;
    /**
     * Exit code when the gRPC server executor fails to terminate.
     */
    public static final int ER_EXECUTOR_TERMINATION_TIMEOUT = 3;
    /**
     * Exit code on a shutdown command received over gRPC.
     */
    public static final int ER_EXECUTOR_TERMINATION_GRPC = 4;
    private InstanceStarter instanceStarter;
    private final LeaderCandidate leaderCandidate;
    private final CuratorFramework curatorFramework;
    private volatile LeaderState curLeaderState = LeaderState.INSTANCE_STARTED;
    private GrpcConfiguration grpcConf;
    private Leader leader;
    private final ThreadFactory internalThreadFactory = new UniqueNameThreadFactory("atalanta-grpc-master-info-pool");
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final RecoveryLogger recoveryLogger;
    private final MasterStartLogger masterStartLogger;
    private final AtalantaDBConfiguration fastDBConfiguration;
    private final ServerInterceptor masterInterceptor;
    private final MeterRegistry meterRegistry;

    public LeaderStateManagerImpl(LeaderCandidate leaderCandidate, CuratorFramework curatorFramework,
                                  RecoveryLogger recoveryLogger, MasterStartLogger masterStartLogger,
                                  AtalantaDBConfiguration fastDBConfiguration, ServerInterceptor masterInterceptor,
                                  MeterRegistry meterRegistry) {
        this.leaderCandidate = leaderCandidate;
        this.curatorFramework = curatorFramework;
        this.recoveryLogger = recoveryLogger;
        this.masterStartLogger = masterStartLogger;
        this.fastDBConfiguration = fastDBConfiguration;
        this.masterInterceptor = masterInterceptor;
        this.meterRegistry = meterRegistry;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown.set(true)));
    }

    @Autowired
    public void setLeader(@Lazy Leader leader) {
        this.leader = leader;
    }

    // Moving this to the constructor causes a circular dependency
    @Autowired
    public void setInstanceStarter(InstanceStarter instanceStarter) {
        this.instanceStarter = instanceStarter;
    }


    @Autowired
    public void setGrpcConfiguration(GrpcConfiguration grpcConfiguration) {
        this.grpcConf = grpcConfiguration;
    }


    @PostConstruct
    @Override
    public void start() throws Exception {
        LOGGER.info("Master start complete. time:. Time: {}", System.currentTimeMillis() - Application.getBeginTime());
        LOGGER.info("FMaster started. old state: {}", curLeaderState);
        startMonitoringGrpcService();
        curLeaderState = LeaderState.INSTANCE_STARTED;
        instanceStarter.start(this);
        toFollowerMode();
    }

    @Override
    public void toFollowerMode() {
        LOGGER.info("FMaster is follower. old state: {}", curLeaderState);
        // If we were the leader before, exit
        if (curLeaderState == LeaderState.LEADER
                || curLeaderState == LeaderState.LEADER_RECONNECTING) {
            LOGGER.error("Leader shutdown!");
            shutdown(ER_TO_FOLLOWER);
        }
    }

    @Override
    public LeaderState getState() {
        return curLeaderState;
    }

    @SuppressFBWarnings({"DM_EXIT"})
    @Override
    public void shutdown(int exitCode) {
        Runtime.getRuntime().halt(exitCode);
    }

    @Override
    public void isLeader() {
        LOGGER.info("FMaster is leader candidate. old state: {}", curLeaderState);
        if (curLeaderState == LeaderState.LEADER) {
            LOGGER.info("FMaster is already a leader.");
            masterStartLogger.setState(MasterStartState.ALREADY_LEADER);
            return;
        }
        if (curLeaderState == LeaderState.LEADER_RECONNECTING) {
            curLeaderState = LeaderState.LEADER;
            LOGGER.info("FMaster leader successfully reconnecting.");
            masterStartLogger.setState(MasterStartState.LEADER_AFTER_RECONNECTING);
            return;
        }
        try {
            curLeaderState = LeaderState.LEADER_CANDIDATE;
            masterStartLogger.setState(MasterStartState.LEADER_CANDIDATE);
            if (leaderCandidate.checkTakeALeader()) {
                if (shutdown.get()) {
                    LOGGER.info("FMaster is shutdown now, there is no need to be a leader");
                    masterStartLogger.setState(MasterStartState.NO_NEED_TO_BE_LEADER);
                } else {
                    LOGGER.info("FMaster is leader. old state: {}", curLeaderState);
                    curLeaderState = LeaderState.LEADER;
                    masterStartLogger.setState(MasterStartState.NEW_LEADER);
                    leader.start();
                    // Write our URL to the persistent root node of the leader election, so that the availability
                    // of the FMaster can be checked with a gRPC ping.
                    // This ping prevents two leaders from appearing when the current leader
                    // loses its ZooKeeper connection but keeps its gRPC server.
                    curatorFramework.setData()
                            .forPath(ZK_SEPARATOR + F_MASTER_NODE, grpcConf.myName().getBytes(UTF_8));
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Cannot take a leadership", ex);
        }
    }

    @Override
    public void notLeader() {
        LOGGER.info("LeaderLatch reported not leader. Scheduling delayed check before switching to follower mode.");
        curLeaderState = LeaderState.LEADER_RECONNECTING;
        Executors.newSingleThreadScheduledExecutor().schedule(() -> {
            if (curLeaderState == LeaderState.LEADER_RECONNECTING) {
                LOGGER.error("Leader did not recover within grace period. Switching to follower mode.");
                toFollowerMode();
            } else {
                LOGGER.info("Leader has recovered within the grace period; remain active.");
            }
        }, GRACE_PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    private static boolean isAddressInUse(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof BindException)                 return true;
            String msg = t.getMessage();
            if (msg != null &&
                    (msg.contains("Address already in use") ||  // JDK
                            msg.contains("EADDRINUSE")))               // Netty
                return true;
        }
        return false;
    }

    private void startMonitoringGrpcService() {
        LOGGER.info("Starting grpc info service");

        BindableService service = new MasterMonitoringGrpcImpl(recoveryLogger, masterStartLogger, meterRegistry);
        int port = fastDBConfiguration.getGrpcConfiguration().getMasterMonitoringPort();

        while (port <= 65_535) {
            Server server = ServerBuilder.forPort(port)
                    .addService(service)
                    .executor(Executors.newFixedThreadPool(
                            fastDBConfiguration.getGrpcConfiguration().getThreadPoolSize(),
                            internalThreadFactory))
                    .intercept(masterInterceptor)
                    .build();
            try {
                server.start();
                LOGGER.info("Master monitoring gRPC server started successfully on port {}", port);
                return;
            } catch (IOException e) {
                server.shutdownNow();
                if (isAddressInUse(e)) {
                    LOGGER.warn("Port {} is already in use, trying {}", port, port + 1);
                    port++;
                } else {
                    LOGGER.error("Cannot start master monitoring server", e);
                    shutdown(ER_GRPC_SERVER);
                    return;
                }
            }
        }

        LOGGER.error("No free port found for master monitoring server starting from {}",
                fastDBConfiguration.getGrpcConfiguration().getMasterMonitoringPort());
        shutdown(ER_GRPC_SERVER);
    }

}
