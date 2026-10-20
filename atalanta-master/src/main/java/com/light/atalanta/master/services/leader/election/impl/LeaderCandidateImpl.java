/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.leader.election.impl;

import com.google.common.base.Preconditions;
import com.google.protobuf.Empty;
import com.light.atalanta.grpc.MasterProto;
import com.light.atalanta.grpc.MasterServiceGrpc;
import com.light.atalanta.master.services.leader.election.LeaderCandidate;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.master.vo.GrpcConfiguration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.internal.DnsNameResolverProvider;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.light.atalanta.ATALANTAConstants.F_MASTER_NODE;

public class LeaderCandidateImpl implements LeaderCandidate {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderCandidateImpl.class.getName());

    public static final int HOSTNAME_INDEX = 0;
    public static final int PORT_INDEX = 1;
    public static final int MASTER_URL_CHUNK_COUNT = 2;

    public static final long GRACE_PERIOD_MS = 5000;
    private static final int RETRY_COUNT = 10;
    private static final long RETRY_INTERVAL_MS = 300;

    private final GrpcConfiguration grpcConf;
    private final ZookeeperHelper zooHelper;

    public LeaderCandidateImpl(GrpcConfiguration grpcConf, ZookeeperHelper zooHelper) {
        this.grpcConf = grpcConf;
        this.zooHelper = zooHelper;
    }

    @Override
    public boolean checkTakeALeader() throws Exception {
        return zooHelper.get(F_MASTER_NODE)
                .map(this::checkTakeALeader)
                .orElse(true); // If the node doesn't exist in ZK yet, there is no leader, so we can become one
    }

    private boolean checkTakeALeader(String masterUrl) {
        LOGGER.info("current zk /fmaster: '{}'", masterUrl);
        return StringUtils.isEmpty(masterUrl) || !pingAndShutdownLeader(masterUrl);
    }

    private boolean pingAndShutdownLeader(String masterUrl) {
        String myUrl = grpcConf.myName();
        LOGGER.info("Ping master. my url: {}, current master url: {}", myUrl, masterUrl);
        String[] split = masterUrl.split(":");
        Preconditions.checkArgument(split.length == MASTER_URL_CHUNK_COUNT, "Wrong masterUrl format: " + masterUrl);
        String host = split[HOSTNAME_INDEX];
        Integer port = Integer.valueOf(split[PORT_INDEX]);
        boolean res = true;
        Long start = System.currentTimeMillis();
        for (int i = 0; i < RETRY_COUNT; ++i) {
            try {
                ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                        .usePlaintext()
                        .build();
                MasterServiceGrpc.MasterServiceBlockingStub stub = MasterServiceGrpc.newBlockingStub(channel);
                MasterProto.PingResponse grpcRes = stub.ping(Empty.getDefaultInstance());
                if (System.currentTimeMillis() - start < GRACE_PERIOD_MS && myUrl.equals(masterUrl)) {
                    LOGGER.info("Leader is ok. Leader shutdown is not needed. leader url: {}", myUrl);
                    return true;
                } else {
                    LOGGER.error("Leader of fmaster already running!. Ping res: {}", grpcRes);
                    LOGGER.info("Send shutdown command to leader '{}'", masterUrl);
                    grpcRes = stub.shutdown(Empty.getDefaultInstance());
                    LOGGER.info("Shutdown res: {}", grpcRes);
                    channel.shutdown();
                    channel.awaitTermination(5, TimeUnit.SECONDS);
                    LockSupport.parkNanos(RETRY_INTERVAL_MS * 1_000_000L);
                }
            } catch (Exception ex) {
                // Not an error: this exception is expected when there is no leader
                LOGGER.info("ping result: {}", ex.getMessage());
                res = false;
                break;
            }
        }
        if (res) {
            LOGGER.error("Cannot shutdown old leader! old leader url: {}", masterUrl);
        }
        return res;
    }
}
