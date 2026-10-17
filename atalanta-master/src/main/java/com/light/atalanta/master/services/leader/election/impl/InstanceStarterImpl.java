/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.leader.election.impl;

import com.light.atalanta.master.services.leader.election.InstanceStarter;
import com.light.atalanta.master.vo.GrpcConfiguration;
import com.light.atalanta.master.vo.ZooKeeperConfiguration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.zookeeper.common.IOUtils;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Closeable;

import static com.light.atalanta.ATALANTAConstants.F_MASTER_NODE;
import static com.light.atalanta.ATALANTAConstants.ZK_SEPARATOR;

public class InstanceStarterImpl implements InstanceStarter, Closeable {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(InstanceStarterImpl.class.getName());

    private CuratorFramework curatorFramework;

    private final ZooKeeperConfiguration zooConf;

    private final GrpcConfiguration grpcConf;

    private LeaderLatch leaderLatch = null;

    public InstanceStarterImpl(ZooKeeperConfiguration zooKeeperConfiguration, GrpcConfiguration grpcConf) {
        this.zooConf = zooKeeperConfiguration;
        this.grpcConf = grpcConf;
    }

    @Autowired
    public void setCuratorFramework(CuratorFramework curatorFramework) {
        this.curatorFramework = curatorFramework;
    }

    @Override
    public boolean start(LeaderLatchListener leaderLatchListener) throws Exception {
        leaderLatch = new LeaderLatch(curatorFramework,
                ZK_SEPARATOR + F_MASTER_NODE,
                grpcConf.myName());
        leaderLatch.addListener(leaderLatchListener);
        leaderLatch.start();
        return true;
    }

    @Override
    public void close() {
        IOUtils.closeStream(leaderLatch);
    }
}
