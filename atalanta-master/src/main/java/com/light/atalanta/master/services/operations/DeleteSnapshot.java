/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations;

import com.light.atalanta.exceptions.SnapshotException;
import com.light.atalanta.exceptions.SnapshotNotFoundException;
import com.light.atalanta.master.services.grpc.RegionServerConnector;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.services.ZookeeperHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.SNAPSHOT_ZNODE;
import static com.light.atalanta.ATALANTAConstants.ZK_SEPARATOR;

public class DeleteSnapshot {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteSnapshot.class);
    private static final int RS_REQUEST_POOL_SIZE = 128;
    private static ExecutorService reginServersRequestThreadPool = new ForkJoinPool(RS_REQUEST_POOL_SIZE,
            pool -> {
                final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                worker.setName("atalanta-snapshot-clone-pool-" + worker.getPoolIndex());
                return worker;
            }, null, false);


    private final ZookeeperHelper zooHelper;
    private final ZooKeeperCoreContext zooContext;
    private final RegionServerConnector regionServerConnector;

    public DeleteSnapshot(ZookeeperHelper zooHelper, ZooKeeperCoreContext zooContext, RegionServerConnector regionServerConnector) {
        this.zooHelper = zooHelper;
        this.zooContext = zooContext;
        this.regionServerConnector = regionServerConnector;
    }


    public void action(String name) {
        final String snapshotZnode = String.join(ZK_SEPARATOR, SNAPSHOT_ZNODE, name);
        try {

            if (!zooHelper.checkExists(snapshotZnode)) {
                throw new SnapshotNotFoundException(name, snapshotZnode);
            }
            final List<String> ns = zooHelper.getChildren(snapshotZnode);
            final String snapshotNsZnode = String.join(ZK_SEPARATOR, snapshotZnode, ns.get(0));
            final List<String> table = zooHelper.getChildren(snapshotNsZnode);
            final String snapshotTableZnode = String.join(ZK_SEPARATOR, snapshotNsZnode, table.get(0));
            final List<String> rsList = zooHelper.getChildren(snapshotTableZnode);
            Set<RegionServerModel> rsModelSet = rsList.stream()
                    .map(rsName -> zooContext.getRegionServerModel(rsName)).collect(Collectors.toSet());
            sendRequestToRegionServers(name, rsModelSet);
            zooHelper.delete(snapshotZnode);
        } catch (Exception e) {
            LOGGER.error("Zookeeper error. znode: " + snapshotZnode, e);
            throw new SnapshotException(e);
        }
    }

    private boolean sendRequestToRegionServers(String name, Set<RegionServerModel> regionServers) {
        try {
            AtomicBoolean res = new AtomicBoolean(true);
            reginServersRequestThreadPool.submit(() -> {
                        regionServers.parallelStream().forEach(rs -> {
                            if (!regionServerConnector.deleteSnapshot(name, rs)) {
                                res.set(false);
                            }
                        });
                    }
            ).get();
            return res.get();
        } catch (InterruptedException e) {
            LOGGER.warn("Snapshot creating has been interrupted!", e);
            return false;
        } catch (ExecutionException e) {
            e.printStackTrace();
            return false;
        }
    }
}
