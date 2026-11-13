/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations;


import com.google.common.base.Preconditions;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.SnapshotAlreadyExistsException;
import com.light.atalanta.exceptions.SnapshotException;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.master.services.grpc.RegionServerConnector;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.util.ZookeeperStatus;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.light.atalanta.ATALANTAConstants.SNAPSHOT_ZNODE;
import static com.light.atalanta.ATALANTAConstants.ZK_SEPARATOR;

public class CreateSnapshot {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateSnapshot.class);
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

    public CreateSnapshot(ZookeeperHelper zooHelper, ZooKeeperCoreContext zooContext, RegionServerConnector regionServerConnector) throws ZKException {
        this.zooHelper = zooHelper;
        this.zooContext = zooContext;
        this.regionServerConnector = regionServerConnector;
        zooHelper.createIfAbsent(SNAPSHOT_ZNODE, ZookeeperStatus.CREATING.name());
    }


    public void action(String name, TableName tableName) {
        LOGGER.info("==> action name: {}, tableName: {}", name, tableName);
        String snapshotZnode = String.join(ZK_SEPARATOR, SNAPSHOT_ZNODE, name, tableName.getNamespace(), tableName.getTableName());
        try {
            if (zooHelper.checkExists(snapshotZnode)) {
                throw new SnapshotAlreadyExistsException(name);
            }
            zooHelper.createIfAbsent(SNAPSHOT_ZNODE, ZookeeperStatus.CREATING.name());
            List<RegionDistributionModel> regions = zooContext.getRegionDistributionWithStatuses(tableName);
            Set<RegionServerModel> regionServers = new HashSet<>();
            List<CuratorOp> ops = new ArrayList();
            for (RegionDistributionModel region : regions) {

                regionServers.add(region.getRegionServerModel());
                final String regionServerZnode = String.join(ZK_SEPARATOR, snapshotZnode,
                        region.getRegionServerModel().getName());
                if (!zooHelper.checkExists(regionServerZnode)) {
                    zooHelper.create(regionServerZnode, ZookeeperStatus.CREATING.name());
                }
                final String regionZnode = String.join(ZK_SEPARATOR, regionServerZnode, region.getRegionModel().getName());
                if (!zooHelper.checkExists(regionZnode)) {
                    zooHelper.create(regionZnode, ZookeeperStatus.CREATING.name());
                }
                String regionReplicaZnode = String.join(ZK_SEPARATOR,
                        regionZnode, String.valueOf(region.getRegionModel().getReplicaNum()));
                zooHelper.create(regionReplicaZnode, ZookeeperStatus.CREATING.name());
                ops.add(zooHelper.createTrans(String.join(ZK_SEPARATOR, regionReplicaZnode, "info"),
                        String.valueOf(region.getRegionModel().toBase64String())));
            }
            if (ops.size() > 0) {
                Preconditions.checkArgument(zooHelper.forOperations(ops).size() == ops.size(), "unexpected transaction error");
                final ZookeeperStatus zookeeperStatus = sendRequestToRegionServers(name, tableName, regionServers, zooHelper);
                zooHelper.setData(String.join(ZK_SEPARATOR, SNAPSHOT_ZNODE, name), zookeeperStatus.name());
                LOGGER.info("==> action name: {}, tableName: {}. status: {}", name, tableName, zookeeperStatus);
            }
        } catch (Exception e) {
            LOGGER.error("Zookeeper error. znode:" + snapshotZnode, e);
            LOGGER.info("==> action name: {}, tableName: {}. error!", name, tableName);
            throw new SnapshotException(e);
        }
    }

    private ZookeeperStatus sendRequestToRegionServers(String name, TableName tableName, Set<RegionServerModel> regionServers, ZookeeperHelper zooHelper) {
        try {
            AtomicBoolean res = new AtomicBoolean(true);
            reginServersRequestThreadPool.submit(() -> {
                regionServers.parallelStream().forEach(rs -> {
                    LOGGER.info("send creating snapshot request to rs: {}, name: {}, tableName: {}", regionServers, name, tableName);
                    if (!regionServerConnector.createSnapshot(name, tableName, rs, zooHelper)) {
                        res.set(false);
                    }
                    LOGGER.info("complete creating snapshot request to rs: {}, name: {}, tableName: {}", rs, name, tableName);
                });
            }).get();
            return res.get() ? ZookeeperStatus.CREATED : ZookeeperStatus.FAILED;
        } catch (InterruptedException e) {
            LOGGER.warn("Snapshot creating has been interrupted!", e);
            return ZookeeperStatus.FAILED;
        } catch (ExecutionException e) {
            e.printStackTrace();
            return ZookeeperStatus.FAILED;
        }
    }
}
