/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations;

import com.google.common.base.Preconditions;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.SnapshotException;
import com.light.atalanta.exceptions.SnapshotNotFoundException;
import com.light.atalanta.client.exceptions.TableAlreadyExistsException;
import com.light.atalanta.master.services.LockManager;
import com.light.atalanta.master.services.TableLock;
import com.light.atalanta.master.services.grpc.RegionServerConnector;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.RegionServerModel;

import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.util.FutureUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.light.atalanta.ATALANTAConstants.SNAPSHOT_ZNODE;
import static com.light.atalanta.ATALANTAConstants.STATUS_PREFIX;
import static com.light.atalanta.ATALANTAConstants.ZK_SEPARATOR;
import static com.light.atalanta.util.ZookeeperStatus.*;

public class CloneSnapshot {
    private static final Logger LOGGER = LoggerFactory.getLogger(CloneSnapshot.class);
    private static final int RS_REQUEST_POOL_SIZE = 128;
    private static final ExecutorService reginServersRequestThreadPool = new ForkJoinPool(RS_REQUEST_POOL_SIZE,
            pool -> {
                final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                worker.setName("atalanta-snapshot-clone-pool-" + worker.getPoolIndex());
                return worker;
            }, null, false);
    private final ZookeeperHelper zooHelper;
    private final ZooKeeperCoreContext zooContext;
    private final RegionServerConnector regionServerConnector;
    private final LockManager lockManager;
    private final AtalantaDBConfiguration conf;
    private final EnableTable enableTable;
    private final ExecutorService executorService;

    public CloneSnapshot(ZookeeperHelper zooHelper, ZooKeeperCoreContext zooContext,
                         RegionServerConnector regionServerConnector, LockManager lockManager,
                         AtalantaDBConfiguration conf, EnableTable enableTable, ExecutorService executorService) {
        this.zooHelper = zooHelper;
        this.zooContext = zooContext;
        this.regionServerConnector = regionServerConnector;
        this.lockManager = lockManager;
        this.conf = conf;
        this.enableTable = enableTable;
        this.executorService = executorService;
    }

    public void action(String name, TableName tableName) {
        final String snapshotZnode = String.join(ZK_SEPARATOR, SNAPSHOT_ZNODE, name);
        final String targetTableZnode = String.join(ZK_SEPARATOR, tableName.getNamespace(), tableName.getTableName());
        try {
            if (!zooHelper.checkExists(snapshotZnode)) {
                throw new SnapshotNotFoundException(name, snapshotZnode);
            }
            final String tablePath = ZooKeeperContextHelper.getTablePath(tableName);
            if (zooHelper.checkExists(tablePath)) {
                throw new TableAlreadyExistsException(tableName);
            }
            final List<CuratorOp> ops = new ArrayList();
            final Set<RegionServerModel> regionServers = new HashSet<>();
            final List<String> ns = zooHelper.getChildren(snapshotZnode);
            final String snapshotNsZnode = String.join(ZK_SEPARATOR, snapshotZnode, ns.get(0));
            final List<String> table = zooHelper.getChildren(snapshotNsZnode);
            final String snapshotTableZnode = String.join(ZK_SEPARATOR, snapshotNsZnode, table.get(0));
            final List<String> rslist = zooHelper.getChildren(snapshotTableZnode);
            ops.add(zooHelper.createTrans(targetTableZnode, STATUS_PREFIX + DISABLING.getValue()));
            Map<String, String> newRegionMap = new HashMap<>();
            boolean storageSettingsRestored = false;
            for (String rs : rslist) {
                final String snapshotRegionZnode = String.join(ZK_SEPARATOR, snapshotTableZnode, rs);
                final List<String> regionList = zooHelper.getChildren(snapshotRegionZnode);
                regionList.forEach(r -> newRegionMap.computeIfAbsent(r, v -> RegionModel.makeRandomRegionName()));
                if (!storageSettingsRestored) {
                    for (String region : regionList) {
                        String infoPath = String.join(ZK_SEPARATOR, snapshotRegionZnode, region, "0", "info");
                        if (zooHelper.checkExists(infoPath)) {
                            RegionModel regionInfo = RegionModel.fromBase64String(zooHelper.getNotNullString(infoPath));
                            Preconditions.checkArgument(regionInfo != null && regionInfo.getReplicaNum() == 0,
                                    "Invalid primary replica metadata in snapshot: " + infoPath);
                            // Restore table-wide settings once, before parallel RS cloning and redistribution.
                            zooContext.addExtParamToPlan(ops, tableName, regionInfo);
                            storageSettingsRestored = true;
                            break;
                        }
                    }
                }
                regionServers.add(zooContext.getRegionServerModel(rs));
            }
            Preconditions.checkArgument(storageSettingsRestored, "No primary replica metadata in snapshot: " + name);
            if (ops.size() > 0) {
                Preconditions.checkArgument(zooHelper.forOperations(ops).size() == ops.size(), "unexpected transaction error");
                sendRequestToRegionServers(name, tableName, regionServers, newRegionMap);
                zooHelper.getNotNullString(targetTableZnode);
                zooHelper.setData(targetTableZnode, STATUS_PREFIX + DISABLED.getValue());
                enableTable(tableName);
            }
        } catch (Exception e) {
            LOGGER.error("Zookeeper error. znode: + snapshotZnode", e);
            throw new SnapshotException(e);
        }
    }


    private boolean sendRequestToRegionServers (String name, TableName tableName, Set<RegionServerModel> regionServers, Map<String, String> newRegionMap) {
        try {
            AtomicBoolean res = new AtomicBoolean(true);
            reginServersRequestThreadPool.submit(() -> {
                        regionServers.parallelStream().forEach(rs -> {
                            if (!regionServerConnector.cloneSnapshot(name, tableName, rs, newRegionMap)) {
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

    private void enableTable(TableName tableName) throws Exception {
        createDistributeRegionsTasks(tableName);
        String operationName = "ENABLE_TABLE";
        TableLock tableLock = lockManager.acquireRef(tableName, operationName);
        if (!tableLock.tryLock(conf.getDataConfiguration().getLockWaitingTimeoutMs(), operationName)) {
            tableLock.releaseRef();
            throw new RuntimeException("Lock waiting timeout for table: " + tableName);
        }
        enableTable.runSyncThenAsyncOnError(tableLock);
    }

    private void createDistributeRegionsTasks(TableName tableName) throws Exception {
        List<RegionDistributionModel> regMap = zooContext.getRegionDistributionWithStatuses(tableName);
        Map<RegionDistributionModel, String> leaderToReservedMapping = new HashMap<>();
        Map<RegionDistributionModel, String> reservedMapping = new HashMap<>();

        regMap.forEach(rdm -> {
            if (rdm.getRegionModel().getReplicaNum() == 0) {
                // Save the reserve replica addresses to the map
                leaderToReservedMapping.put(rdm, zooContext.getReservedAddresses(regMap,
                        rdm.getRegionModel().getName(), conf.getDataConfiguration().getReplicationFactor()));
            } else {
                // Write an empty string for a reserve replica
                reservedMapping.put(rdm, StringUtils.EMPTY);
            }
        });

        distributeRegions(tableName, executorService, reservedMapping);
        distributeRegions(tableName, executorService, leaderToReservedMapping);
    }

    private void distributeRegions(TableName tableName, ExecutorService executorService, Map<RegionDistributionModel, String> distributionMapping) throws IOException {
        List<CompletableFuture<RegionDistributionModel>> futures = new ArrayList<>();
        for (RegionDistributionModel region : distributionMapping.keySet()) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    // enableRegion == distributeRegion
                    // distributeRegion != startRegion
                    String createTableStr = String.format("Creating table '%s': distributing region '%s'", tableName, region.toShortString());
                    regionServerConnector.monitor(tableName, createTableStr, region, distributionMapping.get(region), (stub, req, stream) ->
                            stub.get().distributeRegion(req, stream)
                    );
                } catch (Throwable ex) {
                    LOGGER.error("Clone table '{}': unable to distribute region '{}'", tableName, region.toShortString(), ex);
                    throw new CompletionException(ex);
                }
                // Commit the region if the previous step succeeded and the thread was not interrupted
                return Thread.currentThread().isInterrupted() ? null : region;

            }, executorService));
        }
        FutureUtil.waitAndGet(futures);
    }
}
