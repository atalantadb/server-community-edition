/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.recovery;


import com.google.common.base.Preconditions;
import com.light.atalanta.TableName;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.FMasterRuntimeException;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.master.services.grpc.RegionServerConnector;
import com.light.atalanta.master.services.runners.RegionsRecoveryRunner;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.util.ZookeeperStatus;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.light.atalanta.ATALANTAConstants.SNAPSHOT_ZNODE;
import static com.light.atalanta.ATALANTAConstants.ZK_SEPARATOR;

public class SnapshotRecovery {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotRecovery.class);
    private final ZookeeperHelper zooHelper;
    private final ZooKeeperMasterContext zooContext;
    private final RegionServerConnector regionServerConnector;

    public SnapshotRecovery(ZookeeperHelper zooHelper, ZooKeeperMasterContext zooContext, RegionServerConnector regionServerConnector) {
        this.zooHelper = zooHelper;
        this.zooContext = zooContext;
        this.regionServerConnector = regionServerConnector;
    }

    public void recovery(List<RegionServerModel> lostRS) {
        LOGGER.info("==> recovery (lostRS.size: {})", lostRS.size());
        List<String> rsList = lostRS.stream().map(rs -> rs.getName()).collect(Collectors.toList());
        Set<String> snapshotToRecover = findSnapshotToRecover(rsList);
        snapshotToRecover.parallelStream().forEach(sn -> recoverySnapshot(sn, rsList));
        LOGGER.info("<== recovery (lostRS.size: {})", lostRS.size());
    }

    private void recoverySnapshot(String snapshot, List<String> rsList) {
        LOGGER.info("==> recoverySnapshot (snapshot: {}, lostRS.size: {})", snapshot, rsList.size());
        getSnapshotRegionServers(snapshot)
                .filter(rsList::contains)
                .forEach(rs -> recoveryRsRegion(rs, snapshot));
        LOGGER.info("<== recoverySnapshot (snapshot: {}, lostRS.size: {})", snapshot, rsList.size());
    }


    private void recoveryRsRegion(String rs, String snapshot) {
        LOGGER.debug("==> recoveryRsRegion (rs: {}, snapshot: {})", rs, snapshot);
        final String snapshotZnode = String.join(ZK_SEPARATOR, SNAPSHOT_ZNODE, snapshot);
        final String ns = getChildren(snapshotZnode).get(0);
        final String snapshotNsZnode = String.join(ZK_SEPARATOR, SNAPSHOT_ZNODE, snapshot, ns);
        final String table = getChildren(snapshotNsZnode).get(0);
        final String snapshotRsZnode = String.join(ZK_SEPARATOR, SNAPSHOT_ZNODE, snapshot, ns, table, rs);
        final TableName tableName = TableName.valueOf(ns, table);
        List<String> regions = getChildren(snapshotRsZnode);
        regions.forEach(r -> {
            String regionZnode = String.join(ZK_SEPARATOR, snapshotRsZnode, r);
            getChildren(regionZnode).forEach(
                    replicaNum -> {
                        try {
                            String regionInfo = zooHelper.get(String.join(ZK_SEPARATOR, regionZnode, replicaNum, "info")).get();
                            recoveryRegion(snapshot, tableName, r, Integer.parseInt(replicaNum), rs,
                                    String.join(ZK_SEPARATOR, SNAPSHOT_ZNODE, snapshot, ns, table), regionInfo);
                        } catch (Exception e) {
                            LOGGER.error("error in recoveryRsRegion", e);
                            throw new FMasterRuntimeException(e);
                        }
                    });
        });
        LOGGER.debug("<== recoveryRsRegion (rs: {}, snapshot: {})", rs, snapshot);
    }

    private void recoveryRegion(String snapshot, TableName tableName, String region, int replicaNum, String rs, String snapshotTableZnode, String regionInfo) throws Exception {
        String srcRs = getChildren(snapshotTableZnode).stream()
                .filter(regServ -> !regServ.equals(rs))
                .filter(regServ -> getChildren(String.join(ZK_SEPARATOR, snapshotTableZnode, rs)).stream()
                        .anyMatch(r -> region.equals(region)))
                .findAny().get();
        List<RegionServerModel> targetRsList = RegionsRecoveryRunner.getAliveRegionServers(zooContext).stream().filter(rsModel -> !rsModel.getName().equals(srcRs)).collect(Collectors.toList());
        if (!targetRsList.isEmpty()) {
            moveRegion(snapshot, tableName, region, replicaNum, srcRs,
                    targetRsList.get((int) (Math.random() * targetRsList.size())), snapshotTableZnode, regionInfo, rs);
        } else {
            LOGGER.error("Target region server not found. region: {}, rs: {}, sn: {}",
                    region, rs, snapshotTableZnode);
        }
    }


    private void moveRegion(String snapshot, TableName tableName, String region, int replicaNum, String srcRs,
                            RegionServerModel targetRs, String snapshotTableZnode, String regionInfo, String oldRs) throws Exception {
        LOGGER.info("==> moveRegion (r: {}, srcRs: {}, targetRs: {}", region, srcRs, targetRs);
        regionServerConnector.copySnapshotRegion(snapshot, tableName, region, replicaNum, zooContext.getRegionServerModel(srcRs), targetRs);
        updateZk(region, replicaNum, targetRs, snapshotTableZnode, regionInfo, oldRs);
        LOGGER.info("<== moveRegion (r: {}, srcRs: {}, targetRs: {}", region, srcRs, targetRs);
    }

    private void updateZk(String region, int replicaNum, RegionServerModel targetRs,
                          String snapshotTableZnode, String regionInfo, String oldRs) throws Exception {
        List<CuratorOp> ops = new ArrayList();
        final String regionServerZnode = String.join(ZK_SEPARATOR, snapshotTableZnode, targetRs.getName());
        if (!zooHelper.checkExists(regionServerZnode)) {
            ops.add(zooHelper.createTrans(regionServerZnode, ZookeeperStatus.CREATING.name()));
        }
        final String regionZnode = String.join(ZK_SEPARATOR, regionServerZnode, region);
        if (!zooHelper.checkExists(regionZnode)) {
            ops.add(zooHelper.createTrans(regionZnode, ZookeeperStatus.CREATING.name()));
        }
        String regionReplicaZnode = String.join(ZK_SEPARATOR, regionZnode, String.valueOf(replicaNum));
        if (!zooHelper.checkExists(regionReplicaZnode)) {
            ops.add(zooHelper.createTrans(regionReplicaZnode, ZookeeperStatus.CREATING.name()));
        }
        String infoZpath = String.join(ZK_SEPARATOR, regionReplicaZnode, "info");
        if (!zooHelper.checkExists(infoZpath)) {
            ops.add(zooHelper.createTrans(infoZpath, regionInfo));
        }
        if (!ops.isEmpty()) {
            Preconditions.checkArgument(zooHelper.forOperations(ops).size() == ops.size(), "unexpected transaction error");
        } // TODO: Make this transactional
        zooHelper.delete(String.join(ZK_SEPARATOR, snapshotTableZnode, oldRs));
    }


    private Set<String> findSnapshotToRecover(List<String> rsList) {
        return snapshotToRecoverStream(rsList)
                .collect(Collectors.toSet());
    }

    private Stream<String> snapshotToRecoverStream(List<String> rsList) {
        return getChildren(SNAPSHOT_ZNODE).stream()
                .filter(snapshotName -> getSnapshotRegionServers(snapshotName).anyMatch(rsList::contains));
    }

    private Stream<String> getSnapshotRegionServers(String snapshotName) {

        String snapshotZnode = String.join(ZK_SEPARATOR, SNAPSHOT_ZNODE, snapshotName);
        return getChildren(snapshotZnode).stream()
                .flatMap(ns -> getChildren(String.join(ZK_SEPARATOR, snapshotZnode, ns)).stream()
                        .flatMap(t -> getChildren(String.join(ZK_SEPARATOR, snapshotZnode, ns, t)).stream()));
    }

    private List<String> getChildren(String Znode) {
        try {
            return zooHelper.getChildren(Znode);
        } catch (ZKException e) {
            LOGGER.error("cannot read ZK info. znode: {}", Znode);
            throw new FMasterRuntimeException(e);
        }
    }

    public Set<RegionServerModel> getSnapshotRs() {
        return getChildren(SNAPSHOT_ZNODE).stream()
                .flatMap(snapshotName -> getSnapshotRegionServers(snapshotName)).map(rs -> zooContext.getRegionServerModel(rs))
                .collect(Collectors.toSet());
    }
}
