/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.model;

import java.util.List;

public class FMasterStat {
    private String mode;
    private long createNamespace;
    private List<FMasetrInfoMsg> createNamespaceMsgList;
    private long createTable;
    private List<FMasetrInfoMsg> createTableMsgList;
    private long enableTable;
    private List<FMasetrInfoMsg> enableTableMsgList;
    private long disableTable;
    private List<FMasetrInfoMsg> disableTableMsgList;
    private long dropTable;
    private List<FMasetrInfoMsg> dropTableMsgList;
    private long moveRegion;
    private List<FMasetrInfoMsg> moveRegionMsgList;
    private long splitRegion;
    private List<FMasetrInfoMsg> splitRegionMsgList;
    private long startReplicasRebalanceAsync;
    private List<FMasetrInfoMsg> startReplicasRebalanceAsyncMsgList;
    private long syncTableReplicas;
    private List<FMasetrInfoMsg> syncTableReplicasMsgList;
    private long stopReplicasRebalance;
    private List<FMasetrInfoMsg> stopReplicasRebalanceMsgList;
    private long startRegionsRebalanceAsync;
    private List<FMasetrInfoMsg> startRegionsRebalanceAsyncMsgList;
    private long stopRegionsRebalance;
    private List<FMasetrInfoMsg> stopRegionsRebalanceMsgList;
    private long createSnapshot;
    private List<FMasetrInfoMsg> createSnapshotMsgList;
    private long cloneSnapshot;
    private List<FMasetrInfoMsg> cloneSnapshotMsgList;
    private long deleteSnapshot;
    private List<FMasetrInfoMsg> deleteSnapshotMsgList;
    private long startRegionServer;
    private List<FMasetrInfoMsg> startRegionServerMsgList;
    private long stopRegionServer;
    private List<FMasetrInfoMsg> stopRegionServerMsgList;
    private long recovery;
    private List<FMasetrInfoMsg> recoveryMsgList;

    public List<FMasetrInfoMsg> getMasterStartMsgList() {
        return masterStartMsgList;
    }

    public void setMasterStartMsgList(List<FMasetrInfoMsg> masterStartMsgList) {
        this.masterStartMsgList = masterStartMsgList;
    }

    private List<FMasetrInfoMsg> masterStartMsgList;

    private long total;

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public long getCreateNamespace() {
        return createNamespace;
    }

    public void setCreateNamespace(long createNamespace) {
        this.createNamespace = createNamespace;
    }

    public long getCreateTable() {
        return createTable;
    }

    public void setCreateTable(long createTable) {
        this.createTable = createTable;
    }

    public long getEnableTable() {
        return enableTable;
    }

    public void setEnableTable(long enableTable) {
        this.enableTable = enableTable;
    }

    public long getDisableTable() {
        return disableTable;
    }

    public void setDisableTable(long disableTable) {
        this.disableTable = disableTable;
    }

    public long getDropTable() {
        return dropTable;
    }

    public void setDropTable(long dropTable) {
        this.dropTable = dropTable;
    }

    public long getMoveRegion() {
        return moveRegion;
    }

    public void setMoveRegion(long moveRegion) {
        this.moveRegion = moveRegion;
    }

    public long getSplitRegion() {
        return splitRegion;
    }

    public void setSplitRegion(long splitRegion) {
        this.splitRegion = splitRegion;
    }

    public long getStartReplicasRebalanceAsync() {
        return startReplicasRebalanceAsync;
    }

    public void setStartReplicasRebalanceAsync(long startReplicasRebalanceAsync) {
        this.startReplicasRebalanceAsync = startReplicasRebalanceAsync;
    }

    public long getSyncTableReplicas() {
        return syncTableReplicas;
    }

    public void setSyncTableReplicas(long syncTableReplicas) {
        this.syncTableReplicas = syncTableReplicas;
    }

    public long getStopReplicasRebalance() {
        return stopReplicasRebalance;
    }

    public void setStopReplicasRebalance(long stopReplicasRebalance) {
        this.stopReplicasRebalance = stopReplicasRebalance;
    }

    public long getStartRegionsRebalanceAsync() {
        return startRegionsRebalanceAsync;
    }

    public void setStartRegionsRebalanceAsync(long startRegionsRebalanceAsync) {
        this.startRegionsRebalanceAsync = startRegionsRebalanceAsync;
    }

    public long getStopRegionsRebalance() {
        return stopRegionsRebalance;
    }

    public void setStopRegionsRebalance(long stopRegionsRebalance) {
        this.stopRegionsRebalance = stopRegionsRebalance;
    }

    public long getCreateSnapshot() {
        return createSnapshot;
    }

    public void setCreateSnapshot(long createSnapshot) {
        this.createSnapshot = createSnapshot;
    }

    public long getCloneSnapshot() {
        return cloneSnapshot;
    }

    public void setCloneSnapshot(long cloneSnapshot) {
        this.cloneSnapshot = cloneSnapshot;
    }

    public long getDeleteSnapshot() {
        return deleteSnapshot;
    }

    public void setDeleteSnapshot(long deleteSnapshot) {
        this.deleteSnapshot = deleteSnapshot;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public void setStartRegionServer(long startRegionServer) {
        this.startRegionServer = startRegionServer;
    }

    public long getStartRegionServer() {
        return startRegionServer;
    }


    public void setStopRegionServer(long stopRegionServer) {
        this.stopRegionServer = stopRegionServer;
    }

    public long getStopRegionServer() {
        return stopRegionServer;
    }

    public List<FMasetrInfoMsg> getCreateNamespaceMsgList() {
        return createNamespaceMsgList;
    }

    public void setCreateNamespaceMsgList(List<FMasetrInfoMsg> createNamespaceMsgList) {
        this.createNamespaceMsgList = createNamespaceMsgList;
    }

    public List<FMasetrInfoMsg> getCreateTableMsgList() {
        return createTableMsgList;
    }

    public void setCreateTableMsgList(List<FMasetrInfoMsg> createTableMsgList) {
        this.createTableMsgList = createTableMsgList;
    }

    public List<FMasetrInfoMsg> getEnableTableMsgList() {
        return enableTableMsgList;
    }

    public void setEnableTableMsgList(List<FMasetrInfoMsg> enableTableMsgList) {
        this.enableTableMsgList = enableTableMsgList;
    }

    public List<FMasetrInfoMsg> getDisableTableMsgList() {
        return disableTableMsgList;
    }

    public void setDisableTableMsgList(List<FMasetrInfoMsg> disableTableMsgList) {
        this.disableTableMsgList = disableTableMsgList;
    }

    public List<FMasetrInfoMsg> getDropTableMsgList() {
        return dropTableMsgList;
    }

    public void setDropTableMsgList(List<FMasetrInfoMsg> dropTableMsgList) {
        this.dropTableMsgList = dropTableMsgList;
    }

    public List<FMasetrInfoMsg> getMoveRegionMsgList() {
        return moveRegionMsgList;
    }

    public void setMoveRegionMsgList(List<FMasetrInfoMsg> moveRegionMsgList) {
        this.moveRegionMsgList = moveRegionMsgList;
    }

    public List<FMasetrInfoMsg> getSplitRegionMsgList() {
        return splitRegionMsgList;
    }

    public void setSplitRegionMsgList(List<FMasetrInfoMsg> splitRegionMsgList) {
        this.splitRegionMsgList = splitRegionMsgList;
    }

    public List<FMasetrInfoMsg> getStartReplicasRebalanceAsyncMsgList() {
        return startReplicasRebalanceAsyncMsgList;
    }

    public void setStartReplicasRebalanceAsyncMsgList(List<FMasetrInfoMsg> startReplicasRebalanceAsyncMsgList) {
        this.startReplicasRebalanceAsyncMsgList = startReplicasRebalanceAsyncMsgList;
    }

    public List<FMasetrInfoMsg> getSyncTableReplicasMsgList() {
        return syncTableReplicasMsgList;
    }

    public void setSyncTableReplicasMsgList(List<FMasetrInfoMsg> syncTableReplicasMsgList) {
        this.syncTableReplicasMsgList = syncTableReplicasMsgList;
    }

    public List<FMasetrInfoMsg> getStopReplicasRebalanceMsgList() {
        return stopReplicasRebalanceMsgList;
    }

    public void setStopReplicasRebalanceMsgList(List<FMasetrInfoMsg> stopReplicasRebalanceMsgList) {
        this.stopReplicasRebalanceMsgList = stopReplicasRebalanceMsgList;
    }

    public List<FMasetrInfoMsg> getStartRegionsRebalanceAsyncMsgList() {
        return startRegionsRebalanceAsyncMsgList;
    }

    public void setStartRegionsRebalanceAsyncMsgList(List<FMasetrInfoMsg> startRegionsRebalanceAsyncMsgList) {
        this.startRegionsRebalanceAsyncMsgList = startRegionsRebalanceAsyncMsgList;
    }

    public List<FMasetrInfoMsg> getStopRegionsRebalanceMsgList() {
        return stopRegionsRebalanceMsgList;
    }

    public void setStopRegionsRebalanceMsgList(List<FMasetrInfoMsg> stopRegionsRebalanceMsgList) {
        this.stopRegionsRebalanceMsgList = stopRegionsRebalanceMsgList;
    }

    public List<FMasetrInfoMsg> getCreateSnapshotMsgList() {
        return createSnapshotMsgList;
    }

    public void setCreateSnapshotMsgList(List<FMasetrInfoMsg> createSnapshotMsgList) {
        this.createSnapshotMsgList = createSnapshotMsgList;
    }

    public List<FMasetrInfoMsg> getCloneSnapshotMsgList() {
        return cloneSnapshotMsgList;
    }

    public void setCloneSnapshotMsgList(List<FMasetrInfoMsg> cloneSnapshotMsgList) {
        this.cloneSnapshotMsgList = cloneSnapshotMsgList;
    }

    public List<FMasetrInfoMsg> getDeleteSnapshotMsgList() {
        return deleteSnapshotMsgList;
    }

    public void setDeleteSnapshotMsgList(List<FMasetrInfoMsg> deleteSnapshotMsgList) {
        this.deleteSnapshotMsgList = deleteSnapshotMsgList;
    }

    public List<FMasetrInfoMsg> getStartRegionServerMsgList() {
        return startRegionServerMsgList;
    }

    public void setStartRegionServerMsgList(List<FMasetrInfoMsg> startRegionServerMsgList) {
        this.startRegionServerMsgList = startRegionServerMsgList;
    }

    public List<FMasetrInfoMsg> getStopRegionServerMsgList() {
        return stopRegionServerMsgList;
    }

    public void setStopRegionServerMsgList(List<FMasetrInfoMsg> stopRegionServerMsgList) {
        this.stopRegionServerMsgList = stopRegionServerMsgList;
    }

    public long getRecovery() {
        return recovery;
    }

    public void setRecovery(long recovery) {
        this.recovery = recovery;
    }

    public List<FMasetrInfoMsg> getRecoveryMsgList() {
        return recoveryMsgList;
    }

    public void setRecoveryMsgList(List<FMasetrInfoMsg> recoveryMsgList) {
        this.recoveryMsgList = recoveryMsgList;
    }

    public void calcTotal() {
        setTotal(createNamespace
                + createTable
                + enableTable
                + disableTable
                + dropTable
                + moveRegion
                + splitRegion
                + startReplicasRebalanceAsync
                + syncTableReplicas
                + stopReplicasRebalance
                + startRegionsRebalanceAsync
                + stopRegionsRebalance
                + createSnapshot
                + cloneSnapshot
                + startRegionServer
                + stopRegionServer
                + deleteSnapshot);
    }


}
