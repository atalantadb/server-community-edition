/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services;

import com.light.atalanta.TableName;
import com.light.atalanta.master.services.operations.move.MoveRegion;
import com.light.atalanta.master.services.operations.OperationRunner;
import com.light.atalanta.master.services.operations.RecoverTable;
import com.light.atalanta.master.services.rebalance.RegionRebalanceModel;
import com.light.atalanta.master.services.recovery.RecoveryRSContext;
import com.light.atalanta.master.services.recovery.RegionRecoveryModel;
import com.light.atalanta.model.CompressType;
import com.light.atalanta.model.RecordCompressionType;
import com.light.atalanta.model.RegionServerModel;
import io.grpc.Server;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

public interface Master extends Closeable {

    boolean start(Server server) throws Exception;

    List<RegionServerModel> getShutdownRegionServers() throws Exception;

    void stopForShutdown() throws IOException;

    void createNamespace(String namespace) throws Exception;

    void createTable(TableName tableName, List<byte[]> splitKeys, int dataBlockSize, CompressType compressType,
                     RecordCompressionType recordCompressionType, boolean dictionary, Long recordTargetCount) throws Throwable;

    void enableTable(TableName tableName, TableLock tableLock) throws Throwable;

    void disableTable(TableName tableName, TableLock tableLock) throws Throwable;

    void dropTable(TableName tableName, TableLock tableLock) throws Throwable;

    void moveRegion(TableName tableName, List<RegionRebalanceModel> moveModels, TableLock tableLock) throws Throwable;

    void splitTable(TableName tableName, List<String> regionNamesToSplit, TableLock tableLock) throws Throwable;

    void recoverTable(TableName tableName, List<RegionServerModel> lostRS, Runnable actualizeRS, ReadWriteLock lock, boolean checkLostRS) throws Exception;

    void rebalanceTableReplicas(List<TableName> tablesToRebalance, AtomicLong rebalancingTables);

    void stopRebalanceTableReplicas(List<TableName> tablesToStop);

    void rebalanceTableRegions(List<TableName> tablesToRebalance, AtomicLong rebalancingTables);

    void stopRebalanceTableRegions(List<TableName> tablesToStop);

    void syncTableReplicas(List<TableName> tablesToSync, AtomicLong syncingTables);

    boolean isNeedToRecoverTable(TableName tableName) throws Exception;

    List<RegionRecoveryModel> findRecoveryModels(TableName tableName) throws Exception;

    TableLock notifyTableRecovering(TableName tableName);

    boolean getRsStartPermission(RegionServerModel regionServer) throws Exception;

    List<TableName> getTableNamesToRebalance(String rebalanceEntity, boolean checkStatus) throws Exception;

    List<TableName> getTableNamesToSync(String syncEntity) throws Exception;

    OperationRunner getOperationRunner();

    MoveRegion getMoveRegionInstance();

    RecoverTable getRecoverTableInstance();

    RecoveryRSContext getRecoverRSContextInstance();

    void createSnapshot(String name, TableName tableName);

    void cloneSnapshot(String name, TableName tableName);

    void deleteSnapshot(String name);

    void startRegionServer(String value);

    void stopRegionServer(String value);

    boolean checkDiskSizeForSplit(TableName table, List<String> regionNamesToSplit) throws IOException;

    void modifyTable(TableName tableName, CompressType compressType, RecordCompressionType recordCompressionType, Long recordTargetCount, Boolean dictionary) throws InterruptedException, IOException;

    void modificationCompleteNotification();
}
