/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.Empty;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.NoRetryException;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.master.services.LockManager;
import com.light.atalanta.master.services.TableLock;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.master.vo.DataConfiguration;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.model.*;
import com.light.atalanta.services.RegionServerGrpcStub;
import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.util.ZookeeperStatus;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.*;
import static com.light.atalanta.services.ZooKeeperContextHelper.ENABLING_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.REGION_SERVER_REGIONS_NODE;
import static com.light.atalanta.util.ZookeeperStatus.*;

/**
 * Fault-tolerant change of the data storage format.
 * Changes the block compression (COMPRESSION), the record compression (RECORD_COMPRESSION)
 * and the use of a dictionary (DICTIONARY).
 * <p>
 * On the master:
 * When a client requests a new storage format for a table, the master:
 * 1. Checks the table status; if the change is possible, sets it to MODIFYING.
 * 2. Creates the ZooKeeper znode /__atalanta_modify/TABLE_NAME with the new format (storeCfg).
 * 3. For each region replica of the table, creates the znode /__atalanta_modify/TABLE_NAME/regionId_replicaN
 *    with the current modification stage of the region: NEW.
 * 4. Notifies all RUNNING RS about the new modification request with the newModifyNotification gRPC request.
 * On start, the leader master runs a modification monitoring thread that:
 * 1. Reads all tables from the znode /__atalanta_modify and counts the region replicas in them.
 * 2. If a table has no region replicas left, all RS have finished the modification: the master
 *    sets the table status to MODIFY and deletes the znode /__atalanta_modify/TABLE_NAME. The modification is done.
 * 3. Waits on the notification semaphore for up to TIMEOUT seconds, then goes back to step 1,
 *    either on a notification from an RS or on the timeout.
 * When the master receives a notification from an RS:
 * After modifying a region replica, the RS must send the modificationCompleteNotification request,
 * on which the master releases the notification semaphore and so wakes up the monitoring thread.
 * <p>
 * On the RS:
 * On start, the RS runs a modification monitoring thread that:
 * 1. Reads all tables from the znode /__atalanta_modify and looks for its own region replicas in them.
 * 2. For its replicas with the NEW status, creates the znode /__atalanta_modify/TABLE_NAME/regionId_replicaN/RS_id
 *    with the MODIFYING status, and writes the MODIFYING status to /__atalanta_modify/TABLE_NAME/regionId_replicaN.
 * 3. For its replicas with the NEW or MODIFYING status, writes MODIFYING to
 *    /__atalanta_modify/TABLE_NAME/regionId_replicaN/RS_id, stops the region in the core, creates (or cleans,
 *    if it exists) the folder regionId_replicaN_modify next to the region folder, writes the data there
 *    in the new format, and then sets the status to READY_TO_BACKUP.
 * 4. For its replicas with the READY_TO_BACKUP status, renames the folder regionId_replicaN (if it exists)
 *    to regionId_replicaN_bak and sets the status to READY_TO_COPY.
 * 5. For its replicas with the READY_TO_COPY status, renames the folder regionId_replicaN_modify (if it exists)
 *    to regionId_replicaN and sets the status to READY_TO_ENABLED.
 * 6. For its replicas with the READY_TO_ENABLED status, starts the region in the core
 *    and deletes the znode /__atalanta_modify/TABLE_NAME/regionId_replicaN.
 * 7. Waits on the notification semaphore for up to TIMEOUT seconds, then goes back to step 1,
 *    either on a notification from the master or on the timeout.
 * When the RS receives a notification from the master, it releases the notification semaphore
 * and so wakes up the monitoring thread.
 */

public class ModifyTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ModifyTable.class);
    private final ZooKeeperMasterContext zooContext;
    private final ZookeeperHelper zooHelper;
    private final AtalantaDBConfiguration conf;
    private final Empty empty = Empty.newBuilder().build();
    private Thread thread;
    private volatile boolean stopped;
    private final EnableTable enableTable;
    private final LockManager lockManager;

    Semaphore semaphore = new Semaphore(1);

    public ModifyTable(AtalantaDBConfiguration conf, ZooKeeperMasterContext zooContext, ZookeeperHelper zooHelper, EnableTable enableTable, LockManager lockManager) {
        this.zooContext = zooContext;
        this.zooHelper = zooHelper;
        this.conf = conf;
        this.enableTable = enableTable;
        this.lockManager = lockManager;
    }

    public void action(TableName tableName, StorageParam storageCfg) {
        int retryCount = 1;
        while (true) {
            try {
                List<RegionDistributionModel> rdmList = zooContext.getRegionDistributionWithStatuses(tableName);
                if (isNoNeedModification(storageCfg, rdmList)) {
                    LOGGER.info("No need modification. Table {} current format equals to {}", tableName, storageCfg);
                    return;
                }
                checkTableStatus(tableName);
                modifyAction(tableName, storageCfg, rdmList);
                return;
            } catch (NoRetryException ex) {
                throw new OperationException(ex);
            } catch (Throwable ex) {
                final DataConfiguration dataConf = conf.getDataConfiguration();
                LOGGER.error("Error modify table: '{}', wait {} ms and retrying #{}...", tableName, dataConf.getSplitTableRetryDelayMs(), retryCount, ex);
                try {
                    Thread.sleep(dataConf.getSplitTableRetryDelayMs());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new OperationException(ex);
                }
                retryCount++;
            }
        }
    }

    private boolean isNoNeedModification(StorageParam storageCfg, List<RegionDistributionModel> rdmList) {
        for (RegionDistributionModel rdm : rdmList) {
            if (!storageCfg.compareWithRegion(rdm.getRegionModel())) {
                return false;
            }
        }
        return true;
    }

    public void checkTableStatus(TableName tableName) throws Exception {
        String tablePath = ZooKeeperContextHelper.getTablePath(tableName);
        ZookeeperStatus tableStatus = ZookeeperStatus.fromString(zooHelper.getNotNullString(tablePath));
        if (tableStatus != ASSIGNED
                && tableStatus != CREATED
                && tableStatus != ENABLED) {
            throw new NoRetryException("The modification is not allowed for table in status " + tableStatus.name());
        }
    }

    private void modifyAction(TableName tableName, StorageParam storageCfg, List<RegionDistributionModel> rdmList) throws Exception {
        LOGGER.info("Start modify table: '{}' to: {}", tableName, storageCfg);
        createNewModificationInZookeeper(tableName, storageCfg, rdmList);
        sendRsGrpcNotification(rdmList);
        LOGGER.info("Complete modify table: '{}' to: {}", tableName, storageCfg);
    }

    private void sendRsGrpcNotification(List<RegionDistributionModel> rdmList) {
        for (RegionDistributionModel rdm : rdmList) {
            RegionServerGrpcStub.Sync stub = RegionServerGrpcStub.sync(rdm.getRegionServerModel(),
                    conf.getGrpcConfiguration().getRsReconnectTimeoutMs());
            stub.get().newModifyNotification(empty);
        }
    }

    private void createNewModificationInZookeeper(TableName tableName, StorageParam storageCfg,
                                                  List<RegionDistributionModel> rdmList) throws Exception {
        List<CuratorOp> plan = new ArrayList<>(rdmList.size() + 16);
        plan.add(zooHelper.setDataTrans(ZooKeeperContextHelper.getTablePath(tableName), STATUS_PREFIX + MODIFYING.getValue()));
        String path = createIfNeeded(plan, ATALANTA_MODIFY);
        path = createIfNeeded(plan, path + ZK_SEPARATOR + tableName.getNamespace());
        String baseTablePath = path + ZK_SEPARATOR + tableName.getTableName();
        plan.add(zooHelper.createTrans(baseTablePath, storageCfg.toYaml()));
        for (RegionDistributionModel rdm : rdmList) {
            String replicaName = rdm.getRegionModel().getName() + "_" + rdm.getRegionModel().getReplicaNum();
            plan.add(zooHelper.createTrans(baseTablePath + ZK_SEPARATOR + replicaName, "NEW"));
        }
        zooHelper.forOperationsNoCache(plan);
    }

    private String createIfNeeded(List<CuratorOp> plan, String path) throws ZKException {
        if (!zooHelper.checkExists(path)) {
            plan.add(zooHelper.createTrans(path));
        }
        return path;
    }

    public synchronized void stopMonitoring() {
        stopped = true;
        if (thread != null) thread.interrupt();
    }

    public synchronized void startMonitoring() {
        if (stopped) return;
        if (thread != null && thread.isAlive()) {
            LOGGER.warn("Modification monitoring already started.");
            return;
        }
        thread = new Thread(this::monitoringProcess, "atalanta-modify-monitoring");
        thread.setDaemon(true);
        thread.start();
        LOGGER.info("Modification monitoring thread started.");
    }

    private void monitoringProcess() {
        while (!stopped) {
            try {
                if (zooHelper.checkExists(ATALANTA_MODIFY)) {
                    List<String> nsList = zooHelper.getChildren(ATALANTA_MODIFY);
                    LOGGER.info("modifying nsList size: {}", nsList.size());
                    for (String ns : nsList) {
                        processingNs(nsList, ns);
                    }
                }
                semaphore.acquire();
                if (semaphore.tryAcquire(conf.getDataConfiguration().getModifyPullTimeoutMs(), TimeUnit.MILLISECONDS)) {
                    LOGGER.info("New modification...");
                }
                semaphore.release();
            } catch (InterruptedException e) {
                LOGGER.info("Interrupted!", e);
                throw new RuntimeException(e);
            } catch (ZKException e) {
                LOGGER.error("ZK error!", e);
            } catch (JsonProcessingException e) {
                LOGGER.error("Json error!", e);
            } catch (Exception e) {
                LOGGER.error("Some error!", e);
            }
        }
    }

    private void processingNs(List<String> nsList, String ns) throws Exception {
        String curNsPath = ATALANTA_MODIFY + ZK_SEPARATOR + ns;
        List<String> tableList = zooHelper.getChildren(curNsPath);
        LOGGER.info("Modifying ns: {} table count: {}", nsList.size(), tableList.size());
        for (String table : tableList) {
            processingTable(ns, curNsPath, table);
        }
    }

    private void processingTable(String ns, String curNsPath, String table) throws Exception {
        String curTablePath = curNsPath + ZK_SEPARATOR + table;
        List<String> replicasList = zooHelper.getChildren(curTablePath);
        LOGGER.info("Modifying table: {}:{} replicas count: {}", ns, table, replicasList.size());
        if (replicasList.isEmpty()) {
            modificationIsComplete(ns, table, curTablePath);
        }
    }

    private void modificationIsComplete(String ns, String table, String curModifyTablePath) throws Exception {
        LOGGER.info("Start update ZK after modification for: {}:{}", ns, table);
        String pathToTable = ns + ZK_SEPARATOR + table;
        StorageParam storageParam = StorageParam.fromYaml(zooHelper.get(curModifyTablePath).orElseThrow(
                () -> new RuntimeException("Unknown storage param for path: " + curModifyTablePath)));
        List<String> rsList = zooHelper.getChildren(pathToTable);
        TableName tableName = TableName.valueOf(ns, table);
        List<CuratorOp> plan = new ArrayList<>(rsList.size() + 16);
        for (String rs : rsList) {
            String pathToRegions = pathToTable + ZK_SEPARATOR + rs + REGION_SERVER_REGIONS_NODE;
            String regions = zooHelper.get(pathToRegions).orElse(StringUtils.EMPTY);
            String[] regionArray = ZooKeeperContextHelper.splitCsvByLines(regions);
            List<RegionModel> regionModelList = new ArrayList<>(regionArray.length);
            for (String region : regionArray) {
                RegionModel r = RegionModel.fromBase64String(region);
                regionModelList.add(new RegionModel(r.getName(), r.getReplicaNum(), ENABLING_REGION_STATUS,
                        r.getBeginKey(), r.getEndKey(), r.getDataBlockSize(),
                        storageParam.getCompressType(), storageParam.getRecordCompressionType(),
                        storageParam.getDictionary(), storageParam.getRecordTargetCount()));
            }
            String rsData = regionModelList.stream()
                    .map(RegionModel::toBase64StringForZk)
                    .collect(Collectors.joining(CSV_NEWLINE));
            plan.add(zooHelper.setDataTrans(pathToRegions, rsData));

            zooContext.addExtParamToPlan(plan, tableName, regionModelList);
        }
        plan.add(zooHelper.setDataTrans(pathToTable, STATUS_PREFIX + ENABLING.getValue()));
        plan.add(zooHelper.deleteTrans(curModifyTablePath));
        zooHelper.forOperations(plan);
        enableTable(ns, table);
        LOGGER.info("Complete update ZK after modification for: {}:{}", ns, table);
    }

    private void enableTable(String ns, String table) throws InterruptedException {
        String operationName = "ENABLE_TABLE";
        TableName tableName = TableName.valueOf(ns, table);
        TableLock tableLock = lockManager.acquireRef(tableName, operationName);
        if (!tableLock.tryLock(conf.getDataConfiguration().getLockWaitingTimeoutMs(), operationName)) {
            tableLock.releaseRef();
            throw new RuntimeException("Lock waiting timeout for table: " + tableName);
        }
        enableTable.runSyncThenAsyncOnError(tableLock);
    }

    public void modificationCompleteNotification() {
        LOGGER.info("modification notification from RS");
        semaphore.release();
    }
}
