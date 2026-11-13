/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations;

import com.google.common.base.Preconditions;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.TableNotExistsException;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.exceptions.NoRetryException;
import com.light.atalanta.master.services.Master;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.master.vo.DataConfiguration;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.util.ZookeeperStatus;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.CSV_NEWLINE;
import static com.light.atalanta.services.ZooKeeperContextHelper.SYNCING_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.getReferencingRegionStatus;
import static com.light.atalanta.util.ZookeeperStatus.getRecoveringStatus;

/**
 * Synchronizes the reserve replicas with the leader replica.
 * The table must be disabled first.
 */
@SuppressFBWarnings("SF_SWITCH_FALLTHROUGH")
public class SyncTableReplicas {
    private static final Logger LOGGER = LoggerFactory.getLogger(SyncTableReplicas.class);

    private final DataConfiguration dataConf;

    private final ZooKeeperMasterContext zooContext;

    private final ZookeeperHelper zooHelper;

    private final Master master;

    public SyncTableReplicas(AtalantaDBConfiguration conf, ZooKeeperMasterContext zooContext, ZookeeperHelper zooHelper, Master master) {
        this.zooContext = zooContext;
        this.zooHelper = zooHelper;
        this.dataConf = conf.getDataConfiguration();
        this.master = master;
    }

    public void action(TableName tableName) {
        int retryCount = 1;
        while (true) {
            try {
                retryableAction(tableName);
                return;
            } catch (NoRetryException ex) {
                LOGGER.error("Error syncing table replicas '{}'", tableName, ex);
                return;
            } catch (Throwable ex) {
                LOGGER.error("Error syncing table replicas: '{}', wait {} ms and retrying #{}...", tableName, dataConf.getRecoverTableRetryDelayMs(), retryCount, ex);
                try {
                    Thread.sleep(dataConf.getRecoverTableRetryDelayMs());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                }
                retryCount++;
            }
        }
    }

    private void retryableAction(TableName tableName) throws Exception {
        LOGGER.info("Start syncing table replicas: '{}'", tableName);

        // Check the replication factor
        if (dataConf.getReplicationFactor() < 2) {
            LOGGER.info("Skipping table '{}' replicas sync, replication factor is {}", tableName, dataConf.getReplicationFactor());
            return;
        }

        // Check the table status
        try {
            checkTableStatus(tableName);
        } catch (Exception ex) {
            throw new NoRetryException(ex);
        }

        // Recovery can't handle several recovering replicas, so process each replica num separately
        // Moreover, recovering all reserve replicas at once is dangerous: the data is effectively overwritten,
        // which raises all kinds of risks
        for (int replicaNum = 1; replicaNum < dataConf.getReplicationFactor(); replicaNum++) {
            // Pre-commit the table and regions for recovery
            makePreCommits(tableName, replicaNum);

            // Recover the table and wait
            recoverTable(tableName, master);
        }
    }

    private void makePreCommits(TableName tableName, int replicaNum) throws Exception {
        LOGGER.debug("Syncing table replicas '{}': pre-commits started for replica num: {}", tableName, replicaNum);
        try {
            List<CuratorOp> plan = new ArrayList<>();
            Map<RegionServerModel, String> rsRegionsData;
            AtomicReference<String> tableStatus = new AtomicReference<>(zooHelper.getNotNullString(ZooKeeperContextHelper.getTablePath(tableName)));
            // Read the table layout
            List<RegionDistributionModel> rdms = zooContext.getRegionDistributionWithStatuses(tableName);
            // Model the replica pre-commits
            rsRegionsData = referencePreCommits(tableName, rdms);
            rsRegionsData = recoveryPreCommits(tableName, rdms, rsRegionsData, replicaNum);
            // Table status
            plan.add(zooHelper.setDataTrans(ZooKeeperContextHelper.getTablePath(tableName), getRecoveringStatus(tableStatus.get())));
            // Build transactions from the models
            for (Map.Entry<RegionServerModel, String> rsData : rsRegionsData.entrySet()) {
                String rsPath = ZooKeeperContextHelper.getRegionServerPath(tableName, rsData.getKey());
                String rsRegionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, rsData.getKey());
                if (StringUtils.isBlank(rsData.getValue()) || rsData.getValue().equals(CSV_NEWLINE)) {
                    // No regions left on the RS: this is bad
                    throw new Exception("unexpected RS ZooKeeper distribution for: " + rsPath);
                } else if (zooHelper.checkExists(rsRegionsPath)) {
                    // Regions are left on the RS and it exists: write the changes
                    plan.add(zooHelper.setDataTrans(rsRegionsPath, rsData.getValue()));
                } else {
                    // No such RS: this is bad too
                    throw new Exception("unable to find RS in ZooKeeper: " + rsPath);
                }
            }
            List<CuratorTransactionResult> results = zooHelper.forOperations(plan);
            Preconditions.checkArgument(results.size() == plan.size(), "Unexpected transaction error");
            LOGGER.debug("Syncing table replicas '{}': pre-commits finished for replica num: {}", tableName, replicaNum);
        } catch (ZKException ex) {
            if (ex.getCause() instanceof KeeperException) {
                KeeperException kExp = (KeeperException)ex.getCause();
                throw new Exception("Unable to pre-commit table replicas sync, ZK path: " + kExp.getPath() + ", ZK message: " + kExp.getMessage() + ", ZK code: " + kExp.code());
            }
            throw new Exception("Unable to pre-commit replicas syncing ZK error", ex.getCause());
        } catch (Exception ex) {
            throw new Exception("Unable to pre-commit replicas syncing", ex);
        }
    }

    private Map<RegionServerModel, String> referencePreCommits(TableName tableName, List<RegionDistributionModel> rdms) throws Exception {
        Map<RegionServerModel, String> rsRegionsData = new HashMap<>();
        List<RegionDistributionModel> leaders = rdms.stream()
                .filter(rdm -> rdm.getRegionModel().getReplicaNum() == 0)
                .collect(Collectors.toList());
        // Mark the leaders as references
        for (RegionDistributionModel referenceRdm : leaders) {
            LOGGER.debug("Syncing table replicas '{}': planning reference pre-commit for {}", tableName, referenceRdm.getRegionModel().toIdentityString());
            RegionServerModel referenceRS = referenceRdm.getRegionServerModel();
            String referenceRSData = rsRegionsData.get(referenceRS);
            if (referenceRSData == null) {
                referenceRSData = getRSRegionsData(tableName, referenceRS);
            }
            // TODO: Decide whether force referencing must be set here
            int referenceStatus = getReferencingRegionStatus(referenceRdm.getRegionModel());
            referenceRSData = zooContext.setRegionStatusForData(referenceRSData, tableName, referenceRdm, referenceStatus);
            rsRegionsData.put(referenceRS, referenceRSData);
        }
        return rsRegionsData;
    }

    private Map<RegionServerModel, String> recoveryPreCommits(TableName tableName, List<RegionDistributionModel> rdms, Map<RegionServerModel, String> rsRegionsData, int replicaNum) throws Exception {
        Map<RegionServerModel, String> rsRegionsDataCopy = new HashMap<>(rsRegionsData);
        List<RegionDistributionModel> reserved_1 = rdms.stream()
                .filter(rdm -> rdm.getRegionModel().getReplicaNum() == replicaNum)
                .collect(Collectors.toList());
        for (RegionDistributionModel recoveryRdm : reserved_1) {
            LOGGER.debug("Syncing table '{}': planning recovery pre-commit for {}", tableName, recoveryRdm.getRegionModel().toIdentityString());
            RegionServerModel recoveryRS = recoveryRdm.getRegionServerModel();
            String recoveryRSData = rsRegionsDataCopy.get(recoveryRS);
            if (recoveryRSData == null) {
                recoveryRSData = getRSRegionsData(tableName, recoveryRS);
            }
            recoveryRSData = zooContext.setRegionStatusForData(recoveryRSData, tableName, recoveryRdm, SYNCING_REGION_STATUS);
            rsRegionsDataCopy.put(recoveryRS, recoveryRSData);
        }
        return rsRegionsDataCopy;
    }

    private void recoverTable(TableName tableName, Master master) throws NoRetryException {
        master.getRecoverTableInstance().action(
                tableName, Collections.emptyList(), () -> {},
                new ReentrantReadWriteLock(), false
        );
    }

    private void checkTableStatus(TableName tableName) throws Exception {
        String zkPath = ZooKeeperContextHelper.getTablePath(tableName);
        boolean tableExists;
        try {
            tableExists = zooHelper.checkExists(zkPath);
        } catch (Exception ex) {
            throw new Exception("unable to check table exists", ex);
        }
        if (!tableExists) {
            throw new TableNotExistsException(tableName);
        }
        ZookeeperStatus status = ZookeeperStatus.fromString(zooHelper.getNotNullString(zkPath));
        // Only a disabled table is accepted
        ZookeeperStatus allowed = ZookeeperStatus.DISABLED;
        if (Objects.requireNonNull(status) == allowed) {
            return;
        }
        throw new Exception("not allowed status for syncing table: " + status + ". allowed: " + allowed);
    }

    private String getRSRegionsData(TableName tableName, RegionServerModel regionServer) throws Exception {
        String result = StringUtils.EMPTY;
        try {
            String rsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, regionServer);
            if (zooHelper.checkExists(rsPath)) {
                result = zooHelper.getNotNullString(rsPath);
            }
        } catch (Exception ex) {
            throw new Exception("Unable to read regions from RS " + regionServer, ex);
        }
        return result;
    }
}
