/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations.move;

import com.google.common.base.Preconditions;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZookeeperHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.light.atalanta.ATALANTAConstants.CSV_NEWLINE;
import static com.light.atalanta.ATALANTAConstants.STATUS_PREFIX;
import static com.light.atalanta.services.ZooKeeperContextHelper.ASSIGNED_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.UNASSIGNING_REGION_STATUS;
import static com.light.atalanta.util.ZookeeperStatus.ASSIGNED;
import static com.light.atalanta.util.ZookeeperStatus.ASSIGNING;

public class MovePreCommitHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(MovePreCommitHelper.class);

    private final ZookeeperHelper zooHelper;
    private final ZooKeeperMasterContext zooContext;

    public MovePreCommitHelper(ZookeeperHelper zooHelper, ZooKeeperMasterContext zooContext) {
        this.zooHelper = zooHelper;
        this.zooContext = zooContext;
    }

    public void makePreCommits(MoveRegionContext context) throws Exception {
        LOGGER.info("Moving table regions '{}': making pre-commits", context.getTableName());
        try {
            List<CuratorOp> plan = new ArrayList<>();
            Map<RegionServerModel, String> rsRegionsData;
            // Model the replica pre-commits
            rsRegionsData = preparePreCommits1(context, plan);
            rsRegionsData = preparePreCommits2(context, rsRegionsData);
            // Table status
            plan.add(zooHelper.setDataTrans(
                    ZooKeeperContextHelper.getTablePath(context.getTableName()),
                    STATUS_PREFIX + ASSIGNING.getValue()
            ));
            // Build transactions from the models
            for (Map.Entry<RegionServerModel, String> rsData : rsRegionsData.entrySet()) {
                String rsPath = ZooKeeperContextHelper.getRegionServerPath(context.getTableName(), rsData.getKey());
                String rsApiLocationPath = ZooKeeperContextHelper.getRegionServerApiLocationPath(context.getTableName(), rsData.getKey());
                String rsRegionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(context.getTableName(), rsData.getKey());
                // No regions left on the RS
                if (StringUtils.isBlank(rsData.getValue()) || rsData.getValue().equals(CSV_NEWLINE)) {
                    // And it exists: clean it up
                    if (zooHelper.checkExists(rsRegionsPath)) {
                        // No regions left on the RS: safe to delete its nodes
                        plan.add(zooHelper.deleteTrans(rsRegionsPath));
                        plan.add(zooHelper.deleteTrans(rsApiLocationPath));
                        plan.add(zooHelper.deleteTrans(rsPath));
                    }
                } else if (zooHelper.checkExists(rsRegionsPath)) {
                    // Regions are left on the RS and it exists: write the changes
                    plan.add(zooHelper.setDataTrans(rsRegionsPath, rsData.getValue()));
                } else {
                    // No such RS: create it
                    String rsApiLocationData = rsData.getKey().getLocationString();
                    plan.add(zooHelper.createTrans(rsPath));
                    plan.add(zooHelper.createTrans(rsApiLocationPath, rsApiLocationData));
                    plan.add(zooHelper.createTrans(rsRegionsPath, rsData.getValue()));
                }
            }
            List<CuratorTransactionResult> results = zooHelper.forOperations(plan);
            Preconditions.checkArgument(results.size() == plan.size(), "Unexpected transaction error");
        } catch (ZKException ex) {
            if (ex.getCause() instanceof KeeperException) {
                KeeperException kExp = (KeeperException) ex.getCause();
                throw new Exception("Unable to pre-commit move, ZK path: " + kExp.getPath() + ", ZK message: " + kExp.getMessage() + ", ZK code: " + kExp.code());
            }
            throw new Exception("Unable to pre-commit move ZK error", ex.getCause());
        } catch (Exception ex) {
            throw new Exception("unable to pre-commit move", ex);
        }
        // Update the region layout in the context
        context.updateRdms();
        context.reflectPreCommits();
        LOGGER.info("Moving table regions '{}': pre-commits were made successfully", context.getTableName());
    }

    // Add the target regions
    private Map<RegionServerModel, String> preparePreCommits1(MoveRegionContext context, List<CuratorOp> plan) throws Exception {
        Map<RegionServerModel, String> rsRegionsDataCopy = new HashMap<>();
        for (RegionMoveProjection mp : context.getMoveProjections()) {
            RegionDistributionModel targetRdm = mp.getTarget();
            LOGGER.debug("Moving table regions '{}': planning move pre-commit1 for {}", context.getTableName(), targetRdm.toShortString());
            RegionServerModel targetRS = targetRdm.getRegionServerModel();
            String targetRSData = rsRegionsDataCopy.get(targetRS);
            if (targetRSData == null) {
                targetRSData = getRSRegionsData(context.getTableName(), targetRS);
            }
            if (StringUtils.isBlank(targetRSData)) {
                targetRSData = targetRdm.getRegionModel().toBase64StringForZk();
            } else {
                targetRSData = targetRSData + CSV_NEWLINE + targetRdm.getRegionModel().toBase64StringForZk();
            }
            rsRegionsDataCopy.put(targetRS, targetRSData);
        }
        return rsRegionsDataCopy;
    }

    // Change the status of the source regions to UNASSIGNING
    private Map<RegionServerModel, String> preparePreCommits2(MoveRegionContext context, Map<RegionServerModel, String> rsRegionsData) throws Exception {
        Map<RegionServerModel, String> rsRegionsDataCopy = new HashMap<>(rsRegionsData);
        for (RegionMoveProjection mp : context.getMoveProjections()) {
            RegionDistributionModel sourceRdm = mp.getSource();
            RegionServerModel sourceRS = sourceRdm.getRegionServerModel();
            String sourceRSData = rsRegionsDataCopy.get(sourceRS);
            if (sourceRSData == null) {
                sourceRSData = getRSRegionsData(context.getTableName(), sourceRS);
            }
            sourceRSData = zooContext.setRegionStatusForData(sourceRSData, context.getTableName(), sourceRdm, UNASSIGNING_REGION_STATUS);
            rsRegionsDataCopy.put(sourceRS, sourceRSData);
        }
        return rsRegionsDataCopy;
    }

    void makeDestCommits(MoveRegionContext context) throws Exception {
        LOGGER.info("Moving table regions '{}': making dest commits", context.getTableName());
        try {
            List<CuratorOp> plan = new ArrayList<>();
            // Model the replica pre-commits
            Map<RegionServerModel, String> rsRegionsData = prepareDestCommits(context);
            // Build transactions from the models
            for (Map.Entry<RegionServerModel, String> rsData : rsRegionsData.entrySet()) {
                String rsRegionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(context.getTableName(), rsData.getKey());
                plan.add(zooHelper.setDataTrans(rsRegionsPath, rsData.getValue()));
            }
            List<CuratorTransactionResult> results = zooHelper.forOperations(plan);
            Preconditions.checkArgument(results.size() == plan.size(), "Unexpected transaction error");
        } catch (ZKException ex) {
            if (ex.getCause() instanceof KeeperException) {
                KeeperException kExp = (KeeperException) ex.getCause();
                throw new Exception("Unable to make dest commits, ZK path: " + kExp.getPath() + ", ZK message: " + kExp.getMessage() + ", ZK code: " + kExp.code());
            }
            throw new Exception("Unable to make dest commits ZK error", ex.getCause());
        } catch (Exception ex) {
            throw new Exception("unable to make dest commits", ex);
        }
        // Update the region layout in the context
        context.updateRdms();
        context.reflectDestCommits();
        LOGGER.info("Moving table regions '{}': dest commits were made successfully", context.getTableName());
    }

    // Change the status of the target regions to ASSIGNED
    private Map<RegionServerModel, String> prepareDestCommits(MoveRegionContext context) throws Exception {
        Map<RegionServerModel, String> rsRegionsDataCopy = new HashMap<>();
        for (RegionMoveProjection mp : context.getMoveProjections()) {
            RegionDistributionModel targetRdm = mp.getTarget();
            RegionServerModel targetRS = targetRdm.getRegionServerModel();
            String targetRSData = rsRegionsDataCopy.get(targetRS);
            if (targetRSData == null) {
                targetRSData = getRSRegionsData(context.getTableName(), targetRS);
            }
            targetRSData = zooContext.setRegionStatusForData(targetRSData, context.getTableName(), targetRdm, ASSIGNED_REGION_STATUS);
            rsRegionsDataCopy.put(targetRS, targetRSData);
        }
        return rsRegionsDataCopy;
    }

    void makeFinishCommits(MoveRegionContext context) throws Exception {
        LOGGER.info("Moving table regions '{}': making finish commits", context.getTableName());
        try {
            List<CuratorOp> plan = new ArrayList<>();
            // Table status
            plan.add(zooHelper.setDataTrans(
                    ZooKeeperContextHelper.getTablePath(context.getTableName()),
                    STATUS_PREFIX + ASSIGNED.getValue()
            ));
            // Model the replica pre-commits
            Map<RegionServerModel, String> rsRegionsData = prepareFinishCommits(context);
            // Build transactions from the models
            for (Map.Entry<RegionServerModel, String> rsData : rsRegionsData.entrySet()) {
                String rsPath = ZooKeeperContextHelper.getRegionServerPath(context.getTableName(), rsData.getKey());
                String rsApiLocationPath = ZooKeeperContextHelper.getRegionServerApiLocationPath(context.getTableName(), rsData.getKey());
                String rsRegionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(context.getTableName(), rsData.getKey());
                // No regions left on the RS
                if (StringUtils.isBlank(rsData.getValue()) || rsData.getValue().equals(CSV_NEWLINE)) {
                    // And it exists: clean it up
                    if (zooHelper.checkExists(rsRegionsPath)) {
                        // No regions left on the RS: safe to delete its nodes
                        plan.add(zooHelper.deleteTrans(rsRegionsPath));
                        plan.add(zooHelper.deleteTrans(rsApiLocationPath));
                        plan.add(zooHelper.deleteTrans(rsPath));
                    }
                } else if (zooHelper.checkExists(rsRegionsPath)) {
                    // Regions are left on the RS and it exists: write the changes
                    plan.add(zooHelper.setDataTrans(rsRegionsPath, rsData.getValue()));
                } else {
                    // No such RS: create it
                    String rsApiLocationData = rsData.getKey().getLocationString();
                    plan.add(zooHelper.createTrans(rsPath));
                    plan.add(zooHelper.createTrans(rsApiLocationPath, rsApiLocationData));
                    plan.add(zooHelper.createTrans(rsRegionsPath, rsData.getValue()));
                }
            }
            List<CuratorTransactionResult> results = zooHelper.forOperations(plan);
            Preconditions.checkArgument(results.size() == plan.size(), "Unexpected transaction error");
        } catch (ZKException ex) {
            if (ex.getCause() instanceof KeeperException) {
                KeeperException kExp = (KeeperException) ex.getCause();
                throw new Exception("Unable to make finish commits, ZK path: " + kExp.getPath() + ", ZK message: " + kExp.getMessage() + ", ZK code: " + kExp.code());
            }
            throw new Exception("Unable to make finish commits ZK error", ex.getCause());
        } catch (Exception ex) {
            throw new Exception("unable to make finish commits", ex);
        }
        LOGGER.info("Moving table regions '{}': finish commits were made successfully", context.getTableName());
    }

    // Set the ASSIGNED status for the table and remove the source replicas
    private Map<RegionServerModel, String> prepareFinishCommits(MoveRegionContext context) throws Exception {
        Map<RegionServerModel, String> rsRegionsDataCopy = new HashMap<>();
        for (RegionMoveProjection mp : context.getMoveProjections()) {
            RegionDistributionModel sourceRdm = mp.getSource();
            RegionServerModel sourceRS = sourceRdm.getRegionServerModel();
            String sourceRSData = rsRegionsDataCopy.get(sourceRS);
            if (sourceRSData == null) {
                sourceRSData = getRSRegionsData(context.getTableName(), sourceRS);
            }
            sourceRSData = zooContext.removeRegionForData(sourceRSData, context.getTableName(), sourceRdm);
            rsRegionsDataCopy.put(sourceRS, sourceRSData);
        }
        return rsRegionsDataCopy;
    }

    void rollbackPreCommits(MoveRegionContext context) throws Exception {
        LOGGER.info("Moving table regions '{}': rollback pre-commits", context.getTableName());
        try {
            List<CuratorOp> plan = new ArrayList<>();
            // Table status
            plan.add(zooHelper.setDataTrans(
                    ZooKeeperContextHelper.getTablePath(context.getTableName()),
                    STATUS_PREFIX + ASSIGNED.getValue()
            ));
            // Model the replica pre-commits
            Map<RegionServerModel, String> rsRegionsData = prepareRollbackPreCommits1(context);
            rsRegionsData = prepareRollbackPreCommits2(context, rsRegionsData);
            // Build transactions from the models
            for (Map.Entry<RegionServerModel, String> rsData : rsRegionsData.entrySet()) {
                String rsRegionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(context.getTableName(), rsData.getKey());
                plan.add(zooHelper.setDataTrans(rsRegionsPath, rsData.getValue()));
            }
            List<CuratorTransactionResult> results = zooHelper.forOperations(plan);
            Preconditions.checkArgument(results.size() == plan.size(), "Unexpected transaction error");
        } catch (ZKException ex) {
            if (ex.getCause() instanceof KeeperException) {
                KeeperException kExp = (KeeperException) ex.getCause();
                throw new Exception("Unable to rollback pre-commits, ZK path: " + kExp.getPath() + ", ZK message: " + kExp.getMessage() + ", ZK code: " + kExp.code());
            }
            throw new Exception("Unable to rollback pre-commits ZK error", ex.getCause());
        } catch (Exception ex) {
            throw new Exception("unable to rollback pre-commits commits", ex);
        }
        // Update the region layout in the context
        context.updateRdms();
        context.reflectRollbackPreCommits();
        LOGGER.info("Moving table regions '{}': pre-commits were rollbacked successfully", context.getTableName());
    }

    // Change the status of the source regions to ASSIGNED
    private Map<RegionServerModel, String> prepareRollbackPreCommits1(MoveRegionContext context) throws Exception {
        Map<RegionServerModel, String> rsRegionsDataCopy = new HashMap<>();
        for (RegionMoveProjection mp : context.getMoveProjections()) {
            RegionDistributionModel sourceRdm = mp.getSource();
            RegionServerModel sourceRS = sourceRdm.getRegionServerModel();
            String sourceRSData = rsRegionsDataCopy.get(sourceRS);
            if (sourceRSData == null) {
                sourceRSData = getRSRegionsData(context.getTableName(), sourceRS);
            }
            sourceRSData = zooContext.setRegionStatusForData(sourceRSData, context.getTableName(), sourceRdm, ASSIGNED_REGION_STATUS);
            rsRegionsDataCopy.put(sourceRS, sourceRSData);
        }
        return rsRegionsDataCopy;
    }

    // Remove the target regions
    private Map<RegionServerModel, String> prepareRollbackPreCommits2(MoveRegionContext context, Map<RegionServerModel, String> rsRegionsData) throws Exception {
        Map<RegionServerModel, String> rsRegionsDataCopy = new HashMap<>(rsRegionsData);
        for (RegionMoveProjection mp : context.getMoveProjections()) {
            RegionDistributionModel targetRdm = mp.getTarget();
            RegionServerModel targetRS = targetRdm.getRegionServerModel();
            String targetRSData = rsRegionsDataCopy.get(targetRS);
            if (targetRSData == null) {
                targetRSData = getRSRegionsData(context.getTableName(), targetRS);
            }
            targetRSData = zooContext.removeRegionForData(targetRSData, context.getTableName(), targetRdm);
            rsRegionsDataCopy.put(targetRS, targetRSData);
        }
        return rsRegionsDataCopy;
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
