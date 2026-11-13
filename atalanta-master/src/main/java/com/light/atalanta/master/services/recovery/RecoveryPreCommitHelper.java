/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.recovery;

import com.google.common.base.Preconditions;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.master.services.grpc.RsHealthChecker;
import com.light.atalanta.master.services.operations.RecoverTable;
import com.light.atalanta.master.services.operations.SplitTable;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.util.ZookeeperStatus;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.light.atalanta.ATALANTAConstants.CSV_NEWLINE;
import static com.light.atalanta.services.ZooKeeperContextHelper.DISABLED_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.FORCE_RECOVERING_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.SYNCING_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.getReferencedRegionStatus;
import static com.light.atalanta.services.ZooKeeperContextHelper.getReferencingRegionStatus;
import static com.light.atalanta.services.ZooKeeperContextHelper.isReferencingRegionStatus;
import static com.light.atalanta.util.ZookeeperStatus.getRecoveredStatus;
import static com.light.atalanta.util.ZookeeperStatus.getRecoveringStatus;

public class RecoveryPreCommitHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryPreCommitHelper.class);

    private final ZookeeperHelper zooHelper;
    private final ZooKeeperMasterContext zooContext;
    private final RecoveryRSContext recoveryRSContext;

    public RecoveryPreCommitHelper(ZookeeperHelper zooHelper, ZooKeeperMasterContext zooContext, RecoveryRSContext recoveryRSContext) {
        this.zooHelper = zooHelper;
        this.zooContext = zooContext;
        this.recoveryRSContext = recoveryRSContext;
    }

    /**
     * Works at retry level zero only:
     *  - RECOVERING status for the table
     *  - REFERENCING status for the reference
     *  - adds the RECOVERING region to the region list
     */
    public void makePreCommits(TableName tableName, List<RegionRecoveryModel> recoveryModels) throws Exception {
        LOGGER.debug("Recovering table '{}': pre-commits started", tableName);
        try {
            List<CuratorOp> plan = new ArrayList<>();
            Map<RegionServerModel, String> rsRegionsData;
            AtomicReference<String> tableStatus = new AtomicReference<>(zooHelper.getNotNullString(ZooKeeperContextHelper.getTablePath(tableName)));
            // Model the replica pre-commits
            rsRegionsData = referencePreCommits(tableName, recoveryModels);
            rsRegionsData = recoveryPreCommits(tableName, recoveryModels, rsRegionsData, plan);
            // Table status, if needed
            if (ZookeeperStatus.fromString(tableStatus.get()) == ZookeeperStatus.RECOVERING) {
                LOGGER.info("Recovering table '{}': table is already in RECOVERING status, skip precommiting it again", tableName);
            } else {
                LOGGER.debug("Recovering table '{}': precommiting RECOVERING status", tableName);
                plan.add(zooHelper.setDataTrans(ZooKeeperContextHelper.getTablePath(tableName), getRecoveringStatus(tableStatus.get())));
            }
            // Build transactions from the models
            for (Map.Entry<RegionServerModel, String> rsData : rsRegionsData.entrySet()) {
                String rsPath = ZooKeeperContextHelper.getRegionServerPath(tableName, rsData.getKey());
                String rsApiLocationPath = ZooKeeperContextHelper.getRegionServerApiLocationPath(tableName, rsData.getKey());
                String rsRegionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, rsData.getKey());
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
            LOGGER.debug("Recovering table '{}': pre-commits finished", tableName);
        } catch (ZKException ex) {
            if (ex.getCause() instanceof KeeperException) {
                KeeperException kExp = (KeeperException)ex.getCause();
                throw new Exception("Unable to pre-commit region recovery, ZK path: " + kExp.getPath() + ", ZK message: " + kExp.getMessage() + ", ZK code: " + kExp.code());
            }
            throw new Exception("Unable to pre-commit region recovery ZK error", ex.getCause());
        } catch (Exception ex) {
            throw new Exception("Unable to pre-commit region recovery", ex);
        }
    }

    private Map<RegionServerModel, String> referencePreCommits(TableName tableName, List<RegionRecoveryModel> recoveryModels) throws Exception {
        Map<RegionServerModel, String> rsRegionsData = new HashMap<>();
        // Replicas already marked as references
        List<String> referencedRdms = new ArrayList<>();
        for (RegionRecoveryModel recoveryModel : recoveryModels) {
            // Reference status in ZK and in the models
            RegionDistributionModel referenceRdm = recoveryModel.getReferenceRdm();
            RegionServerModel referenceRS = referenceRdm.getRegionServerModel();
            int referenceStatus = getReferencingRegionStatus(referenceRdm.getRegionModel());
            if (!referencedRdms.contains(referenceRdm.getRegionModel().toIdentityString())) {
                String referenceRSData = rsRegionsData.get(referenceRS);
                if (referenceRSData == null) {
                    referenceRSData = getRSRegionsData(tableName, referenceRS);
                }
                referenceRSData = zooContext.setRegionStatusForData(referenceRSData, tableName, referenceRdm, referenceStatus);
                rsRegionsData.put(referenceRS, referenceRSData);
                referencedRdms.add(referenceRdm.getRegionModel().toIdentityString());
            }
            if (!isReferencingRegionStatus(referenceRdm.getRegionModel())) {
                referenceRdm = zooContext.makeRdmCopy(referenceRdm, referenceRS, referenceStatus);
                recoveryModel.setReferenceRdm(referenceRdm);
            }
        }
        return rsRegionsData;
    }

    private Map<RegionServerModel, String> recoveryPreCommits(TableName tableName, List<RegionRecoveryModel> recoveryModels, Map<RegionServerModel, String> rsRegionsData, List<CuratorOp> plan) throws Exception {
        Map<RegionServerModel, String> rsRegionsDataCopy = new HashMap<>(rsRegionsData);
        for (RegionRecoveryModel recoveryModel : recoveryModels) {
            // Add the recovery replica
            RegionDistributionModel recoveryRdm = recoveryModel.getRecoveryRdm();
            RegionServerModel recoveryRS = recoveryRdm.getRegionServerModel();
            String recoveryRSData = rsRegionsDataCopy.get(recoveryRS);
            if (recoveryRSData == null) {
                recoveryRSData = getRSRegionsData(tableName, recoveryRS);
            }
            if (StringUtils.isBlank(recoveryRSData)) {
                recoveryRSData = recoveryRdm.getRegionModel().toBase64StringForZk();
            } else {
                recoveryRSData = recoveryRSData + CSV_NEWLINE + recoveryRdm.getRegionModel().toBase64StringForZk();
            }
            rsRegionsDataCopy.put(recoveryRS, recoveryRSData);
        }
        return rsRegionsDataCopy;
    }

    /**
     * Corrects the pre-commits:
     *  - restores the REFERENCED status of obsolete references
     *  - sets the REFERENCING status of new references
     *  - removes obsolete RECOVERING replicas
     *  - adds new RECOVERING replicas
     */
    public void correctPreCommits(TableName tableName, List<RegionRecoveryModel> recoveryModels) throws Exception {
        LOGGER.debug("Recovering table '{}': correct pre-commits process started", tableName);
        try {
            List<CuratorOp> plan = new ArrayList<>();
            Map<RegionServerModel, String> rsRegionsData;
            // Model the pre-commit changes for the replicas
            rsRegionsData = correctReferencePreCommits(tableName, recoveryModels);
            rsRegionsData = correctRecoveryPreCommits(tableName, recoveryModels, rsRegionsData, plan);
            // Build transactions from the models
            for (Map.Entry<RegionServerModel, String> rsData : rsRegionsData.entrySet()) {
                String rsPath = ZooKeeperContextHelper.getRegionServerPath(tableName, rsData.getKey());
                String rsApiLocationPath = ZooKeeperContextHelper.getRegionServerApiLocationPath(tableName, rsData.getKey());
                String rsRegionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, rsData.getKey());
                if (StringUtils.isBlank(rsData.getValue()) || rsData.getValue().equals(CSV_NEWLINE)) {
                    // No regions left on the RS: safe to delete its nodes
                    plan.add(zooHelper.deleteTrans(rsRegionsPath));
                    plan.add(zooHelper.deleteTrans(rsApiLocationPath));
                    plan.add(zooHelper.deleteTrans(rsPath));
                } else {
                    // If any are left, write the changes
                    if (zooHelper.checkExists(rsRegionsPath)) {
                        // If such an RS exists, update its information
                        plan.add(zooHelper.setDataTrans(rsRegionsPath, rsData.getValue()));
                    } else {
                        // No such RS: create it
                        String rsApiLocationData = rsData.getKey().getLocationString();
                        plan.add(zooHelper.createTrans(rsPath));
                        plan.add(zooHelper.createTrans(rsApiLocationPath, rsApiLocationData));
                        plan.add(zooHelper.createTrans(rsRegionsPath, rsData.getValue()));
                    }
                }
            }
            if (plan.isEmpty()) {
                LOGGER.debug("Recovering table '{}': correct pre-commits process finished (empty)", tableName);
                return;
            }
            List<CuratorTransactionResult> results = zooHelper.forOperations(plan);
            Preconditions.checkArgument(results.size() == plan.size(), "Unexpected transaction error");
            LOGGER.debug("Recovering table '{}': correct pre-commits process finished", tableName);
        } catch (ZKException ex) {
            if (ex.getCause() instanceof KeeperException) {
                KeeperException kExp = (KeeperException)ex.getCause();
                throw new Exception("Unable to do correction pre-commit procedure, ZK path: " + kExp.getPath() + ", ZK message: " + ex.getMessage() + ", ZK code: " + kExp.code());
            }
            throw new Exception("Unable to do correction pre-commit procedure ZK error", ex.getCause());
        } catch (Exception ex) {
            throw new Exception("Unable to do correction pre-commit procedure", ex);
        }
    }

    private Map<RegionServerModel, String> correctReferencePreCommits(TableName tableName, List<RegionRecoveryModel> recoveryModels) throws Exception {
        Map<RegionServerModel, String> rsRegionsData = new HashMap<>();
        // Replicas already marked as new references
        List<String> newReferencedRdms = new ArrayList<>();
        // Replicas whose status has already been restored
        List<String> obsoleteReferencedRdms = new ArrayList<>();
        for (RegionRecoveryModel recoveryModel : recoveryModels) {
            // If the lost replica suddenly came back, restore the status of its REFERENCE when:
            // - no other replicas refer to it
            // - its status has not been restored yet
            RegionDistributionModel suddenlyAliveReferenceRdm = recoveryModel.getReferenceRdm();
            if (recoveryModel.isSuddenlyAlive()
                    && !obsoleteReferencedRdms.contains(suddenlyAliveReferenceRdm.getRegionModel().toIdentityString())
                    && recoveryModels.stream()
                    .filter(rm -> !rm.equals(recoveryModel))
                    .filter(rm -> !rm.isSuddenlyAlive())
                    .noneMatch(rm -> rm.getReferenceRdm().getRegionModel().toIdentityString().equals(suddenlyAliveReferenceRdm.getRegionModel().toIdentityString()))) {
                RegionServerModel referenceRS = suddenlyAliveReferenceRdm.getRegionServerModel();
                String referenceRSData = rsRegionsData.get(referenceRS);
                if (referenceRSData == null) {
                    referenceRSData = getRSRegionsData(tableName, referenceRS);
                }
                referenceRSData = zooContext.setRegionStatusForData(referenceRSData, tableName, suddenlyAliveReferenceRdm, getReferencedRegionStatus(suddenlyAliveReferenceRdm.getRegionModel()));
                rsRegionsData.put(referenceRS, referenceRSData);
                obsoleteReferencedRdms.add(suddenlyAliveReferenceRdm.getRegionModel().toIdentityString());
                // Nothing to change in the models
                continue;
            }
            if (recoveryModel.getObsoleteReferenceRdm() == null || recoveryModel.isSuddenlyAlive()) {
                continue;
            }
            // Restore the status of the obsolete source when:
            // - no other replicas refer to it
            // - its status has not been restored yet
            RegionDistributionModel obsoleteReferenceRdm = recoveryModel.getObsoleteReferenceRdm();
            if (!obsoleteReferencedRdms.contains(obsoleteReferenceRdm.getRegionModel().toIdentityString())
                    && recoveryModels.stream()
                    .filter(rm -> !rm.equals(recoveryModel))
                    .filter(rm -> !rm.isSuddenlyAlive())
                    .noneMatch(rm -> rm.getReferenceRdm().getRegionModel().toIdentityString().equals(obsoleteReferenceRdm.getRegionModel().toIdentityString()))) {
                RegionServerModel referenceRS = obsoleteReferenceRdm.getRegionServerModel();
                String obsoleteReferenceRSData = rsRegionsData.get(referenceRS);
                if (obsoleteReferenceRSData == null) {
                    obsoleteReferenceRSData = getRSRegionsData(tableName, referenceRS);
                }
                obsoleteReferenceRSData = zooContext.setRegionStatusForData(obsoleteReferenceRSData, tableName, obsoleteReferenceRdm, getReferencedRegionStatus(obsoleteReferenceRdm.getRegionModel()));
                rsRegionsData.put(referenceRS, obsoleteReferenceRSData);
                obsoleteReferencedRdms.add(obsoleteReferenceRdm.getRegionModel().toIdentityString());
                // Nothing to change in the models
            }
            // Set the status of the new source when:
            // - none of the former models refer to it (otherwise the status is already set)
            // - we haven't restored its status yet
            RegionDistributionModel newReferenceRdm = recoveryModel.getReferenceRdm();
            int referenceStatus = getReferencingRegionStatus(newReferenceRdm.getRegionModel());
            if (!newReferencedRdms.contains(newReferenceRdm.getRegionModel().toIdentityString())
                    && recoveryModels.stream()
                    .filter(rm -> !rm.equals(recoveryModel))
                    .filter(rm -> !rm.isSuddenlyAlive())
                    .filter(rm -> rm.getObsoleteReferenceRdm() == null)
                    .noneMatch(rm -> rm.getReferenceRdm().getRegionModel().toIdentityString().equals(newReferenceRdm.getRegionModel().toIdentityString()))) {
                RegionServerModel newReferenceRS = newReferenceRdm.getRegionServerModel();
                String newReferenceRSData = rsRegionsData.get(newReferenceRS);
                if (newReferenceRSData == null) {
                    newReferenceRSData = getRSRegionsData(tableName, newReferenceRS);
                }
                newReferenceRSData = zooContext.setRegionStatusForData(newReferenceRSData, tableName, newReferenceRdm, referenceStatus);
                rsRegionsData.put(newReferenceRS, newReferenceRSData);
                recoveryModel.setReferenceRdm(newReferenceRdm);
                newReferencedRdms.add(newReferenceRdm.getRegionModel().toIdentityString());
            }
            // Reflect the changes in the models, if needed
            if (!isReferencingRegionStatus(newReferenceRdm.getRegionModel())) {
                RegionDistributionModel newReferenceRdmCopy = zooContext.makeRdmCopy(newReferenceRdm, newReferenceRdm.getRegionServerModel(), referenceStatus);
                recoveryModel.setReferenceRdm(newReferenceRdmCopy);
            }
        }
        recoveryModels.forEach(rm -> rm.setObsoleteReferenceRdm(null));
        return rsRegionsData;
    }

    private Map<RegionServerModel, String> correctRecoveryPreCommits(TableName tableName, List<RegionRecoveryModel> recoveryModels, Map<RegionServerModel, String> rsRegionsData, List<CuratorOp> plan) throws Exception {
        Map<RegionServerModel, String> rsRegionsDataCopy = new HashMap<>(rsRegionsData);
        for (RegionRecoveryModel recoveryModel : recoveryModels) {
            // If the lost replica suddenly came back, remove its RECOVERY copy
            if (recoveryModel.isSuddenlyAlive()) {
                RegionDistributionModel suddenlyAliveRecoveryRdm = recoveryModel.getRecoveryRdm();
                RegionServerModel recoveryRS = suddenlyAliveRecoveryRdm.getRegionServerModel();
                String recoveryRSData = rsRegionsDataCopy.get(recoveryRS);
                if (recoveryRSData == null) {
                    recoveryRSData = getRSRegionsData(tableName, recoveryRS);
                }
                recoveryRSData = zooContext.removeRegionForData(recoveryRSData, tableName, suddenlyAliveRecoveryRdm);
                rsRegionsDataCopy.put(recoveryRS, recoveryRSData);
                continue;
            }
            if (recoveryModel.getObsoleteRecoveryRdm() == null) {
                continue;
            }
            // Remove the old recovery replica
            RegionDistributionModel obsoleteRecoveryRdm = recoveryModel.getObsoleteRecoveryRdm();
            RegionServerModel obsoleteRecoveryRS = obsoleteRecoveryRdm.getRegionServerModel();
            String obsoleteRecoveryRSData = rsRegionsDataCopy.get(obsoleteRecoveryRS);
            if (obsoleteRecoveryRSData == null) {
                obsoleteRecoveryRSData = getRSRegionsData(tableName, obsoleteRecoveryRS);
            }
            obsoleteRecoveryRSData = zooContext.removeRegionForData(obsoleteRecoveryRSData, tableName, obsoleteRecoveryRdm);
            rsRegionsDataCopy.put(obsoleteRecoveryRS, obsoleteRecoveryRSData);
            // Add the new recovery replica
            RegionDistributionModel newRecoveryRdm = recoveryModel.getRecoveryRdm();
            RegionServerModel newRecoveryRS = newRecoveryRdm.getRegionServerModel();
            String newRecoveryRSData = rsRegionsDataCopy.get(newRecoveryRS);
            if (newRecoveryRSData == null) {
                newRecoveryRSData = getRSRegionsData(tableName, newRecoveryRS);
            }
            if (StringUtils.isBlank(newRecoveryRSData)) {
                newRecoveryRSData = newRecoveryRdm.getRegionModel().toBase64StringForZk();
            } else {
                newRecoveryRSData = newRecoveryRSData + CSV_NEWLINE + newRecoveryRdm.getRegionModel().toBase64StringForZk();
            }
            rsRegionsDataCopy.put(newRecoveryRS, newRecoveryRSData);
        }
        recoveryModels.forEach(rm -> rm.setObsoleteRecoveryRdm(null));
        return rsRegionsDataCopy;
    }

    public boolean correctRecoveryModels(TableName tableName, List<RegionRecoveryModel> recoveryModels, RecoverTable.SplitContext splitContext, List<RegionDistributionModel> tableRdms, List<RegionServerModel> aliveRS, List<RegionServerModel> runningRS) throws Exception {
        LOGGER.info("Start checking recovery models relevance for table: '{}'", tableName);
        boolean changesMade = false;
        // New models, used to set a single reference for the replicas of one region
        List<RegionRecoveryModel> newRecoveryModels = new ArrayList<>();
        // Avoid logging twice
        List<RegionServerModel> loggedSuddenlyAliveRS = new ArrayList<>();
        Map<String, RegionServerModel> splitRSCache = new HashMap<>();
        for (RegionRecoveryModel recoveryModel : recoveryModels) {
            // reference
            RegionDistributionModel referenceRdm = recoveryModel.getReferenceRdm();
            RegionServerModel referenceRS = referenceRdm.getRegionServerModel();
            // recovery
            RegionDistributionModel recoveryRdm = recoveryModel.getRecoveryRdm();
            RegionServerModel recoveryRS = recoveryRdm.getRegionServerModel();
            // lost
            RegionDistributionModel lostRdm = recoveryModel.getLostRdm();
            RegionServerModel lostRS = lostRdm.getRegionServerModel();
            // Check whether the lost replica is really lost, unless it is in FORCE_RECOVERING or SYNCING mode
            if (recoveryRdm.getRegionModel().getStatusId() != FORCE_RECOVERING_REGION_STATUS
                    && recoveryRdm.getRegionModel().getStatusId() != SYNCING_REGION_STATUS
                    && aliveRS.contains(lostRS)) {
                if (!loggedSuddenlyAliveRS.contains(lostRS)) {
                    LOGGER.info("Recovering table '{}': found suddenly alive RS: {}", tableName, lostRS);
                    loggedSuddenlyAliveRS.add(lostRS);
                    // Keep monitoring this RS
                    recoveryRSContext.addSkippedTables(lostRS);
                }
                recoveryModel.setSuddenlyAlive(true);
                changesMade = true;
                continue;
            }
            // Check whether the references are alive
            if (!aliveRS.contains(referenceRS)) {
                LOGGER.debug("Recovering table '{}': searching new reference for {}", tableName, lostRdm.getRegionModel().toIdentityString());
                RegionDistributionModel newReferenceRdm = RecoverTable.chooseOrdinaryReferenceRdm(zooContext, tableName, lostRdm, newRecoveryModels, tableRdms, aliveRS);
                recoveryModel.setObsoleteReferenceRdm(referenceRdm);
                recoveryModel.setReferenceRdm(newReferenceRdm);
                newRecoveryModels.add(recoveryModel);
                changesMade = true;
            }
            // Check whether the target replicas are alive
            if (!aliveRS.contains(recoveryRS)) {
                LOGGER.debug("Recovering table '{}': searching new recovery for {}", tableName, lostRdm.getRegionModel().toIdentityString());
                RegionServerModel newRecoveryRS;
                // Target RS must be picked in pairs for SPLITTING/SPLITTED copies, so such RS need a cache
                if (splitContext.isSplittingModel(lostRdm)) {
                    newRecoveryRS = splitRSCache.get(lostRdm.getRegionModel().toIdentityString());
                    if (newRecoveryRS == null) {
                        newRecoveryRS = RecoverTable.chooseRecoverRegionServer(zooContext, lostRdm, tableRdms, aliveRS, runningRS);
                        splitRSCache.put(lostRdm.getRegionModel().toIdentityString(), newRecoveryRS);
                    }
                } else if (splitContext.isSplittedModel(lostRdm)) {
                    // splittingName-splittedName_0 -> splittingName_0
                    String splittingCopy = lostRdm.getRegionModel().getName();
                    splittingCopy = splittingCopy.substring(0, splittingCopy.indexOf("-"));
                    splittingCopy += "_" + lostRdm.getRegionModel().getReplicaNum();
                    newRecoveryRS = splitRSCache.get(splittingCopy);
                    if (newRecoveryRS == null) {
                        newRecoveryRS = RecoverTable.chooseRecoverRegionServer(zooContext, lostRdm, tableRdms, aliveRS, runningRS);
                        splitRSCache.put(splittingCopy, newRecoveryRS);
                    }
                } else {
                    newRecoveryRS = RecoverTable.chooseRecoverRegionServer(zooContext, lostRdm, tableRdms, aliveRS, runningRS);
                }
                RegionDistributionModel newRecoveryRdm = zooContext.makeRdmCopy(lostRdm, newRecoveryRS, recoveryRdm.getRegionModel().getStatusId());
                recoveryModel.setObsoleteRecoveryRdm(recoveryRdm);
                recoveryModel.setRecoveryRdm(newRecoveryRdm);
                changesMade = true;
            }
        }
        LOGGER.info("Finish checking recovery models relevance for table: '{}'", tableName);
        return changesMade;
    }

    /**
     *  - Restores the original status of the reference
     *  - sets the target status of the recovered replica
     */
    public void makeRegionFinishCommits(TableName tableName, List<RegionRecoveryModel> recoveryModels, RecoverTable.SplitContext splitContext, ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers) throws Exception {
        LOGGER.info("Recovering table '{}': region finish commits", tableName);
        try {
            List<CuratorOp> plan = new ArrayList<>();
            Map<RegionServerModel, String> rsRegionsData;
            // Model the replica commits
            rsRegionsData = referenceFinishCommits(tableName, recoveryModels);
            rsRegionsData = lostFinishCommits(tableName, recoveryModels, rsRegionsData);
            rsRegionsData = recoveryFinishCommits(tableName, recoveryModels, rsRegionsData);
            // If the split was interrupted at level 0 or 1, move the SPLITTED replicas in ZK (metadata only, no data)
            if (splitContext.getRetryMode() != null && (splitContext.getRetryMode() == SplitTable.RetryMode.ZERO_LEVEL || splitContext.getRetryMode() == SplitTable.RetryMode.FIRST_LEVEL)) {
                rsRegionsData = splittedFinishCommits(tableName, recoveryModels, splitContext, rsRegionsData, plan);
            }
            // Build transactions from the models
            for (Map.Entry<RegionServerModel, String> rsData : rsRegionsData.entrySet()) {
                String rsPath = ZooKeeperContextHelper.getRegionServerPath(tableName, rsData.getKey());
                String rsApiLocationsPath = ZooKeeperContextHelper.getRegionServerApiLocationPath(tableName, rsData.getKey());
                String rsRegionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, rsData.getKey());
                if (StringUtils.isBlank(rsData.getValue()) || rsData.getValue().equals(CSV_NEWLINE)) {
                    // No regions left on the RS: safe to delete its nodes
                    plan.add(zooHelper.deleteTrans(rsRegionsPath));
                    plan.add(zooHelper.deleteTrans(rsApiLocationsPath));
                    plan.add(zooHelper.deleteTrans(rsPath));
                } else {
                    // If any are left, write the changes
                    plan.add(zooHelper.setDataTrans(rsRegionsPath, rsData.getValue()));
                }
            }
            // Monitor RS
            RsHealthChecker.checkRsErrors(pingCheckers);
            if (plan.isEmpty()) {
                LOGGER.debug("Recovering table '{}': region finish commits finished (empty)", tableName);
                return;
            }
            // Commit the regions
            List<CuratorTransactionResult> results = zooHelper.forOperations(plan);
            Preconditions.checkArgument(results.size() == plan.size(), "Unexpected transaction error");
            // Start monitoring all target RS
            recoveryModels.stream()
                    .map(rm -> rm.getRecoveryRdm().getRegionServerModel())
                    .forEach(recoveryRSContext::addPayloaded);
            LOGGER.debug("Recovering table '{}': region finish commits finished", tableName);
        } catch (ZKException ex) {
            if (ex.getCause() instanceof KeeperException) {
                KeeperException kExp = (KeeperException)ex.getCause();
                throw new Exception("Unable to finish commit region recovery, ZK path: " + kExp.getPath() + ", ZK message: " + kExp.getMessage() + ", ZK code: " + kExp.code());
            }
            throw new Exception("Unable to finish commit region recovery ZK error", ex.getCause());
        } catch (Exception ex) {
            throw new Exception("Unable to finish commit region recovery", ex);
        }
    }

    /**
     *  - Restores the original status of the table
     */
    public void makeTableFinishCommit(TableName tableName) throws Exception {
        LOGGER.info("Recovering table '{}': table finish commit", tableName);
        try {
            List<CuratorOp> plan = new ArrayList<>();
            // Table status
            String tableStatus = zooHelper.getNotNullString(ZooKeeperContextHelper.getTablePath(tableName));
            plan.add(zooHelper.setDataTrans(ZooKeeperContextHelper.getTablePath(tableName), getRecoveredStatus(tableStatus)));
            // Commit the table
            List<CuratorTransactionResult> results = zooHelper.forOperations(plan);
            Preconditions.checkArgument(results.size() == plan.size(), "Unexpected transaction error");
            LOGGER.debug("Recovering table '{}': table finish commit finished", tableName);
        } catch (ZKException ex) {
            if (ex.getCause() instanceof KeeperException) {
                KeeperException kExp = (KeeperException)ex.getCause();
                throw new Exception("Unable to finish commit region recovery, ZK path: " + kExp.getPath() + ", ZK message: " + kExp.getMessage() + ", ZK code: " + kExp.code());
            }
            throw new Exception("Unable to finish commit region recovery ZK error", ex.getCause());
        } catch (Exception ex) {
            throw new Exception("Unable to finish commit region recovery", ex);
        }
    }

    private Map<RegionServerModel, String> referenceFinishCommits(TableName tableName, List<RegionRecoveryModel> recoveryModels) throws Exception {
        Map<RegionServerModel, String> rsRegionsData = new HashMap<>();
        // Replicas already restored as references
        List<RegionDistributionModel> referencedRdms = new ArrayList<>();
        for (RegionRecoveryModel recoveryModel : recoveryModels) {
            // Reference status
            RegionDistributionModel referenceRdm = recoveryModel.getReferenceRdm();
            if (!referencedRdms.contains(referenceRdm)) {
                RegionServerModel referenceRS = referenceRdm.getRegionServerModel();
                String referenceRSData = rsRegionsData.get(referenceRS);
                if (referenceRSData == null) {
                    referenceRSData = getRSRegionsData(tableName, referenceRS);
                }
                referenceRSData = zooContext.setRegionStatusForData(referenceRSData, tableName, referenceRdm, getReferencedRegionStatus(referenceRdm.getRegionModel()));
                rsRegionsData.put(referenceRS, referenceRSData);
                referencedRdms.add(referenceRdm);
            }
        }
        return rsRegionsData;
    }

    private Map<RegionServerModel, String> lostFinishCommits(TableName tableName, List<RegionRecoveryModel> recoveryModels, Map<RegionServerModel, String> rsRegionsData) throws Exception {
        Map<RegionServerModel, String> rsRegionsDataCopy = new HashMap<>(rsRegionsData);
        // There are no lost replicas in SYNC mode
        if (isSyncMode(recoveryModels)) {
            return rsRegionsDataCopy;
        }
        for (RegionRecoveryModel recoveryModel : recoveryModels) {
            // Remove the lost replica
            RegionDistributionModel lostRdm = recoveryModel.getLostRdm();
            RegionServerModel lostRS = lostRdm.getRegionServerModel();
            String lostRSData = rsRegionsDataCopy.get(lostRS);
            if (lostRSData == null) {
                lostRSData = getRSRegionsData(tableName, lostRS);
            }
            lostRSData = zooContext.removeRegionForData(lostRSData, tableName, lostRdm);
            rsRegionsDataCopy.put(lostRS, lostRSData);
        }
        return rsRegionsDataCopy;
    }

    private Map<RegionServerModel, String> recoveryFinishCommits(TableName tableName, List<RegionRecoveryModel> recoveryModels, Map<RegionServerModel, String> rsRegionsData) throws Exception {
        Map<RegionServerModel, String> rsRegionsDataCopy = new HashMap<>(rsRegionsData);
        for (RegionRecoveryModel recoveryModel : recoveryModels) {
            // Status of the recovered replica
            RegionDistributionModel recoveryRdm = recoveryModel.getRecoveryRdm();
            RegionServerModel recoveryRS = recoveryRdm.getRegionServerModel();
            String recoveryRSData = rsRegionsDataCopy.get(recoveryRS);
            if (recoveryRSData == null) {
                recoveryRSData = getRSRegionsData(tableName, recoveryRS);
            }
            int targetStatus;
            if (isSyncMode(recoveryModels)) {
                targetStatus = DISABLED_REGION_STATUS;
            } else {
                targetStatus = recoveryModel.getLostRdm().getRegionModel().getStatusId();
            }
            recoveryRSData = zooContext.setRegionStatusForData(recoveryRSData, tableName, recoveryRdm, targetStatus);
            rsRegionsDataCopy.put(recoveryRS, recoveryRSData);
        }
        return rsRegionsDataCopy;
    }

    // Moves splitted models to the matching RS
    private Map<RegionServerModel, String> splittedFinishCommits(TableName tableName, List<RegionRecoveryModel> recoveryModels, RecoverTable.SplitContext splitContext, Map<RegionServerModel, String> rsRegionsData, List<CuratorOp> plan) throws Exception {
        Map<RegionServerModel, String> rsRegionsDataCopy = new HashMap<>(rsRegionsData);
        for (RegionRecoveryModel recoveryModel : recoveryModels) {
            // Continue only if the pair of the splitted copy was moved
            if (!splitContext.isSplittingModel(recoveryModel.getLostRdm())) {
                continue;
            }
            // Goal: move the splitted copy to the same RS as the splitting one
            // Remove the splitted copy from the old RS
            RegionDistributionModel lostRdm = recoveryModel.getLostRdm();
            RegionDistributionModel splittedRdm = splitContext.getSplitModels().stream()
                    .filter(sm -> sm.getSplitting().equals(lostRdm))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unable to find lost splitting replica"))
                    .getSplitted();
            RegionServerModel lostRS = lostRdm.getRegionServerModel();
            String lostRSData = rsRegionsDataCopy.get(lostRS);
            if (lostRSData == null) {
                lostRSData = getRSRegionsData(tableName, lostRS);
            }
            lostRSData = zooContext.removeRegionForData(lostRSData, tableName, splittedRdm);
            rsRegionsDataCopy.put(lostRS, lostRSData);
            // Add the splitted copy to the new RS
            RegionDistributionModel recoveryRdm = recoveryModel.getRecoveryRdm();
            RegionServerModel recoveryRS = recoveryRdm.getRegionServerModel();
            RegionDistributionModel newSplittedRdm = zooContext.makeRdmCopy(splittedRdm, recoveryRS, splittedRdm.getRegionModel().getStatusId());
            String recoveryRSData = rsRegionsDataCopy.get(recoveryRS);
            if (recoveryRSData == null) {
                recoveryRSData = getRSRegionsData(tableName, recoveryRS);
            }
            if (StringUtils.isBlank(recoveryRSData)) {
                recoveryRSData = newSplittedRdm.getRegionModel().toBase64StringForZk();
            } else {
                recoveryRSData = recoveryRSData + CSV_NEWLINE + newSplittedRdm.getRegionModel().toBase64StringForZk();
            }
            rsRegionsDataCopy.put(recoveryRS, recoveryRSData);
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

    // If any lost replica is missing, we are in SYNC mode
    // SYNC: the mode in which reserve replicas are synchronized with the leader
    public boolean isSyncMode(List<RegionRecoveryModel> recoveryModels) {
        return recoveryModels.stream().anyMatch(rm -> rm.getLostRdm() == null);
    }
}
