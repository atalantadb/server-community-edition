/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations;

import com.google.common.base.Preconditions;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.NoRetryException;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.master.services.grpc.RegionServerConnector;
import com.light.atalanta.master.services.grpc.RsHealthChecker;
import com.light.atalanta.master.services.recovery.RecoveryRSContext;
import com.light.atalanta.master.services.split.RegionSplitModel;
import com.light.atalanta.master.services.split.RegionSplitNameGenerator;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.master.vo.DataConfiguration;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.master.vo.GrpcConfiguration;
import com.light.atalanta.metrics.RegionMetrics;
import com.light.atalanta.metrics.RegionServerMetrics;
import com.light.atalanta.metrics.RsJmxClient;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionModel;
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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.CSV_NEWLINE;
import static com.light.atalanta.ATALANTAConstants.STATUS_PREFIX;
import static com.light.atalanta.master.services.split.RegionSplitModel.SPLITTED_DELIMITER;
import static com.light.atalanta.services.ZooKeeperContextHelper.*;
import static com.light.atalanta.util.ZookeeperStatus.*;

/**
 * Region split.
 * regionIdentity is set when a specific region must be split.
 * To split all large regions of the table, pass an empty value or null.
 * <p>
 * regionNamesToSplit == null when called from PreCommitRunner
 * regionNamesToSplit == Collections.emptyList() when findSplitModels() is called from OperationRunner
 * regionNamesToSplit == a non-empty list in all other cases
 */
@SuppressFBWarnings("SF_SWITCH_FALLTHROUGH")
public class SplitTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitTable.class);
    private static final long MISSING_DISK_METRICS_RETRY_DELAY_MS = 70_000L;

    private final DataConfiguration dataConf;

    private final GrpcConfiguration grpcConf;

    private final ZooKeeperMasterContext zooContext;

    private final ZookeeperHelper zooHelper;

    private final RegionServerConnector regionServerConnector;

    private final RecoveryRSContext recoveryRSContext;
    private final RsJmxClient rsJmxClient;

    private final AtalantaDBConfiguration conf;
    private final ExecutorService executorService;

    public SplitTable(AtalantaDBConfiguration conf, ZooKeeperMasterContext zooContext, ZookeeperHelper zooHelper, RecoveryRSContext recoveryRSContext, RegionServerConnector regionServerConnector, RsJmxClient rsJmxClient, ExecutorService executorService) {
        this.zooContext = zooContext;
        this.zooHelper = zooHelper;
        this.dataConf = conf.getDataConfiguration();
        this.grpcConf = conf.getGrpcConfiguration();
        this.regionServerConnector = regionServerConnector;
        this.recoveryRSContext = recoveryRSContext;
        this.rsJmxClient = rsJmxClient;
        this.conf = conf;
        this.executorService = executorService;
    }

    public void action(TableName tableName, List<String> regionNamesToSplit) throws OperationException {
        int retryCount = 1;
        while (true) {
            ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers = new ConcurrentHashMap<>();
            try {
                retryableAction(tableName, new HashSet<>(regionNamesToSplit), pingCheckers);
                return;
            } catch (NoSplittingNeeded ex) {
                LOGGER.info("Splitting table '{}': no recovery needed because of status", tableName);
                return;
            } catch (NoRetryException | InterruptedException ex) {
                throw new OperationException(ex);
            } catch (Throwable ex) {
                if (containsInterrupted(ex)) {
                    throw new OperationException(ex);
                }
                LOGGER.error("Error splitting table: '{}', wait {} ms and retrying #{}...", tableName, dataConf.getSplitTableRetryDelayMs(), retryCount, ex);
                try {
                    Thread.sleep(dataConf.getSplitTableRetryDelayMs());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new OperationException(ex);
                }
                retryCount++;
            } finally {
                pingCheckers.values().forEach(RsHealthChecker::close);
            }
        }
    }

    public void retryableAction(TableName tableName, Set<String> regionNamesToSplit, ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers) throws Throwable {
        LOGGER.info("Start splitting table: '{}'", tableName);
        RegionSplitNameGenerator nameGenerator = new RegionSplitNameGenerator();
        // Check whether operations for table are allowed
        recoveryRSContext.checkIncomeTableRequestsEnabled(tableName);

        List<RegionDistributionModel> tableRdms = zooContext.getRegionDistributionWithStatuses(tableName);
        RetryMode retryMode = chooseRetryMode(tableName, tableRdms);
        LOGGER.info("Splitting table '{}': retry mode is {}", tableName, retryMode);

        List<RegionServerModel> aliveRS = zooContext.getAliveRegionServers();
        LOGGER.debug("Splitting table '{}': alive RS is {}", tableName, aliveRS);

        Set<RegionServerModel> allRS = tableRdms.stream()
                .map(RegionDistributionModel::getRegionServerModel)
                .collect(Collectors.toSet());
        aliveRS.forEach(allRS::remove);
        Preconditions.checkArgument(allRS.isEmpty(), "following RS is unavailable: " + allRS);

        List<RegionSplitModel> splitModels = findSplitModels(tableName, tableRdms, retryMode, regionNamesToSplit, nameGenerator, "Splitting table");
        LOGGER.info("Splitting table '{}': split models are {}", tableName, splitModels);

        if (regionNamesToSplit != null && !regionNamesToSplit.isEmpty() && retryMode != RetryMode.ZERO_LEVEL) {
            Set<String> zkSplitModels = splitModels.stream()
                    .map(RegionSplitModel::getSplitting)
                    .map(rdm -> rdm.getRegionModel().getName())
                    .collect(Collectors.toSet());
            if (!zkSplitModels.equals(regionNamesToSplit)) {
                LOGGER.info("Splitting table '{}': user split models are {}", tableName, retryMode);
                throw new NoRetryException("there is already started split process on table, unable to run another one");
            }
        }

        // We need to initialize ping checkers for all retry levels
        initializePingCheckers(pingCheckers, splitModels);

        switch (retryMode) {
            case ZERO_LEVEL:
                // Pre-commits
                makePreCommits(tableName, splitModels);

            case FIRST_LEVEL:
                // Stop the leaders
                stopLeadersIfNeeded(tableName, splitModels);
                // Split regions in parallel and wait, step 1
                splitReplicas1(tableName, splitModels, pingCheckers);
                // Intermediate commits (DISABLE statuses for splitted copies)
                makeCommits1(tableName, splitModels);
                // Monitor RS
                RsHealthChecker.checkRsErrors(pingCheckers);

            case SECOND_LEVEL:
                // Split regions in parallel and wait, step 2
                splitReplicas2(tableName, splitModels);
                // Intermediate commits (new bounds for splitting copies and ENABLING status for all)
                makeCommits2(tableName, splitModels);
                // Monitor RS
                RsHealthChecker.checkRsErrors(pingCheckers);

            case THIRD_LEVEL:
                // Assign regions in parallel and wait
                assignReplicas(tableName, splitModels);
                // Final commits (table status, names of splitted copies and ENABLED status for all)
                makeFinishCommits(tableName, splitModels, pingCheckers);
                break;

            default:
                throw new Exception("unknown retry mode: " + retryMode);
        }
    }

    // Run RS monitoring threads
    private void initializePingCheckers(ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers,
                                        List<RegionSplitModel> splitModels
    ) {
        for (RegionSplitModel rsm : splitModels) {
            RegionServerModel splitting = rsm.getSplitting().getRegionServerModel();
            if (!pingCheckers.containsKey(splitting)) {
                pingCheckers.put(splitting, new RsHealthChecker(splitting, grpcConf));
            }
            // splitted should be at the same RS, but leaving this here just in case
            RegionServerModel splitted = rsm.getSplitting().getRegionServerModel();
            if (!pingCheckers.containsKey(splitted)) {
                pingCheckers.put(splitted, new RsHealthChecker(splitted, grpcConf));
            }
        }
    }

    private void splitReplicas1(TableName tableName, List<RegionSplitModel> splitModels, ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers) throws Exception {
        int splitTasksAmount = splitModels.size();
        CountDownLatch latch = new CountDownLatch(splitTasksAmount);

        AtomicBoolean error = new AtomicBoolean();
        for (RegionSplitModel splitModel : splitModels) {
            executorService.submit(() -> {
                try {
                    // Send the splitting region with its new bounds already set,
                    // keeping in mind that ZooKeeper still has the original ones
                    String split1LogStr = String.format("Splitting1 table '%s': splitting region '%s'", tableName, splitModel.getSplitting().getRegionModel().toIdentityString());
                    regionServerConnector.monitor(tableName, split1LogStr, splitModel.makeSplittingRdm(), splitModel.getSplitted().getRegionModel().toBase64String(), (stub, req, stream) ->
                            stub.get().splitRegion1(req, stream)
                    );
                } catch (Throwable ex) {
                    LOGGER.error("Splitting1 table '{}': unable to perform split1 on RS for {}", tableName, splitModel.getSplitting().getRegionModel().toIdentityString(), ex);
                    error.set(true);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        if (error.get()) {
            throw new Exception("found errors while splitting1 regions on RS. Please, check the logs");
        }

    }

    private void splitReplicas2(TableName tableName, List<RegionSplitModel> splitModels) throws Exception {
        int splitTasksAmount = splitModels.size();
        CountDownLatch latch = new CountDownLatch(splitTasksAmount);

        AtomicBoolean error = new AtomicBoolean();
        for (RegionSplitModel splitModel : splitModels) {
            executorService.submit(() -> {
                try {
                    String split2LogStr = String.format("Splitting2 table '%s': splitting region '%s'", tableName, splitModel.getSplitting().getRegionModel().toIdentityString());
                    regionServerConnector.monitor(tableName, split2LogStr, splitModel.getSplitting(), StringUtils.EMPTY, (stub, req, stream) ->
                            stub.get().splitRegion2(req, stream)
                    );
                } catch (Throwable ex) {
                    LOGGER.error("Splitting2 table '{}': unable to perform split2 on RS for {}", tableName, splitModel.getSplitting().getRegionModel().toIdentityString(), ex);
                    error.set(true);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        if (error.get()) {
            throw new Exception("found errors while splitting2 regions on RS. Please, check the logs");
        }

    }

    private void assignReplicas(TableName tableName, List<RegionSplitModel> splitModels) throws Exception {
        List<RegionDistributionModel> projection = new ArrayList<>();
        splitModels.forEach(sm -> {
            projection.add(sm.getSplitting());
            // Temporarily replace the tmp name of the splitted copy with the target one
            RegionModel modified = new RegionModel(sm.getTargetSplittedName(),
                    sm.getSplitted().getRegionModel().getReplicaNum(),
                    sm.getSplitted().getRegionModel().getStatusId(),
                    sm.getSplitted().getRegionModel().getBeginKey(),
                    sm.getSplitted().getRegionModel().getEndKey(),
                    sm.getSplitted().getRegionModel().getDataBlockSize(),
                    sm.getSplitted().getRegionModel().getCompressType(),
                    sm.getSplitted().getRegionModel().getRecordCompressionType(),
                    sm.getSplitted().getRegionModel().getDictionary(),
                    sm.getSplitted().getRegionModel().getRecordTargetCount());
            projection.add(new RegionDistributionModel(modified, sm.getSplitted().getRegionServerModel()));
        });
        int splitTasksAmount = projection.size();
        CountDownLatch latch = new CountDownLatch(splitTasksAmount);

        AtomicInteger totalSuccessCount = new AtomicInteger(0);
        for (RegionDistributionModel rdm : projection) {
            executorService.submit(() -> {
                // Try to assign each region 10 times
                // Each retry will be +1 second timeout
                // If we don't see the expected number of retries at the end – error out
                // In order words totalSuccessCount should be equal splitTasksAmount
                //
                // If any of the RS will be restarted during our attempts, RsHealth checker should recognize
                // this case right before finish commits happen and restart split operation
                int retryTimeoutMs = 1000;
                int retryCount = 10;
                try {
                    for (int i = 1; i <= retryCount; i++) {
                        try {
                            String reservedReplicasIfNeeded = rdm.getRegionModel().getReplicaNum() == 0 ? findReservedReplicas(zooContext, tableName, rdm.getRegionModel().getName(), projection, dataConf.getReplicationFactor()) : StringUtils.EMPTY;
                            String additionalInfo = reservedReplicasIfNeeded + "@";
                            String distributeLogStr = String.format("Splitting table '%s': distributing region '%s'", tableName, rdm.getRegionModel().toIdentityString());
                            regionServerConnector.monitor(tableName, distributeLogStr, rdm, additionalInfo, (stub, req, stream) ->
                                    stub.get().distributeRegion(req, stream)
                            );
                            totalSuccessCount.incrementAndGet();
                            break;
                        } catch (Throwable ex) {
                            if (i == retryCount) {
                                LOGGER.error("Splitting table '{}': unable to perform assign on RS for {}. Error out",
                                        tableName, rdm.getRegionModel().toIdentityString(), ex);
                            } else {
                                LOGGER.error("Splitting table '{}': unable to perform assign on RS for {}. Retry {} of {} after: {}",
                                        tableName, rdm.getRegionModel().toIdentityString(), i, retryCount, retryTimeoutMs, ex);
                                Thread.sleep(retryTimeoutMs);
                                retryTimeoutMs += 1000;
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException("Splitting table was interrupted");
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        if (totalSuccessCount.get() != splitTasksAmount) {
            throw new Exception("found errors while assigning regions on RS. Please, check the logs");
        }

    }

    public static String findReservedReplicas(ZooKeeperMasterContext zooContext, TableName tableName, String regionName, List<RegionDistributionModel> projection, int replicationFactor) throws Exception {
        String reservedReplicas;
        try {
            reservedReplicas = zooContext.getReservedAddresses(projection, regionName, replicationFactor);
            LOGGER.info("Splitting table '{}': reserved replicas projection for '{}' is: {}", tableName, regionName, reservedReplicas);
        } catch (Exception ex) {
            throw new Exception("unable to prepare reserved replicas for: " + regionName, ex);
        }
        return reservedReplicas;
    }

    private void stopLeadersIfNeeded(TableName tableName, List<RegionSplitModel> splitModels) throws Exception {
        List<RegionDistributionModel> projection = splitModels.stream()
                .map(RegionSplitModel::getSplitting)
                .collect(Collectors.toList());
        Map<String, RegionDistributionModel> leaders = findLeadersToStop(tableName, splitModels, projection);
        CountDownLatch latch = new CountDownLatch(leaders.size());
        AtomicBoolean error = new AtomicBoolean();

        leaders.forEach((regionName, leader) -> executorService.submit(() -> {
            try {
                String stopLogStr = String.format("Splitting table '%s': stopping leader: '%s'", tableName, leader.getRegionModel().toIdentityString());
                regionServerConnector.monitor(tableName, stopLogStr, leader, StringUtils.EMPTY, (stub, req, stream) ->
                        stub.get().stopRegion(req, stream)
                );
            } catch (Throwable ex) {
                LOGGER.error("Splitting table '{}': unable to stop leader for {}", tableName, regionName, ex);
                error.set(true);
            } finally {
                latch.countDown();
            }
        }));
        latch.await();
        if (error.get()) {
            throw new Exception("found errors while stopping leaders. Please, check the logs");
        }

    }

    private Map<String, RegionDistributionModel> findLeadersToStop(TableName tableName, List<RegionSplitModel> splitModels, List<RegionDistributionModel> projection) {
        Map<String, RegionDistributionModel> leaders = new HashMap<>();
        for (RegionSplitModel sm : splitModels) {
            String regionName = sm.getSplitting().getRegionModel().toIdentityString();
            RegionDistributionModel leader = findLeaderToStop(sm, projection);
            if (leader != null && !leaders.containsKey(regionName)) {
                LOGGER.info("Splitting table '{}': found leader to stop for '{}': it is: {}", tableName, regionName, leader.toShortString());
                leaders.put(sm.getSplitting().getRegionModel().getName(), leader);
            }
            if (leader == null) {
                LOGGER.info("Splitting table '{}': leader to stop for '{}' not found", tableName, regionName);
            }
        }
        return leaders;
    }

    private RegionDistributionModel findLeaderToStop(RegionSplitModel splitModel, List<RegionDistributionModel> projection) {
        List<RegionDistributionModel> leaderCandidates = projection.stream()
                .filter(rdm -> rdm.getRegionModel().getName().equals(splitModel.getSplitting().getRegionModel().getName()))
                .filter(rdm -> rdm.getRegionModel().getReplicaNum() == 0)
                .collect(Collectors.toList());
        // Protect against duplicates
        Preconditions.checkArgument(leaderCandidates.size() <= 1, "found multiply leaders");
        if (leaderCandidates.isEmpty()) {
            return null;
        }
        return leaderCandidates.get(0);
    }

    private List<RegionSplitModel> makeSplitModels(TableName tableName, List<RegionDistributionModel> tableRdms, Set<String> regionNamesToSplit, RegionSplitNameGenerator nameGenerator) throws NoRetryException {
        List<RegionSplitModel> splitModels = new ArrayList<>();
        if (regionNamesToSplit == null || regionNamesToSplit.isEmpty()) {
            throw new NoRetryException("region list for split must not be empty");
        }
        tableRdms.stream()
                .filter(rdm -> regionNamesToSplit.contains(rdm.getRegionModel().getName()))
                .forEach(rdm -> splitModels.add(new RegionSplitModel(zooContext, rdm, nameGenerator)));
        // Check that all replicas to split were found
        if (splitModels.size() != regionNamesToSplit.size() * dataConf.getReplicationFactor()) {
            LOGGER.warn("Splitting table '{}': ZooKeeper distribution is: {}", tableName, tableRdms);
            LOGGER.warn("Splitting table '{}': region names to split is: {}", tableName, regionNamesToSplit);
            throw new NoRetryException("unable to find all requested region replicas for split");
        }
        return splitModels;
    }

    /**
     * Builds the list of models from scratch on the first split run,
     * or from ZooKeeper on a retry.
     */
    public List<RegionSplitModel> findSplitModels(TableName tableName, List<RegionDistributionModel> tableRdms, RetryMode retryMode, Set<String> regionNamesToSplit, RegionSplitNameGenerator nameGenerator, String operationName) throws Exception {
        List<RegionSplitModel> splitModels = new ArrayList<>();
        switch (retryMode) {
            case ZERO_LEVEL:
                // Plan the region split from scratch
                splitModels.addAll(makeSplitModels(tableName, tableRdms, regionNamesToSplit, nameGenerator));
                break;
            case FIRST_LEVEL:
                for (RegionDistributionModel tableRdm : tableRdms) {
                    // SPLITTING implies that SPLITTED exists
                    if (tableRdm.getRegionModel().getStatusId() == SPLITTING_REGION_STATUS) {
                        LOGGER.debug(operationName + " '{}': analyzing splitting replica {}", tableName, tableRdm.toShortString());
                        RegionDistributionModel splittedRdm;
                        try {
                            splittedRdm = zooContext.getTableSplittedReplica(tableRdms, tableRdm.getRegionModel().getName(), tableRdm.getRegionModel().getReplicaNum(), SPLITTED_REGION_STATUS);
                        } catch (Exception ex) {
                            throw new Exception("unable to find splitted replica for: " + tableRdm.toShortString(), ex);
                        }
                        splitModels.add(new RegionSplitModel(zooContext, tableRdm, splittedRdm));
                    }
                }
                break;
            case SECOND_LEVEL:
                for (RegionDistributionModel tableRdm : tableRdms) {
                    // SPLITTING implies that DISABLED exists
                    if (tableRdm.getRegionModel().getStatusId() == SPLITTING_REGION_STATUS) {
                        LOGGER.debug(operationName + " '{}': analyzing splitting replica {}", tableName, tableRdm.toShortString());
                        RegionDistributionModel splittedRdm;
                        try {
                            splittedRdm = zooContext.getTableSplittedReplica(tableRdms, tableRdm.getRegionModel().getName(), tableRdm.getRegionModel().getReplicaNum(), DISABLED_REGION_STATUS);
                        } catch (Exception ex) {
                            throw new Exception("unable to find splitted replica for: " + tableRdm.toShortString(), ex);
                        }
                        splitModels.add(new RegionSplitModel(zooContext, tableRdm, splittedRdm));
                    }
                }
                break;
            case THIRD_LEVEL:
                for (RegionDistributionModel tableRdm : tableRdms) {
                    // ENABLING implies that ENABLING exists
                    if (tableRdm.getRegionModel().getStatusId() == ENABLING_REGION_STATUS && !tableRdm.getRegionModel().getName().contains(SPLITTED_DELIMITER)) {
                        LOGGER.debug(operationName + " '{}': analyzing splitting replica {}", tableName, tableRdm.toShortString());
                        RegionDistributionModel splittedRdm;
                        try {
                            splittedRdm = zooContext.getTableSplittedReplica(tableRdms, tableRdm.getRegionModel().getName(), tableRdm.getRegionModel().getReplicaNum(), ENABLING_REGION_STATUS);
                        } catch (Exception ex) {
                            throw new Exception("unable to find splitted replica for: " + tableRdm.toShortString(), ex);
                        }
                        splitModels.add(new RegionSplitModel(zooContext, tableRdm, splittedRdm));
                    }
                }
                break;
        }
        return splitModels;
    }

    private void makePreCommits(TableName tableName, List<RegionSplitModel> splitModels) throws Exception {
        LOGGER.debug("Splitting table '{}': pre-commits started", tableName);
        try {
            List<CuratorOp> plan = new ArrayList<>();
            Map<RegionServerModel, String> rsRegionsData = new HashMap<>();
            // Table status
            plan.add(zooHelper.setDataTrans(ZooKeeperContextHelper.getTablePath(tableName), STATUS_PREFIX + SPLITTING.getValue()));
            // Status of each splitting region model, and adding the splitted ones
            for (RegionSplitModel splitModel : splitModels) {
                String rsData = rsRegionsData.get(splitModel.getRs());
                if (rsData == null) {
                    rsData = getRSRegionsData(tableName, splitModel.getRs());
                }
                rsData = zooContext.setRegionStatusForData(rsData, tableName, splitModel.getSplitting(), SPLITTING_REGION_STATUS);
                rsData = rsData + CSV_NEWLINE + splitModel.getSplitted().getRegionModel().toBase64StringForZk();
                rsRegionsData.put(splitModel.getRs(), rsData);
            }
            // Build transactions from the models
            for (Map.Entry<RegionServerModel, String> rsData : rsRegionsData.entrySet()) {
                String rsRegionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, rsData.getKey());
                plan.add(zooHelper.setDataTrans(rsRegionsPath, rsData.getValue()));
            }
            List<CuratorTransactionResult> results = zooHelper.forOperations(plan);
            Preconditions.checkArgument(results.size() == plan.size(), "unexpected transaction error");
            // Apply the changes to the models
            splitModels.forEach(sm -> sm.setSplittingStatus(SPLITTING_REGION_STATUS));
            LOGGER.debug("Splitting table '{}': pre-commits finished", tableName);
        } catch (ZKException ex) {
            if (ex.getCause() instanceof KeeperException) {
                KeeperException kExp = (KeeperException) ex.getCause();
                throw new Exception("unable to pre-commit table splitting, ZK path: " + kExp.getPath() + ", ZK message: " + kExp.getMessage() + ", ZK code: " + kExp.code());
            }
            throw new Exception("unable to pre-commit table splitting ZK error", ex.getCause());
        } catch (Exception ex) {
            throw new Exception("unable to pre-commit table splitting", ex);
        }
    }

    private void makeCommits1(TableName tableName, List<RegionSplitModel> splitModels) throws Exception {
        LOGGER.debug("Splitting table '{}': commits1 started", tableName);
        try {
            List<CuratorOp> plan = new ArrayList<>();
            Map<RegionServerModel, String> rsRegionsData = new HashMap<>();
            // DISABLE status for each splitted copy of a region
            for (RegionSplitModel splitModel : splitModels) {
                String rsData = rsRegionsData.get(splitModel.getRs());
                if (rsData == null) {
                    rsData = getRSRegionsData(tableName, splitModel.getRs());
                }
                rsData = zooContext.setRegionStatusForData(rsData, tableName, splitModel.getSplitted(), DISABLED_REGION_STATUS);
                rsRegionsData.put(splitModel.getRs(), rsData);
            }
            // Build transactions from the models
            for (Map.Entry<RegionServerModel, String> rsData : rsRegionsData.entrySet()) {
                String rsRegionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, rsData.getKey());
                plan.add(zooHelper.setDataTrans(rsRegionsPath, rsData.getValue()));
            }
            List<CuratorTransactionResult> results = zooHelper.forOperations(plan);
            Preconditions.checkArgument(results.size() == plan.size(), "unexpected transaction error");
            // Apply the changes to the models
            splitModels.forEach(sm -> sm.setSplittedStatus(DISABLED_REGION_STATUS));
            LOGGER.debug("Splitting table '{}': commits1 finished", tableName);
        } catch (ZKException ex) {
            if (ex.getCause() instanceof KeeperException) {
                KeeperException kExp = (KeeperException) ex.getCause();
                throw new Exception("unable to commit1 table splitting, ZK path: " + kExp.getPath() + ", ZK message: " + kExp.getMessage() + ", ZK code: " + kExp.code());
            }
            throw new Exception("unable to commit1 table splitting ZK error", ex.getCause());
        } catch (Exception ex) {
            throw new Exception("unable to commit1 table splitting", ex);
        }
    }

    private void makeCommits2(TableName tableName, List<RegionSplitModel> splitModels) throws Exception {
        LOGGER.debug("Splitting table '{}': commits2 started", tableName);
        try {
            List<CuratorOp> plan = new ArrayList<>();
            Map<RegionServerModel, String> rsRegionsData = new HashMap<>();
            // New bounds for each splitting copy of a region and ENABLING status for all
            for (RegionSplitModel splitModel : splitModels) {
                String rsData = rsRegionsData.get(splitModel.getRs());
                if (rsData == null) {
                    rsData = getRSRegionsData(tableName, splitModel.getRs());
                }
                byte[] beginKey = splitModel.makeSplittingRdm().getRegionModel().getBeginKey();
                byte[] endKey = splitModel.makeSplittingRdm().getRegionModel().getEndKey();
                rsData = zooContext.setRegionBordersForData(rsData, tableName, splitModel.getSplitting(), ENABLING_REGION_STATUS, beginKey, endKey);
                rsData = zooContext.setRegionStatusForData(rsData, tableName, splitModel.getSplitted(), ENABLING_REGION_STATUS);
                rsRegionsData.put(splitModel.getRs(), rsData);
            }
            // Build transactions from the models
            for (Map.Entry<RegionServerModel, String> rsData : rsRegionsData.entrySet()) {
                String rsRegionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, rsData.getKey());
                plan.add(zooHelper.setDataTrans(rsRegionsPath, rsData.getValue()));
            }
            List<CuratorTransactionResult> results = zooHelper.forOperations(plan);
            Preconditions.checkArgument(results.size() == plan.size(), "unexpected transaction error");
            // Apply the changes to the models
            for (RegionSplitModel splitModel : splitModels) {
                byte[] beginKey = splitModel.makeSplittingRdm().getRegionModel().getBeginKey();
                byte[] endKey = splitModel.makeSplittingRdm().getRegionModel().getEndKey();
                splitModel.setSplittingBorders(beginKey, endKey);
                splitModel.setSplittingStatus(ENABLING_REGION_STATUS);
                splitModel.setSplittedStatus(ENABLING_REGION_STATUS);
            }
            LOGGER.debug("Splitting table '{}': commits2 finished", tableName);
        } catch (ZKException ex) {
            if (ex.getCause() instanceof KeeperException) {
                KeeperException kExp = (KeeperException) ex.getCause();
                throw new Exception("unable to commit2 table splitting, ZK path: " + kExp.getPath() + ", ZK message: " + kExp.getMessage() + ", ZK code: " + kExp.code());
            }
            throw new Exception("unable to commit2 table splitting ZK error", ex.getCause());
        } catch (Exception ex) {
            throw new Exception("unable to commit2 table splitting", ex);
        }
    }

    private void makeFinishCommits(TableName tableName, List<RegionSplitModel> splitModels, ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers) throws Exception {
        LOGGER.debug("Splitting table '{}': finish commits started", tableName);
        try {
            List<CuratorOp> plan = new ArrayList<>();
            Map<RegionServerModel, String> rsRegionsData = new HashMap<>();
            // Target names for each splitted copy of a region
            for (RegionSplitModel splitModel : splitModels) {
                String rsData = rsRegionsData.get(splitModel.getRs());
                if (rsData == null) {
                    rsData = getRSRegionsData(tableName, splitModel.getRs());
                }
                String targetName = splitModel.getTargetSplittedName();
                rsData = zooContext.setRegionNameAndStatusForData(rsData, tableName, splitModel.getSplitted(), targetName, ENABLED_REGION_STATUS);
                rsData = zooContext.setRegionStatusForData(rsData, tableName, splitModel.getSplitting(), ENABLED_REGION_STATUS);
                RegionModel targetRegionModel = toTargetRegionModel(splitModel.getSplitted().getRegionModel(), targetName);
                rsRegionsData.put(splitModel.getRs(), rsData);
            }
            // Build transactions from the models
            for (Map.Entry<RegionServerModel, String> rsData : rsRegionsData.entrySet()) {
                String rsRegionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, rsData.getKey());
                plan.add(zooHelper.setDataTrans(rsRegionsPath, rsData.getValue()));
            }
            // Table status
            plan.add(zooHelper.setDataTrans(ZooKeeperContextHelper.getTablePath(tableName), STATUS_PREFIX + CREATED.getValue()));
            // Monitor RS
            RsHealthChecker.checkRsErrors(pingCheckers);
            // Stop monitoring RS
            pingCheckers.values().forEach(RsHealthChecker::close);
            List<CuratorTransactionResult> results = zooHelper.forOperations(plan);
            Preconditions.checkArgument(results.size() == plan.size(), "unexpected transaction error");
            LOGGER.debug("Splitting table '{}': finish commits finished", tableName);
        } catch (ZKException ex) {
            if (ex.getCause() instanceof KeeperException) {
                KeeperException kExp = (KeeperException) ex.getCause();
                throw new Exception("unable to finish commit table splitting, ZK path: " + kExp.getPath() + ", ZK message: " + kExp.getMessage() + ", ZK code: " + kExp.code());
            }
            throw new Exception("unable to finish commit table splitting ZK error", ex.getCause());
        } catch (Exception ex) {
            throw new Exception("unable to finish commit table splitting", ex);
        }
    }

    private RegionModel toTargetRegionModel(RegionModel region, String targetName) {
        return new RegionModel(targetName,
                region.getReplicaNum(),
                region.getStatusId(),
                region.getBeginKey(),
                region.getEndKey(),
                region.getDataBlockSize(),
                region.getCompressType(),
                region.getRecordCompressionType(),
                region.getDictionary(),
                region.getRecordTargetCount());
    }

    private String getRSRegionsData(TableName tableName, RegionServerModel regionServer) throws Exception {
        String result = StringUtils.EMPTY;
        try {
            String rsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, regionServer);
            if (zooHelper.checkExists(rsPath)) {
                result = zooHelper.getNotNullString(rsPath);
            }
        } catch (Exception ex) {
            throw new Exception("unable to read regions from RS " + regionServer, ex);
        }
        return result;
    }

    public RetryMode chooseRetryMode(TableName tableName, List<RegionDistributionModel> tableRdms) throws Exception {
        ZookeeperStatus tableStatus;
        String tableStatusStr;
        try {
            String tablePath = ZooKeeperContextHelper.getTablePath(tableName);
            tableStatusStr = zooHelper.getNotNullString(tablePath);
            if (tableStatusStr.contains(RECOVERING.getValue())) {
                tableStatusStr = ZookeeperStatus.getRecoveredStatus(tableStatusStr);
            }
            tableStatus = ZookeeperStatus.fromString(tableStatusStr);
        } catch (Exception ex) {
            throw new Exception("unable to read table status", ex);
        }
        // First determine the retry level from the table status
        switch (tableStatus) {
            case CREATING:
            case DROPPING:
            case ENABLING:
            case ASSIGNING:
            case DISABLING:
            case MODIFYING:
            case DISABLED:
                throw new NoSplittingNeeded();
            case CREATED:
            case ENABLED:
            case ASSIGNED:
                return RetryMode.ZERO_LEVEL;
            case SPLITTING:
                break;
            default:
                throw new NoRetryException("unexpected table status: " + tableStatus);
        }
        // Then infer the status from the SPLITTED statuses
        if (tableRdms.stream().anyMatch(rdm -> rdm.getRegionModel().getStatusId() == SPLITTED_REGION_STATUS)) {
            return RetryMode.FIRST_LEVEL;
        }
        // Then infer the status from the DISABLED statuses
        if (tableRdms.stream().anyMatch(rdm -> rdm.getRegionModel().getStatusId() == DISABLED_REGION_STATUS)) {
            return RetryMode.SECOND_LEVEL;
        }
        // Then infer the status from the ENABLING statuses
        if (tableRdms.stream().anyMatch(rdm -> rdm.getRegionModel().getStatusId() == ENABLING_REGION_STATUS)) {
            return RetryMode.THIRD_LEVEL;
        }
        LOGGER.warn("Splitting table '{}': ZooKeeper distribution is: {}", tableName, tableRdms);
        throw new Exception("unable to choose retry mode: unknown region statuses combination");
    }

    public boolean checkDiskSizeForSplit(TableName tableName, List<String> regionNamesToSplit) throws IOException {
        Set<String> selectedNames = new HashSet<>(regionNamesToSplit);
        long reservedBytes = Math.multiplyExact(conf.getDataConfiguration().getRegionsAutoSplitReservedMb(), 1024L * 1024);
        for (int attempt = 0; attempt < 2; attempt++) {
            List<RegionDistributionModel> replicas = zooContext.getRegionDistributionWithStatuses(tableName).stream()
                    .filter(rdm -> selectedNames.contains(rdm.getRegionModel().getName()))
                    .collect(Collectors.toList());
            Map<RegionServerModel, RegionServerMetrics> rsMetrics = new HashMap<>();
            Map<SplitDisk, Long> requiredBytes = new HashMap<>();
            Map<SplitDisk, Long> availableBytes = new HashMap<>();
            String missingReplica = null;
            for (RegionDistributionModel replica : replicas) {
                RegionServerModel rs = replica.getRegionServerModel();
                Map<Integer, RegionMetrics> regionMetrics = rsMetrics.computeIfAbsent(rs, rsJmxClient::getRsJmxMetrics)
                        .getRegionsMetrics().get(replica.getRegionModel().getName());
                RegionMetrics metrics = regionMetrics == null ? null : regionMetrics.get(replica.getRegionModel().getReplicaNum());
                if (metrics == null) {
                    missingReplica = replica.getRegionModel().toIdentityString();
                    break;
                }
                SplitDisk disk = new SplitDisk(rs.getHost(), metrics.getDiskName());
                requiredBytes.merge(disk, metrics.getSize(), Math::addExact);
                // This field contains FileStore.getUsableSpace(), despite its name
                availableBytes.merge(disk, metrics.getUsageDiskSpace() - reservedBytes, Math::min);
            }
            if (missingReplica != null) {
                if (attempt == 1) {
                    throw new IOException("No disk metrics for replica: " + missingReplica);
                }
                LOGGER.warn("No disk metrics for replica: {}. Retrying split disk check in 70 seconds", missingReplica);
                try {
                    Thread.sleep(MISSING_DISK_METRICS_RETRY_DELAY_MS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for disk metrics for replica: " + missingReplica, ex);
                }
                continue;
            }
            boolean checkOk = true;
            for (Map.Entry<SplitDisk, Long> entry : requiredBytes.entrySet()) {
                if (entry.getValue() > availableBytes.get(entry.getKey())) {
                    LOGGER.warn("No space for splitting table '{}': disk {}, needed {} bytes, available {} bytes",
                            tableName, entry.getKey(), entry.getValue(), availableBytes.get(entry.getKey()));
                    checkOk = false;
                }
            }
            return checkOk;
        }
        throw new IllegalStateException("Unreachable split disk check state");
    }

    private record SplitDisk(String host, String name) {
    }

    public enum RetryMode {
        // No retry (start from the beginning)
        ZERO_LEVEL,
        // Retry after pre-commits
        FIRST_LEVEL,
        // Retry after intermediate commits #1
        SECOND_LEVEL,
        // Retry after intermediate commits #2
        THIRD_LEVEL
    }

    private boolean containsInterrupted(Throwable ex) {
        if (ex.getClass().equals(InterruptedException.class)) {
            return true;
        }
        if (ex.getCause() != null) {
            return containsInterrupted(ex.getCause());
        }
        return false;
    }

    private static class NoSplittingNeeded extends Exception {
    }
}
