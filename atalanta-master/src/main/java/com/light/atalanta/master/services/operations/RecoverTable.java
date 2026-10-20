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
import com.light.atalanta.exceptions.NotEnoughRegionServerInstances;
import com.light.atalanta.master.services.grpc.RegionServerConnector;
import com.light.atalanta.master.services.grpc.RsHealthChecker;
import com.light.atalanta.master.services.recovery.RecoveryPreCommitHelper;
import com.light.atalanta.master.services.recovery.RecoveryRSContext;
import com.light.atalanta.master.services.recovery.RegionRecoveryModel;
import com.light.atalanta.master.services.split.RegionSplitModel;
import com.light.atalanta.master.services.split.RegionSplitNameGenerator;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.master.vo.DataConfiguration;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.master.vo.GrpcConfiguration;
import com.light.atalanta.metrics.RsJmxClient;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.util.ZookeeperStatus;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

import static com.light.atalanta.master.services.recovery.InitialRecoveryState.*;
import static com.light.atalanta.services.ZooKeeperContextHelper.*;

/**
 * Region recovery.
 */
@SuppressFBWarnings("SF_SWITCH_FALLTHROUGH")
public class RecoverTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecoverTable.class);

    private final DataConfiguration dataConf;

    private final GrpcConfiguration grpcConf;

    private final ZooKeeperMasterContext zooContext;

    private final ZookeeperHelper zooHelper;

    private final RegionServerConnector regionServerConnector;

    private final RecoveryRSContext recoveryRSContext;

    private final SplitTable splitTable;
    private final ExecutorService executorService;

    public RecoverTable(AtalantaDBConfiguration conf, ZooKeeperMasterContext zooContext, ZookeeperHelper zooHelper, RecoveryRSContext recoveryRSContext, RegionServerConnector regionServerConnector, RsJmxClient rsJmxClient, ExecutorService executorService) {
        this.zooContext = zooContext;
        this.zooHelper = zooHelper;
        this.dataConf = conf.getDataConfiguration();
        this.grpcConf = conf.getGrpcConfiguration();
        this.regionServerConnector = regionServerConnector;
        this.recoveryRSContext = recoveryRSContext;
        this.executorService = executorService;
        this.splitTable = new SplitTable(conf, zooContext, zooHelper, recoveryRSContext, regionServerConnector, rsJmxClient, executorService);
    }

    public void action(TableName tableName, List<RegionServerModel> lostRS, Runnable actualizeRS, ReadWriteLock lock, boolean checkLostRS) throws NoRetryException {
        int retryCount = 1;
        while (true) {
            ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers = new ConcurrentHashMap<>();
            try {
                retryableAction(tableName, lostRS, pingCheckers, lock, checkLostRS);
                return;
            } catch (NoRecoveryNeeded ex) {
                LOGGER.info("Recovering table '{}': no recovery needed because of status", tableName);
                return;
            } catch (NoRetryException ex) {
                throw ex;
            } catch (Throwable ex) {
                LOGGER.error("Error recovering table: '{}', wait {} ms and retrying #{}...", tableName, dataConf.getRecoverTableRetryDelayMs(), retryCount, ex);
                try {
                    Thread.sleep(dataConf.getRecoverTableRetryDelayMs());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                }
                retryCount++;
                // Update the lost/recovering lists
                actualizeRS.run();
            } finally {
                pingCheckers.values().forEach(RsHealthChecker::close);
            }
        }
    }

    public void retryableAction(TableName tableName, List<RegionServerModel> lostRS, ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers, ReadWriteLock lock, boolean checkLostRS) throws Throwable {
        LOGGER.info("Start recovering table: '{}'", tableName);

        RecoveryPreCommitHelper preCommitHelper = new RecoveryPreCommitHelper(zooHelper, zooContext, recoveryRSContext);

        AtomicBoolean syncMode = new AtomicBoolean(false);
        boolean retryMode = chooseRetryMode(tableName, false);
        LOGGER.info("Recovering table '{}': retry mode is {}", tableName, retryMode);

        // In PreCommitsRunner mode lostRS is empty, but the work still has to be done
        // Execution may be skipped only when we are not retrying
        if (checkLostRS && lostRS.isEmpty() && !retryMode) {
            LOGGER.info("Looks like lostRS are now alive");
            return;
        }

        // TODO: Stop all tasks

        String tablePath = ZooKeeperContextHelper.getTablePath(tableName);
        String tableStatus = zooHelper.getNotNullString(tablePath);
        LOGGER.info("Recovering table '{}': table status is {}", tableName, tableStatus);
        List<RegionDistributionModel> tableRdms = zooContext.getRegionDistributionWithStatuses(tableName);
        SplitContext splitContext = new SplitContext();
        if (tableStatus.contains(ZookeeperStatus.SPLITTING.getValue())) {
            splitContext = new SplitContext(tableName, tableRdms);
        }
        List<RegionServerModel> aliveRS = zooContext.getAliveRegionServers();
        List<RegionServerModel> runningRS = zooContext.getRunningRegionServers();
        LOGGER.trace("Recovering table '{}': alive RS is {}, running RS is: {}", tableName, aliveRS, runningRS);

        List<RegionRecoveryModel> recoveryModels = new ArrayList<>();
        long ts = System.currentTimeMillis();
        LOGGER.info("Recovering table '{}': waiting lock for analyze operation {}", tableName, ts);
        lock.readLock().lock();
        try {
            LOGGER.info("Recovering table '{}': done waiting lock for analyze operation {}", tableName, ts);
            if (checkLostRS && lostRS.isEmpty() && !retryMode) {
                LOGGER.info("Looks like lostRS are now alive");
                return;
            }
            LOGGER.info("Recovering table '{}': analyze operation started", tableName);
            if (retryMode) {
                // In certain circumstances this branch may start recovery from scratch.
                //
                // This happens when we didn't start leader for some region during one of previous recovery
                // iterations because some reserve replicas were off. To keep that fact – we don't commit table
                // status and leave it as RECOVERING while not committing regions, and retry recovery for the table.
                //
                // Recovery logic, when it sees that table has no precommited regions but has RECOVERING status
                // goes and starts all leaders. However, if other table RS will be shut down, at some point we
                // will need to start recovery from beginning. In that case – lostRS list won't be empty.
                if (!retryRecoveryInit(tableName, tableRdms, splitContext, preCommitHelper, recoveryModels, aliveRS, runningRS, lostRS, syncMode)) {
                    return;
                }
            } else {
                if (!noRetryRecoveryInit(tableName, tableRdms, splitContext, preCommitHelper, recoveryModels, aliveRS, runningRS, lostRS, syncMode)) {
                    return;
                }
            }
            LOGGER.info("Recovering table '{}': analyze operation finished", tableName);
        } finally {
            lock.readLock().unlock();
        }

        // Update the region layout after all changes
        LOGGER.info("Recovering table '{}': reading ZK table distribution", tableName);
        tableRdms = zooContext.getRegionDistributionWithStatuses(tableName);

        // Stop the leaders unless in SYNC mode
        if (!syncMode.get()) {
            stopLeadersIfNeeded(tableName, recoveryModels, tableRdms);
        }

        // Distribute regions to RS in parallel and wait
        recoverReplicas(tableName, recoveryModels, splitContext, tableRdms, pingCheckers, syncMode.get());

        // Monitor RS
        RsHealthChecker.checkRsErrors(pingCheckers);

        // Commit the regions
        preCommitHelper.makeRegionFinishCommits(tableName, recoveryModels, splitContext, pingCheckers);

        // Update the region layout after all changes
        LOGGER.info("Recovering table '{}': reading ZK table distribution", tableName);
        tableRdms = zooContext.getRegionDistributionWithStatuses(tableName);

        // Start the leaders unless in SYNC mode
        if (!syncMode.get()) {
            startLeadersIfNeeded(tableName, retryMode, recoveryModels, recoveryModels, splitContext, tableRdms);
        }

        // Commit the table if all leaders have started
        if (!recoveryRSContext.containsSkippedLeaderStartTables(tableName)) {
            preCommitHelper.makeTableFinishCommit(tableName);
        } else {
            LOGGER.info("Recovering table '{}': table is not being committed because of unstarted leaders, retry within next recovery iteration", tableName);
        }

        LOGGER.info("Finish recovering table: '{}'", tableName);
    }

    // returns boolean flag that shows whether we need to continue with recovery
    private boolean retryRecoveryInit(
            TableName tableName,
            List<RegionDistributionModel> tableRdms,
            SplitContext splitContext,
            RecoveryPreCommitHelper preCommitHelper,
            List<RegionRecoveryModel> recoveryModels,
            List<RegionServerModel> aliveRS,
            List<RegionServerModel> runningRS,
            List<RegionServerModel> lostRS,
            AtomicBoolean syncMode
    ) throws Exception {
        // Reset the flag that we may have failed to start leaders earlier
        recoveryRSContext.removeSkippedLeaderStartTable(tableName);

        // Read the recovery pre-commits
        recoveryModels.addAll(findRecoveryModels(tableName, tableRdms));

        // If there are no pre-commits but the table status is RECOVERING, we must:
        //
        // If lostRS is empty:
        // 1. Start all leaders we can
        // 2. Commit the table
        //
        // If lostRS is not empty:
        // 1. Restart recovery from the beginning, without touching the leaders or the table status
        // 2. Make sure recovery is ready to restart with the pre-committed status
        // 3. Start ALL leaders, since we don't know whether earlier leader starts were skipped
        if (recoveryModels.isEmpty()) {
            LOGGER.info("Recovering table '{}': no pre-commits found, checking lostRS list", tableName);
            if (lostRS.isEmpty()) {
                LOGGER.info("Recovering table '{}': lostRS is empty, starting all leaders", tableName);
                startLeadersIfNeeded(tableName, true, Collections.emptyList(), Collections.emptyList(), splitContext, tableRdms);
                if (!recoveryRSContext.containsSkippedLeaderStartTables(tableName)) {
                    preCommitHelper.makeTableFinishCommit(tableName);
                } else {
                    LOGGER.info("Recovering table '{}': table is not being committed because of unstarted leaders, retry within next recovery iteration", tableName);
                }
                return false;
            } else {
                LOGGER.info("Recovering table '{}': lostRS is NOT empty, starting recovery process from scratch", tableName);
                // We should be ready to commit AND start all leaders if we started recovery from scratch –
                // but had to exit early, for example – when all replicas happened to be running
                if (!noRetryRecoveryInit(tableName, tableRdms, splitContext, preCommitHelper, recoveryModels, aliveRS, runningRS, lostRS, syncMode)) {
                    LOGGER.info("Recovering table '{}': exit early, starting all leaders", tableName);
                    startLeadersIfNeeded(tableName, true, Collections.emptyList(), Collections.emptyList(), splitContext, tableRdms);
                    if (!recoveryRSContext.containsSkippedLeaderStartTables(tableName)) {
                        preCommitHelper.makeTableFinishCommit(tableName);
                    } else {
                        LOGGER.info("Recovering table '{}': table is not being committed because of unstarted leaders, retry within next recovery iteration", tableName);
                    }
                    return false;
                }
                return true;
            }
        }

        LOGGER.info("Recovering table '{}': recovery models are {}", tableName, recoveryModels);
        syncMode.set(preCommitHelper.isSyncMode(recoveryModels));
        if (syncMode.get()) {
            LOGGER.info("Recovering table '{}': table is in the SYNC mode", tableName);
        } else {
            // Check whether re-pre-commits are needed. This includes checking for replicas that came back alive.
            boolean changesMade = preCommitHelper.correctRecoveryModels(tableName, recoveryModels, splitContext, tableRdms, aliveRS, runningRS);
            if (changesMade) {
                LOGGER.info("Recovering table '{}': new recovery models are {}", tableName, recoveryModels);
            }
            // Send DISABLE+DROP commands to the target RS of the replicas that came back alive
            List<RegionRecoveryModel> suddenlyAliveModels = recoveryModels.stream()
                    .filter(RegionRecoveryModel::isSuddenlyAlive)
                    .collect(Collectors.toList());
            //
            // ALL leaders will be started by the regular startLeadersIfNeeded() command.
            // We can't start them earlier, because they may still be recovering.
            //
            // If some leader fails to start, we do the following:
            //
            // 1. Commit all regions
            // 2. Don't commit the table
            // 3. Add the table with recoveryRSContext.addSkippedLeaderStartTable()
            // 4. Retry until all leaders start, or until a new recovery is needed.
            //
            if (!suddenlyAliveModels.isEmpty()) {
                processSuddenlyAliveModels(tableName, recoveryModels, new ArrayList<>(suddenlyAliveModels));
            }
            // Write all pre-commit changes to ZooKeeper
            preCommitHelper.correctPreCommits(tableName, recoveryModels);
            // Remove the replicas that suddenly came back alive from the plan
            recoveryModels.removeIf(RegionRecoveryModel::isSuddenlyAlive);
        }
        return true;
    }

    // returns boolean flag that shows whether we need to continue with recovery
    private boolean noRetryRecoveryInit(
            TableName tableName,
            List<RegionDistributionModel> tableRdms,
            SplitContext splitContext,
            RecoveryPreCommitHelper preCommitHelper,
            List<RegionRecoveryModel> recoveryModels,
            List<RegionServerModel> aliveRS,
            List<RegionServerModel> runningRS,
            List<RegionServerModel> lostRS,
            AtomicBoolean syncMode
    ) throws Exception {
        // Plan region recovery from scratch
        recoveryModels.addAll(makeRecoveryModels(tableName, tableRdms, splitContext, aliveRS, runningRS, lostRS));
        if (recoveryModels.isEmpty()) {
            LOGGER.info("Recovering table '{}': all replicas are running", tableName);
            return false;
        }
        LOGGER.info("Recovering table '{}': recovery models are {}", tableName, recoveryModels);
        // Pre-commit the table and regions
        preCommitHelper.makePreCommits(tableName, recoveryModels);
        return true;
    }

    private void processSuddenlyAliveModels(TableName tableName, List<RegionRecoveryModel> recoveryModels, List<RegionRecoveryModel> suddenlyAliveModels) throws Exception {
        List<RegionServerModel> runningRS = zooContext.getRunningRegionServers();
        CountDownLatch latch = new CountDownLatch(suddenlyAliveModels.size());

        suddenlyAliveModels.forEach(rm -> executorService.submit(() -> {
            RegionDistributionModel recoveryRdm = rm.getRecoveryRdm();
            LOGGER.warn("Recovering table '{}': trying to clear recovery artifacts from recovery RS for {}", tableName, recoveryRdm.getRegionModel().toIdentityString());
            try {
                if (runningRS.contains(recoveryRdm.getRegionServerModel())) {
                    String disableLogStr = String.format("Recovering table '%s': disabling region '%s'", tableName, recoveryRdm.getRegionModel().toIdentityString());
                    regionServerConnector.monitor(tableName, disableLogStr, recoveryRdm, StringUtils.EMPTY, (stub, req, stream) ->
                            stub.get().disableRegion(req, stream), false
                    );
                    String recoveryLogStr = String.format("Recovering table '%s': dropping region '%s'", tableName, recoveryRdm.getRegionModel().toIdentityString());
                    regionServerConnector.monitor(tableName, recoveryLogStr, recoveryRdm, StringUtils.EMPTY, (stub, req, stream) ->
                            stub.get().dropRegion(req, stream), false
                    );
                    LOGGER.warn("Recovering table '{}': recovery artifacts have been cleared from recovery RS for {}", tableName, recoveryRdm.getRegionModel().toIdentityString());
                } else {
                    LOGGER.warn("Recovering table '{}': unable to clear recovery artifacts from recovery RS for {}: it's not running", tableName, recoveryRdm.getRegionModel().toIdentityString());
                }
            } catch (Throwable throwable) {
                LOGGER.warn("Recovering table '{}': unable to clear recovery artifacts from recovery RS for {}", tableName, recoveryRdm.getRegionModel().toIdentityString());
            } finally {
                latch.countDown();
            }
        }));
        latch.await();

    }

    /**
     * Starts a leader only if all of the following holds:
     * 1. It is in the region list.
     * 2. All replicas of the region are alive.
     * 3. The leader status was neither DISABLED nor ENABLING.
     * recoveryModelsToAct: models whose leaders will be started.
     * recoveryModelsToConsider: all models.
     * We can ignore the error for some of the RS. More details below.
     */
    private void startLeadersIfNeeded(TableName tableName, boolean retryMode, List<RegionRecoveryModel> recoveryModelsToAct, List<RegionRecoveryModel> allRecoveryModels, SplitContext splitContext, List<RegionDistributionModel> tableRdms) throws Exception {
        LOGGER.info("Recovering table '{}': starting leaders", tableName);
        LOGGER.info("Recovering table '{}': thread pool created", tableName);
        LOGGER.trace("Recovering table '{}': zookeeper distribution is: {}", tableName, tableRdms);
        Map<String, RegionDistributionModel> leaders;
        // If we don't have recovery models, we should start all leaders,
        // keeping in mind that we can't start leader if reserve replicas aren't working,
        // or leader is splitting / is in DISABLED status. In this case
        // we put the table to skippedLeaderStartTables list and do retries.
        //
        // Moreover, if we're in retry mode, we should also start all leaders.
        // That's because we can't be sure that previous attempts restarted all leaders successfully.
        if (retryMode || (recoveryModelsToAct.isEmpty() && allRecoveryModels.isEmpty())) {
            leaders = findAllLeadersToStart(tableName, splitContext, tableRdms);
        } else {
            leaders = findLeadersToStart(tableName, recoveryModelsToAct, allRecoveryModels, splitContext, tableRdms);
        }
        LOGGER.info("Recovering table '{}': found leaders to start", tableName);
        CountDownLatch latch = new CountDownLatch(leaders.size());
        AtomicBoolean error = new AtomicBoolean();

        leaders.forEach((regionName, leader) -> executorService.submit(() -> {
            try {
                int retry = 0;
                while (true) {
                    try {
                        String reservedReplicas = findReservedReplicas(zooContext, tableName, regionName, tableRdms, dataConf.getReplicationFactor());
                        String recoveryLogStr = String.format("Recovering table '%s': starting region '%s'", tableName, leader.getRegionModel().toIdentityString());
                        regionServerConnector.monitor(tableName, recoveryLogStr, leader, reservedReplicas, (stub, req, stream) ->
                                stub.get().startRegion(req, stream), false
                        );
                        break;
                    } catch (Throwable ex) {
                        List<RegionServerModel> aliveRS = getAliveRegionServers();
                        List<RegionServerModel> runningRS = getRunningRegionServers();
                        // We can skip this error when:
                        // 1. If RS is not ALIVE – in this case, it will assign the leader on its own
                        // 2. Or RS is blocked (is in the RecoveringRS list) and not RUNNING – in this case, it will also assign the leader on its own
                        boolean rsIsNotAlive = !aliveRS.contains(leader.getRegionServerModel());
                        boolean rsIsBlockedAndNotRunning = recoveryRSContext.containsRecovering(leader.getRegionServerModel()) && !runningRS.contains(leader.getRegionServerModel());
                        if (rsIsNotAlive || rsIsBlockedAndNotRunning) {
                            LOGGER.warn("Recovering table '{}': unable to start leader for {}, seems like RS '{}' is not in the INITIALIZING neither RUNNING status. skipping this leader replica start", tableName, regionName, leader.getRegionServerModel().getName(), ex);
                            break;
                        } else {
                            LOGGER.error("Recovering table '{}': unable to start leader for {}. waiting 5000 ms and retry #{} ...", tableName, regionName, ++retry, ex);
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException e) {
                                LOGGER.error("Recovering table '{}': start leader: '{}' interrupted", tableName, leader.getRegionModel().toIdentityString());
                                Thread.currentThread().interrupt();
                                error.set(true);
                                throw new RuntimeException(e);
                            }
                        }
                    }
                }
            } finally {
                latch.countDown();
            }
        }));
        latch.await();
        if (error.get()) {
            throw new Exception("found errors while starting leaders. Please, check the logs");
        }

    }

    private List<RegionServerModel> getAliveRegionServers() {
        while (true) {
            try {
                return zooContext.getAliveRegionServers();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOGGER.error("Unable to read alive RS infos: interrupted");
                throw new RuntimeException(ex);
            } catch (Exception ex) {
                LOGGER.error("Unable to read alive RS infos. Wait 5000 ms and retry ...", ex);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException interrupted) {
                    LOGGER.error("Unable to read alive RS infos: interrupted");
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    private List<RegionServerModel> getRunningRegionServers() {
        while (true) {
            try {
                return zooContext.getRunningRegionServers();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOGGER.error("Unable to read running RS infos: interrupted");
                throw new RuntimeException(ex);
            } catch (Exception ex) {
                LOGGER.error("Unable to read running RS infos. Wait 5000 ms and retry ...", ex);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException interrupted) {
                    LOGGER.error("Unable to read running RS infos: interrupted");
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    public static String findReservedReplicas(ZooKeeperMasterContext zooContext, TableName tableName, String regionName, List<RegionDistributionModel> projection, int replicationFactor) throws Exception {
        String reservedReplicas;
        try {
            reservedReplicas = zooContext.getReservedAddresses(projection, regionName, replicationFactor);
            LOGGER.trace("Recovering table '{}': reserved replicas projection for '{}' is: {}", tableName, regionName, reservedReplicas);
        } catch (Exception ex) {
            throw new Exception("unable to prepare reserved replicas for: " + regionName, ex);
        }
        return reservedReplicas;
    }

    private void recoverReplicas(TableName tableName, List<RegionRecoveryModel> recoveryModels, SplitContext splitContext, List<RegionDistributionModel> tableRdms, ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers, boolean syncMode) throws Exception {
        LOGGER.info("Recovering table '{}': recovering replicas", tableName);
        List<RegionDistributionModel> projection = processProjection(tableName, recoveryModels, tableRdms);
        int recoveryTasksAmount = recoveryModels.size();
        CountDownLatch latch = new CountDownLatch(recoveryTasksAmount);

        AtomicBoolean error = new AtomicBoolean();
        for (RegionRecoveryModel recoveryModel : recoveryModels) {
            // Run RS monitoring threads
            // We don't need to do this outside, since we don't have retry levels in the Recovery
            RegionServerModel rsDst = recoveryModel.getRecoveryRdm().getRegionServerModel();
            if (!pingCheckers.containsKey(rsDst)) {
                pingCheckers.put(rsDst, new RsHealthChecker(rsDst, grpcConf));
            }
            executorService.submit(() -> {
                try {
                    // The request includes the address of the source region server and the reserve replica string
                    // when a leader is moved. When a reserve is moved, the string is empty.
                    String referenceRSAddr = recoveryModel.getReferenceRdm().getRegionServerModel().getHost() + ":" + recoveryModel.getReferenceRdm().getRegionServerModel().getJavaPort();
                    String reservedReplicasIfNeeded = recoveryModel.getRecoveryRdm().getRegionModel().getReplicaNum() == 0 ? findReservedReplicas(zooContext, tableName, recoveryModel.getLostRdm().getRegionModel().getName(), projection, dataConf.getReplicationFactor()) : "empty";
                    String referenceReplicaNum = String.valueOf(recoveryModel.getReferenceRdm().getRegionModel().getReplicaNum());
                    int lostRegionStatus;
                    if (syncMode) {
                        lostRegionStatus = DISABLED_REGION_STATUS;
                    } else {
                        lostRegionStatus = recoveryModel.getLostRdm().getRegionModel().getStatusId();
                    }
                    // Assign the region only if it is not involved in a split and its status is neither DISABLED nor DISABLING
                    boolean needToAssign = !splitContext.isSplitting(recoveryModel.getLostRdm()) && lostRegionStatus != DISABLED_REGION_STATUS && lostRegionStatus != DISABLING_REGION_STATUS;
                    String additionalInfo = String.join("@", referenceRSAddr, referenceReplicaNum, reservedReplicasIfNeeded, needToAssign + "", syncMode + "");
                    String recoveryLogStr = String.format("Recovering table '%s': recovering region '%s'", tableName, recoveryModel.getRecoveryRdm().getRegionModel().toIdentityString());
                    regionServerConnector.monitor(tableName, recoveryLogStr, recoveryModel.getRecoveryRdm(), additionalInfo, (stub, req, stream) ->
                            stub.get().recoverRegion(req, stream), false
                    );
                } catch (Throwable ex) {
                    LOGGER.error("Recovering table '{}': unable to perform recovery on RS dst for {}", tableName, recoveryModel.getRecoveryRdm().getRegionModel().toIdentityString(), ex);
                    error.set(true);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        if (error.get()) {
            throw new Exception("found errors while recovering regions on RS. Please, check the logs");
        }

    }

    // Searches leaders only for recovering regions
    private Map<String, RegionDistributionModel> findLeadersToStart(TableName tableName, List<RegionRecoveryModel> recoveryModels, List<RegionRecoveryModel> allRecoveryModels, SplitContext splitContext, List<RegionDistributionModel> projection) throws Exception {
        List<RegionServerModel> runningRS = zooContext.getRunningRegionServers();
        LOGGER.info("Recovering table '{}': found running region servers", tableName);
        Map<String, RegionDistributionModel> foundLeaders = new HashMap<>();
        Set<RegionServerModel> problematicRS = new HashSet<>();
        for (RegionRecoveryModel rm : recoveryModels) {
            String regionName = rm.getLostRdm().getRegionModel().toIdentityString();
            RegionDistributionModel leader = findLeaderToStart(tableName, rm, allRecoveryModels, splitContext, projection, runningRS, problematicRS);
            if (leader != null && !foundLeaders.containsKey(regionName)) {
                LOGGER.trace("Recovering table '{}': found leader to start for '{}': it is: {}", tableName, regionName, leader.toShortString());
                foundLeaders.put(rm.getLostRdm().getRegionModel().getName(), leader);
            }
        }
        if (!problematicRS.isEmpty()) {
            LOGGER.warn("Recovering table '{}': can't start leaders: some replicas are not running; recovery won't be completed until these RS are up: {}", tableName, problematicRS);
        }
        return foundLeaders;
    }

    // If there aren't any recovery models – we should start all leaders,
    // keeping in mind other factors based on which we may not start the leader,
    // but still want to remember this in the future
    private Map<String, RegionDistributionModel> findAllLeadersToStart(TableName tableName, SplitContext splitContext, List<RegionDistributionModel> tableRdms) throws Exception {
        List<RegionServerModel> runningRS = zooContext.getRunningRegionServers();
        Map<String, RegionDistributionModel> foundLeaders = new HashMap<>();
        List<RegionDistributionModel> allLeaders = tableRdms.stream()
                .filter(rdm -> rdm.getRegionModel().getReplicaNum() == 0)
                .collect(Collectors.toList());
        Set<RegionServerModel> problematicRS = new HashSet<>();
        for (RegionDistributionModel leader : allLeaders) {
            if (checkStartLeader(tableName, tableRdms, leader, splitContext, Collections.emptyList(), leader.getRegionModel().getName(), runningRS, problematicRS) != null) {
                foundLeaders.put(leader.getRegionModel().getName(), leader);
            }
        }
        if (!problematicRS.isEmpty()) {
            LOGGER.warn("Recovering table '{}': can't start leaders: some replicas are not running; recovery won't be completed until these RS are up: {}", tableName, problematicRS);
        }
        return foundLeaders;
    }

    private RegionDistributionModel findLeaderToStart(TableName tableName, RegionRecoveryModel recoveryModel, List<RegionRecoveryModel> allRecoveryModels, SplitContext splitContext, List<RegionDistributionModel> projection, List<RegionServerModel> runningRS, Set<RegionServerModel> problematicRS) throws Exception {
        // If the leader suddenly comes back, it is assigned when its RS starts
        if (allRecoveryModels.stream()
                .filter(recMod -> recMod.getLostRdm().getRegionModel().getName().equals(recoveryModel.getLostRdm().getRegionModel().getName()))
                .filter(RegionRecoveryModel::isSuddenlyAlive)
                .anyMatch(recMod -> recMod.getLostRdm().getRegionModel().getReplicaNum() == 0)) {
            LOGGER.info("Recovering table '{}': no need to start suddenly alive leader for '{}': it will start after RS init", tableName, recoveryModel.getLostRdm().getRegionModel().toIdentityString());
            return null;
        }
        List<RegionDistributionModel> leaderCandidates = projection.stream()
                .filter(rdm -> rdm.getRegionModel().getName().equals(recoveryModel.getLostRdm().getRegionModel().getName()))
                .filter(rdm -> rdm.getRegionModel().getReplicaNum() == 0)
                .collect(Collectors.toList());
        // Additional checks
        Preconditions.checkArgument(leaderCandidates.size() <= 1, "found multiply leaders for " + recoveryModel.getLostRdm().getRegionModel().toIdentityString());
        Preconditions.checkArgument(leaderCandidates.size() == 1, "can't find leader for " + recoveryModel.getLostRdm().getRegionModel().toIdentityString());
        RegionDistributionModel leader = leaderCandidates.get(0);
        // Replicas that are allowed not to run: the lost replicas.
        // Replicas being recovered must already run (except suddenlyAlive)
        List<RegionDistributionModel> allRecoveringRdms = new ArrayList<>();
        allRecoveryModels.stream()
                .peek(rm -> {
                    // If lost replica is suddenly alive, it should obviously work
                    if (!rm.isSuddenlyAlive()) {
                        allRecoveringRdms.add(rm.getLostRdm());
                    }
                })
                .forEach(rm -> {
                    // If lost replica is not suddenly alive,
                    // respective recovering replica should already work
                    if (rm.isSuddenlyAlive()) {
                        allRecoveringRdms.add(rm.getRecoveryRdm());
                    }
                });
        // Check that all replicas run (except the ones being recovered), and check the leader status
        return checkStartLeader(tableName, projection, leader, splitContext, allRecoveringRdms, recoveryModel.getLostRdm().getRegionModel().toIdentityString(), runningRS, problematicRS);
    }

    private RegionDistributionModel checkStartLeader(TableName tableName, List<RegionDistributionModel> rdms, RegionDistributionModel leader, SplitContext splitContext, List<RegionDistributionModel> excludedFromRunningCheck, String startLeaderForLog, List<RegionServerModel> runningRS, Set<RegionServerModel> problematicRS) throws Exception {
        // Get the leader status
        int leaderStatus = leader.getRegionModel().getStatusId();
        if (isReferencingRegionStatus(leader.getRegionModel().getStatusId())) {
            leaderStatus = getReferencedRegionStatus(leader.getRegionModel());
        }
        // Not every leader status is acceptable
        if (leaderStatus == DISABLED_REGION_STATUS || leaderStatus == DISABLING_REGION_STATUS || splitContext.isSplitting(leader)) {
            LOGGER.info("Recovering table '{}': no need to start leader for '{}': it is DISABLING/DISABLED/SPLITTING/SPLITTED. recovery still can be completed", tableName, startLeaderForLog);
            return null;
        }
        // All replicas must run
        Set<RegionDistributionModel> notRunningReplicas = zooContext.findNotRunningReplicas(rdms, leader.getRegionModel(), excludedFromRunningCheck, runningRS);
        if (!notRunningReplicas.isEmpty()) {
            LOGGER.trace("Recovering table '{}': can't start leader for '{}': some replicas are not running, recovery won't be completed until replicas are up: {}", tableName, startLeaderForLog, notRunningReplicas);
            recoveryRSContext.addSkippedLeaderStartTable(tableName);
            notRunningReplicas.forEach(rdm -> problematicRS.add(rdm.getRegionServerModel()));
            return null;
        }
        return leader;
    }

    /**
     * Stops the leaders only if all of the following holds:
     * 1. It is in the region list.
     * 2. It is running.
     * 3. The replica being recovered is not the leader (otherwise it is already stopped).
     */
    private void stopLeadersIfNeeded(TableName tableName, List<RegionRecoveryModel> recoveryModels, List<RegionDistributionModel> tableRdms) throws Exception {
        LOGGER.info("Recovering table '{}': stopping leaders", tableName);
        LOGGER.info("Recovering table '{}': thread pool created", tableName);
        List<RegionDistributionModel> projection = processProjection(tableName, recoveryModels, tableRdms);
        LOGGER.info("Recovering table '{}': projection created", tableName);
        Map<String, RegionDistributionModel> leaders = findLeadersToStop(tableName, recoveryModels, projection);
        LOGGER.info("Recovering table '{}': found leaders to stop", tableName);
        CountDownLatch latch = new CountDownLatch(leaders.size());
        AtomicBoolean error = new AtomicBoolean();

        leaders.forEach((regionName, leader) -> executorService.submit(() -> {
            try {
                String recoveryLogStr = String.format("Recovering table '%s': stopping leader: '%s'", tableName, leader.getRegionModel().toIdentityString());
                regionServerConnector.monitor(tableName, recoveryLogStr, leader, StringUtils.EMPTY, (stub, req, stream) ->
                        stub.get().stopRegion(req, stream), false
                );
            } catch (Throwable ex) {
                LOGGER.error("Recovering table '{}': unable to stop leader for {}", tableName, regionName, ex);
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

    private Map<String, RegionDistributionModel> findLeadersToStop(TableName tableName, List<RegionRecoveryModel> recoveryModels, List<RegionDistributionModel> projection) throws Exception {
        Map<String, RegionDistributionModel> leaders = new HashMap<>();
        List<RegionServerModel> runningRS = zooContext.getRunningRegionServers();
        LOGGER.info("Recovering table '{}': found running region servers", tableName);
        for (RegionRecoveryModel rm : recoveryModels) {
            String regionName = rm.getLostRdm().getRegionModel().toIdentityString();
            RegionDistributionModel leader = findLeaderToStop(rm, projection, runningRS);
            if (leader != null && !leaders.containsKey(regionName)) {
                LOGGER.trace("Recovering table '{}': found leader to stop for '{}': it is: {}", tableName, regionName, leader.toShortString());
                leaders.put(rm.getLostRdm().getRegionModel().getName(), leader);
            }
            if (leader == null) {
                LOGGER.trace("Recovering table '{}': leader to stop for '{}' not found", tableName, regionName);
            }
        }
        return leaders;
    }

    private RegionDistributionModel findLeaderToStop(RegionRecoveryModel recoveryModel, List<RegionDistributionModel> tableRdms, List<RegionServerModel> runningRS) {
        List<RegionDistributionModel> leaderCandidates = tableRdms.stream()
                .filter(rdm -> rdm.getRegionModel().getName().equals(recoveryModel.getLostRdm().getRegionModel().getName()))
                .filter(rdm -> !rdm.equals(recoveryModel.getLostRdm()))
                .filter(rdm -> runningRS.contains(rdm.getRegionServerModel()))
                .filter(rdm -> rdm.getRegionModel().getStatusId() != RECOVERING_REGION_STATUS
                        && rdm.getRegionModel().getStatusId() != FORCE_RECOVERING_REGION_STATUS
                        && rdm.getRegionModel().getStatusId() != SYNCING_REGION_STATUS)
                .filter(rdm -> rdm.getRegionModel().getReplicaNum() == 0)
                .collect(Collectors.toList());
        // Protect against duplicates
        Preconditions.checkArgument(leaderCandidates.size() <= 1, "found multiply leaders");
        if (leaderCandidates.isEmpty()) {
            return null;
        }
        return leaderCandidates.get(0);
    }

    /**
     * Builds the recovery models from the region layout in ZooKeeper.
     */
    public List<RegionRecoveryModel> findRecoveryModels(TableName tableName, List<RegionDistributionModel> tableRdms) throws Exception {
        List<RegionRecoveryModel> recoveryModels = new ArrayList<>();
        for (RegionDistributionModel tableRdm : tableRdms) {
            // RECOVERING implies that LOST and REFERENCE exist
            if (tableRdm.getRegionModel().getStatusId() == RECOVERING_REGION_STATUS || tableRdm.getRegionModel().getStatusId() == FORCE_RECOVERING_REGION_STATUS) {
                LOGGER.trace("Recovering table '{}': analyzing recovering replica {}", tableName, tableRdm.toShortString());
                RegionDistributionModel referenceRdm;
                try {
                    referenceRdm = zooContext.getTableReferencingReplica(tableRdms, tableRdm.getRegionModel().getName());
                } catch (Exception ex) {
                    throw new Exception("unable to find reference replica for: " + tableRdm.toShortString(), ex);
                }
                RegionDistributionModel lostRdm;
                try {
                    lostRdm = zooContext.getTableReplicaExcluded(tableRdms, tableRdm.getRegionModel().getName(), tableRdm.getRegionModel().getReplicaNum(), Arrays.asList(RECOVERING_REGION_STATUS, FORCE_RECOVERING_REGION_STATUS, SYNCING_REGION_STATUS));
                } catch (Exception ex) {
                    throw new Exception("unable to find lost replica for: " + tableRdm.toShortString(), ex);
                }
                recoveryModels.add(new RegionRecoveryModel(lostRdm, referenceRdm, tableRdm, ORDINARY));
            }
            // SYNCING implies that REFERENCE exists
            if (tableRdm.getRegionModel().getStatusId() == SYNCING_REGION_STATUS) {
                LOGGER.trace("Recovering table '{}': analyzing syncing replica {}", tableName, tableRdm.toShortString());
                RegionDistributionModel referenceRdm;
                try {
                    referenceRdm = zooContext.getTableReferencingReplica(tableRdms, tableRdm.getRegionModel().getName());
                } catch (Exception ex) {
                    throw new Exception("unable to find reference replica for: " + tableRdm.toShortString(), ex);
                }
                recoveryModels.add(new RegionRecoveryModel(null, referenceRdm, tableRdm, ORDINARY));
            }
        }
        return recoveryModels;
    }

    /**
     * Builds the recovery models from the lists of live and lost RS.
     */
    private List<RegionRecoveryModel> makeRecoveryModels(TableName tableName, List<RegionDistributionModel> tableRdms, SplitContext splitContext, List<RegionServerModel> aliveRS, List<RegionServerModel> runningRS, List<RegionServerModel> lostRS) throws Exception {
        List<RegionRecoveryModel> recoveryModels = new ArrayList<>();
        List<RegionDistributionModel> lostRegions = tableRdms.stream()
                .filter(rdm -> lostRS.contains(rdm.getRegionServerModel()))
                .collect(Collectors.toList());
        Map<String, RegionServerModel> splitRSCache = new HashMap<>();
        // Avoid logging twice
        List<RegionServerModel> loggedSuddenlyAliveRS = new ArrayList<>();
        for (RegionDistributionModel lostRdm : lostRegions) {
            LOGGER.trace("Recovering table '{}': analyzing lost replica {}", tableName, lostRdm.toShortString());
            // Check for duplicates
            if (tableRdms.stream().anyMatch(
                    it -> it.getRegionModel().getName().equals(lostRdm.getRegionModel().getName())
                            && it.getRegionModel().getReplicaNum() == lostRdm.getRegionModel().getReplicaNum()
                            && it.getRegionModel().getStatusId() != lostRdm.getRegionModel().getStatusId()
            )) {
                throw new Exception("found unexpected duplicate for " + lostRdm);
            }
            // Check whether some RS came back
            if (aliveRS.contains(lostRdm.getRegionServerModel())) {
                if (!loggedSuddenlyAliveRS.contains(lostRdm.getRegionServerModel())) {
                    LOGGER.info("Recovering table '{}': found suddenly alive RS: {}", tableName, lostRdm.getRegionServerModel());
                    loggedSuddenlyAliveRS.add(lostRdm.getRegionServerModel());
                    // Keep monitoring this RS
                    recoveryRSContext.addSkippedTables(lostRdm.getRegionServerModel());
                }
            } else if (!splitContext.isSplitting(lostRdm) && ordinaryRecoverRegionStatus(lostRdm.getRegionModel().getStatusId())) {
                // REAL recovery
                RegionDistributionModel referenceRdm = chooseOrdinaryReferenceRdm(zooContext, tableName, lostRdm, recoveryModels, tableRdms, aliveRS);
                RegionServerModel recoveryRS = chooseRecoverRegionServer(zooContext, lostRdm, tableRdms, aliveRS, runningRS);
                RegionDistributionModel recoveryRdm = zooContext.makeRdmCopy(lostRdm, recoveryRS, RECOVERING_REGION_STATUS);
                recoveryModels.add(new RegionRecoveryModel(lostRdm, referenceRdm, recoveryRdm, ORDINARY));
            } else if (splitContext.isSplittingModel(lostRdm)) {
                // REAL recovery
                RegionDistributionModel referenceRdm = chooseOrdinaryReferenceRdm(zooContext, tableName, lostRdm, recoveryModels, tableRdms, aliveRS);
                RegionServerModel recoveryRS = splitRSCache.get(lostRdm.getRegionModel().toIdentityString());
                if (recoveryRS == null) {
                    recoveryRS = chooseRecoverRegionServer(zooContext, lostRdm, tableRdms, aliveRS, runningRS);
                    splitRSCache.put(lostRdm.getRegionModel().toIdentityString(), recoveryRS);
                }
                RegionDistributionModel recoveryRdm = zooContext.makeRdmCopy(lostRdm, recoveryRS, RECOVERING_REGION_STATUS);
                recoveryModels.add(new RegionRecoveryModel(lostRdm, referenceRdm, recoveryRdm, SPLITTING));
            } else if (splitContext.isSplittedModel(lostRdm) && (splitContext.getRetryMode() == SplitTable.RetryMode.SECOND_LEVEL) || (splitContext.getRetryMode() == SplitTable.RetryMode.THIRD_LEVEL)) {
                // REAL recovery
                RegionDistributionModel referenceRdm = chooseOrdinaryReferenceRdm(zooContext, tableName, lostRdm, recoveryModels, tableRdms, aliveRS);
                // splittingName-splittedName_0 -> splittingName_0
                String splittingCopy = lostRdm.getRegionModel().getName();
                splittingCopy = splittingCopy.substring(0, splittingCopy.indexOf("-"));
                splittingCopy += "_" + lostRdm.getRegionModel().getReplicaNum();
                RegionServerModel recoveryRS = splitRSCache.get(splittingCopy);
                if (recoveryRS == null) {
                    recoveryRS = chooseRecoverRegionServer(zooContext, lostRdm, tableRdms, aliveRS, runningRS);
                    splitRSCache.put(splittingCopy, recoveryRS);
                }
                RegionDistributionModel recoveryRdm = zooContext.makeRdmCopy(lostRdm, recoveryRS, RECOVERING_REGION_STATUS);
                recoveryModels.add(new RegionRecoveryModel(lostRdm, referenceRdm, recoveryRdm, SPLITTED));
            } else if (!splitContext.isSplittedModel(lostRdm)) {
                throw new NoRetryException("unknown initial recovery state: " + lostRdm);
            }
        }
        return recoveryModels;
    }

    /**
     * Tries to pick the reference already found for the current region. If there is none, picks a new one
     * with one of the statuses: CREATED, ENABLED, ENABLING, DISABLED, DISABLING, SPLITTING.
     */
    public static RegionDistributionModel chooseOrdinaryReferenceRdm(ZooKeeperMasterContext zooContext, TableName tableName, RegionDistributionModel lostRdm, List<RegionRecoveryModel> recoveryModels, List<RegionDistributionModel> tableRdms, List<RegionServerModel> aliveRS) throws Exception {
        RegionDistributionModel result;
        Optional<RegionDistributionModel> referenceRdm = recoveryModels.stream()
                .filter(rm -> rm.getLostRdm().getRegionModel().getName().equals(lostRdm.getRegionModel().getName()))
                .map(RegionRecoveryModel::getReferenceRdm)
                .findAny();
        if (referenceRdm.isPresent()) {
            LOGGER.trace("Recovering table '{}': using already found reference for {}, it is: {}", tableName, lostRdm.getRegionModel().toIdentityString(), referenceRdm.get().getRegionModel().toIdentityString());
            return referenceRdm.get();
        }
        // Collect candidate reference replicas
        List<RegionDistributionModel> rdmsChooseFrom = tableRdms.stream()
                .filter(rdm -> rdm.getRegionModel().getName().equals(lostRdm.getRegionModel().getName()))
                .filter(rdm -> aliveRS.contains(rdm.getRegionServerModel()))
                .filter(rdm -> !rdm.getRegionServerModel().equals(lostRdm.getRegionServerModel()))
                .collect(Collectors.toList());
        // Try to find another reference replica with a normal status
        // No need to look for SPLITTED: while a candidate reference has this status,
        // SPLITTED replicas don't need to be recovered at all
        List<RegionDistributionModel> chosenRdms = rdmsChooseFrom.stream()
                .filter(rdm -> ordinaryRecoverRegionStatus(rdm.getRegionModel().getStatusId())
                        || rdm.getRegionModel().getStatusId() == ASSIGNED_REGION_STATUS
                        || rdm.getRegionModel().getStatusId() == SPLITTING_REGION_STATUS)
                .collect(Collectors.toList());
        result = zooContext.getMinimumNumberRdm(lostRdm, chosenRdms);
        return result;
    }

    /**
     * Tries to pick an RS that has no other replica of the region. Otherwise picks a random one.
     */
    public static RegionServerModel chooseRecoverRegionServer(ZooKeeperMasterContext zooContext, RegionDistributionModel lostRdm, List<RegionDistributionModel> tableRdms, List<RegionServerModel> aliveRS, List<RegionServerModel> runningRS) throws Exception {
        RegionServerModel regionServer;
        try {
            // If a replica of the region is already placed on this RS, try to pick another one if possible
            regionServer = zooContext.getRandomRunningRegionServerWithoutRegionForRecovery(
                    runningRS,
                    tableRdms,
                    lostRdm.getRegionModel());
        } catch (NotEnoughRegionServerInstances ex1) {
            try {
                // Otherwise pick a random one
                List<RegionServerModel> aliveRsWithout = new ArrayList<>(aliveRS);
                aliveRsWithout.remove(lostRdm.getRegionServerModel());
                List<RegionServerModel> targetRsList = getRegionServerWithMinRegion(tableRdms, aliveRsWithout);
                if (targetRsList.isEmpty()) {
                    LOGGER.warn("targetRsList is empty! tableRdms: {}, aliveRsWithout: {}, aliveRS: {}",
                            tableRdms, aliveRsWithout, aliveRS);
                    targetRsList = aliveRsWithout.isEmpty() ? aliveRS : aliveRsWithout;
                }
                regionServer = zooContext.getRandomRegionServer(targetRsList);
            } catch (Exception ex2) {
                LOGGER.error("Unable to choose destination region server for region recovery", ex2);
                throw ex2;
            }
        }
        return regionServer;
    }

    public static List<RegionServerModel> getRegionServerWithMinRegion(List<RegionDistributionModel> tableRdms, List<RegionServerModel> aliveRsWithout) throws NotEnoughRegionServerInstances {
        // Region distribution across RS, without empty RS
        Map<RegionServerModel, Long> rsMap = tableRdms.stream()
                .filter(rdm -> aliveRsWithout.contains(rdm.getRegionServerModel()))
                .collect(Collectors.groupingBy(RegionDistributionModel::getRegionServerModel, Collectors.counting()));
        // Add empty RS
        aliveRsWithout.forEach(rs -> rsMap.computeIfAbsent(rs, a -> 0L));
        // Find the minimum number of regions on available RS
        Long minRegionCount = rsMap.entrySet().stream()
                .filter(rsm -> aliveRsWithout.contains(rsm.getKey()))
                .mapToLong(rsm -> rsm.getValue())
                .min().orElseThrow(() -> new NotEnoughRegionServerInstances("Unable to find alive RS", 0));
        // Find the RS with the minimum number of regions among available RS
        List<RegionServerModel> targetRsList = rsMap.entrySet().stream()
                .filter(rsm -> rsm.getValue().equals(minRegionCount))
                .map(rsm -> rsm.getKey())
                .collect(Collectors.toList());
        return targetRsList;
    }

    private static boolean ordinaryRecoverRegionStatus(int regionStatus) {
        return regionStatus == CREATED_REGION_STATUS
                || regionStatus == ASSIGNED_REGION_STATUS
                || regionStatus == ENABLING_REGION_STATUS
                || regionStatus == ENABLED_REGION_STATUS
                || regionStatus == DISABLING_REGION_STATUS
                || regionStatus == DISABLED_REGION_STATUS;
    }

    public static List<RegionDistributionModel> processProjection(TableName tableName, List<RegionRecoveryModel> recoveryModels, List<RegionDistributionModel> tableRdms) throws Exception {
        Set<RegionDistributionModel> projection = new HashSet<>(tableRdms);
        LOGGER.trace("Recovering table '{}': zookeeper distribution is: {}", tableName, tableRdms);
        /* Remove the replicas that are really lost */
        recoveryModels.stream()
                .filter(rm -> !rm.isSuddenlyAlive()) // The ones that came back must not be removed
                .map(RegionRecoveryModel::getLostRdm)
                .filter(Objects::nonNull)
                .forEach(projection::remove);
        /* Remove the projections of the replicas that came back */
        recoveryModels.stream()
                .filter(RegionRecoveryModel::isSuddenlyAlive) // The projection of the ones that came back must be removed
                .map(RegionRecoveryModel::getRecoveryRdm)
                .forEach(projection::remove);
        /* Update the target replicas. First remove the existing ones by regionId + status */
        recoveryModels.stream()
                .filter(rm -> !rm.isSuddenlyAlive()) // The projection of the ones that came back must not be added
                .map(RegionRecoveryModel::getRecoveryRdm)
                .map(RegionDistributionModel::getRegionModel)
                .map(rm -> rm.toIdentityString() + "_" + rm.getStatusId())
                .forEach(recoveryIdWithStatus ->
                        projection.removeIf(r -> {
                            String regIdWithStatus = r.getRegionModel().toIdentityString() + "_" + r.getRegionModel().getStatusId();
                            return regIdWithStatus.equals(recoveryIdWithStatus);
                        }));
        recoveryModels.stream()
                .filter(rm -> !rm.isSuddenlyAlive()) // The projection of the ones that came back must not be added
                .map(RegionRecoveryModel::getRecoveryRdm)
                .filter(recoveryRdm -> !projection.contains(recoveryRdm)) // Protect against duplicates on restart
                .forEach(projection::add);
        LOGGER.trace("Recovering table '{}': processed distribution is: {}", tableName, projection);
        checkDuplicates(tableName, projection);
        return new ArrayList<>(projection);
    }

    /**
     * Checks the given projection for duplicate replicas.
     */
    public static void checkDuplicates(TableName tableName, Set<RegionDistributionModel> projection) throws Exception {
        Set<String> identities = new HashSet<>();
        for (RegionDistributionModel rdm : projection) {
            if (identities.contains(rdm.getRegionModel().toIdentityString())) {
                LOGGER.warn("Recovering table '{}': problematic projection is: {}", tableName, projection);
                throw new Exception(String.format(
                        "found multiply replica copies for %s. please, check the logs", rdm.getRegionModel().toIdentityString()
                ));
            }
            identities.add(rdm.getRegionModel().toIdentityString());
        }
    }

    private boolean chooseRetryMode(TableName tableName, boolean analyze) throws Exception {
        ZookeeperStatus tableStatus;
        try {
            tableStatus = zooContext.getTableStatus(tableName);
        } catch (Exception ex) {
            LOGGER.warn("unable to read table status", ex);
            throw new NoRecoveryNeeded();
        }
        switch (tableStatus) {
            case CREATING:
            case DROPPING:
                throw new NoRecoveryNeeded();
            case RECOVERING:
                return true;
            case CREATED:
            case ENABLED:
            case ENABLING:
            case ASSIGNED:
            case DISABLING:
            case DISABLED:
            case SPLITTING:
            case MODIFYING:
                return false;
            case ASSIGNING:
                if (analyze) {
                    return false;
                }
            default:
                throw new NoRetryException("unexpected table status: " + tableStatus);
        }
    }

    private static int getReferencedStatusIfNeeded(RegionModel regionModel) {
        int statusId = regionModel.getStatusId();
        if (isReferencingRegionStatus(statusId)) {
            return getReferencedRegionStatus(regionModel);
        }
        return statusId;
    }

    public boolean isNeedToRecover(TableName tableName) throws Exception {
        try {
            chooseRetryMode(tableName, true);
            return true;
        } catch (NoRecoveryNeeded ex) {
            return false;
        }
    }

    /**
     * Split operation state used by recovery.
     */
    public class SplitContext {
        private final List<RegionSplitModel> splitModels;
        private final SplitTable.RetryMode retryMode;

        private final List<RegionDistributionModel> splittingModels;
        private final List<RegionDistributionModel> splittedModels;

        private SplitContext() {
            this.splitModels = new ArrayList<>();
            this.retryMode = null;
            this.splittingModels = new ArrayList<>();
            this.splittedModels = new ArrayList<>();
        }

        private SplitContext(TableName tableName, List<RegionDistributionModel> tableRdms) throws Exception {
            this.retryMode = splitTable.chooseRetryMode(tableName, tableRdms);
            LOGGER.info("Recovering table '{}': split retry mode is {}", tableName, retryMode);
            LOGGER.info("Recovering table '{}': reading split models", tableName);
            this.splitModels = splitTable.findSplitModels(tableName, tableRdms, retryMode, Collections.emptySet(), new RegionSplitNameGenerator(), "Recovering table");
            LOGGER.info("Recovering table '{}': split models are {}", tableName, splitModels);
            this.splittingModels = splitModels.stream()
                    .map(RegionSplitModel::getSplitting)
                    .collect(Collectors.toList());
            this.splittedModels = splitModels.stream()
                    .map(RegionSplitModel::getSplitted)
                    .collect(Collectors.toList());
        }

        public List<RegionSplitModel> getSplitModels() {
            return splitModels;
        }

        public SplitTable.RetryMode getRetryMode() {
            return retryMode;
        }

        // Whether the region is the "low" part of a split
        public boolean isSplittingModel(RegionDistributionModel region) {
            return splittingModels.stream()
                    .map(rdm -> rdm.getRegionModel().toIdentityString())
                    .anyMatch(rdm -> rdm.equals(region.getRegionModel().toIdentityString()));
        }

        // Whether the region is the "high" part of a split
        public boolean isSplittedModel(RegionDistributionModel region) {
            return splittedModels.stream()
                    .map(rdm -> rdm.getRegionModel().toIdentityString())
                    .anyMatch(rdm -> rdm.equals(region.getRegionModel().toIdentityString()));
        }

        // Whether the region takes part in a split
        public boolean isSplitting(RegionDistributionModel region) {
            if (region == null) {
                return false;
            }
            return isSplittingModel(region) || isSplittedModel(region);
        }
    }

    private static class NoRecoveryNeeded extends Exception {
    }
}
