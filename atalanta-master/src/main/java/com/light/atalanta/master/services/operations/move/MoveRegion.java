/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations.move;

import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.NoRetryException;
import com.light.atalanta.exceptions.TableBlockedException;
import com.light.atalanta.exceptions.TableNotExistsException;
import com.light.atalanta.master.exceptions.UnavailableRsException;
import com.light.atalanta.master.services.grpc.RegionServerConnector;
import com.light.atalanta.master.services.grpc.RsHealthChecker;
import com.light.atalanta.master.services.operations.OperationException;
import com.light.atalanta.master.services.rebalance.RegionRebalanceModel;
import com.light.atalanta.master.services.recovery.RecoveryRSContext;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.util.ZookeeperStatus;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.light.atalanta.services.ZooKeeperContextHelper.*;

/**
 * regionIdentity is regionName_replicaNum.
 * regionServerName is the name of the target region server.
 */
@SuppressFBWarnings("SF_SWITCH_FALLTHROUGH")
public class MoveRegion {
    private static final Logger LOGGER = LoggerFactory.getLogger(MoveRegion.class);

    private final AtalantaDBConfiguration conf;
    private final ZooKeeperMasterContext zooContext;
    private final ZookeeperHelper zooHelper;
    private final RegionServerConnector regionServerConnector;
    private final RecoveryRSContext recoveryRSContext;
    private final ExecutorService executorService;

    public MoveRegion(AtalantaDBConfiguration conf, ZooKeeperMasterContext zooContext, ZookeeperHelper zooHelper, RegionServerConnector regionServerConnector, RecoveryRSContext recoveryRSContext, ExecutorService executorService) {
        this.zooContext = zooContext;
        this.zooHelper = zooHelper;
        this.conf = conf;
        this.regionServerConnector = regionServerConnector;
        this.recoveryRSContext = recoveryRSContext;
        this.executorService = executorService;
    }

    public void action(TableName tableName, List<RegionRebalanceModel> moveModels) throws OperationException {
        int retryCount = 1;
        boolean forceRollback = false;
        ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers = new ConcurrentHashMap<>();
        Set<RegionServerModel> movingRS = new HashSet<>();
        try {
            while (true) {
                try {
                    retryableAction(tableName, moveModels, movingRS, pingCheckers, forceRollback);
                    return;
                } catch (UnavailableRsException ex) {
                    forceRollback = true;
                } catch (NoRetryException | InterruptedException ex) {
                    LOGGER.error("No retry error moving table regions '{}'", tableName, ex);
                    throw new OperationException(ex);
                } catch (Throwable ex) {
                    if (containsInterrupted(ex)) {
                        LOGGER.error("Error moving table regions '{}': interrupted", tableName, ex);
                        throw new OperationException(ex);
                    }
                    LOGGER.error("Error moving table regions '{}'. Wait {} ms and retrying #{}...", tableName, conf.getDataConfiguration().getMoveRegionRetryDelayMs(), retryCount, ex);
                    try {
                        Thread.sleep(conf.getDataConfiguration().getMoveRegionRetryDelayMs());
                    } catch (InterruptedException interrupted) {
                        LOGGER.error("Error moving table regions '{}': interrupted", tableName, ex);
                        Thread.currentThread().interrupt();
                        throw new OperationException(ex);
                    }
                }
                retryCount++;
            }
        } finally {
            releaseRS(movingRS, tableName);
            pingCheckers.values().forEach(RsHealthChecker::close);
        }
    }

    private void retryableAction(TableName tableName, List<RegionRebalanceModel> moveModels, Set<RegionServerModel> movingRS, ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers, boolean forceRollback) throws Exception {
        LOGGER.info("Start moving table regions '{}', move models: {}", tableName, moveModels);

        MoveRegionContext context = buildMoveContext(tableName, moveModels, forceRollback);
        LOGGER.info("Moving table regions '{}': retry mode is {}", tableName, context.getRetryMode());
        if (context.getMoveProjections().isEmpty()) {
            LOGGER.info("Moving table regions '{}': move projections are empty, finishing", tableName);
            return;
        }

        LOGGER.info("Moving table regions '{}': move projections are {}", tableName, context.getMoveProjections());
        movingRS.addAll(getMovingRS(context));
        // We need to block RS from start first
        blockRS(context, movingRS);
        // Only when we are sure that all related RS will remain at their current states,
        // we can check whether they're running.
        // If it's the initial move run – we just return an error.
        if (context.getRetryMode() == RetryMode.ZERO_LEVEL) {
            checkRS(tableName, movingRS);
        }

        // We need to initialize ping checkers for all retry levels
        MovePreCommitHelper preCommitHelper = new MovePreCommitHelper(zooHelper, zooContext);
        if (!context.getRetryMode().isRollback()) {
            initializePingCheckers(movingRS, pingCheckers);
        }

        // Run the steps that match the retry level
        switch (context.getRetryMode()) {
            // Rollback operation
            case ROLLBACK_ZERO_LEVEL:
                LOGGER.info("Moving table regions '{}': zero level rollback checkpoint", tableName);
                throw new NoRetryException("rollback was performed, check the logs");
            case ROLLBACK_FIST_LEVEL:
                LOGGER.info("Moving table regions '{}': first level rollback checkpoint", tableName);
                dropTargetReplicas(context);
                assignSourcesIfNeeded(context);
                startLeadersIfNeeded(context, true);
                preCommitHelper.rollbackPreCommits(context);
                throw new NoRetryException("rollback was performed, check the logs");
            case ROLLBACK_SECOND_LEVEL:
                // This ROLLBACK_SECOND_LEVEL stage doesn't differ from the SECOND_LEVEL
                // Just without RS controllers
                LOGGER.info("Moving table regions '{}': second level rollback checkpoint", tableName);
                dropSourceReplicas(context);
                startLeadersIfNeeded(context, false);
                preCommitHelper.makeFinishCommits(context);
                throw new NoRetryException("rollback was performed, check the logs");
                // Move operation
            case ZERO_LEVEL:
                LOGGER.info("Moving table regions '{}': zero level checkpoint", tableName);
                RsHealthChecker.checkRsErrors(pingCheckers);
                addPayloadedRS(context);
                preCommitHelper.makePreCommits(context);
            case FIST_LEVEL:
                LOGGER.info("Moving table regions '{}': first level checkpoint", tableName);
                RsHealthChecker.checkRsErrors(pingCheckers);
                stopLeadersIfNeeded(context);
                RsHealthChecker.checkRsErrors(pingCheckers);
                moveReplicas(context);
                RsHealthChecker.checkRsErrors(pingCheckers);
                preCommitHelper.makeDestCommits(context);
            case SECOND_LEVEL:
                LOGGER.info("Moving table regions '{}': second level checkpoint", tableName);
                RsHealthChecker.checkRsErrors(pingCheckers);
                dropSourceReplicas(context);
                RsHealthChecker.checkRsErrors(pingCheckers);
                startLeadersIfNeeded(context, false);
                preCommitHelper.makeFinishCommits(context);
                break;
            default:
                throw new Exception("unknown retry mode: " + context.getRetryMode());
        }
        LOGGER.info("Moving table regions '{}': finish", tableName);
    }

    private void addPayloadedRS(MoveRegionContext context) {
        context.getMoveProjections().forEach(
                mp -> recoveryRSContext.addPayloaded(mp.getTarget().getRegionServerModel())
        );
    }

    // TODO: Consider stopping all leaders instead
    private void stopLeadersIfNeeded(MoveRegionContext context) throws Exception {
        LOGGER.info("Moving table regions '{}': searching leaders to stop", context.getTableName());
        Set<RegionDistributionModel> leaders = findLeadersToStop(context);
        if (leaders.isEmpty()) {
            LOGGER.info("Moving table regions '{}': leaders to stop are not found", context.getTableName());
            return;
        }
        LOGGER.info("Moving table regions '{}': leaders to stop are: {}", context.getTableName(), leaders);
        CountDownLatch latch = new CountDownLatch(leaders.size());
        AtomicBoolean error = new AtomicBoolean();

        leaders.forEach(leader -> executorService.submit(() -> {
            try {
                String stopLeaderLogStr = String.format("Moving table regions '%s': stopping leader: '%s'", context.getTableName(), leader.getRegionModel().toIdentityString());
                regionServerConnector.monitor(context.getTableName(), stopLeaderLogStr, leader, StringUtils.EMPTY, (stub, req, stream) ->
                        stub.get().stopRegion(req, stream), false
                );
            } catch (Throwable ex) {
                LOGGER.error("Moving table regions '{}': unable to stop leader: '{}'", context.getTableName(), leader.getRegionModel().toIdentityString(), ex);
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

    private void moveReplicas(MoveRegionContext context) throws Exception {
        CountDownLatch latch = new CountDownLatch(context.getMoveProjections().size());
        AtomicBoolean error = new AtomicBoolean();

        for (RegionMoveProjection rmp : context.getMoveProjections()) {
            RegionDistributionModel source = rmp.getSource();
            RegionDistributionModel target = rmp.getTarget();
            executorService.submit(() -> {
                try {
                    String sourceRsAddr = source.getRegionServerModel().getHost() + ":" + source.getRegionServerModel().getJavaPort();
                    String reservedReplicasIfNeeded = StringUtils.EMPTY;
                    if (source.getRegionModel().getReplicaNum() == 0) {
                        reservedReplicasIfNeeded = context.computeReserved(source);
                        LOGGER.info("Moving table regions '{}': reserved replicas projection for '{}' is: {}", context.getTableName(), source.getRegionModel().getName(), reservedReplicasIfNeeded);
                    }
                    String additionalInfo = sourceRsAddr + "@" + reservedReplicasIfNeeded;
                    String moveReplicaLogStr = String.format("Moving table regions '%s': moving replica: '%s' to '%s'", context.getTableName(), source.getRegionModel().toIdentityString(), target.getRegionServerModel());
                    regionServerConnector.monitor(context.getTableName(), moveReplicaLogStr, target, additionalInfo, (stub, req, stream) ->
                            stub.get().moveRegion(req, stream), false
                    );
                } catch (Throwable ex) {
                    LOGGER.error("Moving table regions '{}': unable to move replica: '{}' to '{}'", context.getTableName(), source.getRegionModel().toIdentityString(), target.getRegionServerModel(), ex);
                    error.set(true);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        if (error.get()) {
            throw new Exception("found errors while moving replicas. Please, check the logs");
        }

    }

    // We ignore error for the start leader command for region servers that are offline, since
    // they will assign those leaders when they start.
    // They will wait until the move is done, since we block such RS from restarts during move operation
    // Retry infinite number of times if RS is up.
    private void startLeadersIfNeeded(MoveRegionContext context, boolean rollback) throws Exception {
        LOGGER.info("Moving table regions '{}': searching leaders to start", context.getTableName());
        Set<RegionDistributionModel> leaders = findLeadersToStart(context);
        if (leaders.isEmpty()) {
            LOGGER.info("Moving table regions '{}': leaders to start are not found", context.getTableName());
            return;
        }
        LOGGER.info("Moving table regions '{}': leaders to start are: {}", context.getTableName(), leaders);
        CountDownLatch latch = new CountDownLatch(leaders.size());
        AtomicBoolean error = new AtomicBoolean();

        leaders.forEach(leader -> executorService.submit(() -> {
            try {
                int retry = 0;
                while (true) {
                    // It's enough to check just running region servers
                    // since RS will be waiting for the move to complete!
                    List<RegionServerModel> runningRS = getRunningRegionServers();
                    try {
                        String reservedReplicasIfNeeded;
                        if (rollback) {
                            reservedReplicasIfNeeded = context.computeReservedBefore(leader);
                        } else {
                            reservedReplicasIfNeeded = context.computeReserved(leader);
                        }
                        LOGGER.info("Moving table regions '{}': reserved replicas projection for '{}' is: {}", context.getTableName(), leader.getRegionModel().getName(), reservedReplicasIfNeeded);
                        String startLeaderLogStr = String.format("Moving table regions '%s': starting leader: '%s'", context.getTableName(), leader.getRegionModel().toIdentityString());

                        regionServerConnector.monitor(context.getTableName(), startLeaderLogStr, leader, reservedReplicasIfNeeded, (stub, req, stream) ->
                                stub.get().startRegion(req, stream), false
                        );
                        break;
                    } catch (TableBlockedException ex) {
                        LOGGER.error("Moving table regions '{}': error starting leader: '{}'", context.getTableName(), leader.getRegionModel().toIdentityString(), ex);
                        error.set(true);
                        break;
                    } catch (Throwable ex) {
                        if (!runningRS.contains(leader.getRegionServerModel())) {
                            LOGGER.warn("Moving table regions '{}': unable to start leader: '{}', seems like RS '{}' is not in the RUNNING status. skipping this leader replica start", context.getTableName(), leader.getRegionModel().toIdentityString(), leader.getRegionServerModel().getName(), ex);
                            break;
                        } else {
                            LOGGER.error("Moving table regions '{}': unable to start leader: '{}'. waiting 5000 ms and retry #{} ...", context.getTableName(), leader.getRegionModel().toIdentityString(), ++retry, ex);
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException e) {
                                LOGGER.error("Moving table regions '{}': start leader: '{}' interrupted", context.getTableName(), leader.getRegionModel().toIdentityString());
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

    private void assignSourcesIfNeeded(MoveRegionContext context) throws Exception {
        List<RegionDistributionModel> sources = context.getMoveProjections().stream()
                .map(RegionMoveProjection::getSource)
                .collect(Collectors.toList());
        LOGGER.info("Moving table regions '{}': sources to assign are: {}", context.getTableName(), sources);
        List<RegionServerModel> runningRS = zooContext.getRunningRegionServers();
        CountDownLatch latch = new CountDownLatch(sources.size());
        AtomicBoolean error = new AtomicBoolean();

        sources.forEach(source -> executorService.submit(() -> {
            try {
                String reservedReplicasIfNeeded = StringUtils.EMPTY;
                if (source.getRegionModel().getReplicaNum() == 0) {
                    reservedReplicasIfNeeded = context.computeReservedBefore(source);
                    LOGGER.info("Moving table regions '{}': reserved replicas projection for '{}' is: {}", context.getTableName(), source.getRegionModel().getName(), reservedReplicasIfNeeded);
                }
                String startLeaderLogStr = String.format("Moving table regions '%s': assigning source: '%s'", context.getTableName(), source.getRegionModel().toIdentityString());
                regionServerConnector.monitor(context.getTableName(), startLeaderLogStr, source, reservedReplicasIfNeeded, (stub, req, stream) ->
                        stub.get().distributeRegion(req, stream), false
                );
            } catch (Throwable ex) {
                if (!runningRS.contains(source.getRegionServerModel()) && context.getRetryMode().isRollback()) {
                    LOGGER.warn("Moving table regions '{}': unable to assign source: '{}', continue rollback", context.getTableName(), source.getRegionModel().toIdentityString(), ex);
                } else {
                    LOGGER.error("Moving table regions '{}': unable to assign source: '{}'", context.getTableName(), source.getRegionModel().toIdentityString(), ex);
                    error.set(true);
                }
            } finally {
                latch.countDown();
            }
        }));
        latch.await();
        if (error.get()) {
            throw new Exception("found errors while assigning sources. Please, check the logs");
        }

    }

    private void dropSourceReplicas(MoveRegionContext context) throws Exception {
        CountDownLatch latch = new CountDownLatch(context.getMoveProjections().size());
        AtomicBoolean error = new AtomicBoolean();
        List<RegionServerModel> runningRS = zooContext.getRunningRegionServers();

        for (RegionMoveProjection rmp : context.getMoveProjections()) {
            RegionDistributionModel source = rmp.getSource();
            executorService.submit(() -> {
                try {
                    String dropSourceLogStr = String.format("Moving table regions '%s': dropping source replica: '%s'", context.getTableName(), source.getRegionModel().toIdentityString());
                    regionServerConnector.monitor(context.getTableName(), dropSourceLogStr, source, StringUtils.EMPTY, (stub, req, stream) ->
                            stub.get().dropRegion(req, stream), false
                    );
                } catch (Throwable ex) {
                    if (!runningRS.contains(source.getRegionServerModel()) && context.getRetryMode().isRollback()) {
                        LOGGER.warn("Moving table regions '{}': unable to drop source replica: '{}', continue rollback", context.getTableName(), source.getRegionModel().toIdentityString(), ex);
                    } else {
                        LOGGER.error("Moving table regions '{}': unable to drop source replica: '{}'", context.getTableName(), source.getRegionModel().toIdentityString(), ex);
                        error.set(true);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        if (error.get()) {
            throw new Exception("found errors while dropping source replicas. Please, check the logs");
        }

    }

    private void dropTargetReplicas(MoveRegionContext context) throws Exception {
        CountDownLatch latch = new CountDownLatch(context.getMoveProjections().size());
        for (RegionMoveProjection rmp : context.getMoveProjections()) {
            RegionDistributionModel target = rmp.getTarget();
            executorService.submit(() -> {
                try {
                    String dropSourceLogStr = String.format("Moving table regions '%s': dropping target replica: '%s'", context.getTableName(), target.getRegionModel().toIdentityString());
                    regionServerConnector.monitor(context.getTableName(), dropSourceLogStr, target, StringUtils.EMPTY, (stub, req, stream) ->
                            stub.get().dropRegion(req, stream), false
                    );
                } catch (Throwable ex) {
                    LOGGER.warn("Moving table regions '{}': unable to drop target replica: '{}', continue rollback", context.getTableName(), target.getRegionModel().toIdentityString(), ex);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
    }

    private Set<RegionServerModel> getMovingRS(MoveRegionContext context) {
        Set<RegionServerModel> movingRS = new HashSet<>();
        for (RegionMoveProjection rmp : context.getMoveProjections()) {
            movingRS.add(rmp.getSource().getRegionServerModel());
            movingRS.add(rmp.getTarget().getRegionServerModel());
        }
        for (RegionDistributionModel rdm : findLeadersToStop(context)) {
            movingRS.add(rdm.getRegionServerModel());
        }
        for (RegionDistributionModel rdm : findLeadersToStart(context)) {
            movingRS.add(rdm.getRegionServerModel());
        }
        return movingRS;
    }

    private Set<RegionServerModel> blockRS(MoveRegionContext context, Set<RegionServerModel> movingRS) {
        movingRS.forEach(rs -> recoveryRSContext.addMoving(rs, context.getTableName()));
        return movingRS;
    }

    private void releaseRS(Set<RegionServerModel> movingRS, TableName tableName) {
        movingRS.forEach(rs -> recoveryRSContext.removeMoving(rs, tableName));
    }

    private MoveRegionContext buildMoveContext(TableName tableName, List<RegionRebalanceModel> moveModels, boolean forceRollback) throws Exception {
        LOGGER.info("Moving table regions '{}': initial table check", tableName);

        // Determined by three signs: the table status and the statuses of the src and dst replicas
        try {
            String zkTablePath = ZooKeeperContextHelper.getTablePath(tableName);
            boolean tableExists;
            try {
                tableExists = zooHelper.checkExists(zkTablePath);
            } catch (Exception ex) {
                throw new Exception("unable to check table exists", ex);
            }
            if (!tableExists) {
                throw new NoRetryException(new TableNotExistsException(tableName));
            }
            ZookeeperStatus tableStatus = ZookeeperStatus.fromString(zooHelper.getNotNullString(zkTablePath));
            switch (tableStatus) {
                case CREATING:
                    throw new NoRetryException("table is creating for now");
                case SPLITTING:
                    throw new NoRetryException("table is splitting for now");
                case MODIFYING:
                    throw new NoRetryException("table is modifying for now");
                case ENABLING:
                    throw new NoRetryException("table is enabling for now");
                case DISABLING:
                    throw new NoRetryException("table is disabling for now");
                case DISABLED:
                    throw new NoRetryException("table disabled. Enable it first");
                case DROPPING:
                    throw new NoRetryException("table is dropping for now");
                case RECOVERING:
                    throw new NoRetryException("table is recovering for now");
                case ASSIGNING:
                    // Move in progress
                    return buildAssigningTableContext(tableName, forceRollback);
                case ASSIGNED:
                case CREATED:
                case ENABLED:
                    // Move from scratch
                    return buildAssignedTableContext(tableName, moveModels, forceRollback);
                default:
                    throw new NoRetryException("unknown table status: " + tableStatus);
            }
        } catch (NoRetryException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new Exception("unable to perform initial table check", ex);
        }
    }

    private Set<RegionDistributionModel> findLeadersToStop(MoveRegionContext context) {
        return context.getRdms().stream()
                .filter(rdm -> rdm.getRegionModel().getReplicaNum() == 0)
                .filter(context::hasSameRegionName)
                .filter(context::notTarget)
                .collect(Collectors.toSet());
    }

    private Set<RegionDistributionModel> findLeadersToStart(MoveRegionContext context) {
        return context.getRdms().stream()
                .filter(rdm -> rdm.getRegionModel().getReplicaNum() == 0)
                .filter(context::hasSameRegionName)
                .filter(context::notSource)
                .filter(context::notTarget)
                .collect(Collectors.toSet());
    }

    // If the table is ASSIGNING, the move is in progress, with two possible cases:
    // 1. Source regions are UNASSIGNING, target regions are ASSIGNING
    // 2. Source regions are UNASSIGNING, target regions are ASSIGNED
    private MoveRegionContext buildAssigningTableContext(TableName tableName, boolean forceRollback) throws Exception {
        RetryMode retryMode = null;
        List<RegionMoveProjection> moveProjections = new ArrayList<>();
        List<RegionDistributionModel> rdms = zooContext.getRegionDistributionWithStatuses(tableName);
        for (RegionDistributionModel rdm : rdms) {
            if (rdm.getRegionModel().getStatusId() == UNASSIGNING_REGION_STATUS) {
                // The target region is ASSIGNING (case 1)
                Optional<RegionDistributionModel> assigning = findExactlyOneRegion(rdms,
                        rdm.getRegionModel().getName(), rdm.getRegionModel().getReplicaNum(),
                        ASSIGNING_REGION_STATUS
                );
                if (assigning.isPresent()) {
                    // Check that the statuses are consistent
                    if (retryMode != null && retryMode != RetryMode.FIST_LEVEL) {
                        throw new Exception("found UNASSIGNING/ASSIGNING pair, but retry mode is: " + retryMode);
                    }
                    retryMode = RetryMode.FIST_LEVEL;
                    moveProjections.add(new RegionMoveProjection(rdm, assigning.get()));
                }

                // The target region is ASSIGNED (case 2)
                Optional<RegionDistributionModel> assigned = findExactlyOneRegion(rdms,
                        rdm.getRegionModel().getName(), rdm.getRegionModel().getReplicaNum(),
                        ASSIGNED_REGION_STATUS
                );
                if (assigned.isPresent()) {
                    // Check that the statuses are consistent
                    if (retryMode != null && retryMode != RetryMode.SECOND_LEVEL) {
                        throw new Exception("found UNASSIGNING/ASSIGNED pair, but retry mode is: " + retryMode);
                    }
                    retryMode = RetryMode.SECOND_LEVEL;
                    moveProjections.add(new RegionMoveProjection(rdm, assigned.get()));
                }

                // Target region not found. Unexpected case.
                if (!assigning.isPresent() && !assigned.isPresent()) {
                    throw new Exception("unable to find ASSIGNING or ASSIGNED pair for: " + rdm.getRegionModel().toIdentityString());
                }
            }
        }
        if (retryMode == null) {
            throw new Exception("unable to compute retry mode");
        }
        // Consider rollback
        Exception exContainer = null;
        try {
            // Check whether operations for table are allowed
            recoveryRSContext.checkIncomeTableRequestsEnabled(tableName);
        } catch (TableBlockedException ex) {
            exContainer = ex;
        }
        if (exContainer != null || forceRollback) {
            switch (retryMode) {
                case FIST_LEVEL:
                    retryMode = RetryMode.ROLLBACK_FIST_LEVEL;
                    break;
                case SECOND_LEVEL:
                    retryMode = RetryMode.ROLLBACK_SECOND_LEVEL;
                    break;
                default:
                    throw new Exception("unexpected retry mode: " + retryMode.toString());
            }
        }
        return new MoveRegionContext(tableName, retryMode, moveProjections, zooContext)
                .withRdms(rdms)
                .withReplicationFactor(conf.getDataConfiguration().getReplicationFactor());
    }

    // If the table is ASSIGNED, we just find the RS by name and all regions by their identity: a move from scratch
    private MoveRegionContext buildAssignedTableContext(TableName tableName, List<RegionRebalanceModel> moveModels, boolean forceRollback) throws Exception {
        if (moveModels.isEmpty()) {
            throw new NoRetryException("no move operation is specified");
        }
        RetryMode retryMode;
        List<RegionDistributionModel> rdms = zooContext.getRegionDistributionWithStatuses(tableName);
        List<RegionMoveProjection> moveProjections = new ArrayList<>();
        try {
            // Consider rollback
            recoveryRSContext.checkIncomeTableRequestsEnabled(tableName);
            retryMode = RetryMode.ZERO_LEVEL;
            for (RegionRebalanceModel mm : moveModels) {
                String name = mm.getRegionId().split("_")[0];
                int replicaNum = Integer.parseInt(mm.getRegionId().split("_")[1]);
                // Find the source replica by the given identity and three possible statuses
                Optional<RegionDistributionModel> source = findExactlyOneRegion(rdms, name, replicaNum, ASSIGNED_REGION_STATUS, CREATED_REGION_STATUS, ENABLED_REGION_STATUS);
                if (!source.isPresent()) {
                    throw new NoRetryException(String.format("unable to find region %s in ASSIGNED, CREATED or ENABLED status", mm.getRegionId()));
                }
                // Find the RS by name
                RegionServerModel targetRegionServer = zooContext.findRunningRegionServerByName(mm.getRsDst());
                // If the region is already on the target RS, log and skip it
                if (source.get().getRegionServerModel().equals(targetRegionServer)) {
                    LOGGER.warn("Moving table regions '{}': region '{}' is already located on: '{}'", tableName, source.get().getRegionModel(), targetRegionServer);
                    continue;
                }
                // Create the projection of the target replica
                RegionDistributionModel target = zooContext.makeRdmCopy(source.get(), targetRegionServer, ASSIGNING_REGION_STATUS);
                moveProjections.add(new RegionMoveProjection(source.get(), target));
            }
        } catch (TableBlockedException ex) {
            retryMode = RetryMode.ROLLBACK_ZERO_LEVEL;
        }
        if (forceRollback) {
            retryMode = RetryMode.ROLLBACK_ZERO_LEVEL;
        }
        return new MoveRegionContext(tableName, retryMode, moveProjections, zooContext)
                .withRdms(rdms)
                .withReplicationFactor(conf.getDataConfiguration().getReplicationFactor());
    }

    private void checkRS(TableName tableName, Set<RegionServerModel> movingRS) throws NoRetryException {
        try {
            List<RegionServerModel> runningRS = zooContext.getRunningRegionServers();
            for (RegionServerModel rs : movingRS) {
                if (!runningRS.contains(rs)) {
                    throw new NoRetryException(String.format("RS '%s' is not running or not exists '%s'", rs, runningRS));
                }
            }
        } catch (NoRetryException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new NoRetryException(String.format("unable to check RS for table '%s'", tableName), ex);
        }
    }

    // Run RS monitoring threads
    private void initializePingCheckers(Set<RegionServerModel> movingRS, ConcurrentHashMap<RegionServerModel, RsHealthChecker> pingCheckers) {
        for (RegionServerModel rs : movingRS) {
            if (!pingCheckers.containsKey(rs)) {
                pingCheckers.put(rs, new RsHealthChecker(rs, conf.getGrpcConfiguration()));
            }
        }
    }

    private Optional<RegionDistributionModel> findExactlyOneRegion(List<RegionDistributionModel> rdms, String name, int replicaNum, int... statusIds) throws Exception {
        List<RegionDistributionModel> sourceCandidates = rdms.stream()
                .filter(it -> it.getRegionModel().getName().equals(name))
                .filter(it -> it.getRegionModel().getReplicaNum() == replicaNum)
                .filter(it -> ArrayUtils.contains(statusIds, it.getRegionModel().getStatusId()))
                .collect(Collectors.toList());
        // Either 0 or 1 is acceptable
        if (sourceCandidates.size() > 1) {
            throw new Exception(
                    String.format("unable to find exactly one region: name=%s, replicaNum=%s, statusId=%s", name, replicaNum, Arrays.toString(statusIds))
            );
        }
        return sourceCandidates.stream().findAny();
    }

    public enum RetryMode {
        // No retry (start from the beginning)
        ZERO_LEVEL,
        // Retry after pre-commits
        FIST_LEVEL,
        // Retry after the target region commit
        SECOND_LEVEL,
        // Rollback of the initial stage (cancel a move that hasn't started)
        ROLLBACK_ZERO_LEVEL,
        // Level 1 rollback
        ROLLBACK_FIST_LEVEL,
        // Level 2 rollback
        ROLLBACK_SECOND_LEVEL;

        public boolean isRollback() {
            return this == ROLLBACK_ZERO_LEVEL || this == ROLLBACK_FIST_LEVEL || this == ROLLBACK_SECOND_LEVEL;
        }
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
}
