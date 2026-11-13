/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations;

import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.NoRetryException;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.master.services.operations.move.MoveRegion;
import com.light.atalanta.master.services.rebalance.RegionRebalanceModel;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.services.ZookeeperHelperExtended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.*;
import static com.light.atalanta.master.services.operations.RecoverTable.getRegionServerWithMinRegion;


public class StartStopRegionServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartStopRegionServer.class);

    private final ZookeeperHelperExtended zookeeperHelperExtended;
    private final ZooKeeperCoreContext zooContext;
    private final MoveRegion moveRegion;
    private final AtalantaDBConfiguration conf;

    private final ExecutorService executorService;

    public StartStopRegionServer(ZookeeperHelperExtended zookeeperHelperExtended,
                                 ZooKeeperCoreContext zooContext, MoveRegion moveRegion, AtalantaDBConfiguration conf,
                                 ExecutorService executorService) {
        this.zookeeperHelperExtended = zookeeperHelperExtended;
        this.zooContext = zooContext;
        this.moveRegion = moveRegion;
        this.conf  =conf;
        this.executorService = executorService;
    }

    public void action(String rs, boolean isStart) {
        try {
            LOGGER.info("==> Start: {},  RS: '{}'", isStart, rs);
            if (isStart) { //Start RS
                zookeeperHelperExtended.updateRsStatus(rs, REGION_SERVER_RUNNING);
                return;
            }
            // Stop RS
            boolean isDisabling = REGION_SERVER_DISABLING.equals(zookeeperHelperExtended.getRegionServerStatus(rs));
            if (isDisabling) {
                LOGGER.warn("RS {} is DISABLING", rs);
            }
            if (zookeeperHelperExtended.getAliveRegionServerIds().contains(rs) || isDisabling) {
                int rsCount = zooContext.getAliveRegionServers().size() + (isDisabling ? 1 : 0);
                if (rsCount < 2) {
                    LOGGER.error("No other RS! rsCount: {}", rsCount);
                    throw new NoRetryException("No other RS!");
                }
                zookeeperHelperExtended.updateRsStatus(rs, REGION_SERVER_DISABLING);
                moveAllRegionsFromRegionServer(rs);
                zookeeperHelperExtended.updateRsStatus(rs, REGION_SERVER_DISABLED);
            } else {
                LOGGER.error("Region server '{}' not started!");
            }
        } catch (Exception ex) {
            LOGGER.error("Cannot start/stop RS!", ex);
            LOGGER.info("<== Start: {},  RS: '{}', error msg: {}", isStart, rs, ex.getMessage());
            throw new RuntimeException(ex);
        } finally {
            LOGGER.info("<== Start: {},  RS: '{}'", isStart, rs);
        }
    }

    public void actionRecovery() {
        try {
            for (String rs : zookeeperHelperExtended.getDisablingRegionServerIds()) {
                moveAllRegionsFromRegionServer(rs);
            }
        } catch (Exception ex) {
            LOGGER.error("Cannot disable RS!", ex);
            throw new RuntimeException(ex);
        }
    }

    private void moveAllRegionsFromRegionServer(String rs) throws Exception {
        LOGGER.info("==> moveAllRegionsFromRegionServer({})", rs);
        RegionServerModel rsm = zooContext.getRegionServerModel(rs);
        Map<TableName, List<RegionDistributionModel>> allDistributions = zooContext.getAllDistributions();
        Map<TableName, List<RegionDistributionModel>> regions =
                zooContext.getRegionServerRegions(allDistributions, rsm);
        List<RegionDistributionModel> rdmList = regions.values().stream().flatMap(List::stream).collect(Collectors.toList());
        LOGGER.info("count regions to move: {}", rdmList.size());
        Map<TableName, List<RegionRebalanceModel>> moveRegionsMap = new HashMap<>();
        Map<TableName, List<RegionDistributionModel>> newDistribution = new HashMap<>();
        Map<TableName, List<RegionDistributionModel>> tablesDistrMap = new HashMap<>();
        for (int i = 0; i < rdmList.size(); ++i) {
            RegionDistributionModel rdm = rdmList.get(i);
            TableName table = zooContext.findTableByRegion(rdm.getRegionModel().getName(), allDistributions);
            LOGGER.info("calc target RS #{}, table {}, region {}", i, table, rdm);
            List<RegionDistributionModel> distributionForTable = tablesDistrMap.computeIfAbsent(table,
                    t -> {
                        try {
                            return zooContext.getRegionDistributionWithStatuses(t);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
            moveRegion(table, rdm, rs, distributionForTable, moveRegionsMap, newDistribution);
        }
        moveTables(rs, moveRegionsMap);
        LOGGER.info("<== moveAllRegionsFromRegionServer({})", rs);
    }

    private void moveTables(String rs, Map<TableName, List<RegionRebalanceModel>> moveRegionsMap) throws Exception {
        List<TableMove> pending = moveRegionsMap.entrySet().stream()
                .map(entry -> new TableMove(entry.getKey(), entry.getValue()))
                .sorted(Comparator.comparingLong(TableMove::slots).reversed()
                        .thenComparing(move -> move.table().toString()))
                .collect(Collectors.toList());
        // Use the actual operation pool, not the gRPC request pool. Unknown executors run one table at a time.
        int budget = executorService instanceof ThreadPoolExecutor pool ? pool.getMaximumPoolSize() : 1;
        BlockingQueue<MoveResult> completed = new LinkedBlockingQueue<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        List<Throwable> failures = new ArrayList<>();
        int next = 0;
        int running = 0;
        long reserved = 0;
        LOGGER.info("Moving tables from RS '{}': tables: {}, operation pool budget: {}", rs, pending.size(), budget);
        try {
            while (next < pending.size() || running > 0) {
                if (Thread.interrupted()) {
                    interrupted.set(true);
                }
                while (!interrupted.get() && next < pending.size()) {
                    TableMove move = pending.get(next);
                    // Preserve largest-first order. An oversized table is allowed to run on its own.
                    if (running > 0 && reserved + move.slots() > budget) {
                        break;
                    }
                    LOGGER.info("Scheduling move from RS '{}': table: {}, replicas: {}, reserved slots: {}/{}",
                            rs, move.table(), move.models().size(), reserved + move.slots(), budget);
                    try {
                        executorService.execute(() -> {
                            Throwable error = null;
                            try {
                                // Normal stop holds the global lock; acquiring table locks here would deadlock
                                moveRegion.action(move.table(), move.models());
                            } catch (Throwable ex) {
                                error = ex;
                            } finally {
                                completed.add(new MoveResult(move, error));
                            }
                        });
                    } catch (RuntimeException ex) {
                        failures.add(new IOException("Unable to schedule move for table: " + move.table(), ex));
                        next = pending.size();
                        break;
                    }
                    running++;
                    reserved += move.slots();
                    next++;
                    if (Thread.interrupted()) {
                        interrupted.set(true);
                    }
                }
                if (running == 0) {
                    break;
                }
                MoveResult result = awaitMove(completed, interrupted, failures);
                running--;
                reserved -= result.move().slots();
            }
        } finally {
            // Never release the caller's global lock while submitted table moves are still running.
            // Do not cancel them: cancelling a Future does not stop an operation on the RS.
            while (running > 0) {
                awaitMove(completed, interrupted, failures);
                running--;
            }
            if (interrupted.get()) {
                Thread.currentThread().interrupt();
            }
        }
        if (interrupted.get() || !failures.isEmpty()) {
            Exception error = interrupted.get()
                    ? new InterruptedException("Moving tables from RS '" + rs + "' interrupted")
                    : new IOException("Unable to move all tables from RS '" + rs + "'");
            failures.forEach(error::addSuppressed);
            throw error;
        }
    }

    private MoveResult awaitMove(BlockingQueue<MoveResult> completed, AtomicBoolean interrupted, List<Throwable> failures) {
        while (true) {
            try {
                MoveResult result = completed.take();
                if (result.error() != null) {
                    LOGGER.error("Unable to move regions for table '{}'", result.move().table(), result.error());
                    failures.add(new IOException("Unable to move regions for table: " + result.move().table(), result.error()));
                } else {
                    LOGGER.info("Finished moving regions for table '{}'", result.move().table());
                }
                return result;
            } catch (InterruptedException ex) {
                interrupted.set(true);
            }
        }
    }

    private record TableMove(TableName table, List<RegionRebalanceModel> models) {
        long slots() {
            // One coordinator waits for the per-replica tasks in the same operation pool
            return (long) models.size() + 1;
        }
    }

    private record MoveResult(TableMove move, Throwable error) {
    }

    private void moveRegion(TableName table, RegionDistributionModel rdm, String rs,
                            List<RegionDistributionModel> distributionForTable,
                            Map<TableName, List<RegionRebalanceModel>> moveRegionsMap,
                            Map<TableName, List<RegionDistributionModel>> newDistribution) throws Exception {
        // List the RS that host replicas of the region being moved,
        // excluding the region the data is moved from
        List<RegionServerModel> curRsList = distributionForTable.stream()
                .filter(curRdm -> curRdm.getRegionModel().getName().equals(rdm.getRegionModel().getName()))
                .map(curRdm -> rdm.getRegionServerModel())
                .filter(curRs -> !curRs.getName().equals(rs))
                .collect(Collectors.toList());
        // List the running RS, except those already hosting replicas of the region.
        // The data will be moved to one of these RS.
        List<RegionServerModel> aliveRsList = zooContext.getAliveRegionServers().stream()
                .filter(curRs -> !curRsList.contains(curRs))
                .collect(Collectors.toList());
        // If there are no free RS, use the ones already in use
        if (aliveRsList.isEmpty()) {
            aliveRsList = curRsList;
        }

        // If there are no regions available to move, cancel the RS decommission
        if (aliveRsList.isEmpty()) {
            zookeeperHelperExtended.updateRsStatus(rs, REGION_SERVER_RUNNING);
            LOGGER.error("Target RS not found!");
            throw new NoRetryException("Target RS not found!");
        }

        // Get the table region distribution before the moves
        List<RegionDistributionModel> regionDistribution = zooContext.getRegionDistributionWithStatuses(table)
                .stream()
                .filter(r -> !r.getRegionServerModel().equals(rs))
                .collect(Collectors.toList());

        // Take the moves into account
        List<RegionDistributionModel> newDistrlList = newDistribution.get(table);
        if (newDistrlList == null) {
            newDistrlList = new ArrayList<>();
            newDistribution.put(table, newDistrlList);
        }
        regionDistribution.addAll(newDistrlList);

        // RS least used by the table regions
        List<RegionServerModel> targetRsList = getRegionServerWithMinRegion(regionDistribution, aliveRsList);
        if (targetRsList.isEmpty()) {
            targetRsList = aliveRsList;
        }

        // Pick the target region
        Random random = new Random();
        RegionServerModel targetRs = targetRsList.get(random.nextInt(targetRsList.size()));

        List<RegionRebalanceModel> moveModelList = moveRegionsMap.get(table);
        if (moveModelList == null) {
            moveModelList = new ArrayList<>();
            moveRegionsMap.put(table, moveModelList);
        }
        String regionId = rdm.getRegionModel().getName() + "_" + rdm.getRegionModel().getReplicaNum();
        int copyReplica = (rdm.getRegionModel().getReplicaNum() + 1) % conf.getDataConfiguration().getReplicationFactor();

        String copyFromRs = getRegionHost(distributionForTable, rdm.getRegionModel().getName(), copyReplica, rdm);
        moveModelList.add(new RegionRebalanceModel(regionId, rs, targetRs.getName(), copyFromRs));
        newDistrlList.add(new RegionDistributionModel(rdm.getRegionModel(), targetRs));
        LOGGER.info("target RS: {}, region: {}", targetRs, rdm);
    }

    private String getRegionHost(List<RegionDistributionModel> distributionForTable, String copyReplicaRegionName,
                                 int replicaNum, RegionDistributionModel regionServerModel) {
        RegionServerModel rsm = distributionForTable.stream()
                .filter(r -> r.getRegionModel().getName().equals(copyReplicaRegionName)
                        && r.getRegionModel().getReplicaNum() == replicaNum)
                .findAny()
                .orElse(regionServerModel)
                .getRegionServerModel();
        return String.join(HOSTNAME_SEPARATOR, rsm.getHost(), String.valueOf(rsm.getJavaPort()));
    }

}
