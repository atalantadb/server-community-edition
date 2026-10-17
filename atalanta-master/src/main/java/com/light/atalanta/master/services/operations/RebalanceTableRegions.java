/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations;

import com.light.atalanta.TableName;
import com.light.atalanta.master.services.Master;
import com.light.atalanta.master.services.rebalance.RebalanceBlueprint;
import com.light.atalanta.master.services.rebalance.RegionRebalanceModel;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZookeeperHelper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.light.atalanta.master.services.helpers.TableCandidateSelector.checkTableStatus;

/**
 * Rebalances all replicas within the table.
 * Not to be confused with RebalanceTableReplicas, which rebalances replicas within each region of the table separately.
 */
@SuppressFBWarnings("SF_SWITCH_FALLTHROUGH")
public class RebalanceTableRegions {
    private static final Logger LOGGER = LoggerFactory.getLogger(RebalanceTableRegions.class);

    private final ZooKeeperMasterContext zooContext;

    private final ZookeeperHelper zooHelper;

    private final Master master;

    public RebalanceTableRegions(ZooKeeperMasterContext zooContext, ZookeeperHelper zooHelper, Master master) {
        this.zooContext = zooContext;
        this.zooHelper = zooHelper;
        this.master = master;
    }

    public void action(TableName tableName, List<RegionRebalanceModel> rebalanceModels) throws OperationException {
        try {
            retryableAction(tableName, rebalanceModels);
        } catch (Exception ex) {
            throw new OperationException("error rebalancing table regions: '" + tableName + "'", ex);
        }
    }

    private void retryableAction(TableName tableName, List<RegionRebalanceModel> rebalanceModels) throws Exception {
        LOGGER.info("Start rebalancing table regions: '{}'", tableName);
        LOGGER.debug("Rebalancing table regions: '{}'. Rebalancing plan is: {}", tableName, rebalanceModels);
        // If a stop of the table rebalancing was requested
        if (master.getOperationRunner().checkTableRegionsRebalanceGracefulStopped(tableName)) {
            LOGGER.warn("Rebalancing table regions '{}': process was stopped on request", tableName);
            return;
        }
        checkTableStatus(zooHelper, tableName);
        // Run each planned region move
        // Check for a stop request on every iteration
        for (RegionRebalanceModel rebalanceModel : rebalanceModels) {
            if (master.getOperationRunner().checkTableRegionsRebalanceGracefulStopped(tableName)) {
                LOGGER.warn("Rebalancing table regions '{}': process was stopped on request", tableName);
                return;
            }
            String regionId = rebalanceModel.getRegionId();
            String rsSrc = rebalanceModel.getRsSrc();
            String rsDst = rebalanceModel.getRsDst();
            try {
                LOGGER.info("Rebalancing table regions '{}': start moving replica '{}' to '{}'. assumed location: {}", tableName, regionId, rsDst, rsSrc);
                // The actual replica move
                master.getMoveRegionInstance().action(tableName, Collections.singletonList(rebalanceModel));
                LOGGER.info("Rebalancing table regions '{}': finish moving replica '{}' to '{}'", tableName, regionId, rsDst);
            } catch (Throwable ex) {
                if (containsInterrupted(ex)) {
                    LOGGER.error("Rebalancing table regions '{}': unable to rebalance replica '{}' to '{}': interrupted", tableName, regionId, rsDst, ex);
                    throw ex;
                }
                LOGGER.error("Rebalancing table regions '{}': unable to rebalance replica '{}' to '{}'", tableName, regionId, rsDst, ex);
            }
        }
        // Reset the rebalance stop flag: if we could have stopped, we would have already
        master.getOperationRunner().removeTableFromGracefulStopRegionsRebalance(tableName);
        LOGGER.info("Done rebalancing table regions: '{}'", tableName);
    }

    /**
     * Plans the rebalancing by running the analysis algorithm.
     * Takes a projection of regions whose replicas are already balanced.
     */
    public RebalanceBlueprint computeBlueprint(List<TableName> tableNames, RebalanceBlueprint replicasBlueprint) throws Exception {
        LOGGER.info("Start computing regions rebalance plan based on rebalanced replicas plan");
        // Start rebalanceModels from the moves of the replica rebalance plan
        Map<TableName, List<RegionRebalanceModel>> rebalanceModels = new HashMap<>(replicasBlueprint.getRebalanceModels());
        Map<TableName, List<RegionDistributionModel>> rebalanceProjection = new HashMap<>();
        for (TableName tableName : tableNames) {
            LOGGER.info("Computing table regions rebalance for '{}'", tableName);
            checkTableStatus(zooHelper, tableName);
            // Read the distribution from the projection of balanced replicas of each region, not from ZooKeeper
            List<RegionDistributionModel> projection = new ArrayList<>(replicasBlueprint.getProjection().get(tableName));
            rebalanceProjection.put(tableName, projection);
            LOGGER.info("Computing table regions rebalance '{}': distribution is: {}", tableName, projection);
            List<RegionServerModel> runningRS = zooContext.getRunningRegionServers();
            boolean changesMade = true;
            RegionServerModel prevMaxRS = null;
            // Exit criteria:
            // 1. The previous iteration made no moves
            // 2. The next iteration of the loop found the same RS_max
            while (true) {
                Map<RegionServerModel, Integer> stats = new HashMap<>();
                // Count the replicas of this table (all of them, not only of the current region) on each running RS
                projection.stream()
                        .filter(it -> runningRS.contains(it.getRegionServerModel()))
                        .forEach(it ->
                                stats.put(it.getRegionServerModel(), stats.getOrDefault(it.getRegionServerModel(), 0) + 1)
                        );
                // Add running RS that have no replicas
                runningRS.stream().filter(rs -> !stats.containsKey(rs)).forEach(rs -> stats.put(rs, 0));
                // Find the best (target) and the worst (source) RS with the comparison logic described above
                Optional<Map.Entry<RegionServerModel, Integer>> minRS = stats.entrySet().stream().min(Map.Entry.comparingByValue());
                Optional<Map.Entry<RegionServerModel, Integer>> maxRS = stats.entrySet().stream().max(Map.Entry.comparingByValue());
                // Skip rebalancing if there is no minimum or maximum RS
                if (!minRS.isPresent() || !maxRS.isPresent() || minRS.equals(maxRS)) {
                    LOGGER.warn("Computing table regions rebalance '{}': finish rebalance because no min or max RS found", tableName);
                    break;
                }
                // Check the loop exit condition
                if (prevMaxRS != null && prevMaxRS.equals(maxRS.get().getKey()) && !changesMade) {
                    LOGGER.debug("Computing table regions rebalance '{}': finish rebalance because no more changes can be applied", tableName);
                    break;
                }
                prevMaxRS = maxRS.get().getKey();
                changesMade = false;
                LOGGER.debug("Computing table regions rebalance '{}': min RS is: {}, table replicas count: {}",
                        tableName, minRS.get().getKey(), minRS.get().getValue()
                );
                LOGGER.debug("Computing table regions rebalance '{}': max RS is: {}, table replicas count: {}",
                        tableName, maxRS.get().getKey(), maxRS.get().getValue()
                );
                // Find the table replicas on maxRS
                List<RegionDistributionModel> rdms = projection.stream()
                        .filter(rdm -> rdm.getRegionServerModel().equals(maxRS.get().getKey()))
                        .collect(Collectors.toList());
                // And iterate over them
                for (RegionDistributionModel rdm : rdms) {
                    String regionId = rdm.getRegionModel().toIdentityString();
                    // Check whether the target RS has any replicas of this region
                    long count = projection.stream()
                            .filter(it -> it.getRegionServerModel().equals(minRS.get().getKey()))
                            .filter(it -> it.getRegionModel().getName().equals(rdm.getRegionModel().getName()))
                            .count();
                    if (count > 0) {
                        LOGGER.debug("Computing table regions rebalance '{}': skipping {} because region replica(s) already exist on target RS", tableName, regionId);
                        continue;
                    }
                    // Skip rebalancing if the difference between the maximum and minimum RS is 0 or 1 in terms of replicas of the current table
                    if (maxRS.get().getValue() - minRS.get().getValue() < 2) {
                        LOGGER.debug("Computing table regions rebalance '{}': skipping {} rebalance because there is no required difference in replica counts", tableName, regionId);
                        continue;
                    }
                    // Before planning a new replica move,
                    // check whether we have already moved this replica.
                    // Without this check we can generate several moves of the same replica:
                    // rs1 -> rs2 -> rs3 instead of rs1 -> rs3.
                    rebalanceModels.compute(tableName, (table, outcome) -> {
                        if (outcome == null) {
                            outcome = new ArrayList<>();
                        }
                        Optional<RegionRebalanceModel> rebalanceModel = outcome.stream().filter(it -> it.getRegionId().equals(regionId)).findAny();
                        if (rebalanceModel.isPresent()) {
                            // If we already planned to move this replica, update the target RS
                            LOGGER.warn("Computing table regions rebalance '{}': found existing planned move for {}. updating target RS to {}",
                                    tableName, regionId, minRS.get().getKey()
                            );
                            rebalanceModel.get().updateRsDst(minRS.get().getKey().getName());
                        } else {
                            // Otherwise create a new move
                            LOGGER.debug("Computing table regions rebalance '{}': will move {} from {} to {}",
                                    tableName, regionId, maxRS.get().getKey().getName(), minRS.get().getKey().getName()
                            );
                            outcome.add(new RegionRebalanceModel(
                                    regionId, maxRS.get().getKey().getName(), minRS.get().getKey().getName()
                            ));
                        }
                        return outcome;
                    });
                    projection.set(projection.indexOf(rdm), new RegionDistributionModel(rdm.getRegionModel(), minRS.get().getKey()));
                    LOGGER.debug("Computing table regions rebalance '{}': projection is: {}", tableName, projection);
                    changesMade = true;
                    // Back to the main loop to find RS_max again
                    break;
                }
            }
        }
        return new RebalanceBlueprint(rebalanceModels, rebalanceProjection);
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
