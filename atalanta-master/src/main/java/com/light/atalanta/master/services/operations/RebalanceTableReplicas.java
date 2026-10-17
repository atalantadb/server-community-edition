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

import static com.light.atalanta.master.services.helpers.TableCandidateSelector.checkTableStatus;

/**
 * Rebalances replicas within each region of the table separately.
 * Not to be confused with RebalanceTableRegions, which rebalances all replicas within the table.
 */
@SuppressFBWarnings("SF_SWITCH_FALLTHROUGH")
public class RebalanceTableReplicas {
    private static final Logger LOGGER = LoggerFactory.getLogger(RebalanceTableReplicas.class);

    private final ZooKeeperMasterContext zooContext;

    private final ZookeeperHelper zooHelper;

    private final Master master;

    public RebalanceTableReplicas(ZooKeeperMasterContext zooContext, ZookeeperHelper zooHelper, Master master) {
        this.zooContext = zooContext;
        this.zooHelper = zooHelper;
        this.master = master;
    }

    public void action(TableName tableName, List<RegionRebalanceModel> rebalanceModels) throws OperationException {
        try {
            retryableAction(tableName, rebalanceModels);
        } catch (Exception ex) {
            throw new OperationException("error rebalancing table replicas: '" + tableName + "'", ex);
        }
    }

    private void retryableAction(TableName tableName, List<RegionRebalanceModel> rebalanceModels) throws Exception {
        LOGGER.info("Start rebalancing table replicas: '{}'", tableName);
        LOGGER.debug("Rebalancing table replicas: '{}'. Rebalancing plan is: {}", tableName, rebalanceModels);
        // If a stop of the table rebalancing was requested
        if (master.getOperationRunner().checkTableReplicasRebalanceGracefulStopped(tableName)) {
            LOGGER.warn("Rebalancing table replicas '{}': process was stopped on request", tableName);
            return;
        }
        checkTableStatus(zooHelper, tableName);
        // Run each planned region move
        // Check for a stop request on every iteration
        for (RegionRebalanceModel rebalanceModel : rebalanceModels) {
            if (master.getOperationRunner().checkTableReplicasRebalanceGracefulStopped(tableName)) {
                LOGGER.warn("Rebalancing table replicas '{}': process was stopped on request", tableName);
                return;
            }
            String regionId = rebalanceModel.getRegionId();
            String rsSrc = rebalanceModel.getRsSrc();
            String rsDst = rebalanceModel.getRsDst();
            try {
                LOGGER.info("Rebalancing table replicas '{}': start moving replica '{}' to '{}'. assumed location: {}", tableName, regionId, rsDst, rsSrc);
                // The actual replica move
                master.getMoveRegionInstance().action(tableName, Collections.singletonList(rebalanceModel));
                LOGGER.info("Rebalancing table replicas '{}': finish moving replica '{}' to '{}'", tableName, regionId, rsDst);
            } catch (Throwable ex) {
                if (containsInterrupted(ex)) {
                    LOGGER.error("Rebalancing table replicas '{}': unable to rebalance replica '{}' to '{}': interrupted", tableName, regionId, rsDst, ex);
                    throw ex;
                }
                LOGGER.error("Rebalancing table replicas '{}': unable to rebalance replica '{}' to '{}'", tableName, regionId, rsDst, ex);
            }
        }
        // Reset the rebalance stop flag: if we could have stopped, we would have already
        master.getOperationRunner().removeTableFromGracefulStopReplicasRebalance(tableName);
        LOGGER.info("Done rebalancing table replicas: '{}'", tableName);
    }

    /**
     * Plans the rebalancing by running the analysis algorithm.
     */
    public RebalanceBlueprint computeBlueprint(List<TableName> tableNames) throws Exception {
        LOGGER.info("Start computing replicas rebalance plan");
        Map<TableName, List<RegionRebalanceModel>> rebalanceModels = new HashMap<>();
        Map<TableName, List<RegionDistributionModel>> rebalanceProjection = new HashMap<>();
        for (TableName tableName : tableNames) {
            LOGGER.info("Computing table replicas rebalance for '{}'", tableName);
            checkTableStatus(zooHelper, tableName);
            List<RegionDistributionModel> tableProjection = zooContext.getRegionDistributionWithStatuses(tableName);
            rebalanceProjection.put(tableName, tableProjection);
            LOGGER.info("Computing table replicas rebalance '{}': distribution is: {}", tableName, tableProjection);
            List<RegionServerModel> runningRS = zooContext.getRunningRegionServers();
            while (true) {
                boolean changesMade = false;
                // Iterate over the table replicas
                for (RegionDistributionModel rdm : tableProjection) {
                    String regionName = rdm.getRegionModel().getName();
                    String regionId = rdm.getRegionModel().toIdentityString();
                    Map<RegionServerModel, Integer> stats = new HashMap<>();
                    // Count the replicas of the current region of this table on each running RS
                    tableProjection.stream()
                            .filter(it -> it.getRegionModel().getName().equals(regionName))
                            .filter(it -> runningRS.contains(it.getRegionServerModel()))
                            .forEach(it ->
                                    stats.put(it.getRegionServerModel(), stats.getOrDefault(it.getRegionServerModel(), 0) + 1)
                            );
                    // Add running RS that have no replicas
                    runningRS.stream().filter(rs -> !stats.containsKey(rs)).forEach(rs -> stats.put(rs, 0));
                    // Find RS max and RS min with the maximum and minimum number of replicas of the current region
                    Optional<Map.Entry<RegionServerModel, Integer>> minRS = stats.entrySet().stream().min(Map.Entry.comparingByValue());
                    Optional<Map.Entry<RegionServerModel, Integer>> maxRS = stats.entrySet().stream().max(Map.Entry.comparingByValue());
                    // Skip rebalancing if there is no minimum or maximum RS
                    if (!minRS.isPresent() || !maxRS.isPresent() || minRS.equals(maxRS)) {
                        LOGGER.warn("Computing table replicas rebalance '{}': finish rebalance because no min or max RS found", tableName);
                        continue;
                    }
                    LOGGER.debug("Computing table replicas rebalance '{}': min RS for {} is: {}, region replicas count: {}",
                            tableName, regionName, minRS.get().getKey(), minRS.get().getValue()
                    );
                    LOGGER.debug("Computing table replicas rebalance '{}': max RS for {} is: {}, region replicas count: {}",
                            tableName, regionName, maxRS.get().getKey(), maxRS.get().getValue()
                    );
                    // Skip rebalancing if the difference between the maximum and minimum RS is 0 or 1 in terms of replicas of the current table
                    if (maxRS.get().getValue() - minRS.get().getValue() < 2) {
                        LOGGER.debug("Computing table replicas rebalance '{}': skipping {} rebalance because there is no required difference in replica counts", tableName, regionId);
                        continue;
                    }
                    LOGGER.debug("Computing table replicas rebalance '{}': will move {} from {} to {}",
                            tableName, regionId, maxRS.get().getKey().getName(), minRS.get().getKey().getName()
                    );
                    RegionRebalanceModel rebalanceModel = new RegionRebalanceModel(
                            regionId, maxRS.get().getKey().getName(), minRS.get().getKey().getName()
                    );
                    rebalanceModels.compute(tableName, (table, outcome) -> {
                        if (outcome == null) {
                            outcome = new ArrayList<>();
                        }
                        outcome.add(rebalanceModel);
                        return outcome;
                    });
                    tableProjection.set(tableProjection.indexOf(rdm), new RegionDistributionModel(rdm.getRegionModel(), minRS.get().getKey()));
                    LOGGER.debug("Computing table replicas rebalance '{}': projection is: {}", tableName, tableProjection);
                    changesMade = true;
                }
                if (!changesMade) {
                    LOGGER.debug("Computing table replicas rebalance '{}': finish rebalance because no more changes can be applied", tableName);
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
