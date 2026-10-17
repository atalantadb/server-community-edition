/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations;

import com.light.atalanta.TableName;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.master.services.grpc.RegionServerConnector;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.master.vo.GrpcConfiguration;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.util.ProtoConverters;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

/**
 * Runs the given operation with the given parameter.
 * Can stop it on request.
 */
public class OperationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(OperationRunner.class);

    private final GrpcConfiguration grpcConf;

    private final ZooKeeperMasterContext zooContext;

    private final RegionServerConnector regionServerConnector;

    private final Object EMPTY = new Object();

    // Tables with a pending request to stop rebalancing region replicas
    private final ConcurrentHashMap<TableName, Object> gacefulRebalanceReplicasStoppedTables;

    // Tables with a pending request to stop rebalancing regions
    private final ConcurrentHashMap<TableName, Object> gacefulRebalanceRegionsStoppedTables;

    public OperationRunner(AtalantaDBConfiguration conf, ZooKeeperMasterContext zooContext, RegionServerConnector regionServerConnector) {
        this.grpcConf = conf.getGrpcConfiguration();
        this.zooContext = zooContext;
        this.regionServerConnector = regionServerConnector;
        this.gacefulRebalanceReplicasStoppedTables = new ConcurrentHashMap<>();
        this.gacefulRebalanceRegionsStoppedTables = new ConcurrentHashMap<>();
    }

    /**
     * Adds the table to the list of tables whose region replica rebalancing must stop.
     */
    public void gracefulStopReplicasRebalance(List<TableName> tableNames) {
        tableNames.forEach(tn -> gacefulRebalanceReplicasStoppedTables.put(tn, EMPTY));
    }

    /**
     * Checks whether the table is in the list of tables whose region replica rebalancing must stop.
     * If it is, returns true and removes the table from the list.
     */
    public boolean checkTableReplicasRebalanceGracefulStopped(TableName tableName) {
        if (gacefulRebalanceReplicasStoppedTables.containsKey(tableName)) {
            gacefulRebalanceReplicasStoppedTables.remove(tableName);
            return true;
        }
        return false;
    }

    public void removeTableFromGracefulStopReplicasRebalance(TableName tableName) {
        gacefulRebalanceReplicasStoppedTables.remove(tableName);
    }

    /**
     * Adds the table to the list of tables whose region rebalancing must stop.
     */
    public void gracefulStopRegionsRebalance(List<TableName> tableNames) {
        tableNames.forEach(tn -> gacefulRebalanceRegionsStoppedTables.put(tn, EMPTY));
    }

    /**
     * Checks whether the table is in the list of tables whose region rebalancing must stop.
     * If it is, returns true and removes the table from the list.
     */
    public boolean checkTableRegionsRebalanceGracefulStopped(TableName tableName) {
        if (gacefulRebalanceRegionsStoppedTables.containsKey(tableName)) {
            gacefulRebalanceRegionsStoppedTables.remove(tableName);
            return true;
        }
        return false;
    }

    public void removeTableFromGracefulStopRegionsRebalance(TableName tableName) {
        gacefulRebalanceRegionsStoppedTables.remove(tableName);
    }

    /**
     * Method waits until all operations for specified table will be stopped on all region servers for the table.
     */
    public void waitAllRSForTable(TableName tableName, int parallelism) {
        List<RegionServerModel> allRS = getAllRegionServers();
        ForkJoinPool forkJoinPool = new ForkJoinPool(parallelism);
        try {
            forkJoinPool.submit(() ->
                    allRS.stream().parallel().forEach(rs -> waitRSForTable(tableName, rs))
            ).get();
        } catch (Exception ex) {
            LOGGER.error(StringUtils.EMPTY, ex);
        } finally {
            forkJoinPool.shutdownNow();
        }
    }

    /**
     * Method waits until all operations for specified table will be stopped on all region servers for all tables.
     */
    public void waitAllRSForAllTables(int parallelism) {
        List<RegionServerModel> allRS = getAllRegionServers();
        ForkJoinPool forkJoinPool = new ForkJoinPool(parallelism);
        try {
            forkJoinPool.submit(() ->
                    allRS.stream().parallel().forEach(rs -> waitRSForTable(null, rs))
            ).get();
        } catch (Exception ex) {
            LOGGER.error(StringUtils.EMPTY, ex);
        } finally {
            forkJoinPool.shutdownNow();
        }
    }

    private void waitRSForTable(TableName tableName, RegionServerModel rs) {
        while (true) {
            List<RegionServerModel> runningRS = getRunningRegionServers();
            try {
                String waitRSStr = String.format("Waiting for all operations on RS '%s' to finish for table '%s'", rs, tableName);
                RegionServerProto.RegionRequest request;
                if (tableName == null) {
                    request = RegionServerProto.RegionRequest.getDefaultInstance();
                } else {
                    request = RegionServerProto.RegionRequest.newBuilder()
                            .setTableName(ProtoConverters.toProtoTableName(tableName))
                            .build();
                }
                regionServerConnector.atomic(rs, waitRSStr, stub -> stub.get()
                        .withDeadlineAfter(grpcConf.getRsResponseTimeoutMs(), TimeUnit.MILLISECONDS)
                        .stopTableRegionTasks(request)
                );
                break;
            } catch (Exception ex) {
                if (runningRS.contains(rs)) {
                    // Retry in case of error with running RS
                    LOGGER.error("Error waiting for all operations on RS '{}' to finish for table '{}', wait 1000 ms and retry ...", rs, tableName, ex);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOGGER.error("Error waiting for all operations on RS '{}' to finish for table '{}', interrupted", rs, tableName);
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                } else {
                    LOGGER.warn("No reply from waiting for all operations request on RS '{}' to finish for table '{}': it may not be in the RUNNING status. message: {}", rs, tableName, ex.getMessage());
                    break;
                }
            }
        }
    }

    private List<RegionServerModel> getAllRegionServers() {
        while (true) {
            try {
                return zooContext.getAllRegionServers();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOGGER.error("Unable to read all RS infos: interrupted");
                throw new RuntimeException(ex);
            } catch (Exception ex) {
                LOGGER.error("Unable to read all RS infos. Wait 5000 ms and retry ...", ex);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException interrupted) {
                    LOGGER.error("Unable to read all RS infos: interrupted");
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
}
