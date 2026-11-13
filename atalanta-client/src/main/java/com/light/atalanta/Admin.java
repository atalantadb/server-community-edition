/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.google.common.base.Preconditions;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Empty;
import com.google.protobuf.StringValue;
import com.light.atalanta.api.DbAdmin;
import com.light.atalanta.api.DbSnapshotDescription;
import com.light.atalanta.api.DbTableDescriptor;
import com.light.atalanta.client.connection.nio.pool.ConnectionNio;
import com.light.atalanta.client.connection.nio.transport.NioShutdownLogging;
import com.light.atalanta.config.AtalantaProperties;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.grpc.ATALANTAProto;
import com.light.atalanta.grpc.MasterProto;
import com.light.atalanta.grpc.MasterServiceGrpc;
import com.light.atalanta.model.CompressType;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.util.BytesComparator;
import com.light.atalanta.util.ProtoConverters;
import com.light.atalanta.util.ZookeeperStatus;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.DEFAULT_DATA_BLOCK_SIZE;
import static com.light.atalanta.ATALANTAConstants.EMPTY_SPLIT_KEYS_BYTES;

public class Admin implements DbAdmin, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Admin.class);

    private final ConnectionThreadLocal connection;

    private final DbConfiguration atalantaConfiguration;

    private final ExecutorService adminExecutor;

    public Admin() {
        connection = null;
        atalantaConfiguration = null;
        adminExecutor = null;
    }

    public Admin(ConnectionThreadLocal connection) {
        this.connection = connection;
        this.atalantaConfiguration = connection.getConfiguration();
        this.adminExecutor = Executors.newCachedThreadPool();
    }

    // Asynchronous operations

    public CompletableFuture<Void> createNamespaceAsync(String namespace) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                createNamespace(namespace);
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
            return null;
        }, adminExecutor);
    }

    public CompletableFuture<Void> createTableAsync(TableName tableName, byte[][] splitKeys) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                createTable(tableName, splitKeys);
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
            return null;
        }, adminExecutor);
    }

    public CompletableFuture<Void> createTableAsync(TableName tableName) {
        // Delegates to the asynchronous overload; no extra Future wrapping is needed
        return createTableAsync(tableName, EMPTY_SPLIT_KEYS_BYTES);
    }

    public CompletableFuture<Void> enableTableAsync(TableName tableName) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                enableTable(tableName);
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
            return null;
        }, adminExecutor);
    }

    public CompletableFuture<Void> disableTableAsync(TableName tableName) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                disableTable(tableName);
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
            return null;
        }, adminExecutor);
    }

    public CompletableFuture<Void> dropTableAsync(TableName tableName) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                deleteTable(tableName);
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
            return null;
        }, adminExecutor);
    }

    // Synchronous operations

    @Override
    public void createNamespace(String namespace) throws IOException {
        LOGGER.debug("Start creating namespace: '{}'", namespace);
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            MasterProto.CreateNamespaceRequest.Builder request = MasterProto.CreateNamespaceRequest.newBuilder()
                    .setNamespace(namespace);
            
            Empty response = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_CREATE_NAMESPACE_TIMEOUT_MS), TimeUnit.MILLISECONDS)
                    .createNamespace(request.build());
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done creating namespace: '{}'", namespace);
        } catch (Exception ex) {
            LOGGER.debug("Error creating namespace: '{}'", namespace, ex);
            throw ex;
        }
    }

    @Override
    public void createTable(TableName tableName, byte[][] splitKeys) throws IOException {
        TableDescriptor tableDescriptor = new TableDescriptor(tableName);
        ColumnDescriptor columnDescriptor = ColumnDescriptor.DEFAULT_COLUMN_DESCRIPTOR;
        columnDescriptor.setBlocksize(DEFAULT_DATA_BLOCK_SIZE);
        columnDescriptor.setDictionary(false);
        tableDescriptor.addFamily(columnDescriptor);
        createTable(tableDescriptor, splitKeys);
    }

    public void createTable(TableName tableName) throws IOException {
        // The delegate already logs failures; do not log them again here
        createTable(tableName, EMPTY_SPLIT_KEYS_BYTES);
    }

    @Override
    public void createTable(DbTableDescriptor tDesc) throws IOException {
        if (!isNamespaceExists(tDesc.getTableName().getNamespace())) {
            createNamespace(tDesc.getTableName().getNamespace());
        }
        createTable(tDesc, EMPTY_SPLIT_KEYS_BYTES);
    }

    public void enableTable(TableName tableName) throws IOException {
        LOGGER.debug("Start enabling table: '{}'", tableName);
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            MasterProto.EnableTableRequest.Builder request = MasterProto.EnableTableRequest.newBuilder()
                    .setTableName(ProtoConverters.toProtoTableName(tableName));
            
            Empty response = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_ENABLE_TIMEOUT_MS), TimeUnit.MILLISECONDS)
                    .enableTable(request.build());
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done enabling table: '{}'", tableName);
        } catch (Exception ex) {
            LOGGER.debug("Error enabling table: '{}'", tableName, ex);
            throw ex;
        }
    }

    @Override
    public void createTable(DbTableDescriptor tableDescriptor, byte[][] split) throws IOException {
        createOrModifyTable(tableDescriptor, split, true);
    }

    private void createOrModifyTable(DbTableDescriptor tableDescriptor, byte[][] split, boolean isNewTable) throws IOException {
        TableName tableName = tableDescriptor.getTableName();
        String opType = isNewTable ? "creating" : "modify";
        LOGGER.debug("Start {} table: '{}'", opType, tableName);
        DbColumnDescriptor columnDescriptor = tableDescriptor.getFamilies().stream().findAny()
                .orElseGet(() -> ColumnDescriptor.DEFAULT_COLUMN_DESCRIPTOR);
        Object compressTypeObj = columnDescriptor.getCompression();
        CompressType compressType = compressTypeObj instanceof CompressType ? (CompressType) compressTypeObj : null;
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            MasterProto.CreateTableRequest.Builder request = MasterProto.CreateTableRequest.newBuilder()
                    .setTableName(ProtoConverters.toProtoTableName(tableName))
                    .setDataBlockSize(columnDescriptor.getBlockSize())
                    .addAllSplitKeys(ProtoConverters.toProtoSplitKeys(split))
                    .setRecordTargetCount(String.valueOf(columnDescriptor.getRecordTargetCount()));
            if (compressType != null) {
                request.setCompressType(compressType.name());
            }
            if (columnDescriptor.getRecordCompressionType() != null) {
                request.setRecordCompressionType(columnDescriptor.getRecordCompressionType().toString());
            }
            if (columnDescriptor.getDictionary() != null) {
                request.setDictionary(String.valueOf(columnDescriptor.getDictionary()));
            }
            if (columnDescriptor.getRecordTargetCount() != null) {
                request.setRecordTargetCount(String.valueOf(columnDescriptor.getRecordTargetCount()));
            }
            
            MasterServiceGrpc.MasterServiceBlockingStub masterServiceBlockingStub = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_CREATE_TABLE_TIMEOUT_MS), TimeUnit.MILLISECONDS);
            Empty response = isNewTable ?  masterServiceBlockingStub.createTable(request.build())
                    : masterServiceBlockingStub.modifyTable(request.build());
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done {} table: '{}'", opType, tableName);
        } catch (Exception ex) {
            LOGGER.debug("Error {} table: '{}'", opType, tableName, ex);
            throw ex;
        }
    }

    @Override
    public void modifyTable(DbTableDescriptor tableDescriptor) throws IOException {
        createOrModifyTable(tableDescriptor, EMPTY_SPLIT_KEYS_BYTES, false);
    }

    @Override
    public DbNamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
        return listNamespace(Pattern.compile(".*")).stream()
                .map(ns -> new NamespaceDescriptor(ns))
                .collect(Collectors.toList())
                .toArray(new DbNamespaceDescriptor[0]);
    }

    @Override
    public void createNamespace(DbNamespaceDescriptor nd) throws IOException {
        createNamespace(nd.getName());
    }

    @Override
    public void snapshot(DbSnapshotDescription snapshotDescription) throws IOException {
        try {
            snapshotAsync(snapshotDescription).get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Error creating snapshot: '{}' for table {}, {}", snapshotDescription.getName(), snapshotDescription.getTableName(), e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public Future<Void> snapshotAsync(DbSnapshotDescription snapshotDescription) throws IOException {
        LOGGER.debug("Start creating snapshot: '{}' for table {}", snapshotDescription.getTableName(), snapshotDescription.getTableName());
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            MasterProto.MasterSnapshotRequest.Builder request = MasterProto.MasterSnapshotRequest.newBuilder()
                    .setTableName(ProtoConverters.toProtoTableName(snapshotDescription.getTableName()))
                    .setName(snapshotDescription.getName());
            
            Empty response = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_CREATE_TABLE_TIMEOUT_MS), TimeUnit.MILLISECONDS)
                    .createSnapshot(request.build());
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done creating snapshot: '{}' for table {}", snapshotDescription.getTableName(), snapshotDescription.getTableName());
        } catch (Exception ex) {
            LOGGER.error("Error creating snapshot: '{}' for table {}, {}",
                    snapshotDescription.getTableName(), snapshotDescription.getTableName(), ex);
            throw ex;
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Future<Void> cloneSnapshotAsync(String snapshotName, TableName tableName) throws IOException {
        LOGGER.debug("Start clone snapshot: '{}' to table {}", snapshotName, tableName);
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            MasterProto.MasterSnapshotRequest.Builder request = MasterProto.MasterSnapshotRequest.newBuilder()
                    .setTableName(ProtoConverters.toProtoTableName(tableName))
                    .setName(snapshotName);
            
            Empty response = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_CREATE_TABLE_TIMEOUT_MS), TimeUnit.MILLISECONDS)
                    .cloneSnapshot(request.build());
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done clone snapshot: '{}' for table {}", tableName, snapshotName);
        } catch (Exception ex) {
            LOGGER.error("Error clone snapshot: '{}' for table {}, {}",
                    tableName, snapshotName, ex);
            throw ex;
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Future<Void> deleteSnapshot(String snapshotName) throws IOException {
        LOGGER.debug("Start delete snapshot: '{}'", snapshotName);
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            MasterProto.MasterSnapshotRequest.Builder request = MasterProto.MasterSnapshotRequest.newBuilder().setTableName(ProtoConverters.toProtoTableName(StringUtils.EMPTY))
                    .setName(snapshotName);
            
            Empty response = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_CREATE_TABLE_TIMEOUT_MS), TimeUnit.MILLISECONDS).deleteSnapshot(request.build());
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done delete snapshot: '{}'", snapshotName);
        } catch (Exception ex) {
            LOGGER.error("Error delete snapshot: '{}'", snapshotName, ex);
            throw ex;
        }
        return null;
    }

    public void disableTable(TableName tableName) throws IOException {
        LOGGER.debug("Start disabling table: '{}'", tableName);
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            MasterProto.DisableTableRequest.Builder request = MasterProto.DisableTableRequest.newBuilder()
                    .setTableName(ProtoConverters.toProtoTableName(tableName));
            
            Empty response = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_DISABLE_TABLE_TIMEOUT_MS), TimeUnit.MILLISECONDS)
                    .disableTable(request.build());
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done disabling table: '{}'", tableName);
        } catch (Exception ex) {
            LOGGER.debug("Error disabling table: '{}'", tableName, ex);
            throw ex;
        }
    }

    public void deleteTable(TableName tableName) throws IOException {
        LOGGER.debug("Start dropping table: '{}'", tableName);
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            MasterProto.DropTableRequest.Builder request = MasterProto.DropTableRequest.newBuilder()
                    .setTableName(ProtoConverters.toProtoTableName(tableName));
            
            Empty response = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_DROP_TABLE_TIMEOUT_MS), TimeUnit.MILLISECONDS)
                    .dropTable(request.build());
            Preconditions.checkNotNull(response, "no response received from master");
            connection.close(tableName);
            LOGGER.debug("Done dropping table: '{}'", tableName);
        } catch (Exception ex) {
            LOGGER.debug("Error dropping table: '{}'", tableName, ex);
            throw ex;
        }
    }

    // Key: regionIdentity, value: regionServerName.
    // All regions should belong to the same table.
    public void bulkMove(Map<String, String> moveRequests) throws IOException {
        if (moveRequests.isEmpty()) {
            LOGGER.warn("Empty move request");
            return;
        }
        LOGGER.debug("Start moving table regions: {}", moveRequests);
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            TableName tableName = null;
            MasterProto.BulkMoveRegionRequest.Builder request = MasterProto.BulkMoveRegionRequest.newBuilder();
            for (Map.Entry<String, String> moveRequest : moveRequests.entrySet()) {
                String regionIdentity = moveRequest.getKey();
                String regionServerName = moveRequest.getValue();
                Preconditions.checkArgument(regionIdentity.contains("_")
                        && regionIdentity.split("_").length == 2
                        && NumberUtils.isCreatable(regionIdentity.split("_")[1]), "Region identity is damaged");
                String regionName = regionIdentity.split("_")[0];
                int replicaNum = Integer.parseInt(regionIdentity.split("_")[1]);
                TableName calculatedTableName = connection.getZooKeeperCoreContext().findTableByRegion(regionName, replicaNum);
                if (tableName == null) {
                    tableName = calculatedTableName;
                } else if (!calculatedTableName.equals(tableName)) {
                    throw new IOException("Region doesn't belong to '" + tableName + "'");
                }
                request.addMoveRequest(
                        MasterProto.MoveRegionRequest.newBuilder()
                                .setRegionIdentity(regionIdentity)
                                .setRegionServerName(regionServerName)
                );
            }
            if (tableName == null) {
                throw new IOException("Unable to calculate table name");
            }
            request.setTableName(ProtoConverters.toProtoTableName(tableName));
            
            Empty response = masterStub
                    .withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_MOVE_REGION_TIMEOUT_MS), TimeUnit.MILLISECONDS)
                    .moveRegion(request.build());
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done moving regions: {}", moveRequests);
        } catch (IOException ex) {
            LOGGER.debug("Error moving regions: {}", moveRequests, ex);
            throw ex;
        }
    }

    public void move(String regionIdentity, String regionServerName) throws IOException {
        bulkMove(Collections.singletonMap(regionIdentity, regionServerName));
    }

    public void split(TableName tableName) throws IOException {
        List<String> regionNamesToSplit = connection.getZooKeeperCoreContext().getRegionDistributionWithStatuses(tableName).stream()
                .map(rdm -> rdm.getRegionModel().getName())
                .distinct()
                .collect(Collectors.toList());
        Preconditions.checkArgument(!regionNamesToSplit.isEmpty(), "No regions found in table: " + tableName);
        split(tableName, regionNamesToSplit);
    }

    public void split(TableName tableName, List<String> regionNamesToSplit) throws IOException {
        LOGGER.debug("Start splitting regions '{}:{}'", tableName, regionNamesToSplit);
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            Set<String> tableRegionNames = connection.getZooKeeperCoreContext().getRegionDistributionWithStatuses(tableName).stream()
                    .map(rdm -> rdm.getRegionModel().getName())
                    .collect(Collectors.toSet());
            for (String regionName : regionNamesToSplit) {
                Preconditions.checkArgument(!regionName.contains("_"), "Region name is damaged");
                Preconditions.checkArgument(tableRegionNames.contains(regionName), "Unable to find region: " + regionName + " in table: " + tableName);
            }
            MasterProto.SplitRegionRequest.Builder request = MasterProto.SplitRegionRequest.newBuilder()
                    .setTableName(ProtoConverters.toProtoTableName(tableName))
                    .addAllRegionNamesToSplit(regionNamesToSplit);
            
            Empty response = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_SPLIT_REGION_TIMEOUT_MS), TimeUnit.MILLISECONDS)
                    .splitRegion(request.build());
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done splitting regions '{}:{}'", tableName, regionNamesToSplit);
        } catch (IOException ex) {
            LOGGER.debug("Error splitting regions '{}:{}'", tableName, regionNamesToSplit, ex);
            throw ex;
        }
    }

    public void split(String regionName) throws IOException {
        LOGGER.debug("Start splitting region '{}'", regionName);
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            Preconditions.checkArgument(!regionName.contains("_"), "Region name is damaged");
            TableName tableName = connection.getZooKeeperCoreContext().findTableByRegion(regionName);
            MasterProto.SplitRegionRequest.Builder request = MasterProto.SplitRegionRequest.newBuilder()
                    .setTableName(ProtoConverters.toProtoTableName(tableName))
                    .addRegionNamesToSplit(regionName);
            
            Empty response = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_SPLIT_REGION_TIMEOUT_MS), TimeUnit.MILLISECONDS)
                    .splitRegion(request.build());
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done splitting region '{}:{}'", tableName, regionName);
        } catch (IOException ex) {
            LOGGER.debug("Error splitting region '{}'", regionName, ex);
            throw ex;
        }
    }

    /**
     * Starts replica rebalancing for each region of the matching tables separately.
     * <p>
     * {@code rebalanceEntity} is one of:
     * <ul>
     * <li>{@code !} for all tables in the database;</li>
     * <li>{@code @namespace} for all tables in the namespace;</li>
     * <li>{@code namespace:table} for a single table.</li>
     * </ul>
     * Not to be confused with {@link #rebalanceRegionsAsync(String)}.
     */
    public void rebalanceReplicasAsync(String rebalanceEntity) throws IOException {
        if (rebalanceEntity.equals("!")) {
            LOGGER.debug("Start rebalancing all DB table replicas");
        } else if (rebalanceEntity.startsWith("@")) {
            LOGGER.debug("Start rebalancing namespace '{}' table replicas", rebalanceEntity.replaceFirst("@", StringUtils.EMPTY));
        } else {
            LOGGER.debug("Start rebalancing table replicas '{}'", rebalanceEntity);
        }
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            MasterProto.StartRebalanceRequest.Builder request = MasterProto.StartRebalanceRequest.newBuilder()
                    .setEntity(rebalanceEntity);
            
            Empty response = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_REBALANCE_REGION_START_TIMEOUT_MS), TimeUnit.MILLISECONDS)
                    .startReplicasRebalanceAsync(request.build());
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done starting rebalancing");
        } catch (Exception ex) {
            LOGGER.debug("Error starting rebalancing", ex);
            throw ex;
        }
    }

    /**
     * Marks replica rebalancing tasks on the master for stopping.
     */
    public void markReplicasRebalanceStop(String rebalanceEntity) throws IOException {
        if (rebalanceEntity.equals("!")) {
            LOGGER.debug("Marking rebalancing to stop for all DB replicas");
        } else if (rebalanceEntity.startsWith("@")) {
            LOGGER.debug("Marking rebalancing to stop for namespace '{}' table replicas", rebalanceEntity.replaceFirst("@", StringUtils.EMPTY));
        } else {
            LOGGER.debug("Marking rebalancing to stop for table replicas '{}'", rebalanceEntity);
        }
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            MasterProto.StopRebalanceRequest.Builder request = MasterProto.StopRebalanceRequest.newBuilder()
                    .setEntity(rebalanceEntity);
            
            Empty response = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_STOP_REBALANCE_TASKS_TIMEOUT_MS), TimeUnit.MILLISECONDS)
                    .stopReplicasRebalance(request.build());
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done marking replicas rebalancing to stop");
        } catch (Exception ex) {
            LOGGER.debug("Error marking replicas rebalancing to stop", ex);
            throw ex;
        }
    }

    /**
     * Starts rebalancing of all replicas of the matching tables as a whole.
     * <p>
     * {@code rebalanceEntity} is one of:
     * <ul>
     * <li>{@code !} for all tables in the database;</li>
     * <li>{@code @namespace} for all tables in the namespace;</li>
     * <li>{@code namespace:table} for a single table.</li>
     * </ul>
     * Not to be confused with {@link #rebalanceReplicasAsync(String)}.
     */
    public void rebalanceRegionsAsync(String rebalanceEntity) throws IOException {
        if (rebalanceEntity.equals("!")) {
            LOGGER.debug("Start rebalancing all DB regions");
        } else if (rebalanceEntity.startsWith("@")) {
            LOGGER.debug("Start rebalancing namespace '{}' table regions", rebalanceEntity.replaceFirst("@", StringUtils.EMPTY));
        } else {
            LOGGER.debug("Start rebalancing table regions '{}'", rebalanceEntity);
        }
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            MasterProto.StartRebalanceRequest.Builder request = MasterProto.StartRebalanceRequest.newBuilder()
                    .setEntity(rebalanceEntity);
            
            Empty response = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_REBALANCE_REGION_START_TIMEOUT_MS), TimeUnit.MILLISECONDS)
                    .startRegionsRebalanceAsync(request.build());
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done starting rebalancing");
        } catch (Exception ex) {
            LOGGER.debug("Error starting rebalancing", ex);
            throw ex;
        }
    }

    /**
     * Marks region rebalancing tasks on the master for stopping.
     */
    public void markRegionsRebalanceStop(String rebalanceEntity) throws IOException {
        if (rebalanceEntity.equals("!")) {
            LOGGER.debug("Marking rebalancing to stop for all DB regions");
        } else if (rebalanceEntity.startsWith("@")) {
            LOGGER.debug("Marking rebalancing to stop for namespace '{}' table regions", rebalanceEntity.replaceFirst("@", StringUtils.EMPTY));
        } else {
            LOGGER.debug("Marking rebalancing to stop for table regions '{}'", rebalanceEntity);
        }
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            MasterProto.StopRebalanceRequest.Builder request = MasterProto.StopRebalanceRequest.newBuilder()
                    .setEntity(rebalanceEntity);
            
            Empty response = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_STOP_REBALANCE_TASKS_TIMEOUT_MS), TimeUnit.MILLISECONDS)
                    .stopRegionsRebalance(request.build());
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done marking regions rebalancing to stop");
        } catch (Exception ex) {
            LOGGER.debug("Error marking regions rebalancing to stop", ex);
            throw ex;
        }
    }

    /**
     * Starts synchronization of standby replicas with the leading replica.
     * <p>
     * {@code syncEntity} is one of:
     * <ul>
     * <li>{@code !} for all tables in the database;</li>
     * <li>{@code @namespace} for all tables in the namespace;</li>
     * <li>{@code namespace:table} for a single table.</li>
     * </ul>
     */
    public void syncReplicasAsync(String syncEntity) throws IOException {
        if (syncEntity.equals("!")) {
            LOGGER.debug("Start syncing all DB table replicas");
        } else if (syncEntity.startsWith("@")) {
            LOGGER.debug("Start syncing namespace '{}' table replicas", syncEntity.replaceFirst("@", StringUtils.EMPTY));
        } else {
            LOGGER.debug("Start syncing table replicas '{}'", syncEntity);
        }
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            MasterProto.SynchronizeRequest.Builder request = MasterProto.SynchronizeRequest.newBuilder()
                    .setEntity(syncEntity);
            
            Empty response = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_REBALANCE_REGION_START_TIMEOUT_MS), TimeUnit.MILLISECONDS)
                    .syncTableReplicas(request.build());
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done starting sync");
        } catch (Exception ex) {
            LOGGER.debug("Error starting sync", ex);
            throw ex;
        }
    }

    private ATALANTAProto.ProtoTableName buildProtoTableName(String entity) {
        return entity.contains(":") ? ProtoConverters.toProtoTableName(TableName.fromString(entity))
                : ProtoConverters.toProtoTableName(entity);
    }

    public boolean isTableEnabled(TableName tableName) throws IOException {
        return !isTableDisabled(tableName);
    }

    public boolean isTableDisabled(TableName tableName) throws IOException {
        return getTableStatus(tableName).equals(ZookeeperStatus.DISABLED);
    }

    public boolean isTableCreating(TableName tableName) throws IOException {
        return getTableStatus(tableName).equals(ZookeeperStatus.CREATING);
    }

    public boolean isTableCreated(TableName tableName) throws IOException {
        return getTableStatus(tableName).equals(ZookeeperStatus.CREATED);
    }

    public boolean isTableAssigned(TableName tableName) throws IOException {
        return getTableStatus(tableName).equals(ZookeeperStatus.ASSIGNED);
    }

    public boolean isTableDisabling(TableName tableName) throws IOException {
        return getTableStatus(tableName).equals(ZookeeperStatus.DISABLING);
    }

    public boolean isTableDropping(TableName tableName) throws IOException {
        return getTableStatus(tableName).equals(ZookeeperStatus.DROPPING);
    }

    public boolean isTableRecovering(TableName tableName) throws IOException {
        return getTableStatus(tableName).equals(ZookeeperStatus.RECOVERING);
    }

    public boolean isTableSplitting(TableName tableName) throws IOException {
        return getTableStatus(tableName).equals(ZookeeperStatus.SPLITTING);
    }

    public boolean isTableModifying(TableName tableName) throws IOException {
        return getTableStatus(tableName).equals(ZookeeperStatus.MODIFYING);
    }

    public boolean tableExists(TableName tableName) throws IOException {
        LOGGER.debug("Start checking table: '{}'", tableName);
        try {
            String zooKeeperData;
            try {
                zooKeeperData = connection.getZooHelper().get(ZooKeeperContextHelper.getTablePath(tableName))
                        .orElseThrow(() -> new IOException("table is damaged"));
            } catch (ZKException ex) {
                if (ex.getCause() instanceof KeeperException &&
                        ((KeeperException) ex.getCause()).code().equals(KeeperException.Code.NONODE)) {
                    return false;
                }
                throw new IOException("table is damaged");
            }
            ZookeeperStatus status = ZookeeperStatus.fromString(zooKeeperData);
            LOGGER.debug("Done checking table: '{}'", tableName);
            return status != null;
        } catch (IOException ex) {
            LOGGER.debug("Error checking table: '{}'", tableName, ex);
            throw ex;
        }
    }

    @Override
    public boolean isNamespaceExists(String namespace) throws IOException {
        LOGGER.debug("Start checking namespace: '{}'", namespace);
        try {
            boolean result = connection.getZooHelper().checkExists(namespace);
            LOGGER.debug("Done checking namespace: '{}'", namespace);
            return result;
        } catch (Exception ex) {
            LOGGER.debug("Error checking namespace: '{}'", namespace, ex);
            throw ex;
        }
    }

    private ZookeeperStatus getTableStatus(TableName tableName) throws IOException {
        LOGGER.debug("Start checking table: '{}'", tableName);
        try {
            String zooKeeperData;
            try {
                zooKeeperData = connection.getZooHelper().get(ZooKeeperContextHelper.getTablePath(tableName))
                        .orElseThrow(() -> new IOException("table is damaged"));
            } catch (ZKException ex) {
                if (ex.getCause() instanceof KeeperException &&
                        ((KeeperException) ex.getCause()).code().equals(KeeperException.Code.NONODE)) {
                    throw new IOException("table not found");
                }
                throw ex;
            }
            ZookeeperStatus status = ZookeeperStatus.fromString(zooKeeperData);
            LOGGER.debug("Done checking table: '{}'", tableName);
            return status;
        } catch (IOException ex) {
            LOGGER.debug("Error checking table: '{}'", tableName, ex);
            throw ex;
        }
    }

    public List<String> listNamespace(Pattern pattern) throws IOException {
        LOGGER.debug("Start listing namespaces with pattern: '{}'", pattern);
        try {
            List<String> namespaces = connection.getZooKeeperCoreContext().listNamespace().stream()
                    .filter(r -> pattern.matcher(r).matches())
                    .collect(Collectors.toList());
            LOGGER.debug("Done listing namespaces with pattern: '{}'", pattern);
            return namespaces;
        } catch (Exception ex) {
            LOGGER.debug("Error listing namespaces with pattern: '{}'", pattern, ex);
            throw ex;
        }
    }


    public List<String> listSnapshot(Pattern pattern) throws IOException {
        LOGGER.debug("Start listing snapshot with pattern: '{}'", pattern);
        try {
            List<String> namespaces = connection.getZooKeeperCoreContext().listSnapshot().stream()
                    .filter(r -> pattern.matcher(r).matches())
                    .collect(Collectors.toList());
            LOGGER.debug("Done listing snapshot with pattern: '{}'", pattern);
            return namespaces;
        } catch (Exception ex) {
            LOGGER.debug("Error listing snapshot with pattern: '{}'", pattern, ex);
            throw ex;
        }
    }

    @Override
    public TableName[] listTableNames(String s) throws IOException {
        return listTableNames(Pattern.compile(s));
    }

    public TableName[] listTableNames(Pattern pattern) throws IOException {
        LOGGER.debug("Start listing tables with pattern: '{}'", pattern);
        try {
            List<TableName> tableNames = new ArrayList<>();
            ZooKeeperCoreContext zooKeeperCoreContext = connection.getZooKeeperCoreContext();
            for (String ns : zooKeeperCoreContext.listNamespace()) {
                zooKeeperCoreContext.listNamespaceTables(ns).stream()
                        .filter(tbl -> pattern.matcher(tbl.toString()).matches())
                        .map( tn -> TableName.valueOf(tn.getNamespace(), tn.getTableName()))
                        .forEach(tableNames::add);
            }
            LOGGER.debug("Done listing tables with pattern: '{}'", pattern);
            return tableNames.toArray(new TableName[0]);
        } catch (IOException ex) {
            LOGGER.debug("Error listing tables with pattern: '{}'", pattern, ex);
            throw ex;
        }
    }

    @Override
    public boolean isTableAvailable(TableName tn) throws IOException {
        return this.isTableCreated(tn);
    }

    public List<RegionDistributionModel> listTableRegions(TableName tableName) throws IOException {
        LOGGER.debug("Start listing table regions: '{}'", tableName);
        try {
            List<RegionDistributionModel> distribution = connection.getZooKeeperCoreContext().getRegionDistributionWithStatuses(tableName);
            LOGGER.debug("Done listing table regions: '{}'", tableName);
            return distribution;
        } catch (ZKException ex) {
            if (ex.getCause() instanceof KeeperException &&
                    ((KeeperException) ex.getCause()).code().equals(KeeperException.Code.NONODE)) {
                throw new IOException("table not found");
            }
            throw ex;
        } catch (Exception ex) {
            LOGGER.debug("Error listing table regions: '{}'", tableName, ex);
            throw ex;
        }
    }

    public List<RegionServerModel> listTableRegionServers(TableName tableName) throws IOException {
        LOGGER.debug("Start listing table region servers: '{}'", tableName);
        try {
            List<RegionServerModel> distribution = connection.getZooKeeperCoreContext().getRegionServersForTable(tableName);
            LOGGER.debug("Done listing table region servers: '{}'", tableName);
            return distribution;
        } catch (ZKException ex) {
            if (ex.getCause() instanceof KeeperException &&
                    ((KeeperException) ex.getCause()).code().equals(KeeperException.Code.NONODE)) {
                throw new IOException("table not found");
            }
            throw ex;
        } catch (Exception ex) {
            LOGGER.debug("Error listing table region servers: '{}'", tableName, ex);
            throw ex;
        }
    }

    @Override
    public List<TableName> listTableNamesByNamespace(String namespace) throws IOException {
        LOGGER.debug("Start listing tables for namespace: '{}'", namespace);
        try {
            if (!connection.getZooHelper().checkExists(namespace)) {
                throw new IOException("namespace not found");
            }
            ZooKeeperCoreContext zooKeeperCoreContext = connection.getZooKeeperCoreContext();
            List<TableName> tableNames = zooKeeperCoreContext
                    .listNamespaceTables(namespace).stream()
                    .map(nt -> TableName.valueOf(nt) )
                    .collect(Collectors.toList());
            LOGGER.debug("Done listing tables for namespace: '{}'", namespace);
            return tableNames;
        } catch (IOException ex) {
            LOGGER.debug("Error listing tables for namespace: '{}'", namespace, ex);
            throw ex;
        }
    }

    @Override
    public List<RegionInfo> getRegions(TableName tableName) throws IOException {
        return connection.getRegionLocator(tableName).getAllRegionLocations().stream().map(DbRegionLocation::getRegionModel).map(r -> new RegionInfo(tableName, r.getName())).collect(Collectors.toList());
    }
    @Override
    public void close() throws IOException {
        adminExecutor.shutdownNow();
    }

    public void startRegionServer(String rs) throws IOException {
        LOGGER.debug("Start region server: '{}'", rs);
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            BoolValue response = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_START_REGION_TIMEOUT_MS), TimeUnit.MILLISECONDS)
                    .startRegionServer(StringValue.of(rs));
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done start RS: '{}'", rs);
        } catch (Exception ex) {
            LOGGER.debug("Error start RS: '{}'", rs, ex);
            throw ex;
        }
    }

        public void shutdownRs() throws IOException {
        long timeoutMs = atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_SHUTDOWN_RS_TIMEOUT_MS);
        Preconditions.checkArgument(timeoutMs > 0, AtalantaProperties.ATALANTA_SHUTDOWN_RS_TIMEOUT_MS + " must be positive");
        MasterProto.ShutdownRsRequest.Builder request = MasterProto.ShutdownRsRequest.newBuilder();
        
        // Resolve the current master once, before issuing the request. Do not retry this operation.
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try (NioShutdownLogging.Scope logging = connection instanceof ConnectionNio
                ? ((ConnectionNio) connection).beginShutdownLogging() : null) {
            MasterProto.ShutdownRsResponse response;
            try {
                response = masterStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).shutdownRs(request.build());
            } catch (io.grpc.StatusRuntimeException e) {
                io.grpc.Status.Code code = e.getStatus().getCode();
                String details = "shutdown_rs RPC failed: " + e.getStatus();
                if (code != io.grpc.Status.Code.UNIMPLEMENTED && code != io.grpc.Status.Code.INVALID_ARGUMENT) {
                    details += ". Shutdown result is unknown; the master may still be shutting down. The request was not retried.";
                }
                throw new IOException(details, e);
            }
            if (!response.getSuccess() || response.getErrorsCount() != 0 || !response.getDetails().isEmpty()) {
                StringBuilder details = new StringBuilder("shutdown_rs failed");
                if (!response.getDetails().isEmpty()) details.append(": ").append(response.getDetails());
                for (MasterProto.ShutdownRsError error : response.getErrorsList()) {
                    ATALANTAProto.ProtoRegionServer rs = error.getRegionServer();
                    details.append("\nRS ").append(rs.getName()).append(" (").append(rs.getHost())
                            .append(':').append(rs.getJavaPort()).append("): ").append(error.getDetails());
                }
                if (response.getDetails().isEmpty() && response.getErrorsCount() == 0) {
                    details.append(": master did not confirm shutdown");
                }
                throw new IOException(details.toString());
            }
            if (logging != null) logging.confirm();
        }
    }

    public void stopRegionServer(String rs) throws IOException {
        LOGGER.debug("Stop region server: '{}'", rs);
        MasterServiceGrpc.MasterServiceBlockingStub masterStub = connection.getMasterStub();
        try {
            BoolValue response = masterStub.withDeadlineAfter(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_STOP_REGION_TIMEOUT_MS), TimeUnit.MILLISECONDS)
                    .stopRegionServer(StringValue.of(rs));
            Preconditions.checkNotNull(response, "no response received from master");
            LOGGER.debug("Done stop RS: '{}'", rs);
        } catch (Exception ex) {
            LOGGER.debug("Error stop RS: '{}'", rs, ex);
            throw ex;
        }
    }

    @Override
    public Connection getConnection() {
        return connection;
    }


    @Override
    public void truncateTable(TableName tableName) throws IOException {
        // TODO: Move the truncate logic to the master
        try {
            disableTable(tableName);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        try {
            deleteTable(tableName);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        try {
            createTable(tableName);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public void truncateTablePreserve(TableName tableName) throws IOException {
        // TODO: Move the truncate logic to the master
        try {
            List<RegionLocation> locations = connection.getRegionLocator(tableName).getAllRegionLocations();
            locations.sort((l1, l2) -> BytesComparator.compareTo(l1.getRegionModel().getStartKey(), l2.getRegionModel().getStartKey()));
            byte[][] bytes = new byte[locations.size() - 1][];
            for (int i = 0; i < locations.size() - 1; i++) {
                bytes[i] = locations.get(i).getRegionModel().getEndKey();
            }
            try {
                disableTable(tableName);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
            try {
                deleteTable(tableName);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
            createTable(tableName, bytes);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
