/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.services;

import com.light.atalanta.Preconditions;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.TableNotExistsException;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.model.*;
import com.light.atalanta.util.ShutdownManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.*;
import static com.light.atalanta.services.ZooKeeperContextHelper.*;

/**
 * Works with the database configuration stored in ZooKeeper.
 */
public class ZooKeeperCoreContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperCoreContext.class);
    public static final String RECORD_COMPRESSION_TYPE = "rct";
    public static final String DICTIONARY = "d";
    public static final String RECORD_TARGET_COUNT = "cnt";
    public static final String COMPRESSION_TYPE = "ct";

    private final ZookeeperHelper zooHelper;

    private final ZookeeperHelperExtended zookeeperHelperExtended;

    public ZooKeeperCoreContext(ZookeeperHelper zooHelper, ZookeeperHelperExtended zookeeperHelperExtended) {
        this.zooHelper = zooHelper;
        this.zookeeperHelperExtended = zookeeperHelperExtended;
    }

    public List<RegionServerModel> getRegionServersForTable(TableName tableName) throws IOException {
        List<RegionServerModel> regionServers = new ArrayList<>();
        String tablePath = tableName.getNamespace() + ZK_SEPARATOR + tableName.getTableName();
        if (!zooHelper.checkExists(tablePath)) {
            throw new IOException("Unable to get region servers: table '" + tableName + "' not found");
        }
        for (String regionServer : zooHelper.watchedGetChildren(tablePath)) {
            // Parses a hostname:java_port:c_port string
            String apiLocationData = zooHelper.getNotNullString(ZooKeeperContextHelper.getRegionServerApiLocationPath(tableName, new RegionServerModel(regionServer, "null", 1, 1, 1)));
            Preconditions.checkArgument(apiLocationData.contains(HOSTNAME_SEPARATOR)
                            && apiLocationData.split(HOSTNAME_SEPARATOR).length == 4
                            && NumberUtils.isCreatable(apiLocationData.split(HOSTNAME_SEPARATOR)[1])
                            && NumberUtils.isCreatable(apiLocationData.split(HOSTNAME_SEPARATOR)[2]),
                    "Unable to get region servers for table " + tableName + ": api location for region server " + regionServer + " is damaged");
            String[] apiLocation = apiLocationData.split(HOSTNAME_SEPARATOR);
            regionServers.add(new RegionServerModel(regionServer, apiLocation[0], Integer.parseInt(apiLocation[1]), Integer.parseInt(apiLocation[2]), Integer.parseInt(apiLocation[3])));
        }
        return regionServers;
    }

    /**
     * Returns the RS names for a table. Faster than getRegionServersForTable().
     */
    public List<String> getRegionServerNamesForTable(TableName tableName) throws Exception {
        String tablePath = tableName.getNamespace() + ZK_SEPARATOR + tableName.getTableName();
        if (!zooHelper.checkExists(tablePath)) {
            throw new TableNotExistsException(tableName);
        }
        return zooHelper.watchedGetChildren(tablePath);
    }

    /**
     * Builds a string of standby replica addresses for the given leader, ordered by replicaNum.
     */
    public String getReservedAddresses(List<RegionDistributionModel> distribution, String regionName, int replicationFactor, boolean shutdownOnError) {
        AtomicInteger actualCount = new AtomicInteger(0);
        String reservedReplicas = distribution.stream()
                .filter(rdmReserved -> rdmReserved.getRegionModel().getReplicaNum() > 0)
                .sorted(Comparator.comparingInt(o -> o.getRegionModel().getReplicaNum()))
                .filter(rdmReserved -> rdmReserved.getRegionModel().getName().equals(regionName))
                .map(rdmReserved -> String.join(":",
                        rdmReserved.getRegionServerModel().getHost(),
                        String.valueOf(rdmReserved.getRegionServerModel().getCoreClientPort())))
                .peek(r -> actualCount.incrementAndGet())
                .collect(Collectors.joining(":"));
        int expectedCount = replicationFactor - 1;
        if (expectedCount != actualCount.get()) {
            String errorMsg = String.format("Unexpected reserved replicas count for: %s. Expected: %s, actual: %s", regionName, expectedCount, actualCount.get());
            if (shutdownOnError) {
                ShutdownManager.exitWithError(errorMsg);
            } else {
                throw new RuntimeException(errorMsg);
            }
        }
        return reservedReplicas;
    }

    public String getReservedAddresses(List<RegionDistributionModel> distribution, String regionName, int replicationFactor) {
        return getReservedAddresses(distribution, regionName, replicationFactor, true);
    }



    /**
     * Returns the region distribution for each table.
     */
    public Map<TableName, List<RegionDistributionModel>> getAllDistributions() throws IOException {
        try {
            Map<TableName, List<RegionDistributionModel>> allDistributions = new HashMap<>();
            for (String namespace : listNamespace()) {
                for (TableName tableName : listNamespaceTables(namespace)) {
                    allDistributions.put(tableName, getRegionDistributionWithStatuses(tableName));
                }
            }
            return allDistributions;
        } catch (IOException ex) {
            throw new IOException("Unable to read regions distribution", ex);
        }
    }

    /**
     * Returns all tables in the database.
     */
    public List<TableName> listTables() throws Exception {
        List<TableName> allTableNames = new ArrayList<>();
        try {
            for (String namespace : listNamespace()) {
                allTableNames.addAll(listNamespaceTables(namespace));
            }
        } catch (Exception ex) {
            throw new Exception("unable to read regions distribution", ex);
        }
        return allTableNames;
    }

    /**
     * Finds a table by region ID.
     */
    public TableName findTableByRegion(String regionName, int replicaNum) throws IOException {
        return getAllDistributions().entrySet().stream()
                .filter(e -> e.getValue().stream()
                        .filter(rdm -> rdm.getRegionModel().getName().equals(regionName))
                        .anyMatch(rdm -> rdm.getRegionModel().getReplicaNum() == replicaNum))
                .map(Map.Entry::getKey)
                .findAny().orElseThrow(() -> new IOException("Unable to find table by region identity"));
    }

    /**
     * Finds a table by region name.
     */
    public TableName findTableByRegion(String regionName) throws IOException {
        return findTableByRegion(regionName, getAllDistributions());
    }

    public TableName findTableByRegion(String regionName, Map<TableName, List<RegionDistributionModel>> allDistributions) throws IOException {
        return allDistributions.entrySet().stream()
                .filter(e -> e.getValue().stream().anyMatch(rdm -> rdm.getRegionModel().getName().equals(regionName)))
                .map(Map.Entry::getKey)
                .findAny().orElseThrow(() -> new IOException("Unable to find table by region name"));
    }

    public List<RegionDistributionModel> getRegionDistributionWithStatuses(TableName tableName, int... status) throws IOException {
        List<RegionDistributionModel> distribution = new ArrayList<>();
        // Region servers whose regions need to be checked
        List<RegionServerModel> regionServersToCheck = getRegionServersForTable(tableName);
        // Check the regions of each region server
        for (RegionServerModel regionServer : regionServersToCheck) {
            String regionServerRegionsPath = getRegionServerRegionsPath(tableName, regionServer);
            String[] regionServerRegionsData = splitCsvByLines(zooHelper.getNotNullString(regionServerRegionsPath));
            List<Integer> statuses = Arrays.stream(status).boxed().collect(Collectors.toList());
            List<RegionModel> regions = Arrays.stream(regionServerRegionsData)
                    .map(RegionModel::fromBase64String)
                    .filter(r -> statuses.contains(r.getStatusId()) || statuses.size() == 0)
                    .collect(Collectors.toList());
            addExtParam(tableName, regions);
            regions.forEach(r -> distribution.add(new RegionDistributionModel(r, regionServer)));
        }
        return distribution;
    }

    public List<List<RegionDistributionModel>> groupReplicasByRegion(List<RegionDistributionModel> distribution) {
        Map<String, List<RegionDistributionModel>> grouped = new HashMap<>();
        for (RegionDistributionModel rdm : distribution) {
            String regionName = rdm.getRegionModel().getName();
            if (!grouped.containsKey(regionName)) {
                grouped.put(regionName, new ArrayList<>());
            }
            grouped.get(regionName).add(rdm);
        }
        return new ArrayList<>(grouped.values());
    }

    /**
     * Finds a replica by the given parameters; replicaNum must match.
     */
    public RegionDistributionModel getTableReplica(List<RegionDistributionModel> tableRegions, String name, int replicaNum, int status) throws Exception {
        List<RegionDistributionModel> candidates = tableRegions.stream()
                .filter(rdm -> rdm.getRegionModel().getName().equals(name))
                .filter(rdm -> rdm.getRegionModel().getReplicaNum() == replicaNum)
                .filter(rdm -> rdm.getRegionModel().getStatusId() == status)
                .collect(Collectors.toList());
        if (candidates.size() != 1) {
            throw new Exception(String.format("Unable to find unique region matching conditions: name=%s, replicaNum=%s, statusId=%s",
                    name, replicaNum, status));
        }
        return candidates.get(0);
    }

    /**
     * Finds a replica by the given parameters; replicaNum must match and the status must not be excludedStatus.
     */
    public RegionDistributionModel getTableReplicaExcluded(List<RegionDistributionModel> tableRegions, String name, int replicaNum, List<Integer> excludedStatuses) throws Exception {
        List<RegionDistributionModel> candidates = tableRegions.stream()
                .filter(rdm -> rdm.getRegionModel().getName().equals(name))
                .filter(rdm -> rdm.getRegionModel().getReplicaNum() == replicaNum)
                .filter(rdm -> !excludedStatuses.contains(rdm.getRegionModel().getStatusId()))
                .collect(Collectors.toList());
        if (candidates.size() != 1) {
            throw new Exception(String.format("Unable to find unique region matching conditions: name=%s, replicaNum=%s, !statusId=%s",
                    name, replicaNum, excludedStatuses));
        }
        return candidates.get(0);
    }

    /**
     * Finds the source replica for recovery by the given parameters.
     */
    public RegionDistributionModel getTableReferencingReplica(List<RegionDistributionModel> tableRegions, String regionName) throws Exception {
        List<RegionDistributionModel> candidates = tableRegions.stream()
                .filter(rdm -> rdm.getRegionModel().getName().equals(regionName))
                .filter(rdm -> isReferencingRegionStatus(rdm.getRegionModel().getStatusId()))
                .collect(Collectors.toList());
        if (candidates.size() != 1) {
            throw new Exception(String.format("Unable to find unique region matching conditions: name=%s, statusId=REFERENCING", regionName));
        }
        return candidates.get(0);
    }

    /**
     * Finds the split counterpart of a replica by the given parameters.
     */
    public RegionDistributionModel getTableSplittedReplica(List<RegionDistributionModel> tableRegions, String regionName, int replicaNum, int status) throws Exception {
        List<RegionDistributionModel> candidates = tableRegions.stream()
                .filter(rdm -> rdm.getRegionModel().getName().startsWith(regionName + "-"))
                .filter(rdm -> rdm.getRegionModel().getStatusId() == status)
                .filter(rdm -> rdm.getRegionModel().getReplicaNum() == replicaNum)
                .collect(Collectors.toList());
        if (candidates.size() != 1) {
            throw new Exception(String.format("Unable to find unique region matching conditions: name starts with \"%s\", replicaNum=%s, statusId=%s", regionName, replicaNum, status));
        }
        return candidates.get(0);
    }

    /**
     * Finds a replica in the given status; replicaNum must match.
     */
    public RegionDistributionModel getTableReplica(List<RegionDistributionModel> tableRegions, RegionDistributionModel source, int desiredStatus) throws Exception {
        return getTableReplica(tableRegions, source.getRegionModel().getName(), source.getRegionModel().getReplicaNum(), desiredStatus);
    }

    /**
     * Checks whether a replica in the given status exists; replicaNum must match.
     */
    public boolean isTableContainsReplica(List<RegionDistributionModel> tableRegions, RegionDistributionModel source, int desiredStatus) {
        return tableRegions.stream()
                .filter(rdm -> rdm.getRegionModel().getName().equals(source.getRegionModel().getName()))
                .filter(rdm -> rdm.getRegionModel().getReplicaNum() == source.getRegionModel().getReplicaNum())
                .anyMatch(rdm -> rdm.getRegionModel().getStatusId() == desiredStatus);
    }

    /**
     * Creates a copy of the given replica with changed parameters.
     */
    public RegionDistributionModel makeRdmCopy(RegionDistributionModel referenceRdm, RegionServerModel regionServer, int status) {
        RegionModel original = referenceRdm.getRegionModel();
        RegionModel modified = new RegionModel(original.getName(),
                original.getReplicaNum(), status,
                original.getBeginKey(),
                original.getEndKey(),
                original.getDataBlockSize(),
                original.getCompressType(),
                original.getRecordCompressionType(),
                original.getDictionary(),
                original.getRecordTargetCount());
        return new RegionDistributionModel(modified, regionServer);
    }

    public RegionDistributionModel makeRdmCopy(RegionDistributionModel referenceRdm, RegionServerModel regionServer, byte[] beginKey, byte[] endKey) {
        RegionModel original = referenceRdm.getRegionModel();
        RegionModel modified = new RegionModel(original.getName(),
                original.getReplicaNum(),
                original.getStatusId(),
                beginKey,
                endKey,
                original.getDataBlockSize(),
                original.getCompressType(),
                original.getRecordCompressionType(),
                original.getDictionary(),
                original.getRecordTargetCount());
        return new RegionDistributionModel(modified, regionServer);
    }

    public List<String> listNamespace() throws IOException {
        return zooHelper.watchedGetChildren().stream()
                .filter(n -> !n.equals(SNAPSHOT_DIR))
                .filter(n -> !n.equals(ATALANTA_METADATA))
                .filter(n -> !n.equals(F_MASTER_NODE))
                .filter(n -> !n.equals(REGION_SERVER_INFO_SUBPATH))
                .filter(n -> !n.equals(REGION_SERVER_EPHEMERAL_SUBPATH))
                .collect(Collectors.toList());
    }

    public List<String> listSnapshot() throws IOException {
        return zooHelper.watchedGetChildren(SNAPSHOT_ZNODE);
    }

    // TODO: Handle paths that cannot be a table
    public List<TableName> listNamespaceTables(String namespace) throws IOException {
        return zooHelper.watchedGetChildren(namespace).stream()
                .map(table -> TableName.valueOf(namespace, table))
                .collect(Collectors.toList());
    }

    /**
     * Lists all region servers registered in ZooKeeper. Some of them may be down.
     */
    public List<RegionServerModel> getAllRegionServers() throws Exception {
        return zookeeperHelperExtended.getAllRegionServerInfoIds().stream()
                .map(this::getRegionServerModel)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public List<RegionServerModel> getAliveRegionServers() throws Exception {
        return zookeeperHelperExtended.getAliveRegionServerIds().stream()
                .map(this::getRegionServerModel)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public List<RegionServerModel> getAliveOrDisabledRegionServers() throws Exception {
        return zookeeperHelperExtended.getAliveOrDisabledRegionServerIds().stream()
                .map(this::getRegionServerModel)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public List<RegionServerModel> getRunningRegionServers() throws Exception {
        return zookeeperHelperExtended.getRunningRegionServerIds().stream()
                .map(this::getRegionServerModel)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public RegionServerModel getRegionServerModel(String regionServerId) {
        try {
            String regionServerInfo = zookeeperHelperExtended.getRegionServerInfo(regionServerId);
            String[] regInfoArray = regionServerInfo.split(HOSTNAME_SEPARATOR);
            return new RegionServerModel(
                    regionServerId,
                    regInfoArray[0],
                    regInfoArray.length > 0 ? Integer.parseInt(regInfoArray[1]) : 0,
                    regInfoArray.length > 1 ? Integer.parseInt(regInfoArray[2]) : 0,
                    regInfoArray.length > 2 ? Integer.parseInt(regInfoArray[3]) : 0);
        } catch (Exception e) {
            return null;
        }
    }

    // Returns the regions of the given RS within the given table
    public List<RegionModel> getRegionServerRegions(TableName tableName, RegionServerModel regionServer) throws Exception {
        String regionServerRegionsPath = getRegionServerRegionsPath(tableName, regionServer);
        String[] regionServerRegionsData = splitCsvByLines(zooHelper.getNotNullString(regionServerRegionsPath));
        List<RegionModel> regionModelList = Arrays.stream(regionServerRegionsData)
                .map(RegionModel::fromBase64String)
                .collect(Collectors.toList());
        addExtParam(tableName, regionModelList);
        return addExtParam(tableName, regionModelList);
    }

    public List<RegionModel> addExtParam(TableName tableName, List<RegionModel> regionModelList) {
        if (regionModelList.isEmpty()) {
            return regionModelList;
        }
        RegionModel src = addExtParam(tableName, regionModelList.get(0));
        for(int i = 1; i < regionModelList.size(); ++i) {
            cpyExtParam(src, regionModelList.get(i));
        }
        return regionModelList;
    }

    private void cpyExtParam(RegionModel src, RegionModel regionModel) {
        regionModel.setDictionary(src.getDictionary());
        regionModel.setRecordCompressionType(src.getRecordCompressionType());
        regionModel.setRecordTargetCount(src.getRecordTargetCount());
    }

    public RegionModel addExtParam(TableName tableName, RegionModel rm) {
        try {
            String basePath = getBasePathForTableExtParam(tableName);
            String path = String.join(ZK_SEPARATOR, basePath, RECORD_COMPRESSION_TYPE);
            if (zooHelper.checkExists(path)) {
                String v = zooHelper.getNotNullString(path);
                if (StringUtils.isNotEmpty(v)) {
                    rm.setRecordCompressionType(RecordCompressionType.valueOf(v));
                }
            }
            path = String.join(ZK_SEPARATOR, basePath, DICTIONARY);
            if (zooHelper.checkExists(path)) {
                String v = zooHelper.getNotNullString(path);
                if (StringUtils.isNotEmpty(v)) {
                    rm.setDictionary(Boolean.valueOf(v));
                }
            }
            path = String.join(ZK_SEPARATOR, basePath, RECORD_TARGET_COUNT);
            if (zooHelper.checkExists(path)) {
                String v = zooHelper.getNotNullString(path);
                if (StringUtils.isNotEmpty(v)) {
                    rm.setRecordTargetCount(Long.valueOf(v));
                }
            }
            path = String.join(ZK_SEPARATOR, basePath, COMPRESSION_TYPE);
            if (zooHelper.checkExists(path)) {
                String v = zooHelper.getNotNullString(path);
                if (StringUtils.isNotEmpty(v)) {
                    rm.setCompressType(CompressType.valueOf(v));
                }
            }
        } catch (ZKException ex) {
            LOGGER.error("Cannot get ext region param from ZK", ex);
            throw new RuntimeException(ex);
        }
        return rm;
    }

    public static String getBasePathForTableExtParam(TableName tableName) {
        String basePath = String.join(ZK_SEPARATOR, ATALANTA_NS, tableName.getNamespace(),
                tableName.getTableName());
        return basePath;
    }

    // Returns the regions of the given RS grouped by table
    public Map<TableName, List<RegionDistributionModel>> getRegionServerRegions(Map<TableName, List<RegionDistributionModel>> allDistributions, RegionServerModel regionServer) throws Exception {
        Map<TableName, List<RegionDistributionModel>> result = new HashMap<>();
        allDistributions.forEach((tableName, regions) -> regions.stream()
                .filter(rdm -> rdm.getRegionServerModel().equals(regionServer))
                .forEach(rdm -> {
                    if (result.containsKey(tableName)) {
                        result.get(tableName).add(rdm);
                    } else {
                        List<RegionDistributionModel> rdms = new ArrayList<>();
                        rdms.add(rdm);
                        result.put(tableName, rdms);
                    }
                }));
        return result;
    }

    public List<CuratorOp> addExtParamToPlan(List<CuratorOp> transactions, TableName tableName, RegionModel rm) throws ZKException {
        if (rm.getReplicaNum() != 0) {
            return transactions;
        }
        String basePath = getBasePathForTableExtParam(tableName);
        if (!zooHelper.checkExists(basePath)) {
            zooHelper.create(basePath, "");
        }

        String path = String.join(ZK_SEPARATOR, basePath, DICTIONARY);
        addOp(transactions, path, String.valueOf(rm.getDictionary()));

        if (rm.getRecordCompressionType() != null) {
            path = String.join(ZK_SEPARATOR, basePath, RECORD_COMPRESSION_TYPE);
            addOp(transactions, path, rm.getRecordCompressionType().name());
        }

        if (rm.getRecordTargetCount() != null) {
            path = String.join(ZK_SEPARATOR, basePath, RECORD_TARGET_COUNT);
            addOp(transactions, path, String.valueOf(rm.getRecordTargetCount()));
        }

        if (rm.getCompressType() != null) {
            path = String.join(ZK_SEPARATOR, basePath, COMPRESSION_TYPE);
            addOp(transactions, path, String.valueOf(rm.getCompressType()));
        }

        return transactions;
    }

    private void addOp(List<CuratorOp> transactions, String path, String value) throws ZKException {
        boolean isExist = zooHelper.checkExists(path);
        CuratorOp op = isExist ? zooHelper.setDataTrans(path, value) : zooHelper.createTrans(path, value);
        transactions.add(op);
    }

    public void addExtParamToPlan(List<CuratorOp> plan, TableName tableName, List<RegionModel> regionModelList) throws ZKException {
        if (!regionModelList.isEmpty()) {
            addExtParamToPlan(plan, tableName, regionModelList.get(0));
        }
    }

    public void checkTableNotDisable(TableName tableName) throws IOException {
        String tablePath = tableName.getNamespace() + ZK_SEPARATOR + tableName.getTableName();
        if (zooHelper.checkExists(tablePath)
                && zooHelper.get(tablePath).map( s -> "status=disabled".equals(s)).get()) {
            throw new IOException("Table '" + tableName + "' is disabled");
        }
    }
}
