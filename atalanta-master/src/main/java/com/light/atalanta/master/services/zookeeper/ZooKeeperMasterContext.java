/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.zookeeper;

import com.google.common.base.Preconditions;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.NoRetryException;
import com.light.atalanta.exceptions.NotEnoughRegionServerInstances;
import com.light.atalanta.exceptions.RegionNotFound;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.master.vo.TestConfiguration;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.services.ZookeeperCacheInvalidationListener;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.services.ZookeeperHelperExtended;
import com.light.atalanta.util.Bytes;
import com.light.atalanta.util.ZookeeperStatus;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.CSV_NEWLINE;
import static com.light.atalanta.ATALANTAConstants.ZK_SEPARATOR;
import static com.light.atalanta.master.services.operations.RecoverTable.getRegionServerWithMinRegion;
import static com.light.atalanta.services.ZooKeeperContextHelper.ASSIGNING_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.splitCsvByLines;

/**
 * Works with the database configuration in ZooKeeper on the master side.
 * Extends ZooKeeperCoreContext.
 */
public class ZooKeeperMasterContext extends ZooKeeperCoreContext implements ZookeeperCacheInvalidationListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperMasterContext.class);

    private final ZookeeperHelper zooHelper;

    private final TestConfiguration testConfiguration;

    private Map<TableName, List<RegionDistributionModel>> allDistribution = new ConcurrentHashMap<>();

    public ZooKeeperMasterContext(ZookeeperHelper zooHelper,
                                  ZookeeperHelperExtended zookeeperHelperExtended,
                                  AtalantaDBConfiguration fastDBConf) {
        super(zooHelper, zookeeperHelperExtended);
        this.zooHelper = zooHelper;
        this.testConfiguration = fastDBConf.getTestConfiguration();
        zooHelper.addZookeeperCacheInvalidationListener(ZK_SEPARATOR, this);
    }

    /**
     * Returns a random live region server that doesn't host the given region.
     *
     * @param oldDistribution actual region distribution in ZooKeeper
     * @param newDistribution planned region distribution
     * @param without         region servers hosting this region are excluded
     */
    public RegionServerModel getRandomRunningRegionServerWithoutRegionForCreateTable(List<RegionServerModel> runningRS, List<RegionDistributionModel> oldDistribution, List<RegionDistributionModel> newDistribution, RegionModel without) throws Exception {
        // Region servers to pick N random ones from
        List<RegionServerModel> regionServersChooseFrom = new ArrayList<>(runningRS);
        // Region servers that must not be in the target list
        List<RegionServerModel> regionServerWithout = new ArrayList<>();
        // The actual region distribution
        oldDistribution.stream()
                .filter(region -> region.getRegionModel().getName().equals(without.getName()))
                .map(RegionDistributionModel::getRegionServerModel)
                .forEach(regionServerWithout::add);
        // The planned region distribution
        newDistribution.stream()
                .filter(region -> region.getRegionModel().getName().equals(without.getName()))
                .map(RegionDistributionModel::getRegionServerModel)
                .forEach(regionServerWithout::add);
        regionServersChooseFrom.removeAll(regionServerWithout);
        List<RegionModel> newRegions = newDistribution.stream()
                .map(rdm -> rdm.getRegionModel())
                .collect(Collectors.toList());
        oldDistribution.stream()
                .filter(rdm -> !newRegions.contains(rdm.getRegionModel()))
                .collect(Collectors.toList());
        ArrayList<RegionDistributionModel> tableRdms = new ArrayList<>(oldDistribution);
        tableRdms.addAll(newDistribution);
        List<RegionServerModel> targetRsList = getRegionServerWithMinRegion(tableRdms, regionServersChooseFrom);
        if (targetRsList.isEmpty()) {
            targetRsList = regionServersChooseFrom;
        }
        return getRandomRegionServer(targetRsList);
    }

    /**
     * Returns a random live region server that doesn't host the given region.
     *
     * @param distribution actual region distribution in ZooKeeper
     * @param without      region servers hosting this region are excluded
     */
    public RegionServerModel getRandomRunningRegionServerWithoutRegionForRecovery(List<RegionServerModel> runningRS, List<RegionDistributionModel> distribution, RegionModel without) throws Exception {
        // Region servers to pick N random ones from
        List<RegionServerModel> regionServersChooseFrom = new ArrayList<>(runningRS);
        // Region servers that must not be in the target list
        List<RegionServerModel> regionServerWithout = new ArrayList<>();
        // The actual region distribution
        distribution.stream()
                .filter(region -> region.getRegionModel().getName().equals(without.getName()))
                .map(RegionDistributionModel::getRegionServerModel)
                .forEach(regionServerWithout::add);
        regionServersChooseFrom.removeAll(regionServerWithout);
        List<RegionServerModel> targetRs = getRegionServerWithMinRegion(distribution, regionServersChooseFrom);
        return getRandomRegionServer(targetRs);
    }

    // Returns the replicas of a region that are not running (except excluded)
    public Set<RegionDistributionModel> findNotRunningReplicas(List<RegionDistributionModel> rdms, RegionModel region, List<RegionDistributionModel> excluded, List<RegionServerModel> runningRS) throws Exception {
        Set<RegionDistributionModel> notRunningReplicas = new HashSet<>();
        List<RegionDistributionModel> rdmsToCheck = rdms.stream()
                .filter(rdm -> rdm.getRegionModel().getName().equals(region.getName()))
                .filter(rdm -> !excluded.contains(rdm))
                .collect(Collectors.toList());
        rdmsToCheck.stream()
                .filter(rdm -> !runningRS.contains(rdm.getRegionServerModel()))
                .forEach(notRunningReplicas::add);
        rdmsToCheck.stream()
                .filter(rdm -> rdm.getRegionModel().getStatusId() == ASSIGNING_REGION_STATUS)
                .forEach(notRunningReplicas::add);
        return notRunningReplicas;
    }

    /**
     * Returns a random region server from the given list.
     *
     * @param regionServers region servers to pick from
     */
    public RegionServerModel getRandomRegionServer(List<RegionServerModel> regionServers) throws NotEnoughRegionServerInstances {
        if (regionServers.size() < 1) {
            throw new NotEnoughRegionServerInstances(0);
        }
        Random random = new Random();
        return regionServers.get(random.nextInt(regionServers.size()));
    }

    /**
     * Returns the distributed region model with the lowest replicaNum from the given list.
     *
     * @param rdms distributed region models to pick from
     */
    public RegionDistributionModel getMinimumNumberRdm(RegionDistributionModel lostRdm, List<RegionDistributionModel> rdms) throws Exception {
        if (rdms.size() < 1) {
            throw new NotEnoughRegionServerInstances("unable to find alive replica references for: " + lostRdm.toShortString(), 0);
        }
        return rdms.stream()
                .sorted(Comparator.comparingInt(o -> o.getRegionModel().getReplicaNum()))
                .collect(Collectors.toList()).get(0);
    }

    public RegionServerModel findRunningRegionServerByName(String regionServerName) throws Exception {
        return getRunningRegionServers().stream()
                .filter(rs -> rs.getName().equals(regionServerName))
                .findFirst()
                .orElseThrow(() -> new RSNotFoundOrNotRunningException(regionServerName));
    }

    public List<CuratorOp> deleteRegionServerFromTableTrans(TableName tableName, RegionServerModel regionServer) throws Exception {
        List<CuratorOp> transactions = new ArrayList<>();
        String regionServerPath = ZooKeeperContextHelper.getRegionServerPath(tableName, regionServer);
        String regionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, regionServer);
        String apiLocationPath = ZooKeeperContextHelper.getRegionServerApiLocationPath(tableName, regionServer);
        transactions.add(zooHelper.deleteTrans(regionsPath));
        transactions.add(zooHelper.deleteTrans(apiLocationPath));
        transactions.add(zooHelper.deleteTrans(regionServerPath));
        return transactions;
    }

    /* ------------ REGIONS ------------ */

    public List<CuratorOp> addNewRegionsTrans(TableName tableName, List<RegionModel> regions, RegionServerModel regionServer) throws Exception {
        List<CuratorOp> transactions = new ArrayList<>();
        String regionServerPath = ZooKeeperContextHelper.getRegionServerPath(tableName, regionServer);
        // Add a line to /atalanta/{namespace}/{table_name}/{rs_name}/api_location
        String apiLocationPath = ZooKeeperContextHelper.getRegionServerApiLocationPath(tableName, regionServer);
        String apiLocationData = regionServer.getLocationString();
        if (!zooHelper.checkExists(regionServerPath)) {
            transactions.add(zooHelper.createTrans(regionServerPath));
        }
        if (!zooHelper.checkExists(apiLocationPath)) {
            transactions.add(zooHelper.createTrans(apiLocationPath, apiLocationData));
        }
        // Add new regions to a region server in ZK /atalanta/{namespace}/{table_name}/{rs_name}/regions
        String regionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, regionServer);
        StringBuilder regionsData = new StringBuilder(StringUtils.EMPTY);
        for (RegionModel regionToAdd : regions) {
            if (!regionsData.toString().equals(StringUtils.EMPTY)) {
                regionsData.append(CSV_NEWLINE);
            }
            regionsData.append(regionToAdd.toBase64StringForZk());
        }
        if (zooHelper.checkExists(regionsPath)) {
            String existingData = zooHelper.getNotNullString(regionsPath);
            if (StringUtils.isBlank(existingData)) {
                transactions.add(zooHelper.setDataTrans(regionsPath, regionsData.toString()));
            } else {
                for (RegionModel regionToAdd : regions) {
                    // Check that the current RS has no duplicate replicas
                    Preconditions.checkArgument(
                            Arrays.stream(splitCsvByLines(existingData))
                                    .map(RegionModel::fromBase64String)
                                    .noneMatch(r -> r.toIdentityString().equals(regionToAdd.toIdentityString())),
                            "Unable to add duplicated replica: " + regionToAdd.toIdentityString()
                                    + " to RS: " + regionServer.getName());
                }
                transactions.add(zooHelper.setDataTrans(regionsPath, existingData + CSV_NEWLINE + regionsData.toString()));
            }
        } else {
            transactions.add(zooHelper.createTrans(regionsPath, regionsData.toString()));
        }
        return transactions;
    }

    public boolean setRegionStatus(TableName tableName, List<RegionDistributionModel> distributedRegions, int newStatus) throws Exception {
        Map<RegionServerModel, List<RegionDistributionModel>> m = distributedRegions.stream()
                .collect(Collectors.groupingBy(RegionDistributionModel::getRegionServerModel));
        String regionsPath = StringUtils.EMPTY;
        for (Map.Entry<RegionServerModel, List<RegionDistributionModel>> e : m.entrySet()) {
            RegionServerModel regionServer = e.getKey();
            regionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, regionServer);
            if (!zooHelper.checkExists(regionsPath)) {
                throw new Exception(String.format("unable to change region '%s:%s' status to '%s': ZooKeeper path '%s' is not exists", tableName, regionServer, newStatus, regionsPath));
            }
            String regionsData = zooHelper.getNotNullString(regionsPath);
            for (RegionDistributionModel rdm: e.getValue()) {
                regionsData = setRegionStatusForData(regionsData, tableName, rdm, newStatus);
            }
            zooHelper.setDataNoUpdateCache(regionsPath, Bytes.toBytes(regionsData));
        }
        if (StringUtils.isNotEmpty(regionsPath)) {
            zooHelper.updateCache(regionsPath);
        }
        return true;
    }

    @Deprecated
    public boolean setRegionStatus(TableName tableName, RegionDistributionModel distributedRegion, int newStatus) throws Exception {
        RegionServerModel regionServer = distributedRegion.getRegionServerModel();
        String regionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, regionServer);
        String regionsData = setRegionStatusInternal(tableName, distributedRegion, newStatus);
        zooHelper.setData(regionsPath, regionsData);
        return true;
    }

    // Same as setRegionStatus(), with transactions
    public CuratorOp setRegionStatusesTrans(TableName tableName, List<RegionModel> regions, RegionServerModel regionServer, int newStatus) throws Exception {
        String regionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, regionServer);
        String regionsData = zooHelper.getNotNullString(regionsPath);
        for (RegionModel region : regions) {
            regionsData = setRegionStatusForData(regionsData, tableName, new RegionDistributionModel(region, regionServer), newStatus);
        }
        return zooHelper.setDataTrans(regionsPath, regionsData);
    }

    private String setRegionStatusInternal(TableName tableName, RegionDistributionModel rdm, int newStatus) throws Exception {
        String regionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, rdm.getRegionServerModel());
        if (!zooHelper.checkExists(regionsPath)) {
            throw new Exception(String.format("unable to change region '%s:%s' status to '%s': ZooKeeper path '%s' is not exists", tableName, rdm.getRegionModel(), newStatus, regionsPath));
        }
        String regionsData = zooHelper.getNotNullString(regionsPath);
        return setRegionStatusForData(regionsData, tableName, rdm, newStatus);
    }

    public String setRegionStatusForData(String regionsData, TableName tableName, RegionDistributionModel rdm, int newStatus) throws Exception {
        RegionModel region = rdm.getRegionModel();
        RegionServerModel regionServer = rdm.getRegionServerModel();
        AtomicBoolean regionChanged = new AtomicBoolean(false);
        String[] regions = ZooKeeperContextHelper.splitCsvByLines(regionsData);
        List<String> regionStatuses = Arrays.stream(regions)
                .map(r -> {
                    RegionModel currentRegion = RegionModel.fromBase64String(r);
                    if (currentRegion.equals(region)) {
                        RegionModel newRegion = new RegionModel(region.getName(),
                                region.getReplicaNum(),
                                newStatus,
                                region.getBeginKey(),
                                region.getEndKey(),
                                region.getDataBlockSize(),
                                region.getCompressType(),
                                region.getRecordCompressionType(),
                                region.getDictionary(),
                                region.getRecordTargetCount());
                        if (!regionChanged.get()) {
                            regionChanged.set(true);
                        }
                        return newRegion.toBase64StringForZk();
                    } else {
                        return currentRegion.toBase64StringForZk();
                    }
                }).collect(Collectors.toList());
        if (!regionChanged.get()) {
            throw new RegionNotFound(String.format("unable to change region '%s:%s' status to '%s': not found in regions list of '%s'", tableName, region, newStatus, regionServer));
        }
        return StringUtils.join(regionStatuses, CSV_NEWLINE);
    }

    public String setRegionBordersForData(String regionsData, TableName tableName, RegionDistributionModel rdm, int status, byte[] newBeginKey, byte[] newEndKey) throws Exception {
        RegionModel region = rdm.getRegionModel();
        RegionServerModel regionServer = rdm.getRegionServerModel();
        AtomicBoolean regionChanged = new AtomicBoolean(false);
        String[] regions = ZooKeeperContextHelper.splitCsvByLines(regionsData);
        List<String> regionStatuses = Arrays.stream(regions)
                .map(r -> {
                    RegionModel currentRegion = RegionModel.fromBase64String(r);
                    if (currentRegion.equals(region)) {
                        RegionModel newRegion = new RegionModel(region.getName(),
                                region.getReplicaNum(),
                                status,
                                newBeginKey,
                                newEndKey,
                                region.getDataBlockSize(),
                                region.getCompressType(),
                                region.getRecordCompressionType(),
                                region.getDictionary(),
                                region.getRecordTargetCount());
                        if (!regionChanged.get()) {
                            regionChanged.set(true);
                        }
                        return newRegion.toBase64StringForZk();
                    } else {
                        return currentRegion.toBase64StringForZk();
                    }
                }).collect(Collectors.toList());
        if (!regionChanged.get()) {
            String beginKeyStr = Bytes.toHex(newBeginKey).equals(StringUtil.EMPTY_STRING) ? "0" : Bytes.toHex(newBeginKey);
            String endKeyStr = Bytes.toHex(newEndKey);
            throw new RegionNotFound(String.format("unable to change region '%s:%s' borders to '%s:%s', status to '%s': not found in regions list of '%s'", tableName, region, beginKeyStr, endKeyStr, status, regionServer));
        }
        return StringUtils.join(regionStatuses, CSV_NEWLINE);
    }

    public String setRegionNameAndStatusForData(String regionsData, TableName tableName, RegionDistributionModel rdm, String newName, int newStatus) throws Exception {
        RegionModel region = rdm.getRegionModel();
        RegionServerModel regionServer = rdm.getRegionServerModel();
        AtomicBoolean regionChanged = new AtomicBoolean(false);
        String[] regions = ZooKeeperContextHelper.splitCsvByLines(regionsData);
        List<String> regionStatuses = Arrays.stream(regions)
                .map(r -> {
                    RegionModel currentRegion = RegionModel.fromBase64String(r);
                    if (currentRegion.equals(region)) {
                        RegionModel newRegion = new RegionModel(newName,
                                region.getReplicaNum(),
                                newStatus,
                                region.getBeginKey(),
                                region.getEndKey(),
                                region.getDataBlockSize(),
                                region.getCompressType(),
                                region.getRecordCompressionType(),
                                region.getDictionary(),
                                region.getRecordTargetCount());
                        if (!regionChanged.get()) {
                            regionChanged.set(true);
                        }
                        return newRegion.toBase64StringForZk();
                    } else {
                        return currentRegion.toBase64StringForZk();
                    }
                }).collect(Collectors.toList());
        if (!regionChanged.get()) {
            throw new RegionNotFound(String.format("unable to change region '%s:%s' name to '%s' status to '%s': not found in regions list of '%s'", tableName, region, newName, newStatus, regionServer));
        }
        return StringUtils.join(regionStatuses, CSV_NEWLINE);
    }

    public String removeRegionForData(String regionData, TableName tableName, RegionDistributionModel rdm) throws RegionNotFound {
        String[] regionsData = ZooKeeperContextHelper.splitCsvByLines(regionData);
        AtomicBoolean regionRemoved = new AtomicBoolean(false);
        List<String> regionStatuses = Arrays.stream(regionsData)
                .filter(r -> {
                    boolean found = RegionModel.fromBase64String(r).equals(rdm.getRegionModel());
                    if (!regionRemoved.get()) {
                        regionRemoved.set(found);
                    }
                    return !found;
                })
                .collect(Collectors.toList());
        if (!regionRemoved.get()) {
            throw new RegionNotFound(String.format("unable to delete region '%s:%s': not found in regions list of '%s'", tableName, rdm.getRegionModel(), rdm.getRegionServerModel()));
        }
        return StringUtils.join(regionStatuses, CSV_NEWLINE);
    }

    public CuratorOp removeRegionTrans(TableName tableName, RegionDistributionModel rdm) throws Exception {
        RegionModel region = rdm.getRegionModel();
        RegionServerModel regionServer = rdm.getRegionServerModel();
        String regionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(tableName, regionServer);
        if (!zooHelper.checkExists(regionsPath)) {
            throw new Exception(String.format("unable to delete region '%s:%s': ZooKeeper path '%s' is not exists", tableName, region, regionsPath));
        }
        return zooHelper.setDataTrans(regionsPath, removeRegionForData(zooHelper.getNotNullString(regionsPath), tableName, rdm));
    }

    public int loadAllDistribution() {
        try {
            allDistribution = new ConcurrentHashMap<>();
            for (String namespace : listNamespace()) {
                for (TableName tableName : listNamespaceTables(namespace)) {
                    allDistribution.put(tableName, getRegionDistributionWithStatuses(tableName));
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException("Unable to read regions distribution", ex);
        }
        return allDistribution.size();
    }

    @Override
    public void updateCache(List<String> pathList) {
        Map<String, Set<String>> tablesMap = new HashMap<>();
        for (String path : pathList) {
            String[] splitPath = (path.startsWith(ZK_SEPARATOR) ? path : (ZK_SEPARATOR + path)).split(ZK_SEPARATOR);
            String ns = splitPath.length > 1 ? splitPath[1] : null;
            String table = splitPath.length > 2 ? splitPath[2] : null;
            Set<String> e = tablesMap.get(ns);
            if (e == null) {
                updateCache(ns, table);
                HashSet<String> tableSet = new HashSet<>();
                tableSet.add(table);
                tablesMap.put(ns, tableSet);
            } else if (!e.contains(table)) {
                updateCache(ns, table);
                e.add(table);
            }
        }
    }

    public ZookeeperStatus getTableStatus(TableName tableName) throws ZKException {
        ZookeeperStatus tableStatus;
        String tablePath = ZooKeeperContextHelper.getTablePath(tableName);
        tableStatus = ZookeeperStatus.fromString(zooHelper.getNotNullString(tablePath));
        return tableStatus;
    }

    private void updateCache(String ns, String table) {
        try {

            if (ns == null) {
                loadAllDistribution();
            } else if (table == null) {
                for (TableName tableName : listNamespaceTables(ns)) {
                    allDistribution.put(tableName, getRegionDistributionWithStatuses(tableName));
                }
            } else {
                TableName tableName = TableName.valueOf(ns, table);
                if (zooHelper.checkExists(ns + ZK_SEPARATOR + table)) {
                    allDistribution.put(tableName, getRegionDistributionWithStatuses(tableName));
                } else {
                    allDistribution.remove(tableName);
                }
            }
        } catch (IOException ex) {
            LOGGER.error("update cache error!", ex);
        } finally {
            if (LOGGER.isTraceEnabled()) {
                allDistribution.forEach((k, v) -> LOGGER.trace("table in cache: {}", k));
            }
        }
    }

    @Override
    public Map<TableName, List<RegionDistributionModel>> getAllDistributions() {
        return allDistribution;
    }

    public static class RSNotFoundOrNotRunningException extends NoRetryException {
        public RSNotFoundOrNotRunningException(String regionServerName) {
            super(String.format("region server with name '%s' is not running or not exist", regionServerName));
        }
    }
}
