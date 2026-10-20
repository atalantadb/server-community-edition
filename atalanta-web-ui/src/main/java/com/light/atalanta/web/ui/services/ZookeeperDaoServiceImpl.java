/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.services;

import com.light.atalanta.TableName;
import com.light.atalanta.metrics.RegionMetrics;
import com.light.atalanta.metrics.RegionServerDiskInfo;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.services.ZookeeperHelperExtended;
import com.light.atalanta.util.Bytes;
import com.light.atalanta.web.ui.model.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static com.light.atalanta.ATALANTAConstants.*;
import static com.light.atalanta.web.ui.WebUiUtils.toHumanReadebleDiskSize;
import static io.netty.util.internal.StringUtil.toHexString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

@Service
public class ZookeeperDaoServiceImpl implements ZookeeperDaoService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperDaoServiceImpl.class.getName());
    public static final String RS_STATUS_STOPPED = "STOPPED";

    private final ZooKeeperCoreContext zooKeeperCoreContext;

    private final CuratorFramework curatorFramework;

    private final AtomicBoolean tryConnect = new AtomicBoolean(false);

    private final ZookeeperHelperExtended zookeeperHelperExtended;

    @Autowired
    public void setRsMetricsService(@Lazy RsMetricsService rsMetricsService) {
        this.rsMetricsService = rsMetricsService;
    }

    private RsMetricsService rsMetricsService;

    public ZookeeperDaoServiceImpl(ZooKeeperCoreContext zooKeeperCoreContext, CuratorFramework curatorFramework,
                                   ZookeeperHelperExtended zookeeperHelperExtended) {
        this.zooKeeperCoreContext = zooKeeperCoreContext;
        this.curatorFramework = curatorFramework;
        this.zookeeperHelperExtended = zookeeperHelperExtended;
    }

    @Override
    public boolean checkConnection() {
        if (curatorFramework.getZookeeperClient().isConnected()) {
            return true;
        }
        if (!tryConnect.get()) {
            new Thread(this::tryStartCurator).start();
        }
        return false;
    }

    @Override
    public String getZkConnectionString() {
        return curatorFramework.getZookeeperClient().getCurrentConnectionString();
    }

    @Override
    public String getLeaderInfo() {
        try {
            return getMasterUrl();
        } catch (Exception e) {
            return "none: " + e.getMessage();
        }
    }

    @Override
    public String getMasterUrl() throws Exception {
        byte[] masterUrl = curatorFramework.getData().forPath(ZK_SEPARATOR + F_MASTER_NODE);
        return Optional.ofNullable(masterUrl)
                .map(Bytes::toString)
                .orElseThrow(() -> new RuntimeException("master url is null"));
    }

    @Override
    public List<FMasterFollowerInfo> getFollowerInfo() {
        List<FMasterFollowerInfo> res = Collections.emptyList();
        try {
            List<String> children = curatorFramework.getChildren().forPath(ZK_SEPARATOR + F_MASTER_NODE);
            res = children.parallelStream()
                    .map(this::getFollowerInfo)
                    .sorted(Comparator.comparing(FMasterFollowerInfo::getZnodeNbr))
                    .collect(toList());
            if (!res.isEmpty()) {
                res.get(0).setLeader(true);
            }
        } catch (Exception e) {
            LOGGER.info("cannot read /fmaster children", e);
        }
        return res;
    }

    @Override
    public List<RegionServerModelExt> getAllRegionServers() {
        List<RegionServerModelExt> res = Collections.emptyList();
        try {
            res = getRegionServerModelExts();
            res.forEach(r -> {
                r.setRegionServerMetrics(rsMetricsService.getRsMetrics(r));
                initMinDiskInfo(r);
            });
        } catch (Exception e) {
            LOGGER.info("cannot read /RegionServers children", e);
        }
        return res;
    }

    @Override
    public List<RegionServerModelExt> getRegionServerModelExts() throws Exception {
        List<String> children = curatorFramework.getChildren().forPath(ZK_SEPARATOR + REGION_SERVER_INFO_SUBPATH);
        return children.parallelStream()
                .map(this::getRegionServer)
                .sorted(Comparator.comparing(rs -> rs.getHost() + rs.getJavaPort()))
                .collect(toList());
    }

    @Override
    public List<NsModel> getNsList() {
        List<NsModel> res = Collections.emptyList();
        try {
            List<String> children = curatorFramework.getChildren().forPath(ZK_SEPARATOR);
            res = children.parallelStream()
                    .filter(ns -> !ATALANTA_METADATA.equals(ns)
                            && !SNAPSHOT_DIR.equals(ns)
                            && !REGION_SERVER_INFO_SUBPATH.equals(ns)
                            && !F_MASTER_NODE.equals(ns)
                            && !REGION_SERVER_EPHEMERAL_SUBPATH.equals(ns))
                    .sorted()
                    .map(this::getNsModel)
                    .collect(toList());
        } catch (Exception e) {
            LOGGER.info("cannot read root children", e);
        }
        return res;
    }

    @Override
    public List<SnModel> getSnList() {
        List<SnModel> res = Collections.emptyList();
        try {
            List<String> children = curatorFramework.getChildren().forPath(ZK_SEPARATOR + SNAPSHOT_ZNODE);
            res = children.parallelStream()
                    .sorted()
                    .map(this::getSnModel)
                    .collect(toList());
        } catch (Exception e) {
            LOGGER.info("cannot read root children", e);
        }
        return res;
    }

    private SnModel getSnModel(String s) {
        SnModel res = new SnModel();
        res.setName(s);
        try {
            List<String> nsList = curatorFramework.getChildren().forPath(ZK_SEPARATOR + String.join(ZK_SEPARATOR, SNAPSHOT_ZNODE, s));
            if (nsList.isEmpty()) {
                res.setNs("?");
                res.setTable("?");
            } else {
                String ns = nsList.get(0);
                res.setNs(ns);
                List<String> tableList = curatorFramework.getChildren().forPath(ZK_SEPARATOR + String.join(ZK_SEPARATOR, SNAPSHOT_ZNODE, s, ns));
                res.setTable(tableList.isEmpty() ? "?" : tableList.get(0));
            }
            return res;
        } catch (Exception e) {
            LOGGER.info("cannot read root children", e);
            res.setNs("?");
            res.setTable("?");
        }
        return res;
    }


    @Override

    public List<AtalantaTableModel> getTableList(String ns) {
        LOGGER.info("==> ZookeeperDaoServiceImpl.getTableList(ns: {})", ns);
        List<AtalantaTableModel> res = getTableNamesForNs(ns).parallelStream()
                .map(tableName -> getAtalantaTableModel(ns, tableName.split(":")[1])).collect(toList());
        LOGGER.info("<== ZookeeperDaoServiceImpl.getTableList(ns: {})", ns);
        return res;
    }


    private List<String> getTableNamesForNs(String ns) {
        try {
            return curatorFramework.getChildren().forPath(ZK_SEPARATOR + ns).parallelStream().map(t -> ns + ":" + t)
                    .sorted()
                    .collect(toList());
        } catch (Exception e) {
            LOGGER.info("cannot read /{} children", ns, e);
            return Collections.emptyList();
        }
    }

    @Override
    public int getTableCount(String ns) {
        LOGGER.info("==> ZookeeperDaoServiceImpl.getTableList(ns: {})", ns);
        int res = 0;
        try {
            res = curatorFramework.getChildren().forPath(ZK_SEPARATOR + ns).size();
        } catch (Exception e) {
            LOGGER.info("cannot read /{} children", ns, e);
        }
        LOGGER.info("<== ZookeeperDaoServiceImpl.getTableList(ns: {})", ns);
        return res;
    }

    @Override
    public List<AtalantaTableModel> getTableList(int pg, int pageSize) {
        if (pageSize != -1 && pageSize <= 0) {
            throw new IllegalArgumentException("Page size must be positive or -1 (all)");
        }
        Stream<String> tableNames = getNsList().parallelStream()
                .map(nsModel -> getTableNamesForNs(nsModel.getName()))
                .flatMap(Collection::stream)
                .sorted();
        if (pageSize != -1) {
            tableNames = tableNames.skip((long) Math.max(0, pg) * pageSize).limit(pageSize);
        }
        return tableNames.map(t -> {
                    String[] s = t.split(":");
                    return getAtalantaTableModel(s[0], s[1]);
                }).collect(toList());
    }

    @Override
    public int getTableCount() {
        return getNsList().parallelStream()
                .map(NsModel::getName)
                .mapToInt(this::getTableCount)
                .sum();
    }


    @Override
    public AtalantaTableModel getAtalantaTableModel(String ns, String tableName) {
        LOGGER.info("<==ZookeeperDaoServiceImpl.getAtalantaTableModel(ns: {}, table: {})", ns, tableName);
        AtalantaTableModel res = new AtalantaTableModel();
        res.setName(tableName);
        res.setNs(ns);
        res.setAllRunningRsNames(getRunningRSNames());
        res.setRegionList(getRegionList(ns, tableName));
        res.setStatus(getTableStatus(ns, tableName));
        if (res.getRegionList() != null && !res.getRegionList().isEmpty()) {
            res.setRsCount((long) res.getRegionList().size());
            res.setReplica1Count(res.getRegionList().parallelStream().filter(r -> r.getReplicaNum() == 1).count());
            res.setReplica2Count(res.getRegionList().parallelStream().filter(r -> r.getReplicaNum() == 2).count());
            Map<String, AtalantaTableModel> reginServerCountMap = res.getRegionServerCountMap();
            RegionMetrics regionMetrics = res.getRegionMetrics();
            for (AtalantaRegionInfo region : res.getRegionList()) {

                reginServerCountMap.compute(region.getHostPort(), (hostPort, ftm) -> {
                    AtalantaTableModel r = ftm == null ? new AtalantaTableModel() : ftm;
                    r.setRsCount(r.getRsCount() + 1);
                    RegionMetrics regionMetricsGroup = r.getRegionMetrics();
                    initMetrics(regionMetricsGroup, region);
                    return r;
                });

                initMetrics(regionMetrics, region);
            }
            res.setRowCount(calcRowCount(res.getRegionList()));
            res.setSize(calcSize(res.getRegionList()));
            res.setDataBlockSize(res.getRegionList().get(0).getDataBlockSize());
        }
        LOGGER.info("==> ZookeeperDaoServiceImpl.getAtalantaTableModel(ns: {}, table: {})", ns, tableName);
        return res;
    }

    private void initMetrics(RegionMetrics regionMetricsGroup, AtalantaRegionInfo region) {
        regionMetricsGroup.setCountDel(regionMetricsGroup.getCountDel() + region.getRegionMetrics().getCountDel());
        regionMetricsGroup.setCountPut(regionMetricsGroup.getCountPut() + region.getRegionMetrics().getCountPut());
        regionMetricsGroup.setCountGet(regionMetricsGroup.getCountGet() + region.getRegionMetrics().getCountGet());
        regionMetricsGroup.setMemUsageBytes(regionMetricsGroup.getMemUsageBytes() + region.getRegionMetrics().getMemUsageBytes());
        regionMetricsGroup.setSize(regionMetricsGroup.getSize() + region.getRegionMetrics().getSize());
        regionMetricsGroup.setKeys(regionMetricsGroup.getKeys() + region.getRegionMetrics().getKeys());
    }

    private Long calcSize(List<AtalantaRegionInfo> regionList) {
        return regionList.parallelStream()
                .map(AtalantaRegionInfo::getRegionMetrics)
                .mapToLong(RegionMetrics::getSize)
                .sum();
    }

    private Long calcRowCount(List<AtalantaRegionInfo> regionList) {
        return regionList.parallelStream()
                .map(AtalantaRegionInfo::getRegionMetrics)
                .mapToLong(RegionMetrics::getKeys)
                .sum();
    }

    private String getTableStatus(String ns, String tableName) {
        try {
            byte[] data = curatorFramework.getData().forPath(ZK_SEPARATOR + ns + ZK_SEPARATOR + tableName);
            String st = new String(data, UTF_8);
            int idx = st.indexOf("=");
            return idx > 0 ? st.substring(idx + 1) : st;
        } catch (Exception e) {
            LOGGER.info("cannot read /{}/{}", ns, tableName, e);
        }
        return "unknown";
    }

    private List<String> getRunningRSNames() {
        List<String> res = new ArrayList<>();
        try {
            res = zooKeeperCoreContext.getRunningRegionServers().parallelStream()
                    .map(RegionServerModel::getName)
                    .collect(toList());
        } catch (Exception ex) {
            LOGGER.info("cannot read rs names", ex);
        }
        return res;
    }

    private List<AtalantaRegionInfo> getRegionList(String ns, String tableName) {
        List<AtalantaRegionInfo> res = Collections.emptyList();
        try {
            TableName tblName = TableName.valueOf(ns, tableName);
            res = zooKeeperCoreContext.getRegionServersForTable(tblName).parallelStream()
                    .flatMap(rs -> getAtalantaRegionInfoList(tblName, rs).stream())
                    .sorted(Comparator.comparing(ri -> ri.getReplicaNum() + ri.getBeginKey() + ri.getHostPort()))
                    .collect(toList());
        } catch (Exception e) {
            LOGGER.info("cannot read /{}/{} children", ns, tableName, e);
        }
        return res;
    }

    private List<AtalantaRegionInfo> getAtalantaRegionInfoList(TableName tblName, RegionServerModel rs) {
        List<AtalantaRegionInfo> res = Collections.emptyList();
        try {
            res = zooKeeperCoreContext.getRegionServerRegions(tblName, rs).parallelStream()
                    .map(regionModel -> getAtalantaRegionInfo(tblName, rs, regionModel))
                    .collect(toList());
        } catch (Exception e) {
            LOGGER.info("cannot read /{}/{}/{} children", tblName.getNamespace(), tblName.getTableName(), rs, e);
        }
        return res;
    }

    private AtalantaRegionInfo getAtalantaRegionInfo(TableName tblName, RegionServerModel rs, RegionModel regionModel) {
        AtalantaRegionInfo res = new AtalantaRegionInfo();
        res.setHostPort(rs.getHost() + ":" + rs.getJavaPort());
        res.setNs(tblName.getNamespace());
        res.setReplicaNum(regionModel.getReplicaNum());
        res.setTableName(tblName.getTableName());
        res.setName(rs.getLocationString());
        res.setUuid(regionModel.getName());
        res.setBeginKey(toHexString(regionModel.getBeginKey()));
        res.setEndKey(toHexString(regionModel.getEndKey()));
        res.setRegionMetrics(rsMetricsService.getRsMetrics(rs, regionModel.getName(), regionModel.getReplicaNum()));
        res.setDataBlockSize(regionModel.getDataBlockSize());
        res.setCompression(regionModel.getCompressType(), regionModel.getRecordCompressionType());
        res.setDict(regionModel.getDictionary());
        return res;
    }

    private NsModel getNsModel(String s) {
        NsModel res = new NsModel();
        res.setName(s);
        try {
            final int tableCount = curatorFramework.getChildren().forPath(ZK_SEPARATOR + s).size();
            res.setTableCount(tableCount);
            return res;
        } catch (Exception e) {
            LOGGER.info("cannot read root children", e);
            res.setTableCount(-1);
        }

        return res;
    }

    @Override
    public RegionServerModelExt getRegionServerWithMetrics(String s) {
        RegionServerModelExt res = getRegionServer(s);
        try {
            res.setRegionServerMetrics(rsMetricsService.getRsMetrics(res));
            initMinDiskInfo(res);
            initDiskInfoExt(res);
        } catch (Exception e) {
            LOGGER.error("cannot read stast for RS " + s, e);
        }
        return res;
    }

    private void initDiskInfoExt(RegionServerModelExt res) {
        res.getRegionServerMetrics().getRsDisks().keySet().forEach(disk -> {
            res.getDiskInfoExt().put(disk, new DiskInfoExt());
        });
        res.getRegionServerMetrics().getRegionsMetrics().values()
                .parallelStream()
                .flatMap(m -> m.values().stream())
                .forEach(r -> {
                    DiskInfoExt diskInfoExt = res.getDiskInfoExt().getOrDefault(r.getDiskName(), new DiskInfoExt());
                    diskInfoExt.setRegionCount(diskInfoExt.getRegionCount() + 1);
                    diskInfoExt.setSizeRegionTotal(diskInfoExt.getSizeRegionTotal() + r.getSize());
                    res.getDiskInfoExt().put(r.getDiskName(), diskInfoExt);
                });
    }

    private RegionServerModelExt getRegionServer(String s) {
        String url = "unknown";
        int port = 0;
        int clientPort = 0;
        int jmxPort = 0;
        try {
            byte[] bytes = curatorFramework.getData().forPath(ZK_SEPARATOR + REGION_SERVER_INFO_SUBPATH + ZK_SEPARATOR + s);
            String[] ri = (new String(bytes, UTF_8)).split(":");
            url = ri[0];
            if (ri.length > 1) {
                port = Integer.parseInt(ri[1]);
            }
            if (ri.length > 2) {
                clientPort = Integer.parseInt(ri[2]);
            }
            if (ri.length > 3) {
                jmxPort = Integer.parseInt(ri[3]);
            }
        } catch (Exception e) {
            LOGGER.info("cannot read /RegionServers children " + s, e);
        }
        RegionServerModelExt res = new RegionServerModelExt(s, url, port, clientPort, jmxPort);
        try {
            res.setRsStatus(zookeeperHelperExtended.getRegionServerStatus(res.getName()));
            if (res.getRsStatus().equals(REGION_SERVER_INITIALIZING)) {
                res.setCssClass("table-warning");
            }
        } catch (Exception e) {
            res.setRsStatus(RS_STATUS_STOPPED);
            res.setCssClass("table-danger");
        }
        return res;
    }

    private void initMinDiskInfo(RegionServerModelExt res) {
        String minDiskInfo = StringUtils.EMPTY;
        long min = Long.MAX_VALUE;
        for (Map.Entry<String, RegionServerDiskInfo> e : res.getRegionServerMetrics().getRsDisks().entrySet()) {
            long curMin = e.getValue().getUsable();
            if (curMin < min) {
                min = curMin;
                minDiskInfo = String.format("%,.1f%%, %s (%s/%s) ",
                        e.getValue().getTotalSize() > 0 ? (100 - (double) e.getValue().getUsable() / e.getValue().getTotalSize() * 100) : -1,
                        e.getKey(),
                        toHumanReadebleDiskSize(e.getValue().getUsable()),
                        toHumanReadebleDiskSize(e.getValue().getTotalSize()));
            }
        }
        res.setMinDiskInfo(minDiskInfo);
    }

    private FMasterFollowerInfo getFollowerInfo(String c) {
        FMasterFollowerInfo res = new FMasterFollowerInfo();
        res.setZnode(c);
        res.setZnodeNbr(c.substring(c.length() - 10));
        try {
            byte[] bytes = curatorFramework.getData().forPath(ZK_SEPARATOR + F_MASTER_NODE + ZK_SEPARATOR + c);
            res.setUrl(new String(bytes, UTF_8));
        } catch (Exception e) {
            LOGGER.info("cannot read /fmaster children " + c, e);
            res.setUrl("unknown");
        }
        return res;
    }

    private boolean tryStartCurator() {
        tryConnect.set(true);
        curatorFramework.start();
        LockSupport.parkNanos(10000 * 1_000_000L);
        if (!curatorFramework.getZookeeperClient().isConnected()) {
            curatorFramework.close();
        }
        tryConnect.set(false);
        return curatorFramework.getZookeeperClient().isConnected();
    }


}
