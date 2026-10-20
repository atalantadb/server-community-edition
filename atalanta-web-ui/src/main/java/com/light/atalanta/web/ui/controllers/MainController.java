/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.controllers;

import com.light.atalanta.grpc.MasterProto;
import com.light.atalanta.metrics.RegionMetrics;
import com.light.atalanta.metrics.VersionInfo;
import com.light.atalanta.web.ui.WebUiUtils;
import com.light.atalanta.web.ui.model.*;
import com.light.atalanta.web.ui.services.AtalantaClientService;
import com.light.atalanta.web.ui.services.MasterMetricsService;
import com.light.atalanta.web.ui.services.RsMetricsService;
import com.light.atalanta.web.ui.services.ZookeeperDaoService;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.REGION_SERVER_INITIALIZING;
import static com.light.atalanta.web.ui.services.ZookeeperDaoServiceImpl.RS_STATUS_STOPPED;

@Controller
public class MainController {
    private static final Logger LOGGER = LoggerFactory.getLogger(MainController.class.getName());
    private static final List<Integer> TABLE_PAGE_SIZES = List.of(10, 25, 50, 100, 1000, -1);
    private final ZookeeperDaoService zookeeperDaoService;
    private final AtalantaClientService atalantaClientService;
    private final MasterMetricsService masterMetricsService;
    private final RsMetricsService rsMetricsService;
    private String getRes = StringUtils.EMPTY;
    private String putRes = StringUtils.EMPTY;

    public MainController(ZookeeperDaoService zookeeperDaoService, AtalantaClientService atalantaClientService, CuratorFramework curatorFramework, MasterMetricsService masterMetricsService, RsMetricsService rsMetricsService) {
        this.zookeeperDaoService = zookeeperDaoService;
        this.atalantaClientService = atalantaClientService;
        this.masterMetricsService = masterMetricsService;
        this.rsMetricsService = rsMetricsService;
    }

    @GetMapping("/")
    public String main(Model model) {
        MasterProto.PingResponse pingRes = masterMetricsService.getMasterLeaderPingResponse();
        MasterProto.MasterStartMonitoringInfo masterStartInfo = masterMetricsService.getMasterStartInfo();
        VersionInfo versionInfo = masterMetricsService.getMasterVersionInfo();
        addMasterAttributes(model, pingRes,null, null, masterStartInfo, versionInfo);
        List<RegionServerModelExt> allRegionServers = zookeeperDaoService.getAllRegionServers();
        model.addAttribute("regionServers", allRegionServers);
        model.addAttribute("rsInitCount", getRsCountByStatus(allRegionServers, REGION_SERVER_INITIALIZING));
        model.addAttribute("rsStoppedCount", getRsCountByStatus(allRegionServers, RS_STATUS_STOPPED));
        model.addAttribute("regionServerCount", rsMetricsService.getRegionCount());
        model.addAttribute("regionServerCountFollower1", rsMetricsService.getRegionCountFollower1());
        model.addAttribute("regionServerCountFollower2",rsMetricsService.getRegionCountFollower2());
        model.addAttribute("regionServerDiskSize",WebUiUtils.toHumanReadebleDiskSize(rsMetricsService.getSizeTotal()));
        model.addAttribute("regionServerRows", rsMetricsService.getKeysTotal());
        model.addAttribute("regionServerMem",WebUiUtils.toHumanReadebleDiskSize(rsMetricsService.getMemUsageBytesTotal()));
        model.addAttribute("regionServerGet", rsMetricsService.getCountGetTotal());
        model.addAttribute("regionServerPut", rsMetricsService.getCountPutTotal());
        model.addAttribute("regionServerDel", rsMetricsService.getCountDelTotal());
        return "main";
    }

    private int getRsCountByStatus(List<RegionServerModelExt> allRegionServers, String status) {
        return allRegionServers.stream().filter(rs -> rs.getRsStatus().equals(status))
                .collect(Collectors.toList())
                .size();
    }

    @GetMapping("/master")
    public String master(Model model) {
        MasterProto.PingResponse pingRes = masterMetricsService.getMasterLeaderPingResponse();
        MasterProto.InfoResponse infoRes = masterMetricsService.info();
        MasterProto.RecoveryMonitoringInfo recoveryInfo = masterMetricsService.recoveryInfo();
        MasterProto.MasterStartMonitoringInfo masterStartInfo = masterMetricsService.getMasterStartInfo();
        VersionInfo versionInfo = masterMetricsService.getMasterVersionInfo();
        addMasterAttributes(model, pingRes, infoRes, recoveryInfo, masterStartInfo, versionInfo);
        return "master";
    }

    private void addMasterAttributes(Model model, MasterProto.PingResponse pingRes, MasterProto.InfoResponse infoRes,
                                     MasterProto.RecoveryMonitoringInfo recoveryInfo,
                                     MasterProto.MasterStartMonitoringInfo masterStartInfo, VersionInfo versionInfo) {
        try {
            model.addAttribute("zkConnectionString", zookeeperDaoService.getZkConnectionString());
            model.addAttribute("zkConnectionStatus", zookeeperDaoService.checkConnection());
            model.addAttribute("fmasterLeader", zookeeperDaoService.getLeaderInfo());
            model.addAttribute("fmasterFollowers", zookeeperDaoService.getFollowerInfo());
        } catch (Exception e) {
            LOGGER.error("Cannot add master attr!", e);
        }
        model.addAttribute("fmasterPing", pingRes == null ? false : true);
        model.addAttribute("fmasterStat", toFmasterStat(pingRes, infoRes, recoveryInfo, masterStartInfo));
        model.addAttribute("fmasterBuild", versionInfo.getBuild());
        model.addAttribute("fmasterBuildDate", versionInfo.getDate());
        model.addAttribute("fmasterBuildCommit", versionInfo.getCommit());

    }

    private FMasterStat toFmasterStat(MasterProto.PingResponse pingRes,
                                      MasterProto.InfoResponse infoRes,
                                      MasterProto.RecoveryMonitoringInfo recoveryInfo,
                                      MasterProto.MasterStartMonitoringInfo masterStartInfo) {
        if (pingRes == null) {
            return new FMasterStat();
        }
        FMasterStat res = new FMasterStat();
        res.setMode(pingRes.getMode());
        res.setCreateNamespace(pingRes.getCreateNamespace());
        res.setCreateTable(pingRes.getCreateTable());
        res.setEnableTable(pingRes.getEnableTable());
        res.setDisableTable(pingRes.getDisableTable());
        res.setDropTable(pingRes.getDropTable());
        res.setMoveRegion(pingRes.getMoveRegion());
        res.setSplitRegion(pingRes.getSplitRegion());
        res.setStartReplicasRebalanceAsync(pingRes.getStartReplicasRebalanceAsync());
        res.setSyncTableReplicas(pingRes.getSyncTableReplicas());
        res.setStopReplicasRebalance(pingRes.getStopReplicasRebalance());
        res.setStartRegionsRebalanceAsync(pingRes.getStartRegionsRebalanceAsync());
        res.setStopRegionsRebalance(pingRes.getStopRegionsRebalance());
        res.setCreateSnapshot(pingRes.getCreateSnapshot());
        res.setCloneSnapshot(pingRes.getCloneSnapshot());
        res.setDeleteSnapshot(pingRes.getDeleteSnapshot());
        res.setStartRegionServer(pingRes.getStartRegionServer());
        res.setStopRegionServer(pingRes.getStopRegionServer());
        res.calcTotal();

        if (infoRes != null) {
            res.setCreateNamespaceMsgList(toFMasterInfoMsgList(infoRes.getCreateNamespaceMsgListList()));
            res.setCreateTableMsgList(toFMasterInfoMsgList(infoRes.getCreateTableMsgListList()));
            res.setEnableTableMsgList(toFMasterInfoMsgList(infoRes.getEnableTableMsgListList()));
            res.setDisableTableMsgList(toFMasterInfoMsgList(infoRes.getDisableTableMsgListList()));
            res.setDropTableMsgList(toFMasterInfoMsgList(infoRes.getDropTableMsgListList()));
            res.setMoveRegionMsgList(toFMasterInfoMsgList(infoRes.getMoveRegionMsgListList()));
            res.setSplitRegionMsgList(toFMasterInfoMsgList(infoRes.getSplitRegionMsgListList()));
            res.setStartReplicasRebalanceAsyncMsgList(toFMasterInfoMsgList(infoRes.getStartReplicasRebalanceAsyncMsgListList()));
            res.setSyncTableReplicasMsgList(toFMasterInfoMsgList(infoRes.getSyncTableReplicasMsgListList()));
            res.setStopReplicasRebalanceMsgList(toFMasterInfoMsgList(infoRes.getStopReplicasRebalanceMsgListList()));
            res.setStartRegionsRebalanceAsyncMsgList(toFMasterInfoMsgList(infoRes.getStartRegionsRebalanceAsyncMsgListList()));
            res.setStopRegionsRebalanceMsgList(toFMasterInfoMsgList(infoRes.getStopRegionsRebalanceMsgListList()));
            res.setCreateSnapshotMsgList(toFMasterInfoMsgList(infoRes.getCreateSnapshotMsgListList()));
            res.setCloneSnapshotMsgList(toFMasterInfoMsgList(infoRes.getCloneSnapshotMsgListList()));
            res.setDeleteSnapshotMsgList(toFMasterInfoMsgList(infoRes.getDeleteSnapshotMsgListList()));
            res.setStartRegionServerMsgList(toFMasterInfoMsgList(infoRes.getStartRegionServerMsgListList()));
            res.setStopRegionServerMsgList(toFMasterInfoMsgList(infoRes.getStopRegionServerMsgListList()));
        }

        if (recoveryInfo != null) {
            res.setRecovery(res.getRecovery());
            res.setRecoveryMsgList(toFMasterInfoMsgList(recoveryInfo.getRecoveryInfoMsgList()));
        }

        if (masterStartInfo != null) {
            res.setMasterStartMsgList(toFMasterInfoMsgList(masterStartInfo.getMasterStartInfoMsgList()));
        }

        return res;
    }

    private static List<FMasetrInfoMsg> toFMasterInfoMsgList(List<MasterProto.FMasterInfoMsg> msgList) {
        return msgList.stream()
                .map(m -> new FMasetrInfoMsg(m.getName(), m.getMsg()))
                .collect(Collectors.toList());
    }

    @GetMapping("/ns")
    public String ns(@RequestParam(name = "ns", required = false) String ns, Model model) {
        model.addAttribute("zkConnectionString", zookeeperDaoService.getZkConnectionString());
        model.addAttribute("zkConnectionStatus", zookeeperDaoService.checkConnection());

        if (StringUtils.isEmpty(ns)) {
            model.addAttribute("nsList", zookeeperDaoService.getNsList());
            model.addAttribute("nsModel", new NsModel());
            return "ns";
        } else {
            model.addAttribute("ns", ns);
            List<AtalantaTableModel> tableList = zookeeperDaoService.getTableList(ns);
            model.addAttribute("tables", tableList);
            AtalantaTableModel atalantaTableModel = new AtalantaTableModel();
            atalantaTableModel.setNs(ns);
            RegionMetrics regionMetrics = atalantaTableModel.getRegionMetrics();
            for (AtalantaTableModel cur : tableList) {
                atalantaTableModel.setRsCount(atalantaTableModel.getRsCount() + cur.getRsCount());
                atalantaTableModel.setReplica1Count(atalantaTableModel.getReplica1Count() + cur.getReplica1Count());
                atalantaTableModel.setReplica2Count(atalantaTableModel.getReplica2Count() + cur.getReplica2Count());
                atalantaTableModel.setRowCount(atalantaTableModel.getRowCount() + cur.getRowCount());
                regionMetrics.setMemUsageBytes(regionMetrics.getMemUsageBytes() + cur.getRegionMetrics().getMemUsageBytes());
                regionMetrics.setSize(regionMetrics.getSize() + cur.getRegionMetrics().getSize());
                regionMetrics.setCountGet(regionMetrics.getCountGet() + cur.getRegionMetrics().getCountGet());
                regionMetrics.setCountPut(regionMetrics.getCountPut() + cur.getRegionMetrics().getCountPut());
                regionMetrics.setCountDel(regionMetrics.getCountDel() + cur.getRegionMetrics().getCountDel());
            }
            model.addAttribute("atalantaTableModel", atalantaTableModel);
            return "ns_table";
        }
    }

    @GetMapping("/snapshots")
    public String snapshots(@RequestParam(name = "ns", required = false) String ns, Model model) {
        model.addAttribute("zkConnectionString", zookeeperDaoService.getZkConnectionString());
        model.addAttribute("zkConnectionStatus", zookeeperDaoService.checkConnection());
        if (StringUtils.isEmpty(ns)) {
            model.addAttribute("snapshotList", zookeeperDaoService.getSnList());
            return "snapshots";
        } else {
            return "snapshots";
        }
    }

    @GetMapping("/tables")
    public String table(@RequestParam(name = "ns", required = false) String ns,
                        @RequestParam(name = "table", required = false) String table,
                        @RequestParam(name = "pg", required = false) Integer pg,
                        @RequestParam(name = "pageSize", required = false) Integer pageSize,
                        Model model) {
        model.addAttribute("zkConnectionString", zookeeperDaoService.getZkConnectionString());
        model.addAttribute("zkConnectionStatus", zookeeperDaoService.checkConnection());

        if (StringUtils.isEmpty(table)) {
            int tableCount = zookeeperDaoService.getTableCount();
            int pgSize = pageSize != null && TABLE_PAGE_SIZES.contains(pageSize) ? pageSize : 50;
            boolean all = pgSize == -1;
            int lastPage = all || tableCount == 0 ? 0 : (tableCount - 1) / pgSize;
            int page = all || pg == null ? 0 : Math.min(Math.max(0, pg), lastPage);
            List<AtalantaTableModel> tableList = zookeeperDaoService.getTableList(page, pgSize);
            model.addAttribute("tables", tableList);
            model.addAttribute("pageSize", pgSize);
            model.addAttribute("pageSizes", TABLE_PAGE_SIZES);
            model.addAttribute("pgNext", Math.min(page + 1, lastPage));
            model.addAttribute("pgPrev", Math.max(page - 1, 0));
            model.addAttribute("hasPreviousPage", page > 0);
            model.addAttribute("hasNextPage", page < lastPage);
            model.addAttribute("tableBegIndex", tableCount == 0 ? 0L : all ? 1L : (long) page * pgSize + 1);
            model.addAttribute("tableEndIndex", all ? (long) tableCount : Math.min(((long) page + 1) * pgSize, tableCount));
            model.addAttribute("totalTables", tableCount);
            model.addAttribute("nsList", zookeeperDaoService.getNsList());
            model.addAttribute("atalantaTableModel", new AtalantaTableModel());
            LOGGER.info("<== MainController.table(ns: {}, table: {})", ns, table);
            return "tables";
        } else {
            model.addAttribute("ns", ns);
            model.addAttribute("table", zookeeperDaoService.getAtalantaTableModel(ns, table));
            AtalantaCellModel atalantaGetModel = new AtalantaCellModel();
            atalantaGetModel.setTable(table);
            atalantaGetModel.setNs(ns);
            atalantaGetModel.setRes(getRes);
            model.addAttribute("atalantaGetModel", atalantaGetModel);
            AtalantaCellModel atalantaPutModel = new AtalantaCellModel();
            atalantaPutModel.setTable(table);
            atalantaPutModel.setNs(ns);
            atalantaPutModel.setRes(putRes);
            model.addAttribute("atalantaPutModel", atalantaPutModel);
            return "table_info";
        }
    }

    @GetMapping("/rs")
    public String rs(@RequestParam(name = "name") String name, Model model) {
        RegionServerModelExt rs = zookeeperDaoService.getRegionServerWithMetrics(name);
        model.addAttribute("rs", rs);

        return "rs";
    }

    @PostMapping("/createNs")
    public String createNs(@ModelAttribute NsModel nsModel, Model model) {
        try {
            atalantaClientService.createNs(nsModel);
            return "redirect:/ns";
        } catch (Exception e) {
            model.addAttribute("errorMessage", "Failed to create namespace: " + e.getMessage());
            return "error";
        }
    }

    @PostMapping("/createTable")
    public String createTable(@ModelAttribute AtalantaTableModel atalantaTableModel, Model model) {
        try {
            atalantaClientService.createTable(atalantaTableModel);
            return "redirect:/ns?ns=" + atalantaTableModel.getNs();
        } catch (Exception e) {
            model.addAttribute("errorMessage", "Failed to create table: " + e.getMessage());
            return "error";
        }
    }

    @PostMapping("/getData")
    public String getData(@ModelAttribute AtalantaCellModel atalantaGetModel) {
        getRes = atalantaClientService.getData(atalantaGetModel);
        return "redirect:/tables?ns=" + atalantaGetModel.getNs() + "&table=" + atalantaGetModel.getTable();
    }

    @PostMapping("/putData")
    public String putData(@ModelAttribute AtalantaCellModel atalantaCellModel) {
        putRes = atalantaClientService.putData(atalantaCellModel);
        return "redirect:/tables?ns=" + atalantaCellModel.getNs() + "&table=" + atalantaCellModel.getTable();
    }

    @PostMapping("/dropTable")
    public String dropTable(@ModelAttribute AtalantaTableModel atalantaTableModel, @RequestParam(name = "returnUrl") String returnUrl, Model model) {
        try {
            atalantaClientService.dropTable(atalantaTableModel);
            return "redirect:" + returnUrl;
        } catch (Exception e) {
            model.addAttribute("errorMessage", "Failed to drop table: " + e.getMessage());
            return "error";
        }
    }

    @PostMapping("/enableTable")
    public String enableTable(@ModelAttribute AtalantaTableModel atalantaTableModel, @RequestParam(name = "returnUrl") String returnUrl, Model model) {
        try {
            atalantaClientService.enableTable(atalantaTableModel);
            return "redirect:" + returnUrl;
        } catch (Exception e) {
            model.addAttribute("errorMessage", "Failed to enable table: " + e.getMessage());
            return "error";
        }
    }

    @PostMapping("/disableTable")
    public String disableTable(@ModelAttribute AtalantaTableModel atalantaTableModel, @RequestParam(name = "returnUrl") String returnUrl, Model model) {
        try {
            atalantaClientService.disableTable(atalantaTableModel);
            return "redirect:" + returnUrl;
        } catch (Exception e) {
            model.addAttribute("errorMessage", "Failed to disable table: " + e.getMessage());
            return "error";
        }
    }

    @PostMapping("/moveRegion")
    public String moveRegion(
            @RequestParam(name = "regionId") String regionId,
            @RequestParam(name = "targetRsName") String targetRsName,
            @RequestParam(name = "returnUrl") String returnUrl,
            Model model
    ) {
        try {
            atalantaClientService.moveRegion(regionId, targetRsName);
            return "redirect:" + returnUrl;
        } catch (Exception e) {
            model.addAttribute("errorMessage", "Failed to move region: " + e.getMessage());
            return "error";
        }
    }
}
