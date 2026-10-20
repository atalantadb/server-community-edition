/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.model;

import com.light.atalanta.metrics.RegionMetrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AtalantaTableModel {

    private String ns;

    private String name;

    private String status;

    private Long rsCount = 0L;

    private Long replica1Count = 0L;

    private Long replica2Count = 0L;

    private Long rowCount = 0L;

    private Long size = 0L;

    private List<AtalantaRegionInfo> regionList;

    private List<String> runningRsNames;

    private int dataBlockSize;

    private RegionMetrics regionMetrics = new RegionMetrics();

    private Map<String, AtalantaTableModel> regionServerCountMap = new ConcurrentHashMap<>();

    private Integer regionCount = 1; // Default to 1 region

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNs() {
        return ns;
    }

    public void setNs(String ns) {
        this.ns = ns;
    }

    public Long getRsCount() {
        return rsCount;
    }

    public void setRsCount(Long rsCount) {
        this.rsCount = rsCount;
    }

    public void setRegionList(List<AtalantaRegionInfo> regionList) {
        this.regionList = regionList;
    }

    public List<AtalantaRegionInfo> getRegionList() {
        return regionList;
    }

    public String getCompress() {
        return regionList == null || regionList.isEmpty() ? null : regionList.get(0).getCompress();
    }

    public Boolean getDict() {
        return regionList == null || regionList.isEmpty() ? null : regionList.get(0).getDict();
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getRowCount() {
        return rowCount;
    }

    public void setRowCount(Long rowCount) {
        this.rowCount = rowCount;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "AtalantaTableModel{"
                + "ns='" + ns + '\''
                + ", name='" + name + '\''
                + ", status='" + status + '\''
                + ", rsCount='" + rsCount + '\''
                + ", regionList=" + regionList
                + '}';
    }

    public void setDataBlockSize(int dataBlockSize) {
        this.dataBlockSize = dataBlockSize;
    }

    public int getDataBlockSize() {
        return dataBlockSize;
    }

    public Long getReplica1Count() {
        return replica1Count;
    }

    public void setReplica1Count(Long replica1Count) {
        this.replica1Count = replica1Count;
    }

    public Long getReplica2Count() {
        return replica2Count;
    }

    public void setReplica2Count(Long replica2Count) {
        this.replica2Count = replica2Count;
    }

    public RegionMetrics getRegionMetrics() {
        return regionMetrics;
    }

    public void setRegionMetrics(RegionMetrics regionMetrics) {
        this.regionMetrics = regionMetrics;
    }

    public Map<String, AtalantaTableModel> getRegionServerCountMap() {
        return regionServerCountMap;
    }

    public void setRegionServerCountMap(Map<String, AtalantaTableModel> regionServerCountMap) {
        this.regionServerCountMap = regionServerCountMap;
    }

    public Integer getRegionCount() {
        return regionCount;
    }

    public void setRegionCount(Integer regionCount) {
        this.regionCount = regionCount;
    }

    public List<String> getRunningRsNames() {
        return runningRsNames;
    }

    public void setAllRunningRsNames(List<String> runningRsNames) {
        this.runningRsNames = runningRsNames;
    }

    public Map<String, List<AtalantaRegionInfo>> getRegionsGroupedByRS() {
        if (this.regionList == null || this.regionList.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, List<AtalantaRegionInfo>> map = new ConcurrentHashMap<>();
        for (AtalantaRegionInfo regionInfo : this.regionList) {
            map.putIfAbsent(regionInfo.getName(), new ArrayList<>());
            map.get(regionInfo.getName()).add(regionInfo);
        }
        return map;
    }
}
