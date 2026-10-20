/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.metrics;

public class RegionMetrics {

    private String regionName;

    private int replicaNum;

    private long keys;

    private long memUsageBytes;

    private long countGet;

    private long countPut;

    private long countDel;

    private long size;

    private String ns;

    private String table;

    private String diskName;

    private long totalDiskSpace;

    private long usageDiskSpace;

    public RegionMetrics() {
    }

    public String getRegionName() {
        return regionName;
    }

    public long getKeys() {
        return keys;
    }

    public long getMemUsageBytes() {
        return memUsageBytes;
    }

    public long getCountGet() {
        return countGet;
    }

    public long getCountPut() {
        return countPut;
    }

    public long getCountDel() {
        return countDel;
    }

    public RegionMetrics(String fromString) {
        String[] split = fromString.split(":");
        regionName = split[0];
        String[] s = regionName.split("_");
        replicaNum = s.length > 1 ? Integer.parseInt(s[1]) : -1;
        int curIndex = 1;
        keys = getLongValueFromPos(split, curIndex++);
        memUsageBytes = getLongValueFromPos(split, curIndex++);
        countGet = getLongValueFromPos(split, curIndex++);
        countPut = getLongValueFromPos(split, curIndex++);
        countDel = getLongValueFromPos(split, curIndex);
    }

    private long getLongValueFromPos(String[] split, int curIndex) {
        return split.length > curIndex ? Long.parseLong(split[curIndex]) : 0;
    }

    @Override
    public String toString() {
        return "RegionMetrics{" +
                "regionName='" + regionName + '\'' +
                ", keys=" + keys +
                ", memUsageBytes=" + memUsageBytes +
                ", countGet=" + countGet +
                ", countPut=" + countPut +
                ", countDel=" + countDel +
                ", size=" + size +
                ", ns='" + ns + '\'' +
                ", table='" + table + '\'' +
                ", diskName='" + diskName + '\'' +
                ", totalDiskSpace=" + totalDiskSpace +
                ", usageDiskSpace=" + usageDiskSpace +
                '}';
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public void setKeys(long keys) {
        this.keys = keys;
    }

    public void setMemUsageBytes(long memUsageBytes) {
        this.memUsageBytes = memUsageBytes;
    }

    public void setCountGet(long countGet) {
        this.countGet = countGet;
    }

    public void setCountPut(long countPut) {
        this.countPut = countPut;
    }

    public void setCountDel(long countDel) {
        this.countDel = countDel;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String getNs() {
        return ns;
    }

    public void setNs(String ns) {
        this.ns = ns;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getDiskName() {
        return diskName;
    }

    public void setDiskName(String diskName) {
        this.diskName = diskName;
    }

    public long getTotalDiskSpace() {
        return totalDiskSpace;
    }

    public void setTotalDiskSpace(long totalDiskSpace) {
        this.totalDiskSpace = totalDiskSpace;
    }

    public long getUsageDiskSpace() {
        return usageDiskSpace;
    }

    public void setUsageDiskSpace(long usageDiskSpace) {
        this.usageDiskSpace = usageDiskSpace;
    }

    public int getReplicaNum() {
        return replicaNum;
    }

    public void setReplicaNum(int replicaNum) {
        this.replicaNum = replicaNum;
    }
}
