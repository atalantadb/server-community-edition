/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.metrics;

import java.util.HashMap;
import java.util.Map;

public class RegionServerMetrics {
    private Map<String, Map<Integer, RegionMetrics>> regionsMetrics = new HashMap<>();
    private long keysTotal;
    private long sizeTotal;
    private long memUsageBytesTotal;
    private long countGetTotal;
    private long countPutTotal;
    private long countDelTotal;
    private long regionCountFollower1;
    private long regionCountFollower2;
    private long regionCount;
    private Map<String, RegionServerDiskInfo> rsDisks = new HashMap<>();
    private VersionInfo versionInfo = new VersionInfo();

    public Map<String, Map<Integer, RegionMetrics>> getRegionsMetrics() {
        return regionsMetrics;
    }

    public void setRegionsMetrics(Map<String, Map<Integer, RegionMetrics>> regionsMetrics) {
        this.regionsMetrics = regionsMetrics;
    }

    public long getKeysTotal() {
        return keysTotal;
    }

    public void setKeysTotal(long keysTotal) {
        this.keysTotal = keysTotal;
    }

    public long getSizeTotal() {
        return sizeTotal;
    }

    public void setSizeTotal(long sizeTotal) {
        this.sizeTotal = sizeTotal;
    }

    public long getMemUsageBytesTotal() {
        return memUsageBytesTotal;
    }

    public void setMemUsageBytesTotal(long memUsageBytesTotal) {
        this.memUsageBytesTotal = memUsageBytesTotal;
    }

    public long getCountGetTotal() {
        return countGetTotal;
    }

    public void setCountGetTotal(long countGetTotal) {
        this.countGetTotal = countGetTotal;
    }

    public long getCountPutTotal() {
        return countPutTotal;
    }

    public void setCountPutTotal(long countPutTotal) {
        this.countPutTotal = countPutTotal;
    }

    public long getCountDelTotal() {
        return countDelTotal;
    }

    public void setCountDelTotal(long countDelTotal) {
        this.countDelTotal = countDelTotal;
    }

    public Map<String, RegionServerDiskInfo> getRsDisks() {
        return rsDisks;
    }

    public void setRsDisks(Map<String, RegionServerDiskInfo> rsDisks) {
        this.rsDisks = rsDisks;
    }

    public long getRegionCountFollower1() {
        return regionCountFollower1;
    }

    public void setRegionCountFollower1(long regionCountFollower1) {
        this.regionCountFollower1 = regionCountFollower1;
    }

    public long getRegionCountFollower2() {
        return regionCountFollower2;
    }

    public void setRegionCountFollower2(long regionCountFollower2) {
        this.regionCountFollower2 = regionCountFollower2;
    }

    public long getRegionCount() {
        return regionCount;
    }

    public void setRegionCount(long regionCount) {
        this.regionCount = regionCount;
    }

    public VersionInfo getVersionInfo() {
        return versionInfo;
    }

    public void setVersionInfo(VersionInfo versionInfo) {
        this.versionInfo = versionInfo;
    }
}
