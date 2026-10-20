/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.vo;

public class DataConfiguration {

    private Boolean splitSourceBackupEnabled;
    private String splitSourceBackupDir;
    private Integer simpleRegionDistributionThresholdPercent = 10;
    private Long rebalanceDiskDiffMb = -1L;
    private Integer keepOrphanHours = 24;
    private long modifyPullTimeoutMs = 15_000;
    private int modifyPoolSize = 16;
    private int tryToCompressLevel = 80;
    private boolean checkRegioninfo = true;
    private boolean regionMetricsEnabled = false;
    private boolean tableMetricsEnabled = true;

    public Boolean getSplitSourceBackupEnabled() {
        return splitSourceBackupEnabled;
    }

    public void setSplitSourceBackupEnabled(Boolean splitSourceBackupEnabled) {
        this.splitSourceBackupEnabled = splitSourceBackupEnabled;
    }

    public String getSplitSourceBackupDir() {
        return splitSourceBackupDir;
    }

    public void setSplitSourceBackupDir(String splitSourceBackupDir) {
        this.splitSourceBackupDir = splitSourceBackupDir;
    }

    public Integer getSimpleRegionDistributionThresholdPercent() {
        return simpleRegionDistributionThresholdPercent;
    }

    public void setSimpleRegionDistributionThresholdPercent(Integer simpleRegionDistributionThresholdPercent) {
        this.simpleRegionDistributionThresholdPercent = simpleRegionDistributionThresholdPercent;
    }

    public Long getRebalanceDiskDiffMb() {
        return rebalanceDiskDiffMb;
    }

    public void setRebalanceDiskDiffMb(Long rebalanceDiskDiffMb) {
        this.rebalanceDiskDiffMb = rebalanceDiskDiffMb;
    }

    public void setKeepOrphanHours(Integer keepOrphanHours) {
        this.keepOrphanHours = keepOrphanHours;
    }

    public Integer getKeepOrphanHours() {
        return keepOrphanHours;
    }

    @Override
    public String toString() {
        return "DataConfiguration{" +
                "\n\tsplitSourceBackupEnabled=" + splitSourceBackupEnabled +
                ",\n\tsplitSourceBackupDir='" + splitSourceBackupDir + '\'' +
                ",\n\tsimpleRegionDistributionThresholdPercent=" + simpleRegionDistributionThresholdPercent +
                ",\n\trebalanceDiskDiff=" + rebalanceDiskDiffMb +
                ",\n\tkeepOrphanHours=" + keepOrphanHours +
                '}';
    }

    public long getModifyPullTimeoutMs() {
        return modifyPullTimeoutMs;
    }

    public void setModifyPullTimeoutMs(long modifyPullTimeoutMs) {
        this.modifyPullTimeoutMs = modifyPullTimeoutMs;
    }

    public int getModifyPoolSize() {
        return modifyPoolSize;
    }

    public void setModifyPoolSize(int modifyPoolSize) {
        this.modifyPoolSize = modifyPoolSize;
    }

    public int getTryToCompressLevel() {
        return tryToCompressLevel;
    }

    public void setTryToCompressLevel(int tryToCompressLevel) {
        this.tryToCompressLevel = tryToCompressLevel;
    }

    public boolean isCheckRegioninfo() {
        return checkRegioninfo;
    }

    public void setCheckRegioninfo(boolean checkRegioninfo) {
        this.checkRegioninfo = checkRegioninfo;
    }

    public boolean isRegionMetricsEnabled() {
        return regionMetricsEnabled;
    }

    public void setRegionMetricsEnabled(boolean regionMetricsEnabled) {
        this.regionMetricsEnabled = regionMetricsEnabled;
    }

    public boolean isTableMetricsEnabled() {
        return tableMetricsEnabled;
    }

    public void setTableMetricsEnabled(boolean tableMetricsEnabled) {
        this.tableMetricsEnabled = tableMetricsEnabled;
    }
}
