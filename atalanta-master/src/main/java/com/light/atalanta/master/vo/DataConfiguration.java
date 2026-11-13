/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.vo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataConfiguration.class);
    private static final int MAX_RECOVERY_TABLE_THREADS = 1_000;

    private Boolean operationsMonitoringEnabled;

    private Integer replicationFactor;

    private Integer regionsRecoveryInitialCheckDelayMs;

    private Integer regionsRecoveryCheckDelayMs;

    private Integer regionsRecoveryTimeoutMs;

    private Integer regionsRecoveryTableThreadsCount;

    private Integer regionsRecoveryOperationsStoppageTableThreadsCount = 10;

    private Integer regionsRecoveryOperationsStoppageRSThreadsCount = 128;

    private Boolean regionsAutoSplitEnabled;

    private Integer regionsAutoSplitInitialCheckDelayMs;

    private Integer regionsAutoSplitCheckDelayMs;

    private Long regionsAutoSplitTriggerMb;

    private int regionsAutoSplitBatchMarginPercent = 5;

    private Integer regionsAutoSplitTableThreadsCount;

    private Long regionsAutoSplitReservedMb = 1L;

    private Boolean regionsAutoRebalanceEnabled;

    private Boolean regionsRebalanceAnalyticsOnly;

    private Integer regionsAutoRebalanceInitialCheckDelayMs;

    private Integer regionsAutoRebalanceCheckDelayMs;

    private Integer rebalanceTableThreadsCount = 256;

    private Integer createTableRetryDelayMs;

    private Integer enableTableRetryDelayMs;

    private Integer disableTableRetryDelayMs;

    private Integer dropTableRetryDelayMs;

    private Integer moveRegionRetryDelayMs;

    private Integer lockWaitingTimeoutMs;

    private Integer recoverTableRetryDelayMs;

    private Integer splitTableRetryDelayMs;

    private Boolean lockMonitoringEnabled = false;

    private Integer lockMonitoringDelayMs = 5000;

    private boolean autoCreateNs = false;
    private long modifyPullTimeoutMs = 15_000;

    public Boolean getOperationsMonitoringEnabled() {
        return operationsMonitoringEnabled;
    }

    public void setOperationsMonitoringEnabled(Boolean operationsMonitoringEnabled) {
        this.operationsMonitoringEnabled = operationsMonitoringEnabled;
    }

    public Integer getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(Integer replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public Integer getRegionsRecoveryCheckDelayMs() {
        return regionsRecoveryCheckDelayMs;
    }

    public void setRegionsRecoveryCheckDelayMs(Integer regionsRecoveryCheckDelayMs) {
        this.regionsRecoveryCheckDelayMs = regionsRecoveryCheckDelayMs;
    }

    public Integer getRegionsRecoveryInitialCheckDelayMs() {
        return regionsRecoveryInitialCheckDelayMs;
    }

    public void setRegionsRecoveryInitialCheckDelayMs(Integer regionsRecoveryInitialCheckDelayMs) {
        this.regionsRecoveryInitialCheckDelayMs = regionsRecoveryInitialCheckDelayMs;
    }

    public Integer getRegionsRecoveryTimeoutMs() {
        return regionsRecoveryTimeoutMs;
    }

    public void setRegionsRecoveryTimeoutMs(Integer regionsRecoveryTimeoutMs) {
        this.regionsRecoveryTimeoutMs = regionsRecoveryTimeoutMs;
    }

    public Integer getRegionsRecoveryTableThreadsCount() {
        return regionsRecoveryTableThreadsCount;
    }

    /**
     * Sets the number of threads used to recover tables on an RS.
     * <p>
     * A value &gt; 1000 is capped at {@value #MAX_RECOVERY_TABLE_THREADS}
     * and a warning is logged.
     *
     * @param regionsRecoveryTableThreadsCount desired number of threads
     */
    public void setRegionsRecoveryTableThreadsCount(Integer regionsRecoveryTableThreadsCount) {
        if (regionsRecoveryTableThreadsCount != null && regionsRecoveryTableThreadsCount > MAX_RECOVERY_TABLE_THREADS) {
            LOGGER.warn("regionsRecoveryTableThreadsCount={} too high, set to limit {}", regionsRecoveryTableThreadsCount, MAX_RECOVERY_TABLE_THREADS);
            this.regionsRecoveryTableThreadsCount = MAX_RECOVERY_TABLE_THREADS;
        } else {
            this.regionsRecoveryTableThreadsCount = regionsRecoveryTableThreadsCount;
        }
    }

    public Integer getRegionsRecoveryOperationsStoppageTableThreadsCount() {
        return regionsRecoveryOperationsStoppageTableThreadsCount;
    }

    public void setRegionsRecoveryOperationsStoppageTableThreadsCount(Integer regionsRecoveryOperationsStoppageTableThreadsCount) {
        this.regionsRecoveryOperationsStoppageTableThreadsCount = regionsRecoveryOperationsStoppageTableThreadsCount;
    }

    public Integer getRegionsRecoveryOperationsStoppageRSThreadsCount() {
        return regionsRecoveryOperationsStoppageRSThreadsCount;
    }

    public void setRegionsRecoveryOperationsStoppageRSThreadsCount(Integer regionsRecoveryOperationsStoppageRSThreadsCount) {
        this.regionsRecoveryOperationsStoppageRSThreadsCount = regionsRecoveryOperationsStoppageRSThreadsCount;
    }

    public Integer getRegionsAutoSplitInitialCheckDelayMs() {
        return regionsAutoSplitInitialCheckDelayMs;
    }

    public void setRegionsAutoSplitInitialCheckDelayMs(Integer regionsAutoSplitInitialCheckDelayMs) {
        this.regionsAutoSplitInitialCheckDelayMs = regionsAutoSplitInitialCheckDelayMs;
    }

    public Boolean getRegionsAutoSplitEnabled() {
        return regionsAutoSplitEnabled;
    }

    public void setRegionsAutoSplitEnabled(Boolean regionsAutoSplitEnabled) {
        this.regionsAutoSplitEnabled = regionsAutoSplitEnabled;
    }

    public Integer getRegionsAutoSplitCheckDelayMs() {
        return regionsAutoSplitCheckDelayMs;
    }

    public void setRegionsAutoSplitCheckDelayMs(Integer regionsAutoSplitCheckDelayMs) {
        this.regionsAutoSplitCheckDelayMs = regionsAutoSplitCheckDelayMs;
    }

    public Long getRegionsAutoSplitTriggerMb() {
        return regionsAutoSplitTriggerMb;
    }

    public void setRegionsAutoSplitTriggerMb(Long regionsAutoSplitTriggerMb) {
        this.regionsAutoSplitTriggerMb = regionsAutoSplitTriggerMb;
    }

    public int getRegionsAutoSplitBatchMarginPercent() {
        return regionsAutoSplitBatchMarginPercent;
    }

    public void setRegionsAutoSplitBatchMarginPercent(int regionsAutoSplitBatchMarginPercent) {
        if (regionsAutoSplitBatchMarginPercent < 0 || regionsAutoSplitBatchMarginPercent > 100) {
            throw new IllegalArgumentException("regionsAutoSplitBatchMarginPercent must be between 0 and 100");
        }
        this.regionsAutoSplitBatchMarginPercent = regionsAutoSplitBatchMarginPercent;
    }

    public Integer getRegionsAutoSplitTableThreadsCount() {
        return regionsAutoSplitTableThreadsCount;
    }

    public void setRegionsAutoSplitTableThreadsCount(Integer regionsAutoSplitTableThreadsCount) {
        this.regionsAutoSplitTableThreadsCount = regionsAutoSplitTableThreadsCount;
    }

    public Boolean getRegionsAutoRebalanceEnabled() {
        return regionsAutoRebalanceEnabled;
    }

    public void setRegionsAutoRebalanceEnabled(Boolean regionsAutoRebalanceEnabled) {
        this.regionsAutoRebalanceEnabled = regionsAutoRebalanceEnabled;
    }

    public Boolean getRegionsRebalanceAnalyticsOnly() {
        return regionsRebalanceAnalyticsOnly;
    }

    public void setRegionsRebalanceAnalyticsOnly(Boolean regionsRebalanceAnalyticsOnly) {
        this.regionsRebalanceAnalyticsOnly = regionsRebalanceAnalyticsOnly;
    }

    public Integer getRegionsAutoRebalanceInitialCheckDelayMs() {
        return regionsAutoRebalanceInitialCheckDelayMs;
    }

    public void setRegionsAutoRebalanceInitialCheckDelayMs(Integer regionsAutoRebalanceInitialCheckDelayMs) {
        this.regionsAutoRebalanceInitialCheckDelayMs = regionsAutoRebalanceInitialCheckDelayMs;
    }

    public Integer getRegionsAutoRebalanceCheckDelayMs() {
        return regionsAutoRebalanceCheckDelayMs;
    }

    public void setRegionsAutoRebalanceCheckDelayMs(Integer regionsAutoRebalanceCheckDelayMs) {
        this.regionsAutoRebalanceCheckDelayMs = regionsAutoRebalanceCheckDelayMs;
    }

    public Integer getRebalanceTableThreadsCount() {
        return rebalanceTableThreadsCount;
    }

    public void setRebalanceTableThreadsCount(Integer rebalanceTableThreadsCount) {
        this.rebalanceTableThreadsCount = rebalanceTableThreadsCount;
    }

    public Integer getCreateTableRetryDelayMs() {
        return createTableRetryDelayMs;
    }

    public void setCreateTableRetryDelayMs(Integer createTableRetryDelayMs) {
        this.createTableRetryDelayMs = createTableRetryDelayMs;
    }

    public Integer getDisableTableRetryDelayMs() {
        return disableTableRetryDelayMs;
    }

    public void setDisableTableRetryDelayMs(Integer disableTableRetryDelayMs) {
        this.disableTableRetryDelayMs = disableTableRetryDelayMs;
    }

    public Integer getEnableTableRetryDelayMs() {
        return enableTableRetryDelayMs;
    }

    public void setEnableTableRetryDelayMs(Integer enableTableRetryDelayMs) {
        this.enableTableRetryDelayMs = enableTableRetryDelayMs;
    }

    public Integer getDropTableRetryDelayMs() {
        return dropTableRetryDelayMs;
    }

    public void setDropTableRetryDelayMs(Integer dropTableRetryDelayMs) {
        this.dropTableRetryDelayMs = dropTableRetryDelayMs;
    }

    public Integer getLockWaitingTimeoutMs() {
        return lockWaitingTimeoutMs;
    }

    public void setLockWaitingTimeoutMs(Integer lockWaitingTimeoutMs) {
        this.lockWaitingTimeoutMs = lockWaitingTimeoutMs;
    }

    public Integer getMoveRegionRetryDelayMs() {
        return moveRegionRetryDelayMs;
    }

    public void setMoveRegionRetryDelayMs(Integer moveRegionRetryDelayMs) {
        this.moveRegionRetryDelayMs = moveRegionRetryDelayMs;
    }

    public Integer getRecoverTableRetryDelayMs() {
        return recoverTableRetryDelayMs;
    }

    public void setRecoverTableRetryDelayMs(Integer recoverTableRetryDelayMs) {
        this.recoverTableRetryDelayMs = recoverTableRetryDelayMs;
    }

    public Integer getSplitTableRetryDelayMs() {
        return splitTableRetryDelayMs;
    }

    public void setSplitTableRetryDelayMs(Integer splitTableRetryDelayMs) {
        this.splitTableRetryDelayMs = splitTableRetryDelayMs;
    }

    public Boolean getLockMonitoringEnabled() {
        return lockMonitoringEnabled;
    }

    public void setLockMonitoringEnabled(Boolean lockMonitoringEnabled) {
        this.lockMonitoringEnabled = lockMonitoringEnabled;
    }

    public Integer getLockMonitoringDelayMs() {
        return lockMonitoringDelayMs;
    }

    public void setLockMonitoringDelayMs(Integer lockMonitoringDelayMs) {
        this.lockMonitoringDelayMs = lockMonitoringDelayMs;
    }

    @Override
    public String toString() {
        return "DataConfiguration{" +
                "\n\toperationsMonitoringEnabled=" + operationsMonitoringEnabled +
                ", \n\treplicationFactor=" + replicationFactor +
                ", \n\tregionsRecoveryInitialCheckDelayMs=" + regionsRecoveryInitialCheckDelayMs +
                ", \n\tregionsRecoveryCheckDelayMs=" + regionsRecoveryCheckDelayMs +
                ", \n\tregionsRecoveryTimeoutMs=" + regionsRecoveryTimeoutMs +
                ", \n\tregionsRecoveryTableThreadsCount=" + regionsRecoveryTableThreadsCount +
                ", \n\tregionsRecoveryOperationsStoppageTableThreadsCount=" + regionsRecoveryOperationsStoppageTableThreadsCount +
                ", \n\tregionsRecoveryOperationsStoppageRSThreadsCount=" + regionsRecoveryOperationsStoppageRSThreadsCount +
                ", \n\tregionsAutoSplitEnabled=" + regionsAutoSplitEnabled +
                ", \n\tregionsAutoSplitInitialCheckDelayMs=" + regionsAutoSplitInitialCheckDelayMs +
                ", \n\tregionsAutoSplitCheckDelayMs=" + regionsAutoSplitCheckDelayMs +
                ", \n\tregionsAutoSplitTriggerMb=" + regionsAutoSplitTriggerMb +
                ", \n\tregionsAutoSplitBatchMarginPercent=" + regionsAutoSplitBatchMarginPercent +
                ", \n\tregionsAutoSplitTableThreadsCount=" + regionsAutoSplitTableThreadsCount +
                ", \n\tregionsAutoRebalanceEnabled=" + regionsAutoRebalanceEnabled +
                ", \n\tregionsRebalanceAnalyticsOnly=" + regionsRebalanceAnalyticsOnly +
                ", \n\tregionsAutoRebalanceInitialCheckDelayMs=" + regionsAutoRebalanceInitialCheckDelayMs +
                ", \n\tregionsAutoRebalanceCheckDelayMs=" + regionsAutoRebalanceCheckDelayMs +
                ", \n\trebalanceTableThreadsCount=" + rebalanceTableThreadsCount +
                ", \n\tcreateTableRetryDelayMs=" + createTableRetryDelayMs +
                ", \n\tenableTableRetryDelayMs=" + enableTableRetryDelayMs +
                ", \n\tdisableTableRetryDelayMs=" + disableTableRetryDelayMs +
                ", \n\tdropTableRetryDelayMs=" + dropTableRetryDelayMs +
                ", \n\tmoveRegionRetryDelayMs=" + moveRegionRetryDelayMs +
                ", \n\tlockWaitingTimeoutMs=" + lockWaitingTimeoutMs +
                ", \n\trecoverTableRetryDelayMs=" + recoverTableRetryDelayMs +
                ", \n\tsplitTableRetryDelayMs=" + splitTableRetryDelayMs +
                ", \n\tlockMonitoringEnabled=" + lockMonitoringEnabled +
                ", \n\tlockMonitoringDelayMs=" + lockMonitoringDelayMs +
                '}';
    }

    public long getRegionsAutoSplitReservedMb() {
        return regionsAutoSplitReservedMb;
    }

    public void setRegionsAutoSplitReservedMb(long regionsAutoSplitReservedMb) {
        this.regionsAutoSplitReservedMb = regionsAutoSplitReservedMb;
    }

    public boolean isAutoCreateNs() {
        return autoCreateNs;
    }

    public void setAutoCreateNs(boolean autoCreateNs) {
        this.autoCreateNs = autoCreateNs;
    }

    public long getModifyPullTimeoutMs() {
        return modifyPullTimeoutMs;
    }

    public void setModifyPullTimeoutMs(long modifyPullTimeoutMs) {
        this.modifyPullTimeoutMs = modifyPullTimeoutMs;
    }
}
