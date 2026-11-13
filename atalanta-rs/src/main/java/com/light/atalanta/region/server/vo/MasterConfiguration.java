/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.vo;

public class MasterConfiguration {

    private Integer connectionRetryLimit;

    private Integer connectionRequestTimeoutMs;

    private Integer progressCalculatorIntervalMs;

    private Integer progressCalculatorAbortTimeoutMs;

    private Integer replicationFactor;

    public Integer getConnectionRetryLimit() {
        return connectionRetryLimit;
    }

    public void setConnectionRetryLimit(Integer connectionRetryLimit) {
        this.connectionRetryLimit = connectionRetryLimit;
    }

    public Integer getConnectionRequestTimeoutMs() {
        return connectionRequestTimeoutMs;
    }

    public void setConnectionRequestTimeoutMs(Integer connectionRequestTimeoutMs) {
        this.connectionRequestTimeoutMs = connectionRequestTimeoutMs;
    }

    public Integer getProgressCalculatorIntervalMs() {
        return progressCalculatorIntervalMs;
    }

    public void setProgressCalculatorIntervalMs(Integer progressCalculatorIntervalMs) {
        this.progressCalculatorIntervalMs = progressCalculatorIntervalMs;
    }

    public Integer getProgressCalculatorAbortTimeoutMs() {
        return progressCalculatorAbortTimeoutMs;
    }

    public void setProgressCalculatorAbortTimeoutMs(Integer progressCalculatorAbortTimeoutMs) {
        this.progressCalculatorAbortTimeoutMs = progressCalculatorAbortTimeoutMs;
    }

    public Integer getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(Integer replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    @Override
    public String toString() {
        return "MasterConfiguration{" +
                "\n\tconnectionRetryLimit=" + connectionRetryLimit +
                ", \n\tconnectionRequestTimeoutMs=" + connectionRequestTimeoutMs +
                ", \n\tprogressCalculatorIntervalMs=" + progressCalculatorIntervalMs +
                ", \n\tprogressCalculatorAbortTimeoutMs=" + progressCalculatorAbortTimeoutMs +
                ", \n\treplicationFactor=" + replicationFactor +
                '}';
    }
}
