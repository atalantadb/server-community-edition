/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.vo;

public class ZooKeeperConfiguration {
    private String connectionString;

    private String connectionBasePath;

    private Integer connectionRetryTimes;

    private Integer connectionRetrySleepMs;

    private Integer sessionTimeoutMs = 120_000;

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public String getConnectionBasePath() {
        return connectionBasePath;
    }

    public void setConnectionBasePath(String connectionBasePath) {
        this.connectionBasePath = connectionBasePath;
    }

    public Integer getConnectionRetryTimes() {
        return connectionRetryTimes;
    }

    public void setConnectionRetryTimes(Integer connectionRetryTimes) {
        this.connectionRetryTimes = connectionRetryTimes;
    }

    public Integer getConnectionRetrySleepMs() {
        return connectionRetrySleepMs;
    }

    public void setConnectionRetrySleepMs(Integer connectionRetrySleepMs) {
        this.connectionRetrySleepMs = connectionRetrySleepMs;
    }

    public Integer getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public void setSessionTimeoutMs(Integer sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    @Override
    public String toString() {
        return "ZooKeeperConfiguration{" +
                "\n\tconnectionString='" + connectionString + '\'' +
                ", \n\tconnectionBasePath='" + connectionBasePath + '\'' +
                ", \n\tconnectionRetryTimes=" + connectionRetryTimes +
                ", \n\tconnectionRetrySleepMs=" + connectionRetrySleepMs +
                ", \n\tsessionTimeoutMs=" + sessionTimeoutMs +
                '}';
    }
}
