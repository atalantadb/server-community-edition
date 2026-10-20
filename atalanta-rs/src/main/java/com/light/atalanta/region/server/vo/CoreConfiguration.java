/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.vo;

public class CoreConfiguration {

    private String host;

    private Integer rsPort;

    private Integer clientPort;

    private Long connectionRetryTimes;

    private Long connectionRetrySleepMs;

    private Integer socketTimeoutMs;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getRsPort() {
        return rsPort;
    }

    public void setRsPort(Integer rsPort) {
        this.rsPort = rsPort;
    }

    public Integer getClientPort() {
        return clientPort;
    }

    public void setClientPort(Integer clientPort) {
        this.clientPort = clientPort;
    }

    public Long getConnectionRetryTimes() {
        return connectionRetryTimes;
    }

    public void setConnectionRetryTimes(Long connectionRetryTimes) {
        this.connectionRetryTimes = connectionRetryTimes;
    }

    public Long getConnectionRetrySleepMs() {
        return connectionRetrySleepMs;
    }

    public void setConnectionRetrySleepMs(Long connectionRetrySleepMs) {
        this.connectionRetrySleepMs = connectionRetrySleepMs;
    }

    public Integer getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public void setSocketTimeoutMs(Integer socketTimeoutMs) {
        this.socketTimeoutMs = socketTimeoutMs;
    }


    @Override
    public String toString() {
        return "CoreConfiguration{" +
                "\n\thost='" + host + '\'' +
                ", \n\trsPort=" + rsPort +
                ", \n\tclientPort=" + clientPort +
                ", \n\tconnectionRetryTimes=" + connectionRetryTimes +
                ", \n\tconnectionRetrySleepMs=" + connectionRetrySleepMs +
                ", \n\tsocketTimeoutMs=" + socketTimeoutMs +
                '}';
    }
}
