/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.vo;

import com.light.atalanta.Environment;
import com.light.atalanta.exceptions.FRegionServerRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class GrpcConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcConfiguration.class.getName());

    private Integer regionServerPort;

    private Integer threadPoolSize;

    private Integer activeProcessingRequests;

    private Boolean grpcRequestsMonitoringEnabled;

    private Integer receiveRegionTimeoutMs;

    private Integer regionServerConnectTimeoutMs;

    private Integer regionServerDisconnectTimeoutMs;

    private String myName;

    private String profile;

    public void init(final String profile) {
        try {
            myName = Environment.getLocalAddress(profile, regionServerPort);
        } catch (UnknownHostException e) {
            LOGGER.error("Unknown host name", e);
            throw new FRegionServerRuntimeException(e);
        }
    }

    public Integer getRegionServerPort() {
        return regionServerPort;
    }

    public void setRegionServerPort(Integer regionServerPort) {
        this.regionServerPort = regionServerPort;
    }

    public Integer getActiveProcessingRequests() {
        return activeProcessingRequests;
    }

    public void setActiveProcessingRequests(Integer activeProcessingRequests) {
        this.activeProcessingRequests = activeProcessingRequests;
    }

    public Boolean getGrpcRequestsMonitoringEnabled() {
        return grpcRequestsMonitoringEnabled;
    }

    public void setGrpcRequestsMonitoringEnabled(Boolean grpcRequestsMonitoringEnabled) {
        this.grpcRequestsMonitoringEnabled = grpcRequestsMonitoringEnabled;
    }

    public Integer getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(Integer threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public Integer getReceiveRegionTimeoutMs() {
        return receiveRegionTimeoutMs;
    }

    public void setReceiveRegionTimeoutMs(Integer receiveRegionTimeoutMs) {
        this.receiveRegionTimeoutMs = receiveRegionTimeoutMs;
    }

    public Integer getRegionServerConnectTimeoutMs() {
        return regionServerConnectTimeoutMs;
    }

    public void setRegionServerConnectTimeoutMs(Integer regionServerConnectTimeoutMs) {
        this.regionServerConnectTimeoutMs = regionServerConnectTimeoutMs;
    }

    public Integer getRegionServerDisconnectTimeoutMs() {
        return regionServerDisconnectTimeoutMs;
    }

    public void setRegionServerDisconnectTimeoutMs(Integer regionServerDisconnectTimeoutMs) {
        this.regionServerDisconnectTimeoutMs = regionServerDisconnectTimeoutMs;
    }

    public String myName() {
        return myName;
    }

    public String getProfile() {
        return profile;
    }

    @Override
    public String toString() {
        return "GrpcConfiguration{" +
                "\n\tregionServerPort=" + regionServerPort +
                ", \n\tthreadPoolSize=" + threadPoolSize +
                ", \n\tactiveProcessingRequests=" + activeProcessingRequests +
                ", \n\tgrpcRequestsMonitoringEnabled=" + grpcRequestsMonitoringEnabled +
                ", \n\treceiveRegionTimeoutMs=" + receiveRegionTimeoutMs +
                ", \n\tregionServerConnectTimeoutMs=" + regionServerConnectTimeoutMs +
                ", \n\tregionServerDisconnectTimeoutMs=" + regionServerDisconnectTimeoutMs +
                '}';
    }
}
