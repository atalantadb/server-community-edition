/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.vo;

import com.light.atalanta.Environment;
import com.light.atalanta.exceptions.FMasterRuntimeException;
import com.light.atalanta.master.services.leader.election.impl.LeaderStateManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class GrpcConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderStateManagerImpl.class.getName());

    private Integer masterPort;

    private Integer masterMonitoringPort = -1;

    private Integer threadPoolSize;

    private Integer rsReconnectTimeoutMs;

    private Integer rsResponseTimeoutMs;

    // The total shutdown_rs budget, not a timeout of a single request to an RS
    private long shutdownRsTimeoutMs = 600_000L;

    private Integer rsProgressCalculatorIntervalMs;

    public Integer getMasterPort() {
        return masterPort;
    }

    public void setMasterPort(Integer masterPort) {
        this.masterPort = masterPort;
    }

    private String myName;

    public void init(final String profile) {
        try {
            myName = Environment.getLocalAddress(profile, masterPort);
        } catch (UnknownHostException e) {
            LOGGER.error("Unknown host name", e);
            throw new FMasterRuntimeException(e);
        }
    }

    public String myName() {
        return myName;
    }

    public Integer getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(Integer threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public Integer getRsReconnectTimeoutMs() {
        return rsReconnectTimeoutMs;
    }

    public void setRsConnectTimeoutMs(Integer rsReconnectTimeoutMs) {
        this.rsReconnectTimeoutMs = rsReconnectTimeoutMs;
    }

    public Integer getRsResponseTimeoutMs() {
        return rsResponseTimeoutMs;
    }

    public void setRsResponseTimeoutMs(Integer rsResponseTimeoutMs) {
        this.rsResponseTimeoutMs = rsResponseTimeoutMs;
    }

    public long getShutdownRsTimeoutMs() {
        return shutdownRsTimeoutMs;
    }

    public void setShutdownRsTimeoutMs(long shutdownRsTimeoutMs) {
        if (shutdownRsTimeoutMs <= 0) {
            throw new IllegalArgumentException("shutdownRsTimeoutMs must be positive");
        }
        this.shutdownRsTimeoutMs = shutdownRsTimeoutMs;
    }

    public Integer getRsProgressCalculatorIntervalMs() {
        return rsProgressCalculatorIntervalMs;
    }

    public void setRsProgressCalculatorIntervalMs(Integer rsProgressCalculatorIntervalMs) {
        this.rsProgressCalculatorIntervalMs = rsProgressCalculatorIntervalMs;
    }

    public Integer getMasterMonitoringPort() {
        if (masterMonitoringPort < 0) {
            return masterPort + 1;
        }
        return masterMonitoringPort;
    }

    public void setMasterMonitoringPort(Integer masterMonitoringPort) {
        this.masterMonitoringPort = masterMonitoringPort;
    }

    @Override
    public String toString() {
        return "GrpcConfiguration{" +
                "\n\tmasterPort=" + masterPort +
                ", \n\tthreadPoolSize=" + threadPoolSize +
                ", \n\trsReconnectTimeoutMs=" + rsReconnectTimeoutMs +
                ", \n\trsResponseTimeoutMs=" + rsResponseTimeoutMs +
                ", \n\tshutdownRsTimeoutMs=" + shutdownRsTimeoutMs +
                ", \n\trsProgressCalculatorIntervalMs=" + rsProgressCalculatorIntervalMs +
                '}';
    }
}
