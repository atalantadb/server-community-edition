/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.vo;

@SuppressWarnings("unused")
public class AtalantaDBConfiguration {
    private GrpcConfiguration grpcConfiguration;

    private ZooKeeperConfiguration zooKeeperConfiguration;

    private DataConfiguration dataConfiguration;

    private TestConfiguration testConfiguration;

    private ClientConfiguration clientConfiguration;

    public GrpcConfiguration getGrpcConfiguration() {
        return grpcConfiguration;
    }

    public void setGrpcConfiguration(GrpcConfiguration grpcConfiguration) {
        this.grpcConfiguration = grpcConfiguration;
    }

    public ZooKeeperConfiguration getZooKeeperConfiguration() {
        return zooKeeperConfiguration;
    }

    public void setZooKeeperConfiguration(ZooKeeperConfiguration zooKeeperConfiguration) {
        this.zooKeeperConfiguration = zooKeeperConfiguration;
    }

    public DataConfiguration getDataConfiguration() {
        return dataConfiguration;
    }

    public void setDataConfiguration(DataConfiguration dataConfiguration) {
        this.dataConfiguration = dataConfiguration;
    }

    public TestConfiguration getTestConfiguration() {
        return testConfiguration;
    }

    public void setTestConfiguration(TestConfiguration testConfiguration) {
        this.testConfiguration = testConfiguration;
    }

    public ClientConfiguration getClientConfiguration() {
        return clientConfiguration;
    }

    public void setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
    }

    @Override
    public String toString() {
        return "\n" + grpcConfiguration +
                ", \n" + zooKeeperConfiguration +
                ", \n" + dataConfiguration +
                ", \n" + clientConfiguration +
                '}';
    }
}
