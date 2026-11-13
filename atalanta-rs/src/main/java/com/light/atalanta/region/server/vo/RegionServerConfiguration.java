/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.vo;

import com.light.atalanta.region.server.utils.RsUtils;

import javax.validation.Valid;
import java.util.List;

public class RegionServerConfiguration {

    private ZooKeeperConfiguration zooKeeperConfiguration;
    private List<String> rsDirs;
    private GrpcConfiguration grpcConfiguration;
    private MasterConfiguration masterConfiguration;
    private CoreConfiguration coreConfiguration;
    @Valid
    private HealthCheckerConfiguration healthCheckerConfiguration;
    private DataConfiguration dataConfiguration;

    public ZooKeeperConfiguration getZooKeeperConfiguration() {
        return zooKeeperConfiguration;
    }

    public void setZooKeeperConfiguration(ZooKeeperConfiguration zooKeeperConfiguration) {
        this.zooKeeperConfiguration = zooKeeperConfiguration;
    }

    public List<String> getRsDirs() {
        return RsUtils.rsDirs(rsDirs);
    }

    public void setRsDirs(List<String> rsDirs) {
        this.rsDirs = rsDirs;
    }

    public GrpcConfiguration getGrpcConfiguration() {
        return grpcConfiguration;
    }

    public void setGrpcConfiguration(GrpcConfiguration grpcConfiguration) {
        this.grpcConfiguration = grpcConfiguration;
    }

    public MasterConfiguration getMasterConfiguration() {
        return masterConfiguration;
    }

    public void setMasterConfiguration(MasterConfiguration masterConfiguration) {
        this.masterConfiguration = masterConfiguration;
    }

    public CoreConfiguration getCoreConfiguration() {
        return coreConfiguration;
    }

    public void setCoreConfiguration(CoreConfiguration coreConfiguration) {
        this.coreConfiguration = coreConfiguration;
    }

    public HealthCheckerConfiguration getHealthCheckerConfiguration() {
        return healthCheckerConfiguration;
    }

    public void setHealthCheckerConfiguration(HealthCheckerConfiguration healthCheckerConfiguration) {
        this.healthCheckerConfiguration = healthCheckerConfiguration;
    }

    public DataConfiguration getDataConfiguration() {
        return dataConfiguration;
    }

    public void setDataConfiguration(DataConfiguration dataConfiguration) {
        this.dataConfiguration = dataConfiguration;
    }

    @Override
    public String toString() {
        return "\n" + zooKeeperConfiguration +
                ", \n" + rsDirs +
                ", \n" + grpcConfiguration +
                ", \n" + masterConfiguration +
                ", \n" + coreConfiguration +
                ", \n" + healthCheckerConfiguration +
                ", \n" + dataConfiguration +
                '}';
    }
}
