/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.vo;

public class TestConfiguration {

    private Integer regionServersJavaStartPort;

    private Integer regionServersCoreClientStartPort;

    private Integer regionServersCount;

    public Integer getRegionServersJavaStartPort() {
        return regionServersJavaStartPort;
    }

    public void setRegionServersJavaStartPort(Integer regionServersJavaStartPort) {
        this.regionServersJavaStartPort = regionServersJavaStartPort;
    }

    public Integer getRegionServersCoreClientStartPort() {
        return regionServersCoreClientStartPort;
    }

    public void setRegionServersCoreClientStartPort(Integer regionServersCoreClientStartPort) {
        this.regionServersCoreClientStartPort = regionServersCoreClientStartPort;
    }

    public Integer getRegionServersCount() {
        return regionServersCount;
    }

    public void setRegionServersCount(Integer regionServersCount) {
        this.regionServersCount = regionServersCount;
    }

    @Override
    public String toString() {
        return "TestConfiguration{" +
                "\n\tregionServersJavaStartPort=" + regionServersJavaStartPort +
                ", \n\tregionServersCoreClientStartPort=" + regionServersCoreClientStartPort +
                ", \n\tregionServersCount=" + regionServersCount +
                '}';
    }
}
