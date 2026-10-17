/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.model;

import com.light.atalanta.model.RegionServerModel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RegionServerModelExt extends RegionServerModel {

    private String rsStatus;

    private String minDiskInfo = "";

    private String cssClass = "";

    private Map<String, DiskInfoExt> diskInfoExt = new ConcurrentHashMap<>();

    public RegionServerModelExt(String name, String host, int javaPort, int coreClientPort, int jmxPort) {
        super(name, host, javaPort, coreClientPort, jmxPort);
    }

    public RegionServerModelExt(RegionServerModel rs) {
        super(rs.getName(), rs.getHost(), rs.getJavaPort(), rs.getCoreClientPort(), rs.getJmxPort());
    }

    public String getRsStatus() {
        return rsStatus;
    }

    public void setRsStatus(String rsStatus) {
        this.rsStatus = rsStatus;
    }

    public String getMinDiskInfo() {
        return minDiskInfo;
    }

    public void setMinDiskInfo(String minDiskInfo) {
        this.minDiskInfo = minDiskInfo;
    }

    public Map<String, DiskInfoExt> getDiskInfoExt() {
        return diskInfoExt;
    }

    public void setDiskInfoExt(Map<String, DiskInfoExt> diskInfoExt) {
        this.diskInfoExt = diskInfoExt;
    }

    public String getCssClass() {
        return cssClass;
    }

    public void setCssClass(String cssClass) {
        this.cssClass = cssClass;
    }

}
