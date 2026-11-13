/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.model;

import com.light.atalanta.metrics.RegionMetrics;
import com.light.atalanta.model.CompressType;
import com.light.atalanta.model.RecordCompressionType;

public class AtalantaRegionInfo {
    private String ns;
    private String tableName;
    private String name;
    private String uuid;
    private String beginKey;
    private String endKey;
    private Integer replicaNum;
    private RegionMetrics regionMetrics;
    private int dataBlockSize;
    private String hostPort;
    private String compress;
    private Boolean dict;

    public void setCompression(CompressType blockCompression, RecordCompressionType recordCompression) {
        if (recordCompression != null && recordCompression != RecordCompressionType.none) {
            compress = "record";
        } else if (blockCompression != null && blockCompression != CompressType.none) {
            compress = "block";
        } else if (recordCompression != null || blockCompression != null) {
            compress = "none";
        } else {
            compress = null;
        }
    }

    public String getCompress() {
        return compress;
    }

    public void setDict(boolean dict) {
        this.dict = dict;
    }

    public Boolean getDict() {
        return dict;
    }

    public void setNs(String ns) {
        this.ns = ns;
    }

    public String getNs() {
        return ns;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getUuid() {
        return uuid;
    }

    public void setBeginKey(String beginKey) {
        this.beginKey = beginKey;
    }

    public String getBeginKey() {
        return beginKey;
    }

    public void setEndKey(String endKey) {
        this.endKey = endKey;
    }

    public String getEndKey() {
        return endKey;
    }

    public RegionMetrics getRegionMetrics() {
        return regionMetrics;
    }

    public void setRegionMetrics(RegionMetrics regionMetrics) {
        this.regionMetrics = regionMetrics;
    }

    public Integer getReplicaNum() {
        return replicaNum;
    }

    public void setReplicaNum(Integer replicaNum) {
        this.replicaNum = replicaNum;
    }

    public void setDataBlockSize(int dataBlockSize) {
        this.dataBlockSize = dataBlockSize;
    }

    public int getDataBlockSize() {
        return dataBlockSize;
    }

    public String getHostPort() {
        return hostPort;
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
    }

}
