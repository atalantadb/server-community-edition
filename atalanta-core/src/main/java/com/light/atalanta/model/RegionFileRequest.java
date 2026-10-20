/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.model;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * Region wrapper that includes metadata of the region data file.
 */
public class RegionFileRequest extends RegionModel {

    private final String fileName;

    private final long fileSize;

    private final String receiverIp;

    private final int tcpPort;

    public RegionFileRequest(RegionModel regionModel, String fileName, long fileSize, String receiverIp, int tcpPort) {
        super(
                regionModel.getName(),
                regionModel.getReplicaNum(),
                regionModel.getStatusId(),
                regionModel.getBeginKey(),
                regionModel.getEndKey(),
                regionModel.getDataBlockSize(),
                regionModel.getCompressType(),
                regionModel.getRecordCompressionType(),
                regionModel.getDictionary(),
                regionModel.getRecordTargetCount()
        );
        Preconditions.checkArgument(StringUtils.isNotBlank(fileName), "Region file name can't be null or empty");
        Preconditions.checkArgument(StringUtils.isNotBlank(receiverIp), "Receiver IP can't be empty");
        Preconditions.checkArgument(fileSize >= 0, "Region file size can't be negative");
        Preconditions.checkArgument(tcpPort > 0, "TCP port should be greater than zero");
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.receiverIp = receiverIp;
        this.tcpPort = tcpPort;
    }

    public String getFileName() {
        return fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public String getReceiverIp() {
        return receiverIp;
    }

    public int getTcpPort() {
        return tcpPort;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        RegionFileRequest that = (RegionFileRequest) o;
        return fileSize == that.fileSize && tcpPort == that.tcpPort && Objects.equals(fileName, that.fileName) && Objects.equals(receiverIp, that.receiverIp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fileName, fileSize, receiverIp, tcpPort);
    }
}
