/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.data;

import com.light.atalanta.TableName;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.region.server.services.ReplicaMapperProcessor;
import com.light.atalanta.region.server.services.grpc.RegionTaskFunctionalized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class RegionFileList {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionFileList.class);

    private final ReplicaMapperProcessor replicaMapperProcessor;
    private final FileSystem fileSystem;

    public RegionFileList(ReplicaMapperProcessor replicaMapperProcessor, FileSystem fileSystem) {
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.fileSystem = fileSystem;
    }

    public RegionServerProto.FileList getFileList(TableName tableName, RegionModel regionModel) throws Exception {
        RegionServerProto.FileList.Builder resBuilder = RegionServerProto.FileList.newBuilder();
        try {
            String replicaPath = replicaMapperProcessor.getReplicaDir(regionModel);
            for (File regionFile : fileSystem.getDirFileDetails(replicaPath)) {
                RegionServerProto.FileInfo.Builder fileInfoBuilder = RegionServerProto.FileInfo.newBuilder();
                fileInfoBuilder.setFileName(regionFile.getName());
                fileInfoBuilder.setSize(regionFile.length());
                resBuilder.addFiles(fileInfoBuilder.build());
            }
        } catch (Exception ex) {
            LOGGER.error("Error creating region file list '{}:{}'", tableName, regionModel.toShortString());
            throw new Exception(String.format("Error creating region file list '%s:%s'", tableName, regionModel.toShortString()), ex);
        }
        return resBuilder.build();
    }

}
