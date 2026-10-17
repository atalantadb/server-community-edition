/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.operations.functionalized;

import com.light.atalanta.TableName;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.region.server.services.ReplicaMapperProcessor;
import com.light.atalanta.region.server.services.grpc.RegionTaskFunctionalized;
import com.light.atalanta.region.server.services.operations.RegionOperation;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ListRegionFiles implements RegionOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListRegionFiles.class);

    private final ReplicaMapperProcessor replicaMapperProcessor;
    private final FileSystem fileSystem;

    public ListRegionFiles(ReplicaMapperProcessor replicaMapperProcessor, FileSystem fileSystem) {
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.fileSystem = fileSystem;
    }

    @Override
    public void functionalizedAction(TableName tableName, RegionModel regionModel, RegionTaskFunctionalized task, String reservedReplicas) throws Exception {
        LOGGER.info("Start sending region file list '{}:{}' to '{}'", tableName, regionModel.toShortString(), reservedReplicas);

        try {
            logStatus(tableName, regionModel, "start scanning region files");
            String replicaPath = replicaMapperProcessor.getReplicaDir(regionModel);
            RegionServerProto.RegionDirectoryStructure.Builder regionDirectoryStructure = RegionServerProto.RegionDirectoryStructure.newBuilder();

            for (File regionFile : fileSystem.getDirFileDetails(replicaPath)) {
                RegionServerProto.RegionFile.Builder fileBuilder = RegionServerProto.RegionFile.newBuilder()
                        .setFileName(regionFile.getName())
                        .setFileSize(regionFile.length());
                regionDirectoryStructure.addFiles(fileBuilder);
                LOGGER.info("File {} SHA-256 is: {}", regionFile.getAbsolutePath(), fileSystem.calculateFileHash(regionFile.getAbsolutePath()));
                LOGGER.info("File {} size is: {} bytes", regionFile.getAbsolutePath(), fileSystem.calculateFileSize(regionFile.getAbsolutePath()));
            }
            logStatus(tableName, regionModel, "done scanning region files");
            task.sendResponse(regionDirectoryStructure.build());
            logStatus(tableName, regionModel, "region file list was sent");
        } catch (RegionTaskFunctionalized.GrpcConnectException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new Exception(String.format("Error sending region file list '%s:%s' to '%s'", tableName, regionModel.toShortString(), reservedReplicas), ex);
        }

        LOGGER.info("Done sending region file list '{}:{}' to '{}'", tableName, regionModel.toShortString(), reservedReplicas);
    }

    @Override
    public void progressedAction(TableName tableName, RegionModel regionModel, AtomicReference<String> taskProgress, AtomicBoolean interrupted, String reservedReplicas) throws Exception {
        throw new NotImplementedException("Unable to run functionalized task as progressed");
    }

    private void logStatus(TableName tableName, RegionModel regionModel, String text) {
        LOGGER.info("Sending region file list '{}:{}': {}", tableName, regionModel.toShortString(), text);
    }
}
