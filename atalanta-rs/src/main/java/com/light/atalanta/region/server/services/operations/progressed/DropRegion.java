/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.operations.progressed;

import com.light.atalanta.TableName;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.region.server.services.ReplicaMapperProcessor;
import com.light.atalanta.region.server.services.grpc.RegionTaskFunctionalized;
import com.light.atalanta.region.server.services.operations.RegionOperation;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Drops a disabled region.
 * No UNASSIGN is needed, since it was done when the region was disabled.
 * Only the files are deleted.
 */
public class DropRegion implements RegionOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(DropRegion.class.getName());

    private final ReplicaMapperProcessor replicaMapperProcessor;

    private final FileSystem fileSystem;

    public DropRegion(ReplicaMapperProcessor replicaMapperProcessor, FileSystem fileSystem) {
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.fileSystem = fileSystem;
    }

    @Override
    public void progressedAction(TableName tableName, RegionModel regionModel, AtomicReference<String> taskProgress, AtomicBoolean interrupted, String reservedReplicas) throws Exception {
        LOGGER.info("Start dropping region '{}:{}'", tableName, regionModel.toShortString());

        // If we don't own the region, respond with success right away
        if (!replicaMapperProcessor.contains(regionModel)) {
            taskProgress.set("region already dropped");
            LOGGER.info("Done dropping region '{}:{}': already dropped", tableName, regionModel.toShortString());
            return;
        }

        try {
            updateTaskProgress(taskProgress, tableName, regionModel, "deleting region directory");
            fileSystem.makeSureDirRemoved(replicaMapperProcessor.getReplicaDir(regionModel));

            updateTaskProgress(taskProgress, tableName, regionModel, "removing region path from mapping");
            removeDirectoryFromMapping(tableName, regionModel);
            updateTaskProgress(taskProgress, tableName, regionModel, "region dropped successfully");
        } catch (Exception ex) {
            throw new Exception("Error dropping region: '" + tableName + ":" + regionModel.toShortString() + "'", ex);
        }

        LOGGER.info("Done dropping region '{}:{}'", tableName, regionModel.toShortString());
    }

    @Override
    public void functionalizedAction(TableName tableName, RegionModel regionModel, RegionTaskFunctionalized task, String reservedReplicas) throws Exception {
        throw new NotImplementedException("Unable to run progressed task as functionalized");
    }

    private void removeDirectoryFromMapping(TableName tableName, RegionModel regionModel) throws Exception {
        if (!replicaMapperProcessor.removeReplicaDir(regionModel)) {
            // Unexpected case: the mapping has no path for the replica
            LOGGER.info("Dropping region '{}:{}' region dir was not found in mapping", tableName, regionModel.toShortString());
            // TODO: Decide whether a warning is enough
            throw new Exception("Unable to remove replica path from mapping: it is not exist");
        }
    }

    private void updateTaskProgress(AtomicReference<String> taskProgress, TableName tableName, RegionModel regionModel, String text) {
        taskProgress.set(text);
        LOGGER.info("Dropping region '{}:{}': {}", tableName, regionModel.toShortString(), text);
    }
}
