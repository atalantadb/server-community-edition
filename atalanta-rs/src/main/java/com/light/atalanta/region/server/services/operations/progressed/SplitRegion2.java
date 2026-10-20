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
import com.light.atalanta.region.server.vo.DataConfiguration;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.light.atalanta.region.server.services.operations.progressed.SplitRegion1.BKP_POSTFIX;
import static com.light.atalanta.region.server.services.operations.progressed.SplitRegion1.LOB_BKP_FILE_NAME;

public class SplitRegion2 implements RegionOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitRegion2.class.getName());

    private final ReplicaMapperProcessor replicaMapperProcessor;
    private final DataConfiguration dataConfiguration;
    private final FileSystem fileSystem;

    public SplitRegion2(ReplicaMapperProcessor replicaMapperProcessor, FileSystem fileSystem, DataConfiguration dataConfiguration) {
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.fileSystem = fileSystem;
        this.dataConfiguration = dataConfiguration;
    }

    @Override
    public void progressedAction(TableName tableName, RegionModel replicaToSplit, AtomicReference<String> taskProgress, AtomicBoolean interrupted, String details) throws Exception {
        LOGGER.info("Start splitting2 region '{}:{}'", tableName, replicaToSplit.toShortString());

        for (File backupFile : getBackupDataFiles(replicaToSplit, tableName)) {
            if (dataConfiguration.getSplitSourceBackupEnabled()) {
                // ../data/rs/namespace/table/regionName_replicaNum/000_bkp.data
                String source = backupFile.getAbsolutePath();
                // ../data/bkp/regionName_replicaNum_before_split.data
                String dest = dataConfiguration.getSplitSourceBackupDir() + "/" + replicaToSplit.toIdentityString() + "_before_split.data";
                // If there is a backup, check whether a backup copy exists
                // - if it does, delete it and create a new one
                // - if it doesn't, just create it
                // If the backup is already gone, log a warning
                if (backupFile.exists()) {
                    if (new File(dest).exists()) {
                        LOGGER.info("Splitting2 region '{}:{}': backup copy already exists, removing", tableName, replicaToSplit.toShortString());
                        fileSystem.makeSureFileRemoved(dest);
                    }
                    LOGGER.info("Splitting2 region '{}:{}': making copy of backup file", tableName, replicaToSplit.toShortString());
                    fileSystem.copyFile(source, dest);
                } else {
                    LOGGER.warn("Splitting2 region '{}:{}': backup file is not exists, not making copy; assume it already exists", tableName, replicaToSplit.toShortString());
                }
            }
            updateTaskProgress(taskProgress, tableName, replicaToSplit, "deleting file: " + backupFile.getAbsolutePath());
            fileSystem.makeSureFileRemoved(backupFile.getAbsolutePath());
        }

        updateTaskProgress(taskProgress, tableName, replicaToSplit, "region backup data files was deleted successfully");
        LOGGER.info("Done splitting2 region '{}:{}'", tableName, replicaToSplit.toShortString());
    }

    // Returns the original files (before the split)
    private List<File> getBackupDataFiles(RegionModel regionModel, TableName tableName) throws Exception {
        List<File> fileList = new ArrayList<>();
        String replicaPath = replicaMapperProcessor.getReplicaDir(regionModel);
        fileSystem.getDirFileDetails(replicaPath).forEach(f -> {
            String path = f.getAbsolutePath();
            if (path.endsWith(BKP_POSTFIX) || path.endsWith(LOB_BKP_FILE_NAME)) {
                fileList.add(f);
            }
        });
        return fileList;
    }

    @Override
    public void functionalizedAction(TableName tableName, RegionModel regionModel, RegionTaskFunctionalized task, String reservedReplicas) throws Exception {
        throw new NotImplementedException("Unable to run progressed task as functionalized");
    }

    private void updateTaskProgress(AtomicReference<String> taskProgress, TableName tableName, RegionModel regionModel, String text) {
        taskProgress.set(text);
        LOGGER.info("Splitting2 region '{}:{}': {}", tableName, regionModel.toShortString(), text);
    }
}
