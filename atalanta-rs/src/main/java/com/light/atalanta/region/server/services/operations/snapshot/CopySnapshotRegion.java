/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.operations.snapshot;


import com.google.protobuf.ByteString;
import com.light.atalanta.grpc.ATALANTAProto;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.services.RegionServerConnector;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import com.light.atalanta.services.ZooKeeperCoreContext;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.light.atalanta.ATALANTAConstants.SNAPSHOT_ZNODE;
import static java.util.Collections.EMPTY_LIST;

public class CopySnapshotRegion {
    private static final Logger LOGGER = LoggerFactory.getLogger(CopySnapshotRegion.class.getName());
    private static final int COPY_FILE_BATCH_SIZE = 8 * 1024 * 1024;
    private final RegionServerConfiguration regionServerConfiguration;
    private final FileSystem fileSystem;
    private final RegionServerConnector regionServerConnector;
    private final ZooKeeperCoreContext zooContext;

    public CopySnapshotRegion(RegionServerConfiguration regionServerConfiguration,
                              FileSystem fileSystem, RegionServerConnector regionServerConnector, ZooKeeperCoreContext zooContext) {
        this.regionServerConfiguration = regionServerConfiguration;
        this.fileSystem = fileSystem;
        this.regionServerConnector = regionServerConnector;
        this.zooContext = zooContext;
    }


    public void copySnapshotRegion(String name,

                                   ATALANTAProto.ProtoTableName tableName,
                                   String region, int targetReplicaNum, String targetRegionServer,
                                   StreamObserver<RegionServerProto.CopyRegionSnapshotOperationProgress> responseObserver) throws Exception {
        List<File> filesToCopy = getFilesToCopy(name, tableName, region);
        int n = 1;
        RegionServerProto.TransferSnapshotRegionRequest request = RegionServerProto.TransferSnapshotRegionRequest.newBuilder()
                .setTableName(tableName)
                .setRegion(region)
                .setTargetReplicaNum(targetReplicaNum)
                .setName(name)
                .build();
        String operationName = String.format("Creating snapshot region #%d for '%s'", n, tableName);
        regionServerConnector.atomic(zooContext.getRegionServerModel(targetRegionServer), operationName, stub ->
                stub.get()
                        .withDeadlineAfter(regionServerConfiguration.getGrpcConfiguration().getReceiveRegionTimeoutMs(), TimeUnit.MILLISECONDS)
                        .createSnapshotRegion(request)
        );

        for (File file : filesToCopy) {
            String fileName = file.getName() + " [" + filesToCopy.size() + "/" + n + "]";
            LOGGER.info("start copy: {}", fileName);
            responseObserver.onNext
                    (RegionServerProto.CopyRegionSnapshotOperationProgress.newBuilder()
                            .setFileName(fileName)
                            .setStatus("start_copy")
                            .build());
            copyFileToTargetRs(file, name, tableName, region, targetReplicaNum, targetRegionServer);
            LOGGER.info("copy complete: {}", fileName);
            responseObserver.onNext
                    (RegionServerProto.CopyRegionSnapshotOperationProgress.newBuilder()
                            .setFileName(fileName).setStatus("complete")
                            .build());
            n++;
        }
    }

    private void copyFileToTargetRs(File file, String name, ATALANTAProto.ProtoTableName tableName,
                                    String region, int targetReplicaNum, String targetRegionServer) throws IOException {
        try (FileInputStream fis = new FileInputStream(file);
             BufferedInputStream bis = new BufferedInputStream(fis, COPY_FILE_BATCH_SIZE)) {
            byte[] buffer = new byte[COPY_FILE_BATCH_SIZE];
            int readSize;
            while ((readSize = bis.read(buffer)) > 0) {
                RegionServerProto.TransferSnapshotRegionRequest request = RegionServerProto.TransferSnapshotRegionRequest.newBuilder().setFileName(file.getName())
                        .setRegion(region)
                        .setTableName(tableName)
                        .setName(name)
                        .setTargetReplicaNum(targetReplicaNum)
                        .setData(ByteString.copyFrom(buffer, 0, readSize))
                        .build();
                String operationName = String.format("Copying data to target snapshot region for '%s'", tableName);
                regionServerConnector.atomic(zooContext.getRegionServerModel(targetRegionServer), operationName, stub ->
                        stub.get()
                                .withDeadlineAfter(regionServerConfiguration.getGrpcConfiguration().getReceiveRegionTimeoutMs(), TimeUnit.MILLISECONDS)
                                .copyDataToTargetSnapshotRegion(request)
                );
            }
        }
    }

    private List<File> getFilesToCopy(String name, ATALANTAProto.ProtoTableName tableName, String region) throws IOException {
        for (String dir : regionServerConfiguration.getRsDirs()) {
            final String regionsBaseDir = dir + "/" + SNAPSHOT_ZNODE + "/" + name + "/" + tableName.getNamespace() + "/" + tableName.getTable();
            Optional<String> regionDir = fileSystem.getDirFilesAndDirs(regionsBaseDir).stream()
                    .filter(regDir -> regDir.startsWith(region)).findAny();
            if (regionDir.isPresent()) {
                return fileSystem.getDirFileDetails(regionsBaseDir + "/" + regionDir.get());
            }
        }
        return EMPTY_LIST;
    }
}