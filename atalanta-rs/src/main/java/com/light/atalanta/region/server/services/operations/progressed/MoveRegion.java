/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.operations.progressed;

import com.light.atalanta.TableName;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionFileRequest;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.region.server.services.ReplicaMapperProcessor;
import com.light.atalanta.region.server.services.tcp.TcpFileTransfer;
import com.light.atalanta.region.server.vo.GrpcConfiguration;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import com.light.atalanta.services.RegionServerConnector;
import com.light.atalanta.region.server.services.grpc.RegionTaskFunctionalized;
import com.light.atalanta.region.server.services.operations.RegionOperation;
import com.light.atalanta.util.ProtoConverters;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import com.light.atalanta.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class MoveRegion implements RegionOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(MoveRegion.class.getName());

    private static final String DETAILS_SPLITTER = "@";

    public static final String TEMP_REGION_BATCH_POSTFIX = "_temp";

    private final GrpcConfiguration grpcConf;

    private final RegionServerConnector regionServerConnector;
    private final ReplicaMapperProcessor replicaMapperProcessor;
    private final DistributeRegion distributeRegion;
    private final FileSystem fileSystem;

    public MoveRegion(RegionServerConfiguration conf, RegionServerConnector regionServerConnector, ReplicaMapperProcessor replicaMapperProcessor, DistributeRegion distributeRegion, FileSystem fileSystem) {
        this.regionServerConnector = regionServerConnector;
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.distributeRegion = distributeRegion;
        this.fileSystem = fileSystem;
        this.grpcConf = conf.getGrpcConfiguration();
    }

    @Override
    public void progressedAction(TableName tableName, RegionModel destRegion, AtomicReference<String> taskProgress, AtomicBoolean interrupted, String details) throws Exception {
        LOGGER.info("Start moving region '{}:{}' from '{}'", tableName, destRegion.toShortString(), details);

        // If we already own the region, respond with success right away
        if (replicaMapperProcessor.contains(destRegion)) {
            taskProgress.set("region already moved");
            LOGGER.info("Done moving region '{}:{}': already moved", tableName, destRegion.toShortString());
            return;
        }

        String sourceRegionServer = "unknown";
        try {
            // The details string has the format sourceRegionServer@reservedReplicasString.
            // The first part is the source RS to get the data from
            // The second part is the reserve replica string when a leader is moved. When a reserve is moved, it is empty.
            Preconditions.checkArgument(details.contains(DETAILS_SPLITTER)
                            && (details.split(DETAILS_SPLITTER).length == 1 || details.split(DETAILS_SPLITTER).length == 2),
                    "Invalid details string received from master");
            String[] detailsParts = details.split(DETAILS_SPLITTER);
            sourceRegionServer = detailsParts[0];
            String reservedReplicas = detailsParts.length == 2 ? detailsParts[1] : StringUtils.EMPTY;
            RegionDistributionModel sourceRegion = makeSourceRdm(destRegion, sourceRegionServer);

            // Disable the source region
            // First stop the writes and wait for a successful UNASSIGN, so that
            // the file list reflects the final sizes of the region files.
            // This part (move_region RS request) is not being involved in move rollbacks, therefore, we have the right
            // to interrupt it, if we're asked to do so.
            updateTaskProgress(taskProgress, tableName, destRegion, "disabling source region");
            String disableRegionStr = String.format("Moving region '%s:%s': disabling source region", tableName, destRegion.toShortString());
            regionServerConnector.monitor(tableName, disableRegionStr, sourceRegion, StringUtils.EMPTY, (stub, req, stream) ->
                    stub.get().disableRegion(req, stream), false, interrupted
            );

            // Get the region file list only after the source is disabled
            updateTaskProgress(taskProgress, tableName, destRegion, "receiving region file list from source");
            String regionListLogStr = String.format("Moving region '%s:%s': receiving file list from '%s'", tableName, destRegion.toShortString(), sourceRegion.toShortString());
            final RegionServerProto.RegionRequest request = ProtoConverters.toProtoRegion(tableName, sourceRegion.getRegionModel(), grpcConf.myName());
            List<RegionServerProto.RegionFile> regionFileList = regionServerConnector.result(
                    sourceRegion.getRegionServerModel(), regionListLogStr, stub -> stub.get().listRegionFiles(request)
            ).getFilesList();
            LOGGER.debug("Moving region '{}:{}': {}", tableName, destRegion.toShortString(), "region file list is: " + listToString(regionFileList));

            // Pick the folder for the region
            updateTaskProgress(taskProgress, tableName, destRegion, "choosing data directory");
            String replicaDir = distributeRegion.chooseReplicaDir(tableName, destRegion);

            // If the replica is not in the mapping, clean up its data and temp directory, if any
            fileSystem.makeSureDirRemoved(replicaDir);
            fileSystem.makeSureDirRemoved(replicaDir + TEMP_REGION_BATCH_POSTFIX);

            // Prepare the folder
            String tempRegionDir = replicaDir + TEMP_REGION_BATCH_POSTFIX;
            fileSystem.makeSureDirExists(tempRegionDir);
            destRegion.writeRegionInfoFile(tempRegionDir);


            // Get the region data and write it to the temp directory
            for (RegionServerProto.RegionFile regionFile : regionFileList) {
                updateTaskProgress(taskProgress, tableName, destRegion, "processing file=" + regionFile.getFileName() + ", sizeMb=" + regionFile.getFileSize() / 1024 / 1024);

                try (ServerSocketChannel serverChannel = ServerSocketChannel.open()) {
                    // NIO channels don't support connection timeouts out-of-box;
                    // therefore, we need to get classic ServerSocket from the channel first
                    ServerSocket serverSocket = serverChannel.socket();
                    serverSocket.setSoTimeout(grpcConf.getRegionServerConnectTimeoutMs());
                    serverSocket.bind(new InetSocketAddress(0));

                    int ephemeralPort = serverChannel.socket().getLocalPort();
                    LOGGER.info("Moving region '{}:{}': listening on port: {}", tableName, destRegion.toShortString(), ephemeralPort);

                    // Make gRPC call to request this specific file
                    RegionFileRequest regionFileRequest = new RegionFileRequest(
                            sourceRegion.getRegionModel(),
                            regionFile.getFileName(),
                            regionFile.getFileSize(),
                            grpcConf.myName().substring(0, grpcConf.myName().indexOf(":")),
                            ephemeralPort
                    );
                    RegionServerProto.RegionRequest dataRequest = ProtoConverters.toProtoRegion(
                            tableName, regionFileRequest, grpcConf.myName()
                    );
                    String receiveRegionStr = String.format("Moving region '%s:%s': requesting region file %s from '%s'", tableName, destRegion.toShortString(), regionFile.getFileName(), sourceRegion.toShortString());
                    Preconditions.checkArgument(regionServerConnector.result(
                            sourceRegion.getRegionServerModel(), receiveRegionStr, stub -> stub.get().sendRegionFile(dataRequest)
                    ).getFilesList().isEmpty(), "unexpected source RS response");
                    updateTaskProgress(taskProgress, tableName, destRegion, "waiting for region file=" + regionFile.getFileName() + ", sizeMb=" + regionFile.getFileSize() / 1024 / 1024);

                    // Wait for the sender to connect
                    try (SocketChannel clientChannel = serverSocket.accept().getChannel()) {
                        LOGGER.info("Moving region '{}:{}': sender connected from : {}", tableName, destRegion.toShortString(), clientChannel.getRemoteAddress());
                        // Receive the file
                        long bytesReceived = TcpFileTransfer.receiveSingleFile(grpcConf, clientChannel, regionFile, tempRegionDir);
                        long actualSize = fileSystem.calculateFileSize(new File(tempRegionDir, regionFile.getFileName()).getAbsolutePath());
                        Preconditions.checkArgument(actualSize == bytesReceived, "different files size. please, check the logs");
                        Preconditions.checkArgument(regionFile.getFileSize() == bytesReceived, "different files size. please, check the logs");
                    }
                }
            }

            // Once all data is received, turn the temp directory into the real one
            boolean renameResult = fileSystem.renameDir(replicaDir + TEMP_REGION_BATCH_POSTFIX, replicaDir);
            Preconditions.checkArgument(renameResult,  "Unable to rename temp replica directory");

            // If we were interrupted, don't continue
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }

            // Register the new region with its data folder
            updateTaskProgress(taskProgress, tableName, destRegion, "distributing target region");
            distributeRegion.distributeRegionInternal(tableName, destRegion,  new AtomicReference<>(), reservedReplicas, replicaDir, true);

            updateTaskProgress(taskProgress, tableName, destRegion, "region moved successfully");
        } catch (Throwable ex) {
            throw new Exception(String.format("Error moving region '%s:%s' from '%s'", tableName, destRegion.toShortString(), sourceRegionServer), ex);
        }

        LOGGER.info("Done moving region '{}:{}' from '{}'", tableName, destRegion.toShortString(), sourceRegionServer);
    }

    @Override
    public void functionalizedAction(TableName tableName, RegionModel regionModel, RegionTaskFunctionalized task, String reservedReplicas) throws Exception {
        throw new NotImplementedException("Unable to run progressed task as functionalized");
    }

    private RegionDistributionModel makeSourceRdm(RegionModel regionModel, String regionServerLocation) {
        Preconditions.checkArgument(regionServerLocation.contains(":")
                        && regionServerLocation.split(":").length == 2
                        && NumberUtils.isCreatable(regionServerLocation.split(":")[1]),
                "Region server location is damaged");
        String host = regionServerLocation.split(":")[0];
        int javaPort = Integer.parseInt(regionServerLocation.split(":")[1]);
        return new RegionDistributionModel(regionModel, new RegionServerModel("sourceRegionServer", host, javaPort, 1, 0));
    }

    private void updateTaskProgress(AtomicReference<String> taskProgress, TableName tableName, RegionModel regionModel, String text) {
        taskProgress.set(text);
        LOGGER.info("Moving region '{}:{}': {}", tableName, regionModel.toShortString(), text);
    }

    private String listToString(List<RegionServerProto.RegionFile> fileList) {
        StringBuilder result = new StringBuilder();
        for (RegionServerProto.RegionFile file : fileList) {
            result.append(String.format("%nfile: %s, sizeMb: %d", file.getFileName(), file.getFileSize() / 1024 / 1024));
        }
        if (result.length() == 0) {
            result.append("empty");
        }
        return result.toString();
    }
}
