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
import com.light.atalanta.services.RegionServerConnector;
import com.light.atalanta.region.server.services.grpc.RegionTaskFunctionalized;
import com.light.atalanta.region.server.services.operations.RegionOperation;
import com.light.atalanta.region.server.vo.GrpcConfiguration;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
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
import java.nio.channels.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class RecoverRegion implements RegionOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecoverRegion.class.getName());

    private static final String DETAILS_SPLITTER = "@";

    public static final String TEMP_REGION_BATCH_POSTFIX = "_temp";

    private final GrpcConfiguration grpcConf;

    private final RegionServerConnector regionServerConnector;
    private final ReplicaMapperProcessor replicaMapperProcessor;
    private final DistributeRegion distributeRegion;
    private final FileSystem fileSystem;

    public RecoverRegion(RegionServerConfiguration conf, RegionServerConnector regionServerConnector, ReplicaMapperProcessor replicaMapperProcessor, DistributeRegion distributeRegion, FileSystem fileSystem) {
        this.regionServerConnector = regionServerConnector;
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.distributeRegion = distributeRegion;
        this.fileSystem = fileSystem;
        this.grpcConf = conf.getGrpcConfiguration();
    }

    @Override
    public void progressedAction(TableName tableName, RegionModel recoverRegion, AtomicReference<String> taskProgress, AtomicBoolean interrupted, String details) throws Exception {
        LOGGER.info("Start recovering region '{}:{}' from '{}'", tableName, recoverRegion.toShortString(), details);

        String referenceRegionServer = "unknown";
        try {
            // The details string has the format sourceRegionServer@sourceReplicaNum@reservedReplicasString@lostRegionStatus.
            // sourceRegionServer: the source RS to get the data from
            // sourceReplicaNum: number of the reference replica
            // reservedReplicasString: the reserve replica string when a leader is moved. When a reserve is moved, it is empty.
            // needToAssign: whether the region must be assigned.
            // forceRecovery: whether the region must be recovered in any case
            Preconditions.checkArgument(details.contains(DETAILS_SPLITTER)
                            && details.split(DETAILS_SPLITTER).length == 5
                            && NumberUtils.isCreatable(details.split(DETAILS_SPLITTER)[1])
                            && isCreatableBoolean(details.split(DETAILS_SPLITTER)[3])
                            && isCreatableBoolean(details.split(DETAILS_SPLITTER)[4]),
                    "Invalid details string received from master: " + details);
            String[] detailsParts = details.split(DETAILS_SPLITTER);
            referenceRegionServer = detailsParts[0];
            int referenceReplicaNum = Integer.parseInt(detailsParts[1]);
            String reservedReplicas = detailsParts[2].equals("empty") ? StringUtils.EMPTY : detailsParts[2];
            boolean needToAssign = Boolean.parseBoolean(detailsParts[3]);
            boolean forceRecovery = Boolean.parseBoolean(detailsParts[4]);

            // Respond with success right away if:
            // - we already own the region
            // - forceRecovery = false (see the SYNC mode on the master)
            if (replicaMapperProcessor.contains(recoverRegion) && !forceRecovery) {
                taskProgress.set("region already recovered");
                LOGGER.info("Done recovering region '{}:{}': already recovered", tableName, recoverRegion.toShortString());
                return;
            }

            RegionDistributionModel referenceRegion = makeReferenceRdm(recoverRegion, referenceReplicaNum, referenceRegionServer);

            // Get the region file list
            updateTaskProgress(taskProgress, tableName, recoverRegion, "receiving region file list from source");
            String regionListLogStr = String.format("Recovering region '%s:%s': receiving file list from '%s'", tableName, recoverRegion.toShortString(), referenceRegion.toShortString());
            final RegionServerProto.RegionRequest request = ProtoConverters.toProtoRegion(tableName, referenceRegion.getRegionModel(), grpcConf.myName());
            List<RegionServerProto.RegionFile> regionFileList = regionServerConnector.result(
                    referenceRegion.getRegionServerModel(), regionListLogStr, stub -> stub.get().listRegionFiles(request)
            ).getFilesList();
            LOGGER.debug("Recovering region '{}:{}': {}", tableName, recoverRegion.toShortString(), "region file list is: " + listToString(regionFileList));

            // Pick the folder for the region
            updateTaskProgress(taskProgress, tableName, recoverRegion, "choosing data directory");
            String replicaDir = distributeRegion.chooseReplicaDir(tableName, recoverRegion);

            // If the replica is not in the mapping, clean up its data and temp directory, if any
            fileSystem.makeSureDirRemoved(replicaDir);
            fileSystem.makeSureDirRemoved(replicaDir + TEMP_REGION_BATCH_POSTFIX);

            // Prepare the folder
            String tempRegionDir = replicaDir + TEMP_REGION_BATCH_POSTFIX;
            fileSystem.makeSureDirExists(tempRegionDir);
            recoverRegion.writeRegionInfoFile(tempRegionDir);

            // Get the region data and write it to the temp directory
            for (RegionServerProto.RegionFile regionFile : regionFileList) {
                updateTaskProgress(taskProgress, tableName, recoverRegion, "processing file=" + regionFile.getFileName() + ", sizeMb=" + regionFile.getFileSize() / 1024 / 1024);

                try (ServerSocketChannel serverChannel = ServerSocketChannel.open()) {
                    // NIO channels don't support connection timeouts out-of-box;
                    // therefore, we need to get classic ServerSocket from the channel first
                    ServerSocket serverSocket = serverChannel.socket();
                    serverSocket.setSoTimeout(grpcConf.getRegionServerConnectTimeoutMs());
                    serverSocket.bind(new InetSocketAddress(0));

                    int ephemeralPort = serverChannel.socket().getLocalPort();
                    LOGGER.info("Recovering region '{}:{}': listening on port: {}", tableName, recoverRegion.toShortString(), ephemeralPort);

                    // Make gRPC call to request this specific file
                    RegionFileRequest regionFileRequest = new RegionFileRequest(
                            referenceRegion.getRegionModel(),
                            regionFile.getFileName(),
                            regionFile.getFileSize(),
                            grpcConf.myName().substring(0, grpcConf.myName().indexOf(":")),
                            ephemeralPort
                    );
                    RegionServerProto.RegionRequest dataRequest = ProtoConverters.toProtoRegion(
                            tableName, regionFileRequest,  grpcConf.myName()
                    );
                    String receiveRegionStr = String.format("Recovering region '%s:%s': requesting region file %s from '%s'", tableName, recoverRegion.toShortString(), regionFile.getFileName(), referenceRegion.toShortString());
                    Preconditions.checkArgument(regionServerConnector.result(
                            referenceRegion.getRegionServerModel(), receiveRegionStr, stub -> stub.get().sendRegionFile(dataRequest)
                    ).getFilesList().isEmpty(), "unexpected source RS response");
                    updateTaskProgress(taskProgress, tableName, recoverRegion, "waiting for region file=" + regionFile.getFileName() + ", sizeMb=" + regionFile.getFileSize() / 1024 / 1024);

                    // Wait for the sender to connect
                    try (SocketChannel clientChannel = serverSocket.accept().getChannel()) {
                        LOGGER.info("Recovering region '{}:{}': sender connected from : {}", tableName, recoverRegion.toShortString(), clientChannel.getRemoteAddress());
                        // Receive the file
                        long bytesReceived = TcpFileTransfer.receiveSingleFile(grpcConf, clientChannel, regionFile, tempRegionDir);
                        long actualSize = fileSystem.calculateFileSize(new File(tempRegionDir, regionFile.getFileName()).getAbsolutePath());
                        Preconditions.checkArgument(actualSize == bytesReceived, "different files size. please, check the logs");
                        Preconditions.checkArgument(regionFile.getFileSize() == bytesReceived, "different files size. please, check the logs");
                    }
                }
            }

            if (regionFileList.isEmpty()) {
                LOGGER.info("Recovering region '{}:{}': creating new dir", tableName, recoverRegion.toShortString());
                Preconditions.checkArgument(new File(replicaDir + TEMP_REGION_BATCH_POSTFIX).mkdirs(), "Unable to create new dir");
            }

            // Once all data is received, turn the temp directory into the real one
            boolean renameResult = fileSystem.renameDir(replicaDir + TEMP_REGION_BATCH_POSTFIX, replicaDir);
            Preconditions.checkArgument(renameResult,  "Unable to rename temp replica directory");

            if (needToAssign) {
                // If needed, register the new region with its data folder
                updateTaskProgress(taskProgress, tableName, recoverRegion, "need to assign, distributing target region");
                distributeRegion.distributeRegionInternal(tableName, recoverRegion, new AtomicReference<>(), reservedReplicas, replicaDir, true);
            } else {
                // Otherwise, just put the path into the internal structure
                updateTaskProgress(taskProgress, tableName, recoverRegion, "no need to assign, just updating regions mapping");
                // Add the replica to the mapping (save the path it was distributed to)
                String previousDir = replicaMapperProcessor.upsertNewReplica(recoverRegion, replicaDir, tableName);
                // If the path was already in the mapping, something may have gone wrong. Log a warning
                // This is expected if forceRecovery = true, i.e. in SYNC mode
                if (previousDir != null && !forceRecovery) {
                    LOGGER.warn("Recovering region '{}:{}' region already exists in mapping. Previous dir: {}", tableName, recoverRegion.toShortString(), previousDir);
                    // TODO: Decide whether a warning is enough
                    throw new Exception("Replica path already exists in mapping. See the logs for details");
                }
            }

            updateTaskProgress(taskProgress, tableName, recoverRegion, "region recovered successfully");
        } catch (Throwable ex) {
            throw new Exception(String.format("Error recovering region '%s:%s' from '%s'", tableName, recoverRegion.toShortString(), referenceRegionServer), ex);
        }

        LOGGER.info("Done recovering region '{}:{}' from '{}'", tableName, recoverRegion.toShortString(), referenceRegionServer);
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

    @Override
    public void functionalizedAction(TableName tableName, RegionModel regionModel, RegionTaskFunctionalized task, String reservedReplicas) throws Exception {
        throw new NotImplementedException("Unable to run progressed task as functionalized");
    }

    private RegionDistributionModel makeReferenceRdm(RegionModel recoveryRegion, int referenceReplicaNum, String referenceRegionServerLocation) {
        RegionModel sourceRegion = new RegionModel(recoveryRegion.getName(),
                referenceReplicaNum,
                recoveryRegion.getStatusId(),
                recoveryRegion.getBeginKey(),
                recoveryRegion.getEndKey(),
                recoveryRegion.getDataBlockSize(),
                recoveryRegion.getCompressType(),
                recoveryRegion.getRecordCompressionType(),
                recoveryRegion.getDictionary(),
                recoveryRegion.getRecordTargetCount());
        Preconditions.checkArgument(referenceRegionServerLocation.contains(":")
                && referenceRegionServerLocation.split(":").length == 2
                && NumberUtils.isCreatable(referenceRegionServerLocation.split(":")[1]),
                "Region server location is damaged");
        String host = referenceRegionServerLocation.split(":")[0];
        int javaPort = Integer.parseInt(referenceRegionServerLocation.split(":")[1]);
        return new RegionDistributionModel(sourceRegion, new RegionServerModel("sourceRegionServer", host, javaPort, 1, 0));
    }

    private void updateTaskProgress(AtomicReference<String> taskProgress, TableName tableName, RegionModel regionModel, String text) {
        taskProgress.set(text);
        LOGGER.info("Recovering region '{}:{}': {}", tableName, regionModel.toShortString(), text);
    }

    private boolean isCreatableBoolean(String booleanStr) {
        return booleanStr.equals("false") || booleanStr.equals("true");
    }
}
