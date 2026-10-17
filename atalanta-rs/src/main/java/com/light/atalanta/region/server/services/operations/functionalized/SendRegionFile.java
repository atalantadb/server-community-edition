/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.operations.functionalized;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.light.atalanta.TableName;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.model.RegionFileRequest;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.region.server.services.ReplicaMapperProcessor;
import com.light.atalanta.region.server.services.grpc.RegionTaskFunctionalized;
import com.light.atalanta.region.server.services.operations.RegionOperation;
import com.light.atalanta.region.server.services.tcp.TcpFileTransfer;
import com.light.atalanta.region.server.vo.GrpcConfiguration;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SendRegionFile implements RegionOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(SendRegionFile.class);

    private final ReplicaMapperProcessor replicaMapperProcessor;
    private final GrpcConfiguration grpcConf;

    public SendRegionFile(ReplicaMapperProcessor replicaMapperProcessor, RegionServerConfiguration conf) {
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.grpcConf = conf.getGrpcConfiguration();
    }

    @Override
    public void functionalizedAction(TableName tableName, RegionModel regionModel, RegionTaskFunctionalized task, String reservedReplicas) throws Exception {
        Preconditions.checkArgument(regionModel instanceof RegionFileRequest, "region file request has no required information");

        try {
            RegionFileRequest regionFileRequest = (RegionFileRequest) regionModel;
            LOGGER.info("Start sending region file '{}:{}' fileName: {}, sizeMb: {}, to '{}'",
                    tableName, regionModel.toShortString(), regionFileRequest.getFileName(),
                    regionFileRequest.getFileSize(), reservedReplicas
            );

            logStatus(tableName, regionModel, "initializing region file transfer");
            String regionDirectoryPath = replicaMapperProcessor.getReplicaDir(regionModel);
            File file = new File(regionDirectoryPath, regionFileRequest.getFileName());
            Preconditions.checkArgument(file.isFile(), "region file does not exist: " + file);
            Preconditions.checkArgument(file.length() == regionFileRequest.getFileSize(), "region file size changed before transfer: expected " + regionFileRequest.getFileSize() + ", actual " + file.length()
            );

            new Thread(() -> TcpFileTransfer.sendSingleFile(
                    grpcConf, tableName, regionModel, regionFileRequest.getReceiverIp(), regionFileRequest.getTcpPort(),
                    regionDirectoryPath, regionFileRequest.getFileName(), regionFileRequest.getFileSize()
            )).start();

            logStatus(tableName, regionModel, "done initializing region file transfer");

            task.sendResponse(
                    RegionServerProto.RegionDirectoryStructure.getDefaultInstance()
            );

        } catch (RegionTaskFunctionalized.GrpcConnectException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new Exception(String.format("Error sending region batch '%s:%s' to '%s'", tableName, regionModel.toShortString(), reservedReplicas), ex);
        }

        LOGGER.info("Done sending region batch '{}:{}' to '{}'", tableName, regionModel.toShortString(), reservedReplicas);
    }

    @Override
    public void progressedAction(TableName tableName, RegionModel regionModel, AtomicReference<String> taskProgress, AtomicBoolean interrupted, String reservedReplicas) throws Exception {
        throw new NotImplementedException("Unable to run functionalized task as progressed");
    }

    private void logStatus(TableName tableName, RegionModel regionModel, String text) {
        LOGGER.info("Sending region batch '{}:{}': {}", tableName, regionModel.toShortString(), text);
    }
}
