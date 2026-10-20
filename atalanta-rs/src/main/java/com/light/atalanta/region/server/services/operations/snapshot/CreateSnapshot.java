/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.operations.snapshot;


import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.grpc.ATALANTAProto;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.region.server.services.RegionServerInfoSupplier;
import com.light.atalanta.region.server.services.ReplicaMapperProcessor;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.transport.TRSConnectionStub;
import com.light.atalanta.transport.TransportConfiguration;
import com.light.atalanta.transport.TransportConnectionFactory;
import com.light.atalanta.transport.messages.RsResponse;
import com.light.atalanta.transport.messages.TableRegionActionRequest;
import com.light.atalanta.transport.messages.TransportMessage;
import com.light.atalanta.util.ProtoConverters;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.SNAPSHOT_ZNODE;
import static com.light.atalanta.ATALANTAConstants.ZK_SEPARATOR;

public class CreateSnapshot {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateSnapshot.class.getName());
    private final ZookeeperHelper zooHelper;
    private final RegionServerInfoSupplier regionServerInfoSupplier;
    private final RegionServerConfiguration regionServerConfiguration;
    private final FileSystem fileSystem;
    private final ReplicaMapperProcessor replicaMapperProcessor;
    private final ZooKeeperCoreContext zooContext;
    private final RegionServerInfoSupplier serverInfo;
    private final RegionServerModel myRegionServerModel;


    public CreateSnapshot(ZookeeperHelper zooHelper,
                          RegionServerInfoSupplier regionServerInfoSupplier, RegionServerConfiguration regionServerConfiguration, FileSystem fileSystem,
                          ReplicaMapperProcessor replicaMapperProcessor, ZooKeeperCoreContext zooContext,
                          RegionServerInfoSupplier serverInfo, String profile) throws Exception {
        this.zooHelper = zooHelper;
        this.regionServerInfoSupplier = regionServerInfoSupplier;
        this.regionServerConfiguration = regionServerConfiguration;
        this.fileSystem = fileSystem;
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.zooContext = zooContext;
        this.serverInfo = serverInfo;
        myRegionServerModel = serverInfo.getMe(profile);
    }

    public void createSnapshot(String name,
                               ATALANTAProto.ProtoTableName tableName,
                               StreamObserver<RegionServerProto.RegionSnapshotOperationProgress> responseObserver) throws Exception {
        List<String> regionsNameToCopy = getRegionsNameToCopy(name, tableName);
        int n = 1;
        try {
            stopAllRegion(tableName);
            for (String regionName : regionsNameToCopy) {
                String region = regionName + " [" + regionsNameToCopy + "/" + n + "]";
                responseObserver.onNext(RegionServerProto.RegionSnapshotOperationProgress.newBuilder()
                        .setRegionName(region)
                        .setStatus("start_copy")
                        .build());
                copyFile(regionName, name, tableName);
                responseObserver.onNext(RegionServerProto.RegionSnapshotOperationProgress.newBuilder()
                        .setRegionName(region)
                        .setStatus("complete")
                        .build());
                n++;
            }
        } finally {
            startAllRegion(tableName);
        }
    }


    private void startAllRegion(ATALANTAProto.ProtoTableName protoTableName) throws Exception {
        TableName tableName = ProtoConverters.toTableName(protoTableName);
        List<RegionModel> reglist = zooContext.getRegionServerRegions(tableName, myRegionServerModel);
        for (RegionModel rm : reglist) {
            // TODO: Start the region
        }
    }

    private void stopAllRegion(ATALANTAProto.ProtoTableName protoTableName) throws Exception {
        TableName tableName = ProtoConverters.toTableName(protoTableName);
        List<RegionModel> reglist = zooContext.getRegionServerRegions(tableName, myRegionServerModel);
        for (RegionModel rm : reglist) {
            // TODO: Stop the region
        }
    }

    private void copyFile(String fileName, String name, ATALANTAProto.ProtoTableName tableName) throws IOException, InterruptedException {
        String relDir = tableName.getNamespace() + "/" + tableName.getTable();
        final String relPath = relDir + "/" + fileName;
        for (String dir : regionServerConfiguration.getRsDirs()) {
            final String snapshotDir = dir + "/" + SNAPSHOT_ZNODE;
            final String regionDir = dir + "/" + relPath;
            fileSystem.makeSureDirExists(snapshotDir);
            if (fileSystem.isPathExist(regionDir)) {
                final String commandMkDir = "mkdir -p " + snapshotDir + "/" + name + "/" + relDir;
                LOGGER.info("run cmd: {}", commandMkDir);
                int resMkdir = Runtime.getRuntime().exec(commandMkDir).waitFor();
                LOGGER.info("cmd {} res: {}", commandMkDir, resMkdir);
                final String commandCp = "cp -r " + regionDir + " " + snapshotDir + "/" + name + "/" + relPath;
                LOGGER.info("run cmd: {}", commandCp);
                int resCp = Runtime.getRuntime().exec(commandCp).waitFor();
                LOGGER.info("cmd {} res: {}", commandCp, resCp);
                break;
            }
        }
    }


    private List<String> getRegionsNameToCopy(String name, ATALANTAProto.ProtoTableName tableName) throws ZKException {
        String znode = String.join(ZK_SEPARATOR, SNAPSHOT_ZNODE, name, tableName.getNamespace(), tableName.getTable(),
                regionServerInfoSupplier.regionServerId());
        return zooHelper.getChildren(znode).stream().flatMap(regionName -> {
            try {
                return zooHelper.getChildren(znode + ZK_SEPARATOR + regionName).stream().map(n -> regionName + "_" + n);
            } catch (ZKException e) {
                new RuntimeException(e);
                return null;
            }
        }).collect(Collectors.toList());
    }

    public void stopStartRegion(TableName tableName, RegionModel regionModel, TableRegionActionRequest.Command cmd) throws Exception {
        LOGGER.info("Start/stopping region '{}' region: {}, cmd: {}", tableName, regionModel.toShortString(), cmd);
        TransportConfiguration transportConfiguration = TransportConfiguration.defaultConfiguration().set(TransportConfiguration.ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY, String.valueOf(regionServerConfiguration.getCoreConfiguration().getSocketTimeoutMs()));
        try (TRSConnectionStub connectionStub = TransportConnectionFactory.createConnectionStub(
                transportConfiguration,
                regionServerConfiguration.getCoreConfiguration().getHost(),
                regionServerConfiguration.getCoreConfiguration().getRsPort())) {
            TransportMessage regionRequest = new TableRegionActionRequest(
                    tableName, regionModel, cmd,
                    replicaMapperProcessor.getReplicaDir(regionModel), null);
            RsResponse response = connectionStub.sendRequestToRs(regionRequest);
            if (response.isNegativeAnswer()) {
                throw new Exception("Core" + cmd.name() + " operation error. Response: " + response);
            }
        } catch (Exception ex) {
            throw new Exception("Error start/stop region: "
                    + tableName + "'" + regionModel.toShortString() + "'", ex);
        }
        LOGGER.info("Done start/stopping region '{}' region: {}, cmd: {}", tableName, regionModel.toShortString(), cmd);
    }
}
