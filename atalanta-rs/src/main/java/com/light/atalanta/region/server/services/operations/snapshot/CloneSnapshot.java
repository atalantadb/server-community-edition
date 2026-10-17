/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.operations.snapshot;

import com.google.common.base.Preconditions;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.grpc.ATALANTAProto;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.region.server.services.FileSystem;

import com.light.atalanta.region.server.services.RegionServerInfoSupplier;
import com.light.atalanta.region.server.services.ReplicaMapperProcessor;
import com.light.atalanta.region.server.vo.CoreConfiguration;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;

import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.transport.TRSConnectionStub;
import com.light.atalanta.transport.TransportConfiguration;
import com.light.atalanta.transport.TransportConnectionFactory;
import com.light.atalanta.transport.messages.RsResponse;
import com.light.atalanta.transport.messages.TableRegionActionRequest;
import com.light.atalanta.transport.messages.TransportMessage;
import com.light.atalanta.util.ProtoConverters;
import com.light.atalanta.util.ShutdownManager;
import io.grpc.stub.StreamObserver;
import org.apache.commons.io.FileUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static com.light.atalanta.ATALANTAConstants.CSV_NEWLINE;
import static com.light.atalanta.ATALANTAConstants.SNAPSHOT_ZNODE;
import static com.light.atalanta.ATALANTAConstants.ZK_SEPARATOR;
import static com.light.atalanta.services.ZooKeeperContextHelper.CREATED_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.DISABLED_REGION_STATUS;

public class CloneSnapshot {
    private static final Logger LOGGER = LoggerFactory.getLogger(CloneSnapshot.class.getName());
    public static final int CLONE_RS_WAIT_DELAY = 3_000;
    private final ZookeeperHelper zooHelper;
    private final RegionServerInfoSupplier regionServerInfoSupplier;
    private final RegionServerConfiguration regionServerConfiguration;
    private final FileSystem fileSystem;
    private final ReplicaMapperProcessor replicaMapperProcessor;
    private final ZooKeeperCoreContext zooContext;
    private final RegionServerInfoSupplier serverInfo;
    private final RegionServerModel myRegionServerModel;
    private final ZooKeeperCoreContext zooKeeperCoreContext;
    private final RegionServerConfiguration conf;

    public CloneSnapshot(ZookeeperHelper zooHelper,
                         RegionServerInfoSupplier regionServerInfoSupplier, RegionServerConfiguration regionServerConfiguration, FileSystem fileSystem,
                         ReplicaMapperProcessor replicaMapperProcessor, ZooKeeperCoreContext zooContext,
                         RegionServerInfoSupplier serverInfo, String profile,
                         ZooKeeperCoreContext zooKeeperCoreContext, RegionServerConfiguration conf) throws Exception {
        this.zooHelper = zooHelper;
        this.regionServerInfoSupplier = regionServerInfoSupplier;
        this.regionServerConfiguration = regionServerConfiguration;
        this.fileSystem = fileSystem;
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.zooContext = zooContext;
        this.serverInfo = serverInfo;
        this.zooKeeperCoreContext = zooKeeperCoreContext;
        this.conf = conf;
        myRegionServerModel = serverInfo.getMe(profile);
    }


    public void cloneSnapshot(String name,
                              ATALANTAProto.ProtoTableName protoTableName, Map<String, String> newRegionMapMap,
                              StreamObserver<RegionServerProto.RegionSnapshotOperationProgress> responseObserver) throws Exception {
        final String snapshotNs = getSnapshotNs(name);
        final String snapshotTable = getSnapshotTable(name, snapshotNs);
        List<String> regionsNameToCopy = getRegionsNameToCopy(name);
        TableName tableName = ProtoConverters.toTableName(protoTableName);
        int n = 1;
        try {
            LOGGER.debug("regions count: {}", regionsNameToCopy.size());
            Map<TableName, RegionModel> tablesRegionsNew = new HashMap<>();
            for (String regionName : regionsNameToCopy) {
                String region = regionName + " [" + regionsNameToCopy + "/" + n + "]";
                responseObserver.onNext(RegionServerProto.RegionSnapshotOperationProgress.newBuilder()
                        .setRegionName(region).setStatus("start_copy")
                        .build());
                LOGGER.debug("copy region: {}", region);
                String newRegionName = newRegionMapMap.get(regionName);
                Preconditions.checkNotNull(newRegionName, "Cannot find new region name for region " + region);
                String regNum = getRegionNum(name, regionName);
                String replicaDir = copyFile(name, regionName, regNum, protoTableName, newRegionName, snapshotNs, snapshotTable);
                String regionInfo = getRegionInfo(name, regionName, regNum);
                RegionModel regionModel = RegionModel.fromBase64String(regionInfo);
                RegionModel regionModelNew = new RegionModel(newRegionName, regionModel.getReplicaNum(),
                        regionModel.getStatusId(), regionModel.getBeginKey(), regionModel.getEndKey(),
                        regionModel.getDataBlockSize(), regionModel.getCompressType(),
                        regionModel.getRecordCompressionType(), regionModel.getDictionary(),
                        regionModel.getRecordTargetCount());
                updateZk(protoTableName, newRegionName, regNum, regionModel);
                replicaMapperProcessor.upsertNewReplica(regionModelNew, replicaDir, tableName);
                tablesRegionsNew.put(tableName, regionModelNew);
                responseObserver.onNext(RegionServerProto.RegionSnapshotOperationProgress.newBuilder().setRegionName(region)
                        .setStatus("complete complete copy region: " + region)
                        .build());
                LOGGER.debug("complete copy region: {}", region);
                n++;
            }
            LOGGER.debug("complete clone table: {}", tableName);
        } catch (ZKException ex) {
            LOGGER.error("Cannot clone table!", ex);
            throw new RuntimeException(ex);
        } finally {
        }
    }

    private void sendAssignRegionCmdToCore(TableName tableName, RegionModel regionModel) throws IOException {
        final CoreConfiguration coreConf = conf.getCoreConfiguration();
        final TransportConfiguration transportConfiguration = TransportConfiguration.defaultConfiguration()
                .set(TransportConfiguration.ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY, String.valueOf(coreConf.getSocketTimeoutMs()));
        zooContext.getRegionDistributionWithStatuses(tableName);
        String reservedReplicas = StringUtils.EMPTY;
        // If the replica is a leader, collect the reserve addresses
        if (regionModel.getReplicaNum() == 0) {
            reservedReplicas = zooContext.getReservedAddresses(zooContext.getRegionDistributionWithStatuses(tableName),
                    regionModel.getName(), conf.getMasterConfiguration().getReplicationFactor(), false);
            LOGGER.debug("Reserved replicas for {} is: {}", regionModel.toIdentityString(), reservedReplicas);
        }
        for (int i = 0; i < coreConf.getConnectionRetryTimes(); i++) {
            try (TRSConnectionStub connectionStub = TransportConnectionFactory.createConnectionStub(transportConfiguration,
                    coreConf.getHost(),
                    coreConf.getRsPort())) {
                TransportMessage regionRequest = new TableRegionActionRequest(tableName,
                        regionModel,
                        TableRegionActionRequest.Command.ASSIGN,
                        replicaMapperProcessor.getReplicaDir(regionModel),
                        reservedReplicas);
                RsResponse response = connectionStub.sendRequestToRs(regionRequest);
                if (response.isNegativeAnswer()) {
                    throw new CompletionException(
                            new Exception("Core ASSIGN operation error. Response: " + response + ", region: " + regionModel.getName())
                    );
                }
                break;
            } catch (SocketTimeoutException ex) {
                if (i == coreConf.getConnectionRetryTimes() - 1) {
                    ShutdownManager.exitWithError("Unable to receive core response", ex);
                } else {
                    long sleep = coreConf.getConnectionRetrySleepMs();
                    LOGGER.error("Unable to receive core response, sleep " + sleep + " ms and retry #" + (i + 1), ex);
                    try {
                        Thread.sleep(sleep);
                    } catch (InterruptedException interruptedException) {
                        throw new CompletionException(interruptedException);
                    }
                }
            } catch (Exception ex) {
                throw new CompletionException(new Exception("Error assigning replica " + tableName
                        + " region: " + regionModel.toIdentityString() , ex));
            }
        }
    }


    private String getSnapshotTable(String name, String snapshotNs) throws ZKException {
        final String snapshotZnode = String.join(ZK_SEPARATOR, SNAPSHOT_ZNODE, name);
        final String snapshotNsZnode = String.join(ZK_SEPARATOR, snapshotZnode, snapshotNs);
        return zooHelper.getChildren(snapshotNsZnode).get(0);
    }

    private String getSnapshotNs(String name) throws ZKException {
        final String snapshotZnode = String.join(ZK_SEPARATOR, SNAPSHOT_ZNODE, name);
        return zooHelper.getChildren(snapshotZnode).get(0);
    }

    private void startAllRegion(ATALANTAProto.ProtoTableName protoTableName) throws Exception {
        TableName tableName = ProtoConverters.toTableName(protoTableName);
        List<RegionModel> reglist =
                zooContext.getRegionServerRegions(tableName, myRegionServerModel);
        for (RegionModel rm : reglist) {
            // TODO: Start the region and assign it in the core
        }
    }


    private String copyFile(String snName, String regionName, String regNum, ATALANTAProto.ProtoTableName tableName,
                            String newRegionName, String snapshotNs, String snapshotTable)
            throws IOException, InterruptedException {
        String relPath = snName + "/" + snapshotNs + "/" + snapshotTable + "/" + regionName + "_" + regNum;
        for (String dir : regionServerConfiguration.getRsDirs()) {
            String snapshotDir = dir + "/" + SNAPSHOT_ZNODE;
            if (fileSystem.isPathExist(snapshotDir + "/" + relPath)) {
                String targetDir = dir + "/" + tableName.getNamespace() + "/" + tableName.getTable() + "/" + newRegionName + "_" + regNum;
                String mkdir = "mkdir -p " + targetDir;
                LOGGER.info("run cmd: {}", mkdir);
                int resMkdir = Runtime.getRuntime().exec(mkdir).waitFor();
                LOGGER.info("cmd {} res: {}", mkdir, resMkdir);
                final String srcDir = snapshotDir + "/" + relPath;
                FileUtils.copyDirectory(new File(srcDir), new File(targetDir));
                LOGGER.info("complete! srcDir: {}, targetDir: {}", mkdir, resMkdir);
                return targetDir;
            }
        }
        throw new RuntimeException("Unknown region dir! snName: " + snName + ", regionName: " + regionName
                + ", regNum " + regNum + ", tableName: " + tableName.getNamespace() + ":" + tableName.getTable()
                + "snapshotNs: " + snapshotNs + ", snapshotTable: " + snapshotTable );
    }


    private synchronized void updateZk(ATALANTAProto.ProtoTableName tableName, String newRegionName, String regNum, RegionModel regionInfo) throws Exception {
        TableName table = ProtoConverters.toTableName(tableName);
        final String apiLocationPath = ZooKeeperContextHelper.getRegionServerApiLocationPath(table, myRegionServerModel);
        if (!zooHelper.checkExists(apiLocationPath)) {
            zooHelper.create(apiLocationPath, myRegionServerModel.getLocationString());
        }
        String regionsPath = ZooKeeperContextHelper.getRegionServerRegionsPath(table, myRegionServerModel);
        List<CuratorOp> transactions = new ArrayList<>();
        RegionModel rm = new RegionModel(newRegionName, Integer.parseInt(regNum),
                DISABLED_REGION_STATUS, regionInfo.getBeginKey(), regionInfo.getEndKey(), regionInfo.getDataBlockSize(),
                regionInfo.getCompressType(), regionInfo.getRecordCompressionType(), regionInfo.getDictionary(),
                regionInfo.getRecordTargetCount());
        if (zooHelper.checkExists(regionsPath)) {
            String existingData = zooHelper.getNotNullString(regionsPath);
            if (StringUtils.isBlank(existingData)) {
                transactions.add(zooHelper.setDataTrans(regionsPath, rm.toBase64StringForZk()));
            } else {
                LOGGER.info("ZK before update path: {}, data: {}, new rs data: {}",
                        regionsPath, existingData, rm.toBase64StringForZk());
                transactions.add(zooHelper.setDataTrans(regionsPath,
                        existingData + CSV_NEWLINE + rm.toBase64StringForZk()));
            }
        } else {
            transactions.add(zooHelper.createTrans(regionsPath, rm.toBase64StringForZk()));
        }
        if (transactions.size() > 0) {
            LOGGER.debug("update ZK: regionPath: {}, region: {}", regionsPath, rm.toIdentityString());
            Preconditions.checkArgument(zooHelper.forOperations(transactions).size() == transactions.size(), "unexpected transaction error");
        } else {
            LOGGER.error("Zk transaction is empty. regionPath: {}, region: {}", regionsPath, rm.toIdentityString());
            throw new RuntimeException("Zk transaction is empty");
        }
    }


    private List<String> getRegionsNameToCopy(String name) throws ZKException {
        String znode = getRegionZnodeForSnapshot(name);
        LOGGER.debug("snapshot region server znode: {}", znode);
        return zooHelper.getChildren(znode);
    }

    private String getRegionNum(String name, String regionName) throws ZKException {
        final String rsZnode = getRegionZnodeForSnapshot(name);
        final String znode = String.join(ZK_SEPARATOR, rsZnode, regionName);
        return zooHelper.getChildren(znode).get(0);
    }

    private String getRegionInfo(String name, String regionName, String regNum) throws ZKException {
        final String rsZnode = getRegionZnodeForSnapshot(name);
        final String znode = String.join(ZK_SEPARATOR, rsZnode, regionName, regNum, "info");
        return zooHelper.getNotNullString(znode);
    }

    private String getRegionZnodeForSnapshot(String name) throws ZKException {
        final String snapshotZnode = String.join(ZK_SEPARATOR, SNAPSHOT_ZNODE, name);
        final String ns = getSnapshotNs(name);
        final String snapshotNsZnode = String.join(ZK_SEPARATOR, snapshotZnode, ns);
        final String table = getSnapshotTable(name, ns);
        final String snapshotTableZnode = String.join(ZK_SEPARATOR, snapshotNsZnode, table);
        return String.join(ZK_SEPARATOR, snapshotTableZnode, regionServerInfoSupplier.regionServerId());
    }


    public void stopStartRegion(TableName tableName, RegionModel regionModel, TableRegionActionRequest.Command cmd) throws Exception {
        LOGGER.info("Start stopping region '{}:{}", tableName, regionModel.toShortString());
        TransportConfiguration transportConfiguration = TransportConfiguration.defaultConfiguration().set(TransportConfiguration.ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY, String.valueOf(regionServerConfiguration.getCoreConfiguration().getSocketTimeoutMs()));
        try (TRSConnectionStub connectionStub = TransportConnectionFactory.createConnectionStub(
                transportConfiguration,
                regionServerConfiguration.getCoreConfiguration().getHost(),
                regionServerConfiguration.getCoreConfiguration().getRsPort())) {
            TransportMessage regionRequest = new TableRegionActionRequest(tableName, regionModel, cmd,
                    replicaMapperProcessor.getReplicaDir(regionModel), null);
            RsResponse response = connectionStub.sendRequestToRs(regionRequest);
            if (response.isNegativeAnswer()) {
                throw new Exception("Core STOP operation error. Response: " + response);
            }
        } catch (Exception ex) {
            throw new Exception("Error stopping region:" + tableName + ":" + regionModel.toShortString() + "'", ex);
        }
        LOGGER.info("Done stopping region '{}:{}", tableName, regionModel.toShortString());
    }

}
