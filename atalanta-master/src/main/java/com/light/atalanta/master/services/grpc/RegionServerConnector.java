/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.grpc;

import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.master.services.recovery.RecoveryRSContext;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.master.vo.GrpcConfiguration;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.RegionServerGrpcStub;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.util.ProtoConverters;
import com.light.atalanta.util.ZookeeperStatus;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.light.atalanta.ATALANTAConstants.SNAPSHOT_ZNODE;
import static com.light.atalanta.ATALANTAConstants.ZK_SEPARATOR;

/**
 * Extends {@link com.light.atalanta.services.RegionServerConnector} with master-specific gRPC queries.
 */
public class RegionServerConnector extends com.light.atalanta.services.RegionServerConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionServerConnector.class);

    private final GrpcConfiguration grpcConf;

    public RegionServerConnector(AtalantaDBConfiguration conf, RecoveryRSContext recoveryRSContext) {
        super(
                conf.getGrpcConfiguration().getRsReconnectTimeoutMs(),
                conf.getGrpcConfiguration().getRsResponseTimeoutMs(),
                conf.getGrpcConfiguration().getRsProgressCalculatorIntervalMs(),
                recoveryRSContext::isIncomeTableRequestsBlocked
        );
        this.grpcConf = conf.getGrpcConfiguration();
    }

    public boolean createSnapshot(String name, TableName tableName, RegionServerModel rs, ZookeeperHelper zooHelper) {
        LOGGER.info("==> createSnapshot (name: {}, tableName: {}, rs: {}", name, tableName, rs);
        RegionServerGrpcStub.Async stub = RegionServerGrpcStub.async(rs, grpcConf.getRsReconnectTimeoutMs());
        RegionServerProto.SnapshotRequest request = RegionServerProto.SnapshotRequest.newBuilder()
                .setName(name)
                .setTableName(ProtoConverters.toProtoTableName(tableName))
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        final String snapshotZNode = String.join(
                ZK_SEPARATOR, SNAPSHOT_ZNODE, name, tableName.getNamespace(), tableName.getTableName()
        );
        final String regionServerZNode = String.join(ZK_SEPARATOR, snapshotZNode, rs.getName());
        stub.get().createSnapshot(request, new StreamObserver<RegionServerProto.RegionSnapshotOperationProgress>() {
            @Override
            public void onNext(RegionServerProto.RegionSnapshotOperationProgress regionOperationData) {
                LOGGER.info("status: {}, region {}, name: {}, table: {}, rs: {}",
                        regionOperationData.getStatus(), regionOperationData.getRegionName(), name, tableName, rs
                );
            }

            @Override
            public void onError(Throwable throwable) {
                LOGGER.error("cannot create snapshot! name: {}, table: {}, rs: {}. error: {}",
                        name, tableName, rs,
                        throwable.getMessage());
                try {
                    zooHelper.setData(regionServerZNode, ZookeeperStatus.FAILED.name());
                } catch (ZKException e) {
                    LOGGER.error("cannot update zk info", e);
                }
                error.set(throwable);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                LOGGER.info("complete create snapshot. name: {}, table: {}, rs: {}", name, tableName, rs);
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("interrupted! snapshot name: {}, table: {}, rs: {}", name, tableName, rs);
            return false;
        }
        if (error.get() != null) {
            LOGGER.error("cannot create snapshot! name: {}", name, error.get());
            return false;
        }
        LOGGER.info("<== createSnapshot (name: {}, tableName: {}, rs: {}", name, tableName, rs);
        return true;
    }

    public boolean cloneSnapshot(String name, TableName tableName, RegionServerModel rs, Map<String, String> newRegionMap) {
        LOGGER.info("==> cloneSnapshot (name: {}, tableName: {}, rs: {}", name, tableName, rs);
        RegionServerGrpcStub.Async stub = RegionServerGrpcStub.async(rs, grpcConf.getRsReconnectTimeoutMs());
        RegionServerProto.CloneSnapshotRequest request = RegionServerProto.CloneSnapshotRequest.newBuilder().setName(name)
                .setTableName(ProtoConverters.toProtoTableName(tableName))
                .putAllNewRegionMap(newRegionMap)
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        stub.get().cloneSnapshot(request, new StreamObserver<RegionServerProto.RegionSnapshotOperationProgress>() {
            @Override
            public void onNext(RegionServerProto.RegionSnapshotOperationProgress regionOperationData) {
                LOGGER.info("status: {}, region {}, name: {}, table: {}, rs: {}",
                        regionOperationData.getStatus(), regionOperationData.getRegionName(), name, tableName, rs);
            }

            @Override
            public void onError(Throwable throwable) {
                LOGGER.error("cannot clone snapshot! name: {}, table: {}, rs: {}. error: {}",
                        name, tableName, rs, throwable.getMessage()
                );
                error.set(throwable);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                LOGGER.info("complete clone snapshot. name: {}, table: {}, rs: {}", name, tableName, rs);
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("clone interrupted! snapshot name: {}, table: {}, rs: {}",
                    name, tableName, rs);
            return false;
        }
        if (error.get() != null) {
            LOGGER.error("cannot clone snapshot! name: {}", name, error.get());
            return false;
        }
        LOGGER.info("<== cloneSnapshot (name: {}, tableName: {}, rs: {}", name, tableName, rs);
        return true;
    }

    public void copySnapshotRegion(String snapshot, TableName tableName, String region, int replicaNum, RegionServerModel srcRs, RegionServerModel targetRs) {
        LOGGER.info("==> moveSnapshotRegion (snapshot: {}, region: {}, replicaNum: {}, srcRs: {}, targetRs: {})", snapshot, region, replicaNum, srcRs, targetRs);
        RegionServerGrpcStub.Async stub = RegionServerGrpcStub.async(srcRs, grpcConf.getRsReconnectTimeoutMs());
        RegionServerProto.CopySnapshotRegionRequest request = RegionServerProto.CopySnapshotRegionRequest.newBuilder().setName(snapshot)
                .setTableName(ProtoConverters.toProtoTableName(tableName))
                .setRegion(region)
                .setTargetReplicaNum(replicaNum)
                .setTargetRegionServer(targetRs.getName())
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        stub.get().copySnapshotRegion(request, new StreamObserver<RegionServerProto.CopyRegionSnapshotOperationProgress>() {
            @Override
            public void onNext(RegionServerProto.CopyRegionSnapshotOperationProgress regionOperationData) {
                LOGGER.info("status: {}, file: {}, snapshot: {}, region: {}, replicaNum: {}, srcRs: {}, targetRs: {}",
                        regionOperationData.getStatus(), regionOperationData.getFileName(),
                        snapshot, region, replicaNum, srcRs, targetRs);
            }

            @Override
            public void onError(Throwable throwable) {

                LOGGER.error("cannot copy snapshot region! snapshot: {}, region: {}, replicaNum: {}, srcRs: {}, targetRs: {}. error: {}", snapshot, region, replicaNum, srcRs, targetRs,
                        throwable.getMessage());
                error.set(throwable);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                LOGGER.info("complete copy snapshot region. snapshot: {}, region: {}, replicaNum: {}, srcRs: {}, targetRs: {}", snapshot, region, replicaNum, srcRs, targetRs);
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("copy snapshot region interrupted! snapshot: {}, region: {}, replicaNum: {}, srcRs: {}, targetRs: {}", snapshot, region, replicaNum, srcRs, targetRs);
            return;
        }
        if (error.get() != null) {
            LOGGER.error("cannot copy snapshot region! error: {}", error);
            return;
        }
        LOGGER.info("<== moveSnapshotRegion (snapshot: {}, region: {}, replicaNum: {}, srcRs: {}, targetRs: {})",
                snapshot, region, replicaNum, srcRs, targetRs);
    }


    public boolean deleteSnapshot(String name, RegionServerModel rs) {
        LOGGER.info("==> deleteSnapshot (snapshot: {}, rs: {})", name, rs);
        RegionServerGrpcStub.Async stub = RegionServerGrpcStub.async(rs, grpcConf.getRsReconnectTimeoutMs());
        RegionServerProto.SnapshotRequest request = RegionServerProto.SnapshotRequest.newBuilder()
                .setName(name)
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        stub.get().deleteSnapshot(request, new StreamObserver<RegionServerProto.RegionSnapshotOperationProgress>() {
            @Override
            public void onNext(RegionServerProto.RegionSnapshotOperationProgress regionOperationData) {
                LOGGER.info("status: {}, region: {}, snapshot: {}, rs: {}",
                        regionOperationData.getStatus(), regionOperationData.getRegionName(),
                        name, rs);
            }

            @Override
            public void onError(Throwable throwable) {
                LOGGER.error("cannot copy snapshot region! snapshot: {}, rs: {}. error: {}", name, rs, throwable.getMessage());
                error.set(throwable);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                LOGGER.info("complete copy snapshot region. snapshot: {}, rs: {}", name, rs);
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("delete snapshot region interrupted! snapshot: {}, rs: {}", name, rs);
            return false;
        }
        if (error.get() != null) {
            LOGGER.error("cannot delete snapshot! name: {}", name, error.get());
            return false;
        }
        LOGGER.info("<== deleteSnapshot (snapshot: {}, rs: {}", name, rs);
        return true;
    }
}
