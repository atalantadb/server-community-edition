/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.grpc;

import com.google.common.base.Preconditions;
import com.google.protobuf.*;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.FMasterRuntimeException;
import com.light.atalanta.exceptions.NoRetryException;
import com.light.atalanta.grpc.ATALANTAProto;
import com.light.atalanta.grpc.MasterProto;
import com.light.atalanta.grpc.MasterServiceGrpc;
import com.light.atalanta.master.config.MasterConfig;
import com.light.atalanta.master.exceptions.NoDiskFreeSpaceForSplitException;
import com.light.atalanta.master.services.Master;
import com.light.atalanta.master.services.UniqueNameThreadFactory;
import com.light.atalanta.master.services.leader.election.LeaderStateManager;
import com.light.atalanta.master.services.operations.OperationException;
import com.light.atalanta.master.services.rebalance.RegionRebalanceModel;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.model.CompressType;
import com.light.atalanta.model.RecordCompressionType;
import com.light.atalanta.util.ProtoConverters;
import io.grpc.stub.StreamObserver;
import io.grpc.Context;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.light.atalanta.master.services.grpc.MasterRequestType.*;
import static com.light.atalanta.master.services.leader.election.impl.LeaderStateManagerImpl.ER_EXECUTOR_TERMINATION_GRPC;
import static com.light.atalanta.util.ProtoConverters.recursivelyGetMessage;

/**
 * gRPC service that handles commands. Forwards requests to {@link Master}.
 */
public class MasterServiceGrpcImpl extends MasterServiceGrpc.MasterServiceImplBase implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterServiceGrpcImpl.class);
    public static final int MAX_CUR_ACTIVE_INFO_SIZE = 9;
    public static final String MORE_OBJECT = "...";
    public static final int MAX_JOIN_STR = 2048;

    private final Master master;

    private final LeaderStateManager leaderStateManager;

    private final AtalantaDBConfiguration fastDBConfiguration;

    private final Map<MasterRequestType, AtomicLong> activeRequestCount = new ConcurrentHashMap<>();
    private final Map<MasterRequestType, AtomicLong> totalRequestCount = new ConcurrentHashMap<>();
    private final Map<MasterRequestType, AtomicLong> minRequestTime = new ConcurrentHashMap<>();
    private final Map<MasterRequestType, AtomicLong> maxRequestTime = new ConcurrentHashMap<>();
    private final Map<MasterRequestType, AtomicLong> errorRequestCounter = new ConcurrentHashMap<>();

    private final Map<MasterRequestType, Map<String, Object>> requestMsg = new ConcurrentHashMap<>();

    private final ScheduledExecutorService requestsMonitoring;

    private final MasterInterceptor masterInterceptor;
    private final MasterShutdown masterShutdown;
    private final Map<String, MasterRequestType> activeRequestsMap = new ConcurrentHashMap<>();

    private final HashMap<String, Object> defaultInfoValue = new HashMap<>();

    public MasterServiceGrpcImpl(Master master, LeaderStateManager leaderStateManager, AtalantaDBConfiguration fastDBConfiguration, MasterInterceptor masterInterceptor, MasterShutdown masterShutdown) {
        this.master = master;
        this.leaderStateManager = leaderStateManager;
        this.fastDBConfiguration = fastDBConfiguration;
        this.masterInterceptor = masterInterceptor;
        this.masterShutdown = masterShutdown;
        this.requestsMonitoring = Executors.newSingleThreadScheduledExecutor(new UniqueNameThreadFactory("atalanta-grpc-stats-pool"));
        if (fastDBConfiguration.getDataConfiguration().getOperationsMonitoringEnabled()) {
            startOperationsMonitoring();
        }
        Arrays.stream(MasterRequestType.values()).forEach(v -> activeRequestCount.put(v, new AtomicLong()));
        defaultInfoValue.put("No request", "");
    }

    private void startOperationsMonitoring() {
        requestsMonitoring.scheduleAtFixedRate(() ->
                LOGGER.info("gRPC active requests count: [{}], reqIds: {}", getActiveRequestCount(), getRequestsId()), 1000, 1000, TimeUnit.MILLISECONDS);
    }

    private String getRequestsId() {
        return String.join(",", activeRequestsMap.keySet());
    }

    @Override
    public void createNamespace(MasterProto.CreateNamespaceRequest request, StreamObserver<Empty> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = createNamespace;
        String objName = request.getNamespace();
        final String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        try {

            ATALANTAProto.ProtoTableName tableName = ATALANTAProto.ProtoTableName.newBuilder()
                    .setTable(StringUtils.EMPTY)
                    .setNamespace(request.getNamespace())
                    .build();

            master.createNamespace(request.getNamespace());
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            addSuccessInfo(type, objName, startTime, info);
        } catch (Throwable ex) {
            // The operation exception is logged by the method that does the work
            // Errors of other types must be logged here
            if (!(ex instanceof OperationException || ex instanceof NoRetryException)) {
                LOGGER.error("Error creating namespace '{}'", request.getNamespace(), ex);
            }
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    @Override
    public void createTable(MasterProto.CreateTableRequest request, StreamObserver<Empty> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = createTable;
        String objName = request.getTableName().getNamespace() + ":" + request.getTableName().getTable();
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        try {
            checkSplitKey(request.getSplitKeysList());
            TableName tableName = TableName.valueOf(request.getTableName().getNamespace(), request.getTableName().getTable());
            CompressType compressType = StringUtils.isNotEmpty(request.getCompressType()) ?
                    CompressType.valueOf(request.getCompressType()) : CompressType.none;
            RecordCompressionType recordCompressionType = StringUtils.isNotEmpty(request.getRecordCompressionType()) ?
                    RecordCompressionType.valueOf(request.getRecordCompressionType()) : RecordCompressionType.lz;
            Boolean dictionary =  StringUtils.isNotEmpty(request.getDictionary()) ?
                    Boolean.valueOf(request.getDictionary()) : false;
            String recordTargetCountStr = request.getRecordTargetCount();
            long recordTargetCount = StringUtils.isNotEmpty(recordTargetCountStr) && !recordTargetCountStr.equals("null") ?
                    Long.valueOf(recordTargetCountStr) : 0;
            master.createTable(tableName,
                    ProtoConverters.protoSplitKeysToBytes(request.getSplitKeysList()),
                    request.getDataBlockSize(), compressType, recordCompressionType,
                    dictionary, recordTargetCount);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            addSuccessInfo(type, objName, startTime, info);
        } catch (Throwable ex) {
            // The operation exception is logged by the method that does the work
            // Errors of other types must be logged here
            if (!(ex instanceof OperationException || ex instanceof NoRetryException)) {
                LOGGER.error("Error creating table '{}:{}'", request.getTableName().getNamespace(), request.getTableName().getTable(), ex);
            }
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    private void checkSplitKey(List<ByteString> splitKeysList) {
        if (splitKeysList.stream()
                .filter(bs -> bs.isEmpty())
                .findAny()
                .isPresent() ) {
            throw new IllegalArgumentException("Empty split key must not be passed in the split keys.");
        }
        if (splitKeysList.stream().distinct().count() != splitKeysList.size()) {
            throw new IllegalArgumentException("All split keys must be unique.");
        }
    }

    public void modifyTable(com.light.atalanta.grpc.MasterProto.CreateTableRequest request,
                            io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
        LOGGER.info("Start modify table {}:{}", request.getTableName().getNamespace(), request.getTableName().getNamespace());
        Long startTime = getStartTime();
        MasterRequestType type = modifyTable;
        String objName = request.getTableName().getNamespace() + ":" + request.getTableName().getTable();
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        try {
            TableName tableName = TableName.valueOf(request.getTableName().getNamespace(),
                    request.getTableName().getTable());
            CompressType compressType = StringUtils.isNotEmpty(request.getCompressType()) ?
                    CompressType.valueOf(request.getCompressType()) : null;
            RecordCompressionType recordCompressionType = StringUtils.isNotEmpty(request.getRecordCompressionType()) ?
                    RecordCompressionType.valueOf(request.getRecordCompressionType()) :null;
            Boolean dictionary =  StringUtils.isNotEmpty(request.getDictionary()) ?
                    Boolean.valueOf(request.getDictionary()) : null;
            String recordTargetCountStr = request.getRecordTargetCount();
            Long recordTargetCount = StringUtils.isNotEmpty(recordTargetCountStr) && !recordTargetCountStr.equals("null") ?
                    Long.valueOf(recordTargetCountStr) : null;
            master.modifyTable(tableName, compressType, recordCompressionType, recordTargetCount, dictionary);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            addSuccessInfo(type, objName, startTime, info);
            LOGGER.info("Complete modify table {}:{}", request.getTableName().getNamespace(), request.getTableName().getNamespace());
        } catch (Throwable ex) {
            LOGGER.error("Modify table error!", ex);
            LOGGER.error("Error modify table '{}:{}'", request.getTableName().getNamespace(), request.getTableName().getTable(), ex);
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    @Override
    public void enableTable(MasterProto.EnableTableRequest request, StreamObserver<Empty> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = enableTable;
        String objName = request.getTableName().getNamespace() + ":" + request.getTableName().getTable();
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        try {
            master.enableTable(TableName.valueOf(request.getTableName().getNamespace(), request.getTableName().getTable()), null);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            addSuccessInfo(type, objName, startTime, info);
        } catch (Throwable ex) {
            // The operation exception is logged by the method that does the work
            // Errors of other types must be logged here
            if (!(ex instanceof OperationException || ex instanceof NoRetryException)) {
                LOGGER.error("Error enabling table '{}:{}'", request.getTableName().getNamespace(), request.getTableName().getTable(), ex);
            }
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    @Override
    public void disableTable(MasterProto.DisableTableRequest request, StreamObserver<Empty> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = disableTable;
        String objName = request.getTableName().getNamespace() + ":" + request.getTableName().getTable();
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        try {
            master.disableTable(TableName.valueOf(request.getTableName().getNamespace(), request.getTableName().getTable()), null);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            addSuccessInfo(type, objName, startTime, info);
        } catch (Throwable ex) {
            // The operation exception is logged by the method that does the work
            // Errors of other types must be logged here
            if (!(ex instanceof OperationException || ex instanceof NoRetryException)) {
                LOGGER.error("Error disabling table '{}:{}'", request.getTableName().getNamespace(), request.getTableName().getTable(), ex);
            }
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    @Override
    public void dropTable(MasterProto.DropTableRequest request, StreamObserver<Empty> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = dropTable;
        String objName = request.getTableName().getNamespace() + ":" + request.getTableName().getTable();
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        try {
            master.dropTable(TableName.valueOf(request.getTableName().getNamespace(), request.getTableName().getTable()), null);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            addSuccessInfo(type, objName, startTime, info);
        } catch (Throwable ex) {
            // The operation exception is logged by the method that does the work
            // Errors of other types must be logged here
            if (!(ex instanceof OperationException || ex instanceof NoRetryException)) {
                LOGGER.error("Error dropping table '{}:{}'", request.getTableName().getNamespace(), request.getTableName().getTable(), ex);
            }
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    @Override
    public void moveRegion(MasterProto.BulkMoveRegionRequest request, StreamObserver<Empty> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = moveRegion;
        String regions = joiningFixSize(request.getMoveRequestList().stream()
                .map(MasterProto.MoveRegionRequest::getRegionServerName)
                .collect(Collectors.joining(",")));
        String objName = request.getTableName().getNamespace() + ":" + request.getTableName().getTable() + regions;
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        try {
            TableName tableName = TableName.valueOf(request.getTableName().getNamespace(), request.getTableName().getTable());
            List<RegionRebalanceModel> moveModels = new ArrayList<>();
            for (MasterProto.MoveRegionRequest moveRequest : request.getMoveRequestList()) {
                moveModels.add(new RegionRebalanceModel(
                        moveRequest.getRegionIdentity(),
                        "unknown",
                        moveRequest.getRegionServerName())
                );
            }
            master.moveRegion(tableName, moveModels, null);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            addSuccessInfo(type, objName, startTime, info);
        } catch (Throwable ex) {
            // The operation exception is logged by the method that does the work
            // Errors of other types must be logged here
            if (!(ex instanceof OperationException || ex instanceof NoRetryException)) {
                LOGGER.error("Error moving regions for table '{}:{}': {}",
                        request.getTableName().getNamespace(),
                        request.getTableName().getTable(),
                        request.getMoveRequestList().toString()
                                .replace("\n", " ")
                                .replace("\r\n", ""), ex
                );
            }
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    @Override
    public void splitRegion(MasterProto.SplitRegionRequest request, StreamObserver<Empty> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = splitRegion;
        String regions = joiningFixSize(String.join(",", request.getRegionNamesToSplitList()));
        String objName = request.getTableName().getNamespace() + ":" + request.getTableName().getTable() + regions;
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        try {

            // Do not inherit request cancellation and deadline from the client

            final TableName table = TableName.valueOf(request.getTableName().getNamespace(), request.getTableName().getTable());
            if (master.checkDiskSizeForSplit(table, request.getRegionNamesToSplitList())) {
                master.splitTable(table, request.getRegionNamesToSplitList(), null);
            } else {
                throw new NoDiskFreeSpaceForSplitException(request.getRegionNamesToSplitList().get(0));
            }

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            addSuccessInfo(type, objName, startTime, info);
        } catch (Throwable ex) {
            LOGGER.error("Error splitting regions '{}:{}:{}'", request.getTableName().getNamespace(), request.getTableName().getTable(), request.getRegionNamesToSplitList(), ex);
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    private static String joiningFixSize(String regionNamesToSplitList) {
        String regions = regionNamesToSplitList;
        if (regions.length() > MAX_JOIN_STR) {
            regions = regions.substring(0, MAX_JOIN_STR) + "...";
        }
        return regions;
    }

    @Override
    public void startReplicasRebalanceAsync(MasterProto.StartRebalanceRequest request, StreamObserver<Empty> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = startReplicasRebalanceAsync;
        String objName = request.getEntity();
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        List<TableName> tablesToRebalance;
        try {
            try {
                tablesToRebalance = master.getTableNamesToRebalance(request.getEntity(), true);
                info.put("Last tables in rebalancing",
                        joiningFixSize(tablesToRebalance.stream().map(TableName::toString).collect(Collectors.joining())));
                Preconditions.checkArgument(!tablesToRebalance.isEmpty(), "unable to find any tables matching criteria to rebalancing");
                LOGGER.info("New replicas rebalance request. rebalanceEntity: {}, tableNames: {}", request.getEntity(), tablesToRebalance);
                for (TableName tableName : tablesToRebalance) {
                }
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
            } catch (Exception ex) {
                LOGGER.error("Error starting replicas rebalancing for entity '{}'", request.getEntity(), ex);
                addErrorInfo(type, objName, info, ex);
                responseObserver.onError(ProtoConverters.toProtoException(ex));
                return;
            }
            master.rebalanceTableReplicas(tablesToRebalance, null);
            addSuccessInfo(type, objName, startTime, info);
        } catch (Throwable ex) {
            LOGGER.error("Error rebalancing replicas '{}'", request.getEntity(), ex);
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    @Override
    public void syncTableReplicas(MasterProto.SynchronizeRequest request, StreamObserver<Empty> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = syncTableReplicas;
        String objName = request.getEntity();
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        List<TableName> tablesToSync;
        try {
            tablesToSync = master.getTableNamesToSync(request.getEntity());
            info.put("Last tables in sync",
                    joiningFixSize(tablesToSync.stream().map(TableName::toString).collect(Collectors.joining())));
            Preconditions.checkArgument(!tablesToSync.isEmpty(), "unable to find any tables matching criteria to rebalancing");
            LOGGER.info("New replicas sync request. syncEntity: {}, tableNames: {}", request.getEntity(), tablesToSync);
            for (TableName tableName : tablesToSync) {
            }
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOGGER.error("Error starting replicas sync for entity '{}'", request.getEntity(), ex);
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
            return;
        }
        try {
            master.getRecoverRSContextInstance().notifySyncStart();
            master.syncTableReplicas(tablesToSync, null);
            addSuccessInfo(type, objName, startTime, info);
        } catch (Throwable ex) {
            LOGGER.error("Error syncing replicas '{}'", request.getEntity(), ex);
            addErrorInfo(type, objName, info, ex);
        } finally {
            master.getRecoverRSContextInstance().notifySyncFinish();
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    /**
     * Stops all running replica rebalancing tasks in two steps:
     * 1. Stops the running RebalanceTableReplicas tasks.
     * 2. Stops the executors that iterate over the tables.
     */
    @Override
    public void stopReplicasRebalance(MasterProto.StopRebalanceRequest request, StreamObserver<Empty> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = stopReplicasRebalance;
        String objName = request.getEntity();
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        List<TableName> tablesToRebalance;
        try {
            tablesToRebalance = master.getTableNamesToRebalance(request.getEntity(), false);
            info.put("Last tables in rebalancing",
                    joiningFixSize(tablesToRebalance.stream().map(TableName::toString).collect(Collectors.joining())));
            Preconditions.checkArgument(!tablesToRebalance.isEmpty(), "unable to find any tables matching criteria to stop rebalancing");
            LOGGER.info("New stop rebalance replicas request. rebalanceEntity: {}, tableNames: {}", request.getEntity(), tablesToRebalance);
            for (TableName tableName : tablesToRebalance) {
            }
            master.stopRebalanceTableReplicas(tablesToRebalance);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            addSuccessInfo(type, objName, startTime, info);
            LOGGER.info("Done marking replicas rebalancing to stop. rebalanceEntity: {}, tableNames: {}", request.getEntity(), tablesToRebalance);
        } catch (Throwable ex) {
            LOGGER.error("Error marking replicas rebalancing to stop for '{}'", request.getEntity(), ex);
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    @Override
    public void startRegionsRebalanceAsync(MasterProto.StartRebalanceRequest request, StreamObserver<Empty> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = startRegionsRebalanceAsync;
        String objName = request.getEntity();
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        List<TableName> tablesToRebalance;
        try {
            tablesToRebalance = master.getTableNamesToRebalance(request.getEntity(), true);
            info.put("Last tables in rebalancing",
                    joiningFixSize(tablesToRebalance.stream().map(TableName::toString).collect(Collectors.joining())));
            Preconditions.checkArgument(!tablesToRebalance.isEmpty(), "unable to find any tables matching criteria to rebalancing");
            LOGGER.info("New region rebalance request. rebalanceEntity: {}, tableNames: {}", request.getEntity(), tablesToRebalance);
            for (TableName tableName : tablesToRebalance) {
            }
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOGGER.error("Error starting regions rebalancing for entity '{}'", request.getEntity(), ex);
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
            requestsCountDecrement(startRegionsRebalanceAsync, reqId);
            return;
        }
        try {
            master.rebalanceTableRegions(tablesToRebalance, null);
            addSuccessInfo(type, objName, startTime, info);
        } catch (Throwable ex) {
            LOGGER.error("Error rebalancing '{}'", request.getEntity(), ex);
            addErrorInfo(type, objName, info, ex);
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    /**
     * Stops all running region rebalancing tasks in two steps:
     * 1. Stops the running RebalanceTableRegions tasks.
     * 2. Stops the executors that iterate over the tables.
     */
    @Override
    public void stopRegionsRebalance(MasterProto.StopRebalanceRequest request, StreamObserver<Empty> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = stopRegionsRebalance;
        String objName = request.getEntity();
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        List<TableName> tablesToRebalance;
        try {
            tablesToRebalance = master.getTableNamesToRebalance(request.getEntity(), false);
            info.put("Last tables in rebalancing",
                    joiningFixSize(tablesToRebalance.stream().map(TableName::toString).collect(Collectors.joining())));
            Preconditions.checkArgument(!tablesToRebalance.isEmpty(), "unable to find any tables matching criteria to stop rebalancing");
            LOGGER.info("New stop rebalance request. rebalanceEntity: {}, tableNames: {}", request.getEntity(), tablesToRebalance);
            for (TableName tableName : tablesToRebalance) {
            }
            master.stopRebalanceTableRegions(tablesToRebalance);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            addSuccessInfo(type, objName, startTime, info);
            LOGGER.info("Done marking rebalancing to stop. rebalanceEntity: {}, tableNames: {}", request.getEntity(), tablesToRebalance);
        } catch (Throwable ex) {
            LOGGER.error("Error marking rebalancing to stop for '{}'", request.getEntity(), ex);
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    @Override
    public void ping(Empty request, StreamObserver<MasterProto.PingResponse> responseObserver) {
        LOGGER.debug("Ping request received");
        try {
            responseObserver.onNext(buildPingResponse());
            responseObserver.onCompleted();
            LOGGER.debug("Ping response sent");
        } catch (Exception e) {
            LOGGER.error("Unable to send ping response", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void info(Empty request, StreamObserver<MasterProto.InfoResponse> responseObserver) {
        LOGGER.debug("Ping request received");
        try {
            responseObserver.onNext(buildInfoResponse());
            responseObserver.onCompleted();
            LOGGER.debug("Ping response sent");
        } catch (Exception e) {
            LOGGER.error("Unable to send ping response", e);
            responseObserver.onError(e);
        }
    }


    @Override
    public void shutdownRs(MasterProto.ShutdownRsRequest request, StreamObserver<MasterProto.ShutdownRsResponse> responseObserver) {
        try {
        } catch (Exception e) {
            responseObserver.onError(io.grpc.Status.PERMISSION_DENIED.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        masterShutdown.shutdown(responseObserver);
    }

    @Override
    public void shutdown(Empty request, StreamObserver<MasterProto.PingResponse> responseObserver) {
        LOGGER.info("Shutdown request received");
        try {
            responseObserver.onNext(buildPingResponse());
            responseObserver.onCompleted();
            LOGGER.debug("Shutdown response sent");
        } catch (Exception e) {
            responseObserver.onError(e);
        }
        leaderStateManager.shutdown(ER_EXECUTOR_TERMINATION_GRPC);
    }

    @Override
    public void getRsStartPermission(ATALANTAProto.ProtoRegionServer request, StreamObserver<BoolValue> responseObserver) {
        LOGGER.debug("RS requested start permission: {}", request.getName());
        try {
            boolean permission = master.getRsStartPermission(ProtoConverters.toRegionServerModel(request));
            responseObserver.onNext(BoolValue.newBuilder().setValue(permission).build());
            responseObserver.onCompleted();
            LOGGER.debug("RS start permission '{}' sent: {}", permission, request.getName());
        } catch (Exception ex) {
            LOGGER.error("Unable to grand start permission for: {}", request.getName(), ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        }
    }

    @Override
    public void close() throws IOException {
        requestsMonitoring.shutdownNow();
    }

    private MasterProto.PingResponse buildPingResponse() {
        return MasterProto.PingResponse.newBuilder()
                .setMode(leaderStateManager.getState().name())
                .setCreateNamespace(getRequestsCount(createNamespace))
                .setCreateTable(getRequestsCount(createTable))
                .setEnableTable(getRequestsCount(enableTable))
                .setDisableTable(getRequestsCount(disableTable))
                .setDropTable(getRequestsCount(dropTable))
                .setMoveRegion(getRequestsCount(moveRegion))
                .setSplitRegion(getRequestsCount(splitRegion))
                .setStartReplicasRebalanceAsync(getRequestsCount(startReplicasRebalanceAsync))
                .setSyncTableReplicas(getRequestsCount(syncTableReplicas))
                .setStopReplicasRebalance(getRequestsCount(stopReplicasRebalance))
                .setStartRegionsRebalanceAsync(getRequestsCount(startRegionsRebalanceAsync))
                .setStopRegionsRebalance(getRequestsCount(stopRegionsRebalance))
                .setCreateSnapshot(getRequestsCount(createSnapshot))
                .setCloneSnapshot(getRequestsCount(cloneSnapshot))
                .setDeleteSnapshot(getRequestsCount(deleteSnapshot))
                .setStartRegionServer(getRequestsCount(startRegionServer))
                .setStopRegionServer(getRequestsCount(stopRegionServer))
                .build();
    }

    private MasterProto.InfoResponse buildInfoResponse() {
        return MasterProto.InfoResponse.newBuilder()
                .addAllCreateNamespaceMsgList(getInfoByRequestType(createNamespace))
                .addAllCreateTableMsgList(getInfoByRequestType(createTable))
                .addAllEnableTableMsgList(getInfoByRequestType(enableTable))
                .addAllDisableTableMsgList(getInfoByRequestType(disableTable))
                .addAllDropTableMsgList(getInfoByRequestType(dropTable))
                .addAllMoveRegionMsgList(getInfoByRequestType(moveRegion))
                .addAllSplitRegionMsgList(getInfoByRequestType(splitRegion))
                .addAllStartReplicasRebalanceAsyncMsgList(getInfoByRequestType(startReplicasRebalanceAsync))
                .addAllSyncTableReplicasMsgList(getInfoByRequestType(syncTableReplicas))
                .addAllStopReplicasRebalanceMsgList(getInfoByRequestType(stopReplicasRebalance))
                .addAllStartRegionsRebalanceAsyncMsgList(getInfoByRequestType(startRegionsRebalanceAsync))
                .addAllStopRegionsRebalanceMsgList(getInfoByRequestType(stopRegionsRebalance))
                .addAllCreateSnapshotMsgList(getInfoByRequestType(createSnapshot))
                .addAllCloneSnapshotMsgList(getInfoByRequestType(cloneSnapshot))
                .addAllDeleteSnapshotMsgList(getInfoByRequestType(deleteSnapshot))
                .addAllStartRegionServerMsgList(getInfoByRequestType(startRegionServer))
                .addAllStopRegionServerMsgList(getInfoByRequestType(stopRegionServer))
                .build();
    }

    private List<MasterProto.FMasterInfoMsg> getInfoByRequestType(MasterRequestType type) {
        return requestMsg.getOrDefault(type, defaultInfoValue).entrySet().stream()
                .map(e -> buildMsg(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    private long getRequestsCount(MasterRequestType masterRequestType) {
        return activeRequestCount.getOrDefault(masterRequestType, new AtomicLong(0L)).get();
    }

    private ATALANTAProto.ProtoTableName getProtoNs(String ns) {
        return ATALANTAProto.ProtoTableName.newBuilder()
                .setNamespace(ns)
                .setTable(StringUtils.EMPTY)
                .build();
    }

    @Override
    public void createSnapshot(MasterProto.MasterSnapshotRequest request, StreamObserver<com.google.protobuf.Empty> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = createSnapshot;
        String objName = request.getTableName().getNamespace() + ":" + request.getTableName().getTable();
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        try {
            TableName tableName = TableName.valueOf(request.getTableName().getNamespace(), request.getTableName().getTable());
            master.createSnapshot(request.getName(), tableName);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            addSuccessInfo(type, objName, startTime, info);
        } catch (Throwable ex) {
            // The operation exception is logged by the method that does the work
            // Errors of other types must be logged here
            if (!(ex instanceof OperationException || ex instanceof NoRetryException)) {
                LOGGER.error("Error creating snapshot '{}' for table '{}:{}",
                        request.getName(),
                        request.getTableName().getNamespace(),
                        request.getTableName().getTable(), ex);
            }
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    @Override
    public void cloneSnapshot(com.light.atalanta.grpc.MasterProto.MasterSnapshotRequest request, io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = cloneSnapshot;
        String objName = request.getTableName().getNamespace() + ":" + request.getTableName().getTable();
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        try {
            TableName tableName = TableName.valueOf(request.getTableName().getNamespace(), request.getTableName().getTable());
            master.cloneSnapshot(request.getName(), tableName);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            addSuccessInfo(type, objName, startTime, info);
        } catch (Throwable ex) {
            // The operation exception is logged by the method that does the work
            // Errors of other types must be logged here
            if (!(ex instanceof OperationException || ex instanceof NoRetryException)) {
                LOGGER.error("Error cloning snapshot '{}' for table '{}:{}', error: {}",
                        request.getName(),
                        request.getTableName().getNamespace(),
                        request.getTableName().getTable(), ex.getMessage(), ex);
            }
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    @Override
    public void deleteSnapshot(MasterProto.MasterSnapshotRequest request, StreamObserver<Empty> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = deleteSnapshot;
        String objName = request.getTableName().getNamespace() + ":" + request.getTableName().getTable();
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        try {
            master.deleteSnapshot(request.getName());

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            addSuccessInfo(type, objName, startTime, info);
        } catch (Throwable ex) {
            // The operation exception is logged by the method that does the work
            // Errors of other types must be logged here
            if (!(ex instanceof OperationException || ex instanceof NoRetryException)) {
                LOGGER.error("Error delete snapshot '{}'", request.getName(), ex);
            }
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    @Override
    public void startRegionServer(com.google.protobuf.StringValue request,
                                  io.grpc.stub.StreamObserver<com.google.protobuf.BoolValue> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = startRegionServer;
        String objName = request.getValue();
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        try {
            master.startRegionServer(request.getValue());
            responseObserver.onNext(BoolValue.of(true));
            responseObserver.onCompleted();
            addSuccessInfo(type, objName, startTime, info);
        } catch (Throwable ex) {
            // The operation exception is logged by the method that does the work
            // Errors of other types must be logged here
            if (!(ex instanceof OperationException || ex instanceof NoRetryException)) {
                LOGGER.error("Error in start region server '{}'", request.getValue(), ex);
            }
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    @Override
    public void stopRegionServer(com.google.protobuf.StringValue request,
                                 io.grpc.stub.StreamObserver<com.google.protobuf.BoolValue> responseObserver) {
        Long startTime = getStartTime();
        MasterRequestType type = stopRegionServer;
        String objName = request.getValue();
        String reqId = requestsCountIncrement(type);
        Map<String, Object> info = requestMsg.computeIfAbsent(type, t -> new ConcurrentHashMap<>());
        Set<String> cur = getActiveRequestObjSet(type, objName, info, reqId);
        try {
            Context.current().fork().run(() -> master.stopRegionServer(request.getValue()));
            responseObserver.onNext(BoolValue.of(true));
            responseObserver.onCompleted();
            addSuccessInfo(type, objName, startTime, info);
        } catch (Throwable ex) {
            // The operation exception is logged by the method that does the work
            // Errors of other types must be logged here
            if (!(ex instanceof OperationException || ex instanceof NoRetryException)) {
                LOGGER.error("Error in stop region server '{}'", request.getValue(), ex);
            }
            addErrorInfo(type, objName, info, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            requestsCountDecrement(type, reqId);
            addCommonInfo(type, objName, info, cur);
        }
    }

    @Override
    public void modificationCompleteNotification(com.google.protobuf.Empty request,
                                                 io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
        master.modificationCompleteNotification();
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    public static MasterProto.FMasterInfoMsg buildMsg(String name, Object msg) {
        String msgStr;
        if (msg instanceof String) {
            msgStr = (String) msg;
        } else if (msg instanceof Iterable) {
            msgStr = String.join(",", (Iterable) msg);
        } else {
            msgStr = msg.toString();
        }
        return MasterProto.FMasterInfoMsg.newBuilder().setName(name)
                .setMsg(": " + msgStr)
                .build();
    }

    private String requestsCountIncrement(MasterRequestType masterRequestType) {
        final String requestId = UUID.randomUUID().toString();
        LOGGER.info("New request started: reqId: {}, type: {}, client ip: {}",
                requestId, masterRequestType, masterInterceptor.getClientIp()
        );
        activeRequestsMap.put(requestId, masterRequestType);
        totalRequestCount.computeIfAbsent(masterRequestType, t -> new AtomicLong()).incrementAndGet();
        activeRequestCount.computeIfAbsent(masterRequestType, t -> new AtomicLong()).incrementAndGet();
        return requestId;
    }

    private void requestsCountDecrement(MasterRequestType masterRequestType, String requestId) {
        AtomicLong c = activeRequestCount.get(masterRequestType);
        LOGGER.info("Request finished: reqId: {}, type: {}, client ip: {}",
                requestId, masterRequestType, masterInterceptor.getClientIp()
        );
        activeRequestsMap.remove(requestId);
        c.decrementAndGet();
    }

    private long getActiveRequestCount() {
        return activeRequestCount.values().stream().mapToLong(AtomicLong::get).sum();
    }


    private Long initMinMaxTime(MasterRequestType masterRequestType, Long startTime) {
        long duration = System.currentTimeMillis() - startTime;
        AtomicLong min = minRequestTime.computeIfAbsent(masterRequestType, t -> new AtomicLong(Long.MAX_VALUE));
        min.set(Math.min(duration, min.get()));
        AtomicLong max = maxRequestTime.computeIfAbsent(masterRequestType, t -> new AtomicLong());
        max.set(Math.max(duration, max.get()));
        return duration;
    }

    private static long getStartTime() {
        return System.currentTimeMillis();
    }


    private void addErrorInfo(MasterRequestType type, String objName, Map<String, Object> info, Throwable ex) {
        info.put("Last error", objName);
        info.put("Error msg", recursivelyGetMessage(new StringBuilder(), ex).toString());
        errorRequestCounter.computeIfAbsent(type, t -> new AtomicLong()).incrementAndGet();
    }

    private void addCommonInfo(MasterRequestType type, String objName, Map<String, Object> info, Set<String> cur) {
        AtomicLong min = minRequestTime.get(type);
        info.put("Min time, ms", min != null ?  min.get() : "???");
        AtomicLong max = maxRequestTime.get(type);
        info.put("Max time, ms", max != null ? max.get() : "???");
        info.put("Total errors", errorRequestCounter.computeIfAbsent(type, t -> new AtomicLong()).get());
        cur.remove(objName);
        if (cur.size() < MAX_CUR_ACTIVE_INFO_SIZE - 1) {
            cur.remove(MORE_OBJECT);
        }
    }

    private void addSuccessInfo(MasterRequestType requestType, String objName, Long startTime, Map<String, Object> info) {
        Long duration = initMinMaxTime(requestType, startTime);
        info.put("Last success", objName);
        info.put("Last time, ms", duration);
    }


    private Set<String> getActiveRequestObjSet(MasterRequestType requestType, String objName, Map<String, Object> info, String reqId) {
        info.put("reqId", reqId);
        info.put("Total", String.valueOf(totalRequestCount.get(requestType)));
        Set<String> cur = (Set<String>) info.computeIfAbsent("in progress", t -> new HashSet<String>());
        if (cur.size() < MAX_CUR_ACTIVE_INFO_SIZE) {
            cur.add(objName);
        } else if (cur.size() == MAX_CUR_ACTIVE_INFO_SIZE) {
            cur.add(MORE_OBJECT);
        }
        return cur;
    }

}
