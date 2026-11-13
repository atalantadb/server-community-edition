/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.light.atalanta.RegionTaskType;
import com.light.atalanta.TableName;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.grpc.RegionServerServiceGrpc;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.region.server.data.RegionDataReader;
import com.light.atalanta.region.server.data.RegionFileList;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.region.server.services.RegionServerInfoSupplier;
import com.light.atalanta.region.server.services.ReplicaMapperProcessor;
import com.light.atalanta.region.server.services.operations.progressed.*;
import com.light.atalanta.region.server.services.operations.functionalized.ListRegionFiles;
import com.light.atalanta.region.server.services.operations.RegionOperation;
import com.light.atalanta.region.server.services.operations.functionalized.SendRegionFile;
import com.light.atalanta.region.server.services.operations.snapshot.CloneSnapshot;
import com.light.atalanta.region.server.services.operations.snapshot.CopyDateToTargetSnapshotRegion;
import com.light.atalanta.region.server.services.operations.snapshot.CopySnapshotRegion;
import com.light.atalanta.region.server.services.operations.snapshot.CreateSnapshot;
import com.light.atalanta.region.server.services.operations.snapshot.CreateSnapshotRegion;
import com.light.atalanta.region.server.services.operations.snapshot.DeleteSnapshot;
import com.light.atalanta.region.server.utils.UniqueNameThreadFactory;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import com.light.atalanta.services.RegionServerConnector;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.util.ProtoConverters;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;

/**
 * gRPC service that handles region server commands.
 * The methods are described in RegionTaskType.java.
 */
public class RegionServerServiceGrpcImpl extends RegionServerServiceGrpc.RegionServerServiceImplBase implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionServerServiceGrpcImpl.class);

    // Number of pending gRPC requests (queue size)
    private final AtomicLong requestsCount = new AtomicLong(0L);

    // Number of progressed tasks running now
    private final AtomicLong processingProgressiveRequestsCount = new AtomicLong(0L);

    // Number of functionalized tasks running now
    private final AtomicLong processingFunctionalizedRequestsCount = new AtomicLong(0L);

    private final ScheduledExecutorService scheduledExecutor;
    private final ExecutorService runningTasksExecutor;

    // Queue of progressed tasks to run
    private final LinkedBlockingQueue<RegionTask> progressiveTaskQueue = new LinkedBlockingQueue<>();

    // Queue of functionalized tasks to run
    private final LinkedBlockingQueue<RegionTask> functionalizedTaskQueue = new LinkedBlockingQueue<>();

    // Task storage
    private final Map<RegionTaskDescription, RegionTask> taskStorage = new ConcurrentHashMap<>();

    // Locks that're blocked while tasks are being extracted from the queue and added to the taskRunner
    private final Semaphore addingProgressedTask = new Semaphore(1);
    private final Semaphore addingFunctionalizedTask = new Semaphore(1);

    // Count of active requests to stop region operations
    private final AtomicLong stopOperationsRunning = new AtomicLong(0);

    private final RegionServerConfiguration conf;

    private final TaskRunner taskRunner;

    private final DistributeRegion distributeRegion;
    private final DisableRegion disableRegion;
    private final DropRegion dropRegion;
    private final StartRegion startRegion;
    private final StopRegion stopRegion;
    private final MoveRegion moveRegion;
    private final SplitRegion1 splitRegion1;
    private final SplitRegion2 splitRegion2;
    private final RecoverRegion recoverRegion;
    private final SendRegionFile sendRegionFile;
    private final ListRegionFiles listRegionFiles;
    private final CreateSnapshot createSnapshot;
    private final CloneSnapshot cloneSnapshot;
    private final CopySnapshotRegion copySnapshotRegion;
    private final CreateSnapshotRegion createSnapshotRegion;
    private final CopyDateToTargetSnapshotRegion copyDateToTargetSnapshotRegion;
    private final ModifyRegion modifyRegion;
    private final DeleteSnapshot deleteSnapshot;
    private final ReplicaMapperProcessor replicaMapperProcessor;
    private final ThreadFactory serviceThreadFactory = new UniqueNameThreadFactory("atalanta-service-parent-pool");
    private final ThreadFactory operationsThreadFactory = new UniqueNameThreadFactory("atalanta-tasks-parent-pool");
    private final String profile;
    private final RegionFileList regionFileList;
    private long scapeUpdateTime = 0;
    private String scrape = StringUtils.EMPTY;
    private final MeterRegistry meterRegistry;
    private final RegionServerShutdown shutdown;

    public RegionServerServiceGrpcImpl(RegionServerConfiguration conf, RegionServerInfoSupplier serverInfo, ZookeeperHelper zookeeperHelper, ZooKeeperCoreContext zooKeeperCoreContext, ReplicaMapperProcessor replicaMapperProcessor, FileSystem fileSystem, RegionServerConnector regionServerConnector, ModifyRegion modifyRegion, String profile, MeterRegistry meterRegistry, RegionServerShutdown shutdown) throws Exception {
        this.shutdown = shutdown;
        this.conf = conf;
        this.modifyRegion = modifyRegion;
        this.profile = profile;
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.meterRegistry = meterRegistry;
        this.distributeRegion = new DistributeRegion(conf, replicaMapperProcessor, fileSystem);
        this.disableRegion = new DisableRegion(conf, replicaMapperProcessor);
        this.dropRegion = new DropRegion(replicaMapperProcessor, fileSystem);
        this.startRegion = new StartRegion(conf, replicaMapperProcessor);
        this.stopRegion = new StopRegion(conf, replicaMapperProcessor);
        this.moveRegion = new MoveRegion(conf, regionServerConnector, replicaMapperProcessor, distributeRegion, fileSystem);
        this.splitRegion1 = new SplitRegion1(replicaMapperProcessor, disableRegion, fileSystem, conf.getDataConfiguration().getTryToCompressLevel());
        this.splitRegion2 = new SplitRegion2(replicaMapperProcessor, fileSystem, conf.getDataConfiguration());
        this.recoverRegion = new RecoverRegion(conf, regionServerConnector, replicaMapperProcessor, distributeRegion, fileSystem);
        this.sendRegionFile = new SendRegionFile(replicaMapperProcessor, conf);
        this.listRegionFiles = new ListRegionFiles(replicaMapperProcessor, fileSystem);
        this.createSnapshot = new CreateSnapshot(zookeeperHelper, serverInfo, conf, fileSystem, replicaMapperProcessor, zooKeeperCoreContext, serverInfo, this.profile);
        this.cloneSnapshot = new CloneSnapshot(zookeeperHelper, serverInfo, conf, fileSystem, replicaMapperProcessor, zooKeeperCoreContext, serverInfo, this.profile, zooKeeperCoreContext, conf);
        this.copySnapshotRegion = new CopySnapshotRegion(conf, fileSystem, regionServerConnector, zooKeeperCoreContext);
        this.createSnapshotRegion = new CreateSnapshotRegion(conf, distributeRegion, fileSystem);
        this.copyDateToTargetSnapshotRegion = new CopyDateToTargetSnapshotRegion(conf, fileSystem);
        this.deleteSnapshot = new DeleteSnapshot(conf, fileSystem);
        this.taskRunner = new TaskRunner();
        // The pool has three threads: monitoring, and running tasks from each of the two queues
        this.scheduledExecutor = Executors.newScheduledThreadPool(3, serviceThreadFactory);
        this.runningTasksExecutor = Executors.newCachedThreadPool(operationsThreadFactory);
        regionFileList = new RegionFileList(replicaMapperProcessor, fileSystem);
        if (conf.getGrpcConfiguration().getGrpcRequestsMonitoringEnabled()) {
            startRequestsMonitoring();
        }
        startRequestsProcessing();
    }

    @Override
    public void shutdown(RegionServerProto.RegionServerShutdownRequest request,
                         StreamObserver<RegionServerProto.RegionServerShutdownResponse> responseObserver) {
        shutdown.shutdown(request, responseObserver);
    }

    private void startRequestsMonitoring() {
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            long curProcessingProgressiveRequestsCount = processingProgressiveRequestsCount.get();
            long curProcessingFunctionalizedRequestsCount = processingFunctionalizedRequestsCount.get();
            int curProgressiveTaskQueueSize = progressiveTaskQueue.size();
            int curFunctionalizedTaskQueueSize = functionalizedTaskQueue.size();
            int curTaskStorageSize = taskStorage.size();
            long curRequestCount = requestsCount.get();
            LOGGER.info("gRPC monitor [prunning: {}] [frunning: {}] [pqueue: {}] [fqueue: {}] [saved: {}] [all: {}]",
                    curProcessingProgressiveRequestsCount, curProcessingFunctionalizedRequestsCount,
                    curProgressiveTaskQueueSize, curFunctionalizedTaskQueueSize, curTaskStorageSize, curRequestCount);
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    // Runs tasks from the queues regularly, keeping at most N instances running at once
    private void startRequestsProcessing() {
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                if (stopOperationsRunning.get() > 0) {
                    if (System.currentTimeMillis() % 5000 == 0) {
                        LOGGER.warn("Don't run new progressed operations while WAIT_COMPLETE({}) is in progress, next log after 5000 ms", stopOperationsRunning.get());
                    }
                    return;
                }
                if (processingProgressiveRequestsCount.get() < conf.getGrpcConfiguration().getActiveProcessingRequests() / 2) {
                    addingProgressedTask.acquire();
                    try {
                        RegionTask regionTask = progressiveTaskQueue.poll(5, TimeUnit.SECONDS);
                        if (regionTask == null) {
                            return;
                        }
                        LOGGER.debug("Starting new progressive task: {}", regionTask);
                        runningTasksExecutor.submit(() -> taskRunner.run(regionTask));
                        LOGGER.debug("Progressive task was started: {}", regionTask);
                    } finally {
                        addingProgressedTask.release();
                    }
                }
            } catch (InterruptedException ignore) {
                // ignore
            } catch (Exception ex) {
                LOGGER.error("Task execution error", ex);
                throw ex;
            }
        }, 100, 10, TimeUnit.MILLISECONDS);

        scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                if (stopOperationsRunning.get() > 0) {
                    if (System.currentTimeMillis() % 5000 == 0) {
                        LOGGER.warn("Don't run new functionalized operations while WAIT_COMPLETE({}) is in progress, next log after 5000 ms", stopOperationsRunning.get());
                    }
                    return;
                }
                if (processingFunctionalizedRequestsCount.get() < conf.getGrpcConfiguration().getActiveProcessingRequests() / 2) {
                    addingFunctionalizedTask.acquire();
                    try {
                        RegionTask regionTask = functionalizedTaskQueue.poll(5, TimeUnit.SECONDS);
                        if (regionTask == null) {
                            return;
                        }
                        LOGGER.debug("Starting new functionalized task: {}", regionTask);
                        runningTasksExecutor.submit(() -> taskRunner.run(regionTask));
                        LOGGER.debug("Functionalized task was started: {}", regionTask);
                    } finally {
                        addingFunctionalizedTask.release();
                    }
                }
            } catch (InterruptedException ignore) {
                // ignore
            } catch (Exception ex) {
                LOGGER.error("Task execution error", ex);
                throw ex;
            }
        }, 100, 10, TimeUnit.MILLISECONDS);
    }

    @Override
    public void ping(Empty request, StreamObserver<Empty> responseObserver) {
        LOGGER.trace("Received ping from master");
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void distributeRegion(RegionServerProto.RegionRequest request, StreamObserver<RegionServerProto.RegionOperationProgress> responseObserver) {
        initProgressedTask(new RegionTaskDescription(RegionTaskType.DISTRIBUTE, request), responseObserver);
    }

    @Override
    public void disableRegion(RegionServerProto.RegionRequest request, StreamObserver<RegionServerProto.RegionOperationProgress> responseObserver) {
        initProgressedTask(new RegionTaskDescription(RegionTaskType.DISABLE, request), responseObserver);
    }

    @Override
    public void dropRegion(RegionServerProto.RegionRequest request, StreamObserver<RegionServerProto.RegionOperationProgress> responseObserver) {
        initProgressedTask(new RegionTaskDescription(RegionTaskType.DROP, request), responseObserver);
    }

    @Override
    public void startRegion(RegionServerProto.RegionRequest request, StreamObserver<RegionServerProto.RegionOperationProgress> responseObserver) {
        initProgressedTask(new RegionTaskDescription(RegionTaskType.START, request), responseObserver);
    }

    @Override
    public void stopRegion(RegionServerProto.RegionRequest request, StreamObserver<RegionServerProto.RegionOperationProgress> responseObserver) {
        initProgressedTask(new RegionTaskDescription(RegionTaskType.STOP, request), responseObserver);
    }

    /**
     * This method removes all operations from queues and gracefully waits for running ones to finish.
     * Waits for all tables if tableName is empty/null.
     * .
     * Algorithm:
     * Step 1. Acquire locks for the progressed and functionalized tasks
     * Step 2. Remove task from progressed queue
     * Step 3. Remove task from functionalized queue
     * Step 4. Wait task to complete in task runner
     * Step 5. Remove task from task storage
     * Step 6. Return success.
     * .
     * This is what will happen in case functionalized task has already been started executing:
     * 1. Caller, which is RS (since only RS can send functionalized tasks) will wait for answer from RS_sender
     * 2. In case RS_receiver dies – RS_sender should finish that task based on timeout
     *
     * This is what will happen in case progressed task has already been started executing:
     * 1. Caller, which can be RS (MoveRegion RS operation that triggers DisableRegion RS operation under the hood) or Master
     *    should understand, when the operation is interrupted, because responder will just disappear.
     * 2. Master uses checkIncomeTableRequestsEnabled lambda to understand when it should stop waiting answer/progress from target/responding RS
     * 3. RS uses "interrupted" reference flag to understand the same thing. That flag is being set from
     *    taskRunner::wait method: cur.getKey().sendGracefulStopSignal().
     */
    @Override
    public void stopTableRegionTasks(RegionServerProto.RegionRequest request, StreamObserver<Empty> responseObserver) {
        TableName tableName = request.hasTableName() ? ProtoConverters.toTableName(request.getTableName()) : null;
        String tableNameString = tableName == null ? "all tables" : String.format("'%s'", tableName);
        requestsCount.incrementAndGet();
        stopOperationsRunning.incrementAndGet();
        LOGGER.info("Wait tasks for {}: initialization", tableNameString);
        try {
            // [Step 1]. Acquire locks for the progressed and functionalized tasks
            LOGGER.info("Wait tasks for {}: acquiring progressed task lock", tableNameString);
            addingProgressedTask.acquire();
            LOGGER.info("Wait tasks for {}: acquiring functionalized task lock", tableNameString);
            addingFunctionalizedTask.acquire();
            LOGGER.info("Wait tasks for {}: locks have been acquired", tableNameString);

            // [Step 2]. Remove task from progressed queue
            progressiveTaskQueue.removeIf(cr -> {
                if (tableName == null || cr.getTableName() == tableName) {
                    // We send nothing to the caller back, since:
                    // 1. Master knows when he triggers stopIfExists() for RS, therefore, he is able to cancel waiting process.
                    // 2. RS (sender) also knows when master triggered him stopIfExists() by using sendGracefulStopSignal().
                    LOGGER.info("Wait tasks for {}: removing from queue, task: '{}'", tableNameString, cr);
                    // We should decrement the total (queued + running) requests count,
                    // since it was expected to be decremeneted after the task is finished running, but
                    // we didn't let task to run, so we do that on our own
                    requestsCount.decrementAndGet();
                    return true;
                }
                return false;
            });

            // [Step 3]. Remove task from functionalized queue
            functionalizedTaskQueue.removeIf(cr -> {
                if (tableName == null || cr.getTableName() == tableName) {
                    // Functionalized operation requesters are RS-only; they don't have ability to cancel request waiting.
                    // Hence, we, as a functionalized data-sender, should send them something back.
                    // In this case – error, that the task is interrupted.
                    LOGGER.info("Wait tasks for {}: removing from queue, sending error to caller, task: '{}'", tableNameString, cr);
                    try {
                        cr.getDataResponseObserver().onError(new Exception("task has been interrupted on request"));
                        LOGGER.info("Wait tasks for {}: removing from queue, error to caller has been sent, task: '{}'", tableNameString, cr);
                    } catch (Exception ex) {
                        LOGGER.warn("Wait tasks for {}: removing from queue, can't send the error to caller, task: '{}'", tableNameString, cr, ex);
                    }
                    // We should decrement the total (queued + running) requests count,
                    // since it was expected to be decremented after the task is finished running, but
                    // we didn't let task to run, so we do that on our own
                    requestsCount.decrementAndGet();
                    return true;
                }
                return false;
            });

            // [Step 4]. Wait task to complete in task runner
            LOGGER.info("Wait tasks for {}: waiting tasks in task runner to complete", tableNameString);
            taskRunner.wait(tableName, tableNameString);
            LOGGER.info("Wait tasks for {}: finished waiting tasks in task runner to complete", tableNameString);

            // [Step 5]. Remove task from task storage
            taskStorage.values().removeIf(cr -> {
                if (tableName == null || cr.getTableName() == tableName) {
                    LOGGER.info("Wait tasks for {}: removing from task storage, task: '{}'", tableNameString, cr);
                    return true;
                }
                return false;
            });

            // [Step 6]. Return success
            LOGGER.info("Wait tasks for {}: no more running operations", tableNameString);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOGGER.error("Error waiting tasks for {}", tableNameString, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        } finally {
            stopOperationsRunning.decrementAndGet();
            requestsCount.decrementAndGet();
            addingProgressedTask.release();
            addingFunctionalizedTask.release();
        }
    }

    @Override
    public void moveRegion(RegionServerProto.RegionRequest request, StreamObserver<RegionServerProto.RegionOperationProgress> responseObserver) {
        CountDownLatch done = new CountDownLatch(1);
        // Do not inherit request cancellation and deadline from the master
        Context.current().fork().run(() -> {
            try {
                initProgressedTask(new RegionTaskDescription(RegionTaskType.MOVE, request), responseObserver);
            } finally {
                done.countDown();
            }
        });
        try {
            done.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Move region request: {} was interrupted", request);
        }
    }

    @Override
    public void splitRegion1(RegionServerProto.RegionRequest request, StreamObserver<RegionServerProto.RegionOperationProgress> responseObserver) {
        CountDownLatch done = new CountDownLatch(1);
        // Do not inherit request cancellation and deadline from the master
        Context.current().fork().run(() -> {
            try {
                initProgressedTask(new RegionTaskDescription(RegionTaskType.SPLIT1, request), responseObserver);
            } finally {
                done.countDown();
            }
        });
        try {
            done.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Split1 region request: {} was interrupted", request);
        }
    }

    @Override
    public void splitRegion2(RegionServerProto.RegionRequest request, StreamObserver<RegionServerProto.RegionOperationProgress> responseObserver) {
        CountDownLatch done = new CountDownLatch(1);
        // Do not inherit request cancellation and deadline from the master
        Context.current().fork().run(() -> {
            try {
                initProgressedTask(new RegionTaskDescription(RegionTaskType.SPLIT2, request), responseObserver);
            } finally {
                done.countDown();
            }
        });
        try {
            done.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Split2 region request: {} was interrupted", request);
        }
    }

    @Override
    public void recoverRegion(RegionServerProto.RegionRequest request, StreamObserver<RegionServerProto.RegionOperationProgress> responseObserver) {
        CountDownLatch done = new CountDownLatch(1);
        // Do not inherit request cancellation and deadline from the master
        Context.current().fork().run(() -> {
            try {
                initProgressedTask(new RegionTaskDescription(RegionTaskType.RECOVER, request), responseObserver);
            } finally {
                done.countDown();
            }
        });
        try {
            done.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Recover region request: {} was interrupted", request);
        }
    }

    @Override
    public void sendRegionFile(RegionServerProto.RegionRequest request, StreamObserver<RegionServerProto.RegionDirectoryStructure> responseObserver) {
        initFunctionalizedTask(new RegionTaskDescription(RegionTaskType.SEND_FILE, request), responseObserver);
    }

    @Override
    public void listRegionFiles(RegionServerProto.RegionRequest request, StreamObserver<RegionServerProto.RegionDirectoryStructure> responseObserver) {
        initFunctionalizedTask(new RegionTaskDescription(RegionTaskType.LIST_FILES, request), responseObserver);
    }

    public void newModifyNotification(com.google.protobuf.Empty request,
                                      io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
        modifyRegion.newModifyNotification();
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void getRegionFileList(RegionServerProto.RegionRequest request, StreamObserver<RegionServerProto.FileList> responseObserver) {
        LOGGER.info("==> getRegionFileList tableName: {}:{}, region: {}_{}",
                request.getTableName().getNamespace(), request.getTableName().getTable(),
                request.getRegionName(), request.getReplicaNum());
        try {
            TableName tableName = ProtoConverters.toTableName(request.getTableName());
            Integer replicationFactor = conf.getMasterConfiguration().getReplicationFactor();
            int replicaNum = StringUtils.isNotBlank(request.getCopyFromRs()) ?
                    (request.getReplicaNum() - 1 + replicationFactor) % replicationFactor
                    : request.getReplicaNum();
            RegionModel regionModel = new RegionModel(request.getRegionName(),
                    replicaNum, 1, EMPTY_BYTE_ARRAY, new byte[]{(byte) 0xFF});
            responseObserver.onNext(regionFileList.getFileList(tableName, regionModel));
            responseObserver.onCompleted();
            LOGGER.info("<== getRegionFileList tableName: {}:{}, region: {}_{}",
                    request.getTableName().getNamespace(), request.getTableName().getTable(),
                    request.getRegionName(), request.getReplicaNum());
        } catch (Exception ex) {
            LOGGER.error("getRegionFileList error! tableName: {}:{}, region: {}_{}",
                    request.getTableName().getNamespace(), request.getTableName().getTable(),
                    request.getRegionName(), request.getReplicaNum());
            LOGGER.error("<==getRegionFileList error!", ex);
            responseObserver.onError(ex);
        }
    }

    @Override
    public void createSnapshot(com.light.atalanta.grpc.RegionServerProto.SnapshotRequest request,
                               io.grpc.stub.StreamObserver<com.light.atalanta.grpc.RegionServerProto.RegionSnapshotOperationProgress> responseObserver) {
        LOGGER.info("==> createSnapshot name: {}, tableName: {}", request.getName(), request.getTableName());
        try {
            createSnapshot.createSnapshot(request.getName(), request.getTableName(), responseObserver);
            responseObserver.onCompleted();
            LOGGER.info("<== createSnapshot name: {}, tableName: {}", request.getName(), request.getTableName());
        } catch (Exception ex) {
            LOGGER.error("cannot create snapshot! name: {}", request.getName());
            LOGGER.error("<== createSnapshot cannot create snapshot!", ex);
            responseObserver.onError(ex);
        }
    }

    @Override
    public void cloneSnapshot(RegionServerProto.CloneSnapshotRequest request,
                              io.grpc.stub.StreamObserver<com.light.atalanta.grpc.RegionServerProto.RegionSnapshotOperationProgress> responseObserver) {
        LOGGER.info("==> cloneSnapshot name: {}, tableName: {}", request.getName(), request.getTableName());
        try {
            cloneSnapshot.cloneSnapshot(request.getName(), request.getTableName(), request.getNewRegionMapMap(), responseObserver);
            responseObserver.onCompleted();
            LOGGER.info("<== cloneSnapshot name: {}, tableName: {}", request.getName(), request.getTableName());
        } catch (Exception ex) {
            LOGGER.error("cannot clone snapshot! name: {}", request.getName());
            LOGGER.error("<== cloneSnapshot cannot clone snapshot!", ex);
            responseObserver.onError(ex);
        }
    }

    @Override
    public void copySnapshotRegion(RegionServerProto.CopySnapshotRegionRequest request,
                                   StreamObserver<RegionServerProto.CopyRegionSnapshotOperationProgress> responseObserver) {
        LOGGER.info("==> copySnapshotRegion name: {}, tableName: {}", request.getName(), request.getTableName());
        try {
            copySnapshotRegion.copySnapshotRegion(request.getName(), request.getTableName(), request.getRegion(), request.getTargetReplicaNum(), request.getTargetRegionServer(), responseObserver);
            responseObserver.onCompleted();
            LOGGER.info("<== copySnapshotRegion name: {}, tableName: {}", request.getName(), request.getTableName());
        } catch (Exception ex) {
            LOGGER.error("cannot copy snapshot region! name: {}", request.getName());
            LOGGER.error("<== copySnapshotRegion cannot copy snapshot region!", ex);
            responseObserver.onError(ex);
        }
    }

    @Override
    public void createSnapshotRegion(com.light.atalanta.grpc.RegionServerProto.TransferSnapshotRegionRequest request,
                                     io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
        LOGGER.info("==> createSnapshotRegion: sn: {}, region: {}", request.getName(), request.getRegion());
        try {
            createSnapshotRegion.createSnapshotRegion(request.getName(),
                    request.getTableName(), request.getRegion(), request.getTargetReplicaNum());
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            LOGGER.info("<== createSnapshotRegion: sn: {}, region: {}", request.getName(), request.getRegion());
        } catch (Exception ex) {
            LOGGER.error("cannot create snapshot! name: {}", request.getName());
            LOGGER.error("<== createSnapshotRegion cannot create snapshot!", ex);
            responseObserver.onError(ex);
        }
    }

    @Override
    public void copyDataToTargetSnapshotRegion(com.light.atalanta.grpc.RegionServerProto.TransferSnapshotRegionRequest request,
                                               io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
        LOGGER.info("==> copyDataToTargetSnapshotRegion. name: {}, region: {}_{}, file: {}",
                request.getName(), request.getRegion(), request.getTargetReplicaNum(), request.getFileName());
        try {
            copyDateToTargetSnapshotRegion.copyDataToTargetSnapshotRegion(request.getName(),
                    request.getTableName(), request.getRegion(), request.getTargetReplicaNum(), request.getFileName(),
                    request.getData().asReadOnlyByteBuffer());
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            LOGGER.info("<== copyDataToTargetSnapshotRegion. name: {}, region: {}_{}, file: {}",
                    request.getName(), request.getRegion(), request.getTargetReplicaNum(), request.getFileName());
        } catch (Exception ex) {
            LOGGER.error("cannot copy snapshot region data! name: {}, region: {}_{}, file: {}",
                    request.getName(), request.getRegion(), request.getTargetReplicaNum(), request.getFileName());
            LOGGER.error("<== copyDataToTargetSnapshotRegion cannot copy snapshot region data!", ex);
            responseObserver.onError(ex);
        }
    }

    @Override
    public void getRegionData(com.light.atalanta.grpc.RegionServerProto.RegionDataRequest request,
                              io.grpc.stub.StreamObserver<com.light.atalanta.grpc.RegionServerProto.RegionDataResponse> responseObserver) {
        LOGGER.info("==> getRegionData. tableName: {}:{}, region: {}_{}, file: {}, offset: {}",
                request.getTableName().getNamespace(), request.getTableName().getTable(),
                request.getRegionName(), request.getReplicaNum(), request.getFilename(), request.getOffset());
        try {
            RegionDataReader rdr = new RegionDataReader(getPathToDataFile(request), request.getOffset());
            ByteString data = rdr.read();
            while (data != null) {
                RegionServerProto.RegionDataResponse response = RegionServerProto.RegionDataResponse.newBuilder()
                        .setData(data)
                        .build();
                responseObserver.onNext(response);
                data = rdr.read();
            }
            responseObserver.onCompleted();
            LOGGER.info("<== getRegionData. tableName: {}:{}, region: {}_{}, file: {}, offset: {}",
                    request.getTableName().getNamespace(), request.getTableName().getTable(),
                    request.getRegionName(), request.getReplicaNum(), request.getFilename(), request.getOffset());
        } catch (Exception ex) {
            LOGGER.error("==> getRegionData. tableName: {}:{}, region: {}_{}, file: {}, offset: {}",
                    request.getTableName().getNamespace(), request.getTableName().getTable(),
                    request.getRegionName(), request.getReplicaNum(), request.getFilename(), request.getOffset());
            LOGGER.error("cannot copy data", ex);
            responseObserver.onError(ex);
        }
    }

    @Override
    public void getMetrics(Empty request, StreamObserver<RegionServerProto.RsMetrics> responseObserver) {
        try {
            responseObserver.onNext(buildRsMetricsResponse());
            responseObserver.onCompleted();
        } catch (Throwable ex) {
            LOGGER.error("Cannot send master metrics", ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        }
    }

    private RegionServerProto.RsMetrics buildRsMetricsResponse() {
        return RegionServerProto.RsMetrics.newBuilder()
                .setScrape(getScrape())
                .build();
    }

    synchronized private String getScrape() {
        long curTime = System.currentTimeMillis();
        if (curTime - scapeUpdateTime > 100) {
            scrape = ((PrometheusMeterRegistry) meterRegistry).scrape();
            scapeUpdateTime = curTime;
        }
        return scrape;
    }


    private String getPathToDataFile(RegionServerProto.RegionDataRequest request) throws Exception {
        RegionModel regionModel = new RegionModel(request.getRegionName(),
                request.getReplicaNum(), 1, EMPTY_BYTE_ARRAY, new byte[] {(byte) 0xFF});
        return String.join("/", replicaMapperProcessor.getReplicaDir(regionModel),
                request.getFilename());
    }

    @Override
    public void deleteSnapshot (com.light.atalanta.grpc.RegionServerProto. SnapshotRequest request,
        io.grpc.stub.StreamObserver<com.light.atalanta.grpc.RegionServerProto.RegionSnapshotOperationProgress> responseObserver) {
        try {
            LOGGER.info("Delete snapshot '{}' started...", request.getName());
            deleteSnapshot.deleteSnapshot(request.getName(), responseObserver);
            responseObserver.onCompleted();
            LOGGER.info("Delete snapshot '{}' complete.", request.getName());
        } catch (Exception ex) {
            LOGGER.error("cannot delete snapshot! name: {}", request.getName()); LOGGER.error("cannot delete snapshot!", ex);
            responseObserver.onError(ex);
        }
    }

    /**
     * Handles tasks that report progress to the caller directly.
     * <p>
     * If the task is in the storage:
     * - takes it from the storage
     * - updates its responseObserver, which replaces the gRPC connection used to send the progress, the result or the error
     * <p>
     * If the task is not in the storage:
     * - registers it in the storage
     * - adds it to the run queue
     * <p>
     * Progressed and functionalized tasks use different queues to avoid a deadlock.
     */
    private synchronized void initProgressedTask(RegionTaskDescription taskDescription, StreamObserver<RegionServerProto.RegionOperationProgress> responseObserver) {
        try {
            if (taskStorage.containsKey(taskDescription)) {
                // This is a repeated request
                // Take the task from the map and give it the new gRPC connection
                LOGGER.debug("Found existing progressed task for the request: {}", taskDescription);
                RegionTask regionTask = taskStorage.get(taskDescription);
                regionTask.setProgressResponseObserver(responseObserver);
            } else {
                // Create a new task and put it into the queue
                LOGGER.debug("Register new progressed task for the request: {}", taskDescription);
                RegionTaskProgressed regionTask = new RegionTaskProgressed(RegionTaskContext.newBuilder()
                        .setDescription(taskDescription)
                        .setRegionOperation(getOperationByDescription(taskDescription))
                        .setProgressResponseObserver(responseObserver)
                        .setTaskStorage(taskStorage)
                        .setRegionServerConfiguration(conf)
                        .setRequestsCount(requestsCount)
                        .setProcessingRequestsCount(processingProgressiveRequestsCount)
                        .build());
                taskStorage.put(taskDescription, regionTask);
                progressiveTaskQueue.add(regionTask);
            }
        } catch (Exception ex) {
            LOGGER.error("Unable to register new task: {}", taskDescription, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        }
    }

    /**
     * Handles tasks that return data to the caller. They don't report progress directly.
     * <p>
     * They are not registered in the storage, since they are not meant to run without a caller.
     * This means that a repeated request runs as a new one.
     * They still go through the run queue.
     * These tasks have a separate queue to avoid a deadlock.
     */
    private synchronized void initFunctionalizedTask(RegionTaskDescription taskDescription, StreamObserver<RegionServerProto.RegionDirectoryStructure> responseObserver) {
        try {
            // Create a new task and put it into the queue
            LOGGER.debug("Register new functionalized task for the request: {}", taskDescription);
            RegionTaskFunctionalized regionTask = new RegionTaskFunctionalized(RegionTaskContext.newBuilder()
                    .setDescription(taskDescription)
                    .setRegionOperation(getOperationByDescription(taskDescription))
                    .setDataResponseObserver(responseObserver)
                    .setTaskStorage(taskStorage)
                    .setRegionServerConfiguration(conf)
                    .setRequestsCount(requestsCount)
                    .setProcessingRequestsCount(processingFunctionalizedRequestsCount)
                    .build());
            functionalizedTaskQueue.add(regionTask);
        } catch (Exception ex) {
            LOGGER.error("Unable to register new task: {}", taskDescription, ex);
            responseObserver.onError(ProtoConverters.toProtoException(ex));
        }
    }

    private RegionOperation getOperationByDescription(RegionTaskDescription description) {
        switch (description.getTaskType()) {
            case DISTRIBUTE:
                return distributeRegion;
            case DISABLE:
                return disableRegion;
            case DROP:
                return dropRegion;
            case START:
                return startRegion;
            case STOP:
                return stopRegion;
            case MOVE:
                return moveRegion;
            case SPLIT1:
                return splitRegion1;
            case SPLIT2:
                return splitRegion2;
            case RECOVER:
                return recoverRegion;
            case SEND_FILE:
                return sendRegionFile;
            case LIST_FILES:
                return listRegionFiles;
            default:
                throw new IllegalArgumentException("Unknown operation: " + description.getTaskType());
        }
    }

    // TODO: Define how running tasks must be interrupted on close
    @Override
    public void close() {
        taskRunner.stopAll();
        scheduledExecutor.shutdownNow();
        runningTasksExecutor.shutdownNow();
    }
}
