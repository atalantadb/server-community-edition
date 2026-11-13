/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.grpc;

import com.light.atalanta.RegionTaskType;
import com.light.atalanta.TableName;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.region.server.services.operations.RegionOperation;
import com.light.atalanta.region.server.services.operations.progressed.DropRegion;
import com.light.atalanta.region.server.utils.UniqueNameThreadFactory;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import com.light.atalanta.util.ProtoConverters;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A region operation task. Created when a gRPC request arrives; kept in the run queue and in the task storage until it completes.
 * Returns no data to the caller, only progress.
 */
public class RegionTaskProgressed implements RegionTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionTaskProgressed.class);

    private final RegionTaskContext context;

    private final RegionServerConfiguration regionServerConfiguration;

    private final RegionOperation regionOperation;

    private final RegionTaskDescription description;

    private final AtomicReference<StreamObserver<RegionServerProto.RegionOperationProgress>> responseObserver;

    private final ScheduledExecutorService progressExecutor;
    private final ScheduledExecutorService responseExecutor;

    // Whether at least one progress batch has been sent
    private final CountDownLatch progressSent;

    // Whether the response or the error has been sent
    private final CountDownLatch responseSent;

    // Operation progress. Sent to the caller.
    private final AtomicReference<String> taskProgress;

    // Whether the operation has been interrupted
    private final AtomicBoolean interrupted;

    private final ThreadFactory progressExecutorThreadFactory = new UniqueNameThreadFactory("atalanta-progress-executor-pool");
    private final ThreadFactory responseExecutorThreadFactory = new UniqueNameThreadFactory("atalanta-response-executor-pool");

    private static final Map<String, ReentrantLock> regionLocks = new ConcurrentHashMap<>();
    public RegionTaskProgressed(RegionTaskContext context) {
        context.getRequestsCount().incrementAndGet();
        this.context = context;
        this.description = context.getDescription();
        this.regionServerConfiguration = context.getRegionServerConfiguration();
        this.responseObserver = new AtomicReference<>(context.getProgressResponseObserver());
        this.progressSent = new CountDownLatch(1);
        this.responseSent = new CountDownLatch(1);
        this.taskProgress = new AtomicReference<>("task created");
        this.progressExecutor = Executors.newSingleThreadScheduledExecutor(progressExecutorThreadFactory);
        this.responseExecutor = Executors.newSingleThreadScheduledExecutor(responseExecutorThreadFactory);
        this.regionOperation = context.getRegionOperation();
        this.interrupted = new AtomicBoolean(false);
        // Start the thread that sends progress to the master (Progress calculator) asynchronously
        // Notifications are sent even while the task is queued
        startProgressCalculator();
    }

    @Override
    public void run() {
        LOGGER.debug("Start executing request: '{}'", description);

        try {
            context.getProcessingRequestsCount().incrementAndGet();
            TableName tableName = ProtoConverters.toTableName(description.getRequest().getTableName());
            RegionModel regionModel = ProtoConverters.toRegionModel(description.getRequest());

            // Run the operation
            // Unlike RegionTaskFunctionalized, the result is sent to the caller after action(), in this class.
            // On error, it is sent from this class in the catch block below.
            if (!interrupted.get()) {
                LOGGER.debug("Progressed action: '{}', start. regionLocks size: {}", description, regionLocks.size());
                try {
                    lockRegion(regionModel);
                    regionOperation.progressedAction(tableName, regionModel, taskProgress, interrupted, description.getRequest().getReservedReplicas());
                } finally {
                    unlockRegion(regionModel);
                    if (regionOperation instanceof DropRegion) {
                        clearLockRegion(regionModel);
                    }
                }
                LOGGER.debug("Progressed action: '{}', finish. regionLocks size: {}", description, regionLocks.size());
                // Wait until at least one progress batch is sent
                while (true) {
                    LOGGER.debug("Waiting for the progress to be sent: '{}'", description);
                    boolean successful = progressSent.await(5, TimeUnit.SECONDS);
                    if (successful) {
                        break;
                    } else if (interrupted.get()) {
                        return;
                    }
                }
            }

            // Stop sending progress
            progressExecutor.shutdown();

            // Send the result to the master
            if (!interrupted.get()) {
                sendComplete();
                // Wait until the result is sent to the caller
                while (true) {
                    LOGGER.debug("Waiting for the response to be sent: '{}'", description);
                    boolean successful = responseSent.await(5, TimeUnit.SECONDS);
                    if (successful) {
                        break;
                    } else if (interrupted.get()) {
                        return;
                    }
                }
            }

            if (!interrupted.get()) {
                LOGGER.debug("Successfully finished executing request: {}", description);
            } else {
                LOGGER.debug("Request: {} was interrupted", description);
            }
        } catch (Throwable e) {

            LOGGER.error("Unable to perform request '" + description + "'", e);
            // Send the error to the master
            if (!interrupted.get()) {
                sendError(e);
                // Wait until the error is sent to the caller
                try {
                    while (true) {
                        LOGGER.debug("Waiting for the error to be sent: '{}'", description);
                        boolean successful = responseSent.await(5, TimeUnit.SECONDS);
                        if (successful) {
                            break;
                        } else if (interrupted.get()) {
                            return;
                        }
                    }
                    LOGGER.debug("Error for '{}' was sent to caller", description);
                } catch (InterruptedException ex) {
                    LOGGER.error("The request was interrupted", ex);
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            progressExecutor.shutdownNow();
            responseExecutor.shutdownNow();
            context.getRequestsCount().decrementAndGet();
            context.getProcessingRequestsCount().decrementAndGet();
            context.getTaskStorage().remove(description);
        }
    }

    private void clearLockRegion(RegionModel regionModel) {
        regionLocks.remove(regionModel.toIdentityString());
    }

    private void unlockRegion(RegionModel regionModel) {
        getLockForRegion(regionModel).unlock();
    }

    private void lockRegion(RegionModel regionModel) {
        getLockForRegion(regionModel).lock();
    }

    private ReentrantLock getLockForRegion(RegionModel regionModel) {
        return regionLocks.computeIfAbsent(regionModel.toIdentityString(), k -> new ReentrantLock());
    }

    private void startProgressCalculator() {
        AtomicLong start = new AtomicLong(Long.MAX_VALUE);
        long abortTimeoutMs = regionServerConfiguration.getMasterConfiguration().getProgressCalculatorAbortTimeoutMs();
        progressExecutor.scheduleWithFixedDelay(() -> {
            if (interrupted.get()) {
                return;
            }
            // Operation interruption timeout
            if (System.currentTimeMillis() - start.get() > abortTimeoutMs) {
                LOGGER.warn("Unable to send progress for '{}' more than {} ms. Aborting ...", description, abortTimeoutMs);
                interrupted.set(true);
                progressSent.countDown();
                responseSent.countDown();
            } else {
                try {
                    LOGGER.debug("Sending progress '{}' to caller for '{}'", taskProgress.get(), description);
                    responseObserver.get().onNext(RegionServerProto.RegionOperationProgress.newBuilder()
                            .setDetails(taskProgress.get())
                            .setDone(false)
                            .build());
                    start.set(Long.MAX_VALUE);
                    progressSent.countDown();
                } catch (Exception ex) {
                    LOGGER.error("Unable to send progress to caller for '{}'. Reason: '{}'. Will be another try after {} ms",
                            description, getExceptionText(ex), regionServerConfiguration.getMasterConfiguration().getProgressCalculatorIntervalMs());
                    if (start.get() == Long.MAX_VALUE) {
                        start.set(System.currentTimeMillis());
                    }
                }
            }
        }, 0, regionServerConfiguration.getMasterConfiguration().getProgressCalculatorIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private void sendComplete() {
        long start = System.currentTimeMillis();
        long abortTimeoutMs = regionServerConfiguration.getMasterConfiguration().getProgressCalculatorAbortTimeoutMs();
        responseExecutor.scheduleWithFixedDelay(() -> {
            if (interrupted.get()) {
                return;
            }
            try {
                // Protect against sending twice
                if (responseSent.getCount() > 0) {
                    // Operation interruption timeout
                    if (System.currentTimeMillis() - start > abortTimeoutMs) {
                        LOGGER.warn("Unable to send result for '{}' more than {} ms. Aborting ...", description, abortTimeoutMs);
                        interrupted.set(true);
                        progressSent.countDown();
                    } else {
                        LOGGER.debug("Sending result to caller for '{}'", description);
                        responseObserver.get().onNext(RegionServerProto.RegionOperationProgress.newBuilder()
                                .setDetails("task finished")
                                .setDone(true)
                                .build());
                        // If the connection can't be closed cleanly, that's fine, go on:
                        // what matters is that the operation result has been sent
                        try {
                            responseObserver.get().onCompleted();
                        } catch (Exception ignore) {
                            LOGGER.warn("Unable to close channel to caller properly for '{}'", description);
                        }
                    }
                    responseSent.countDown();
                } else {
                    LOGGER.debug("Multiply result send to caller prevented for '{}'", description);
                }
            } catch (Exception ex) {
                LOGGER.error("Unable to send result to caller for '{}'. Reason: '{}'. Will be another try after {} ms",
                        description, getExceptionText(ex), regionServerConfiguration.getMasterConfiguration().getProgressCalculatorIntervalMs());
            }
        }, 0, regionServerConfiguration.getMasterConfiguration().getProgressCalculatorIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private void sendError(Throwable e) {
        long start = System.currentTimeMillis();
        long abortTimeoutMs = regionServerConfiguration.getMasterConfiguration().getProgressCalculatorAbortTimeoutMs();
        responseExecutor.scheduleWithFixedDelay(() -> {
            if (interrupted.get()) {
                return;
            }
            try {
                // Protect against sending twice
                if (responseSent.getCount() > 0) {
                    // Operation interruption timeout
                    if (System.currentTimeMillis() - start > abortTimeoutMs) {
                        LOGGER.warn("Unable to send error for '{}' more than {} ms. Aborting ...", description, abortTimeoutMs);
                        interrupted.set(true);
                        progressSent.countDown();
                    } else {
                        LOGGER.debug("Sending error to caller for '{}'", description);
                        responseObserver.get().onError(ProtoConverters.toProtoException(e));
                    }
                    responseSent.countDown();
                } else {
                    LOGGER.debug("Multiply error send to caller prevented for '{}'", description);
                }
            } catch (Exception ex) {
                LOGGER.error("Unable to send error to caller for '{}'. Reason: '{}'. Will be another try after {} ms",
                        description, getExceptionText(ex), regionServerConfiguration.getMasterConfiguration().getProgressCalculatorIntervalMs());
            }
        }, 0, regionServerConfiguration.getMasterConfiguration().getProgressCalculatorIntervalMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public TableName getTableName() {
        return ProtoConverters.toTableName(context.getDescription().getRequest().getTableName());
    }

    @Override
    public RegionTaskType getTaskType() {
        return context.getDescription().getTaskType();
    }

    @Override
    public void sendGracefulStopSignal() {
        progressExecutor.shutdownNow();
        responseExecutor.shutdownNow();
        interrupted.set(true);
        progressSent.countDown();
        responseSent.countDown();
    }

    private String getExceptionText(Exception ex) {
        String reason;
        // Find out the cause of the error
        if (ex.getClass().equals(StatusRuntimeException.class) && ((StatusRuntimeException) ex).getStatus().getCode() == Status.Code.CANCELLED) {
            // The master is gone
            reason = "Connection reset";
        } else {
            // Another error
            reason = ex.getClass() + " " + ex.getMessage();
        }
        return reason;
    }

    // Replaces the gRPC connection used to send the progress, the result or the error
    @Override
    public void setProgressResponseObserver(StreamObserver<RegionServerProto.RegionOperationProgress> responseObserver) {
        this.responseObserver.set(responseObserver);
    }

    @Override
    public void setDataResponseObserver(StreamObserver<RegionServerProto.RegionDirectoryStructure> responseObserver) {
        throw new NotImplementedException("Unable to set data response observer to progressed task");
    }

    @Override
    public StreamObserver<RegionServerProto.RegionDirectoryStructure> getDataResponseObserver() {
        throw new NotImplementedException("Unable to get data response observer from progressed task");
    }

    @Override
    public String toString() {
        return description.toString();
    }
}
