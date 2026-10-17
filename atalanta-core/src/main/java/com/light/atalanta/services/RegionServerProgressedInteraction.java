/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.services;

import com.google.common.base.Function;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.NoRetryException;
import com.light.atalanta.exceptions.TableBlockedException;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.util.ProtoConverters;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Interaction with region servers driven by the progress calculator.
 * Runs Progressed tasks.
 */
public class RegionServerProgressedInteraction {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionServerProgressedInteraction.class);

    private final int regionServerTimeoutMs;
    private final int progressCalculatorIntervalMs;
    private final Function<TableName, Boolean> checkIncomeTableRequestsEnabled;

    private static final long LOGGING_INTERVAL_MS = 10 * 60 * 1000L; // 10 min

    public RegionServerProgressedInteraction(int regionServerTimeoutMs, int progressCalculatorIntervalMs, Function<TableName, Boolean> checkIncomeTableRequestsEnabled) {
        this.regionServerTimeoutMs = regionServerTimeoutMs;
        this.progressCalculatorIntervalMs = progressCalculatorIntervalMs;
        this.checkIncomeTableRequestsEnabled = checkIncomeTableRequestsEnabled;
    }

    /**
     * "interruptible" is a flag that shows whether current operation should be interrupted in this case (AND):
     * - we missed progress twice
     * - checkIncomeTableRequestsEnabled returns true
     * .
     * "interrupted" is a reference flag that shows whether current operation should be interrupted in this case (AND):
     * - we missed progress twice
     * - and obviously "interrupted.get()" is true
     */
    public CountDownLatch sendRegionServerRequest(
            TableName tableName,
            String operationName,
            RegionDistributionModel region,
            String reserved,
            AtomicReference<Throwable> exception,
            RegionServerGrpcStub.Async asyncStub,
            ProgressedGrpcAction action,
            boolean interruptible,
            AtomicBoolean interrupted
    ) {
        ScheduledExecutorService progressCalculator = Executors.newSingleThreadScheduledExecutor();
        // Time the last progress update was received from the RS
        AtomicLong lastProgressStamp = new AtomicLong(0L);
        // Time progress was last logged
        AtomicLong lastLoggingStamp = new AtomicLong(System.currentTimeMillis());
        // Start time of the request to the RS
        AtomicLong requestStartStamp = new AtomicLong(System.currentTimeMillis());
        // Deadline of the request to the RS
        AtomicLong requestDeadlineStamp = new AtomicLong(requestStartStamp.get() + regionServerTimeoutMs);
        // Whether the user has been told that the timeout was exceeded
        AtomicBoolean timeoutExceededNotified = new AtomicBoolean(false);

        CountDownLatch latch = new CountDownLatch(1);
        Context.CancellableContext cancellableContext = Context.current().withCancellation();

        // Progress calculator
        progressCalculator.scheduleWithFixedDelay(() -> {
            boolean timeoutExceeded = System.currentTimeMillis() > requestDeadlineStamp.get();
            // The timeout has expired
            if (timeoutExceeded) {
                // Report the exceeded timeout once
                if (!timeoutExceededNotified.get()) {
                    LOGGER.info("{}: timeout exceeded, waiting RS for the progress", operationName);
                    timeoutExceededNotified.set(true);
                }
                boolean progressIsNotSentAtAllowedTime = System.currentTimeMillis() > lastProgressStamp.get() + progressCalculatorIntervalMs * 2L;
                // Abort the request if progress did not arrive in time; otherwise continue
                if (progressIsNotSentAtAllowedTime) {
                    Exception ex = new Exception("Timeout exceeded, frequency of RS progress does not match the required");
                    cancellableContext.cancel(ex);
                    exception.set(ex);
                    latch.countDown();
                    progressCalculator.shutdown();
                    return;
                }
            }
            boolean isFirstProgressShouldArrive = System.currentTimeMillis() > requestStartStamp.get() + (progressCalculatorIntervalMs * 3L / 2);
            if (isFirstProgressShouldArrive) {
                // Check, if progress is missed twice
                boolean isProgressMissingTwice = System.currentTimeMillis() > lastProgressStamp.get() + (progressCalculatorIntervalMs * 5L / 2);
                // For an interruptible task (not recovery and not move) whose progress is missing twice.
                // Moves do not need this: their rollbacks must run even while recovery locks the table
                // (still respecting the lock).
                if (interruptible && isProgressMissingTwice) {
                    LOGGER.info("{}: RS progress is missing twice, checking whether table is blocked by recovery", operationName);
                    // If recovery has locked the table, the RS has stopped the task and will send nothing more.
                    // Stop waiting.
                    boolean incomeTableRequestsBlocked = checkIncomeTableRequestsEnabled.apply(tableName);
                    if (incomeTableRequestsBlocked) {
                        LOGGER.info("{}: RS progress is missing twice, table is blocked by recovery", operationName);
                        Exception ex = new TableBlockedException(tableName);
                        cancellableContext.cancel(ex);
                        exception.set(ex);
                        latch.countDown();
                        progressCalculator.shutdown();
                        return;
                    } else {
                        LOGGER.warn("{}: table is not blocked by recovery", operationName);
                    }
                }
                // If we should interrupt task either way
                if (isProgressMissingTwice) {
                    LOGGER.info("{}: RS progress is missing twice, checking whether the task should be interrupted", operationName);
                    if (interrupted.get()) {
                        LOGGER.info("{}: RS progress is missing twice, interrupting task", operationName);
                        Exception ex = new NoRetryException("task has been interrupted");
                        cancellableContext.cancel(ex);
                        exception.set(ex);
                        latch.countDown();
                        progressCalculator.shutdown();
                    } else {
                        LOGGER.warn("{}: task is not interrupted", operationName);
                    }
                }
            }
        }, 0, 500, TimeUnit.MILLISECONDS);

        cancellableContext.run(() -> {
            StreamObserver<RegionServerProto.RegionOperationProgress> responseObserver = new StreamObserver<RegionServerProto.RegionOperationProgress>() {
                @Override
                public void onNext(RegionServerProto.RegionOperationProgress progress) {
                    lastProgressStamp.set(System.currentTimeMillis());
                    if (progress.getDone()) {
                        progressCalculator.shutdownNow();
                        latch.countDown();
                    } else {
                        // Logging progress once in LOGGING_INTERVAL_MS
                        if (System.currentTimeMillis() - lastLoggingStamp.get() > LOGGING_INTERVAL_MS) {
                            LOGGER.info("{}: received progress '{}' for region '{}'", operationName, progress.getDetails(), region.toShortString());
                            lastLoggingStamp.set(System.currentTimeMillis());
                        }
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    progressCalculator.shutdownNow();
                    // Do not overwrite an error recorded earlier
                    if (exception.get() == null) {
                        exception.set(throwable);
                    }
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    progressCalculator.shutdownNow();
                }
            };

            // Here we shouldn't have LIST_FILES or SEND_BATCH, because they're not progressed,
            // meaning they don't support progress sending.
            // Moreover, we can't shut down progress calculator since this is async action.
            RegionServerProto.RegionRequest request = ProtoConverters.toProtoRegion(tableName, region.getRegionModel(), reserved);
            action.accept(asyncStub, request, responseObserver);
        });

        return latch;
    }
}
