/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.grpc;

import com.light.atalanta.grpc.RegionServerProto.RegionServerShutdownRequest;
import com.light.atalanta.grpc.RegionServerProto.RegionServerShutdownResponse;
import com.light.atalanta.grpc.RegionServerServiceGrpc;
import com.light.atalanta.region.server.scheduled.CoreHealthChecker;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import com.light.atalanta.transport.TRSConnectionStub;
import com.light.atalanta.transport.TransportConfiguration;
import com.light.atalanta.transport.TransportConnectionFactory;
import com.light.atalanta.transport.messages.TableRegionActionRequest;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Status;
import io.grpc.ServerInterceptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Metadata;
import io.grpc.ForwardingServerCallListener;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.*;

/** One core-wide shutdown per RS process, independent of region task queues. */
public final class RegionServerShutdown implements Closeable, ServerInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionServerShutdown.class);
    private final CoreHealthChecker healthChecker;
    private final RegionServerConfiguration conf;
    private final Runnable finish;
    private final CoreRequest coreRequest;
    private final ExecutorService worker = Executors.newSingleThreadExecutor(task -> daemon(task, "atalanta-rs-shutdown"));
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(task -> daemon(task, "atalanta-rs-shutdown-timeout"));
    private CompletableFuture<RegionServerShutdownResponse> result;

    public RegionServerShutdown(RegionServerConfiguration conf, CoreHealthChecker healthChecker, Runnable finish) {
        this(conf, healthChecker, finish, null);
    }

    RegionServerShutdown(RegionServerConfiguration conf, CoreHealthChecker healthChecker, Runnable finish, CoreRequest coreRequest) {
        this.conf = conf;
        this.healthChecker = healthChecker;
        this.finish = finish;
        this.coreRequest = coreRequest == null ? this::requestCoreShutdown : coreRequest;
    }

    @FunctionalInterface
    interface CoreRequest { boolean send(Deadline deadline) throws Exception; }

    private static Thread daemon(Runnable task, String name) {
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        return thread;
    }

    public synchronized boolean isStarted() { return result != null; }

    @Override public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                           Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        if (isStarted() && !isControlMethod(call.getMethodDescriptor().getFullMethodName())) {
            call.close(Status.UNAVAILABLE.withDescription("Region server is shutting down"), new Metadata());
            return new ServerCall.Listener<ReqT>() { };
        }
        ServerCall.Listener<ReqT> listener = next.startCall(call, headers);
        if (isControlMethod(call.getMethodDescriptor().getFullMethodName())) return listener;
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(listener) {
            @Override public void onHalfClose() {
                if (isStarted()) {
                    call.close(Status.UNAVAILABLE.withDescription("Region server is shutting down"), new Metadata());
                } else {
                    super.onHalfClose();
                }
            }
        };
    }

    static boolean isControlMethod(String method) {
        return method.equals(RegionServerServiceGrpc.getShutdownMethod().getFullMethodName())
                || method.equals(RegionServerServiceGrpc.getPingMethod().getFullMethodName());
    }

    public synchronized void shutdown(RegionServerShutdownRequest request,
                                      StreamObserver<RegionServerShutdownResponse> observer) {
        if (request.getTimeoutMs() <= 0) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription("timeoutMs must be positive").asRuntimeException());
            return;
        }
        if (result != null) {
            reply(observer);
            return;
        }
        Deadline deadline = Deadline.after(request.getTimeoutMs(), TimeUnit.MILLISECONDS);
        if (Context.current().getDeadline() != null) deadline = deadline.minimum(Context.current().getDeadline());
        if (deadline.isExpired()) {
            observer.onError(Status.DEADLINE_EXCEEDED.asRuntimeException());
            return;
        }
        result = new CompletableFuture<>();
        reply(observer);
        startShutdown(deadline);
    }

    private void startShutdown(Deadline budget) {
        Future<?> task = worker.submit(() -> runShutdown(budget));
        ScheduledFuture<?> timeout = timer.schedule(() -> {
            if (result.complete(failure("Timed out shutting down core"))) task.cancel(true);
        }, Math.max(0, budget.timeRemaining(TimeUnit.NANOSECONDS)), TimeUnit.NANOSECONDS);
        result.whenComplete((response, error) -> timeout.cancel(false));
    }

    private void reply(StreamObserver<RegionServerShutdownResponse> observer) {
        result.thenAccept(response -> {
            try {
                observer.onNext(response);
                observer.onCompleted();
            } catch (RuntimeException error) {
                LOGGER.warn("Unable to send RS shutdown reply", error);
            }
        });
    }

    private void runShutdown(Deadline budget) {
        try {
            healthChecker.beginShutdown();
            while (!result.isDone() && !budget.isExpired()) {
                boolean accepted = false;
                try {
                    accepted = coreRequest.send(budget);
                    if (!accepted) LOGGER.warn("Core rejected SHUTDOWN; retrying within remaining budget");
                } catch (Exception error) {
                    if (Thread.currentThread().isInterrupted() || error instanceof InterruptedException) throw error;
                    LOGGER.warn("Core SHUTDOWN attempt failed; retrying", error);
                }
                if (accepted) {
                    if (result.isDone() || budget.isExpired()) break;
                    healthChecker.shutdownCore();
                    if (budget.isExpired()) break;
                    if (result.complete(RegionServerShutdownResponse.newBuilder().setSuccess(true).build())) {
                        // All registered observers have been notified. The owner drains gRPC before exit.
                        finish.run();
                    }
                    return;
                }
                if (result.isDone() || budget.isExpired()) break;
                long pause = Math.max(1, conf.getCoreConfiguration().getConnectionRetrySleepMs());
                TimeUnit.NANOSECONDS.sleep(Math.min(TimeUnit.MILLISECONDS.toNanos(pause),
                        Math.max(0, budget.timeRemaining(TimeUnit.NANOSECONDS))));
            }
            result.complete(failure("Timed out shutting down core"));
        } catch (Exception error) {
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
            LOGGER.error("Unable to shut down core", error);
            result.complete(failure(error.toString()));
        }
    }

    private boolean requestCoreShutdown(Deadline budget) throws Exception {
        long remainingMs = Math.max(1, budget.timeRemaining(TimeUnit.MILLISECONDS));
        long socketTimeout = Math.min(remainingMs, conf.getCoreConfiguration().getSocketTimeoutMs());
        TransportConfiguration transport = TransportConfiguration.defaultConfiguration()
                .set(TransportConfiguration.ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY, Long.toString(socketTimeout))
                // Retries belong to the deadline-aware outer loop, not an unbounded transport loop
                .set(TransportConfiguration.ATALANTA_TRANSPORT_SOCKET_RETRIES_NUMBER_PROPERTY, "1");
        try (TRSConnectionStub connection = TransportConnectionFactory.createConnectionStub(transport,
                conf.getCoreConfiguration().getHost(), conf.getCoreConfiguration().getRsPort())) {
            if (result.isDone() || budget.isExpired()) throw new TimeoutException("Shutdown deadline expired");
            return connection.sendRequestToRs(TableRegionActionRequest.shutdown()).isPositiveAnswer();
        }
    }

    private static RegionServerShutdownResponse failure(String details) {
        return RegionServerShutdownResponse.newBuilder().setSuccess(false).setDetails(details).build();
    }

    @Override public void close() {
        worker.shutdownNow();
        timer.shutdownNow();
    }
}
