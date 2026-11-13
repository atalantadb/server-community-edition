/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.grpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.light.atalanta.grpc.MasterProto.ShutdownRsError;
import com.light.atalanta.grpc.MasterProto.ShutdownRsResponse;
import com.light.atalanta.grpc.RegionServerProto.RegionServerShutdownRequest;
import com.light.atalanta.grpc.RegionServerProto.RegionServerShutdownResponse;
import com.light.atalanta.grpc.RegionServerServiceGrpc;
import com.light.atalanta.master.services.Master;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.util.ProtoConverters;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** One operation per master process, independent of the ordinary command pools. */
public final class MasterShutdown implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterShutdown.class);
    private final Master master;
    private final MasterInterceptor interceptor;
    private final long timeoutMs;
    private final Consumer<Boolean> finish;
    private final ExecutorService workers = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
            .setDaemon(true).setNameFormat("atalanta-master-shutdown-%d").build());
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setDaemon(true).setNameFormat("atalanta-master-shutdown-deadline-%d").build());
    // Null means that no response has arrived. Entries are never removed on ZK changes.
    private final Map<RegionServerModel, RegionServerShutdownResponse> responses = new LinkedHashMap<>();
    private CompletableFuture<ShutdownRsResponse> result;
    private Deadline deadline;
    private boolean targetsKnown;
    private String preparationError = "";

    public MasterShutdown(Master master, MasterInterceptor interceptor, long timeoutMs, Consumer<Boolean> finish) {
        this.master = master;
        this.interceptor = interceptor;
        this.timeoutMs = timeoutMs;
        this.finish = finish;
    }

    public synchronized void shutdown(StreamObserver<ShutdownRsResponse> observer) {
        if (result != null) {
            reply(observer);
            return;
        }
        deadline = Deadline.after(timeoutMs, TimeUnit.MILLISECONDS);
        result = new CompletableFuture<>();
        reply(observer);
        // Set the recovery flag before interrupting anything: recovery must not halt JVM early
        master.getRecoverRSContextInstance().beginShutdown();
        interceptor.beginShutdown();
        timer.schedule(() -> complete(true), Math.max(0, deadline.timeRemaining(TimeUnit.NANOSECONDS)), TimeUnit.NANOSECONDS);
        workers.execute(this::prepare);
        LOGGER.info("shutdown_rs accepted, timeout={} ms", timeoutMs);
    }

    private void reply(StreamObserver<ShutdownRsResponse> observer) {
        result.thenAccept(response -> {
            try {
                observer.onNext(response);
                observer.onCompleted();
            } catch (RuntimeException e) {
                // A disconnected client must not cancel shutdown or prevent other replies
                LOGGER.warn("Unable to deliver shutdown_rs result", e);
            }
        });
    }

    private void prepare() {
        try {
            master.stopForShutdown();
        } catch (Exception e) {
            synchronized (this) {
                preparationError = "Unable to stop master background work: " + e;
            }
        }
        try {
            List<RegionServerModel> targets = master.getShutdownRegionServers();
            synchronized (this) {
                if (result.isDone()) return;
                targets.forEach(rs -> responses.put(rs, null));
                targetsKnown = true;
                LOGGER.info("shutdown_rs targets: {}", responses.keySet());
                if (responses.isEmpty()) {
                    complete(false);
                    return;
                }
                for (RegionServerModel rs : responses.keySet()) workers.execute(() -> send(rs));
            }
        } catch (Exception e) {
            synchronized (this) {
                preparationError += " Unable to snapshot running RS: " + e;
                complete(false);
            }
        }
    }

    private void send(RegionServerModel rs) {
        ManagedChannel channel = null;
        RegionServerShutdownResponse response;
        try {
            synchronized (this) {
                if (result.isDone() || deadline.isExpired()) return;
            }
            // Dedicated channel: do not share cancellation/reconnect locks with normal commands
            channel = ManagedChannelBuilder.forAddress(rs.getHost(), rs.getJavaPort()).usePlaintext().build();
            long remainingMs = deadline.timeRemaining(TimeUnit.MILLISECONDS);
            if (remainingMs <= 0) return;
            response = RegionServerServiceGrpc.newBlockingStub(channel).withDeadline(deadline)
                    .shutdown(RegionServerShutdownRequest.newBuilder().setTimeoutMs(remainingMs).build());
        } catch (Exception e) {
            response = RegionServerShutdownResponse.newBuilder().setDetails(e.toString()).build();
        } finally {
            if (channel != null) channel.shutdownNow();
        }
        synchronized (this) {
            if (result.isDone()) return;
            if (deadline.isExpired()) {
                complete(true);
                return;
            }
            responses.put(rs, response);
            if (responses.values().stream().noneMatch(r -> r == null)) complete(false);
        }
    }

    private synchronized void complete(boolean timedOut) {
        if (result.isDone()) return;
        ShutdownRsResponse.Builder response = ShutdownRsResponse.newBuilder();
        String details = preparationError;
        if (!targetsKnown) details += " Running RS snapshot is unavailable.";
        if (timedOut) details += " shutdown_rs deadline exceeded.";
        response.setDetails(details.trim());
        responses.forEach((rs, reply) -> {
            if (reply == null || !reply.getSuccess()) {
                response.addErrors(ShutdownRsError.newBuilder().setRegionServer(ProtoConverters.toProtoRegionServer(rs))
                        .setDetails(reply == null ? "No response before shutdown_rs deadline"
                                : reply.getDetails().isEmpty() ? "RS did not confirm shutdown" : reply.getDetails()));
            }
        });
        response.setSuccess(targetsKnown && details.isEmpty() && response.getErrorsCount() == 0);
        ShutdownRsResponse completed = response.build();
        LOGGER.info("shutdown_rs completed: {}", completed);
        result.complete(completed); // enqueue all client replies before stopping gRPC
        timer.shutdownNow();
        Thread exit = new Thread(() -> {
            workers.shutdownNow();
            finish.accept(completed.getSuccess());
        }, "atalanta-master-shutdown-exit");
        exit.setDaemon(true);
        exit.start();
    }

    @Override public void close() {
        workers.shutdownNow();
        timer.shutdownNow();
    }
}
