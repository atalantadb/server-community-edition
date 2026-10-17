/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.grpc;

import io.grpc.*;

import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;
import com.light.atalanta.grpc.MasterServiceGrpc;

public class MasterInterceptor implements ServerInterceptor {

    private Context.Key<String> clientIp = Context.keyWithDefault("clientIp", "unknown");
    private final Set<Thread> activeCommands = new HashSet<>();
    private volatile boolean shuttingDown;

    public static boolean isControlMethod(String method) {
        return method.equals(MasterServiceGrpc.getShutdownRsMethod().getFullMethodName())
                || method.equals(MasterServiceGrpc.getPingMethod().getFullMethodName());
    }

    public boolean isShuttingDown() {
        return shuttingDown;
    }

    public synchronized void beginShutdown() {
        shuttingDown = true;
        activeCommands.forEach(Thread::interrupt);
    }

    private boolean reject(ServerCall<?, ?> call) {
        if (!shuttingDown) return false;
        call.close(Status.UNAVAILABLE.withDescription("Master is shutting down"), new Metadata());
        return true;
    }
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall,
                                                                 Metadata metadata,
                                                                 ServerCallHandler<ReqT, RespT> serverCallHandler) {
        SocketAddress socketAddress = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
        String remoteIp = socketAddress != null ? socketAddress.toString() : "unknown";
        boolean control = isControlMethod(serverCall.getMethodDescriptor().getFullMethodName());
        if (!control && reject(serverCall)) return new ServerCall.Listener<ReqT>() {};
        ServerCall.Listener<ReqT> listener = Contexts.interceptCall(Context.current().withValue(clientIp, remoteIp),
                serverCall,metadata, serverCallHandler);
        if (control) return listener;
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(listener) {
            @Override public void onHalfClose() {
                synchronized (MasterInterceptor.this) {
                    if (reject(serverCall)) return;
                    activeCommands.add(Thread.currentThread());
                }
                try {
                    super.onHalfClose();
                } finally {
                    synchronized (MasterInterceptor.this) {
                        activeCommands.remove(Thread.currentThread());
                        if (shuttingDown) Thread.interrupted();
                    }
                }
            }
        };
    }

    public String getClientIp() {
        return clientIp.get();
    }
}
