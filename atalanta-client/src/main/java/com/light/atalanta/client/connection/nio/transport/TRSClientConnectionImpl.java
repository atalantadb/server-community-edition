/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.transport;

import com.light.atalanta.*;
import com.light.atalanta.client.connection.nio.pool.ConnectionNio;
import com.light.atalanta.exceptions.AtalantaTransportRuntimeException;
import com.light.atalanta.transport.*;
import com.light.atalanta.transport.buffers.ByteArrayBuffer;
import com.light.atalanta.transport.exceptions.*;
import com.light.atalanta.transport.messages.*;
import com.light.atalanta.transport.values.RegionInfo;
import com.light.atalanta.transport.values.*;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static com.light.atalanta.transport.TransportConfiguration.*;
import static com.light.atalanta.transport.messages.OperationFactory.checkReturnedExceptions;
import static com.light.atalanta.util.FutureUtil.waitAndGet;

/** Shared NIO transport: wire writes are serialized, response waits never hold the send monitor. */
public class TRSClientConnectionImpl implements TRSClientConnection, TRSClientAsyncConnection {

    private enum CloseReason {
        NETWORK_FAILURE,
        SEND_FAILURE,
        RECEIVE_FAILURE,
        REQUEST_TIMEOUT,
        BOOTSTRAP_FAILURE,
        CLOSED_TRANSPORT_DETECTED,
        CONNECTION_SHUTDOWN
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TRSClientConnectionImpl.class);
    private static final long TIMEOUT_CHECK_INTERVAL_MS = 500;
    private static final Set<TRSClientConnectionImpl> ACTIVE_TRANSPORTS = ConcurrentHashMap.newKeySet();
    private static final ScheduledExecutorService TIMEOUT_CHECKER =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "atalanta-nio-request-timeout");
                thread.setDaemon(true);
                return thread;
            });

    static {
        TIMEOUT_CHECKER.scheduleWithFixedDelay(TRSClientConnectionImpl::checkRequestTimeouts,
                TIMEOUT_CHECK_INTERVAL_MS, TIMEOUT_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private SocketChannel socketChannel;
    private ChannelContext context;
    private final String location;
    private final Map<NameOfTable, TableRegionServerInfo> info = new ConcurrentHashMap<>();
    private final NioSelectorLoop nioSelectorLoop;
    private final TransportConfiguration transportConfiguration;
    private final RepeatableExecutorFactory repeatableExecutorFactory;
    private final InetSocketAddress rsAddress;
    private final Map<NameOfTable, Integer> tablePosMap = new ConcurrentHashMap<>();
    private final Map<NameOfTable, TableMapObject> tableMapRequests = new ConcurrentHashMap<>();
    private final Map<NameOfTable, CompletableFuture<Void>> regionMapRefreshes = new ConcurrentHashMap<>();
    private int curTablePos = 0;
    private final boolean isTrace;
    private final int requestTimeoutMs;
    // Owns the transport lifecycle; tables only borrow it
    private final ConnectionNio connectionNio;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object bootstrapLock = new Object();
    // Guarded by this (the same monitor as wire writes). Never wait for a response under it:
    // selector callbacks can submit another request before processing the AUTH/MAP response.
    private Thread bootstrapOwner;
    private final Queue<DeferredSend> deferredSends = new ArrayDeque<>();

    private static final class DeferredSend {
        final ByteBuffer buffer;
        final NameOfTable tableName;
        final boolean regionMapRefresh;
        final CompletableFuture<byte[]> future;

        DeferredSend(ByteBuffer buffer, NameOfTable tableName, boolean regionMapRefresh,
                     CompletableFuture<byte[]> future) {
            this.buffer = buffer;
            this.tableName = tableName;
            this.regionMapRefresh = regionMapRefresh;
            this.future = future;
        }
    }

    public TRSClientConnectionImpl(TransportConfiguration configuration, String host, int port,
                                   NioSelectorLoop nioSelectorLoop, ConnectionNio connectionNio) throws IOException {
        this.transportConfiguration = configuration;
        this.nioSelectorLoop = nioSelectorLoop;
        this.connectionNio = connectionNio;
        this.location = host + ":" + port;
        this.rsAddress = new InetSocketAddress(host, port);
        this.repeatableExecutorFactory = new RepeatableExecutorFactory(transportConfiguration);
        this.isTrace = LOGGER.isTraceEnabled();
        this.requestTimeoutMs = Integer.parseInt(configuration.get(ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY));
        connect();
        ACTIVE_TRANSPORTS.add(this);
        LOGGER.trace("Created core transport: transportId={}, location={}, connectionId={}",
                Integer.toHexString(System.identityHashCode(this)), location,
                Integer.toHexString(System.identityHashCode(connectionNio)));
    }

    private void connect() throws IOException {
        socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        int receiveBufferSize = Integer.parseInt(transportConfiguration.get(ATALANTA_TRANSPORT_SOCKET_RECEIVE_BUFFER_SIZE_PROPERTY));
        int sendBufferSize = Integer.parseInt(transportConfiguration.get(ATALANTA_TRANSPORT_SOCKET_SEND_BUFFER_SIZE_PROPERTY));
        Preconditions.checkArgument(receiveBufferSize >= 0,
                ATALANTA_TRANSPORT_SOCKET_RECEIVE_BUFFER_SIZE_PROPERTY + " must be greater than or equal to zero");
        Preconditions.checkArgument(sendBufferSize >= 0,
                ATALANTA_TRANSPORT_SOCKET_SEND_BUFFER_SIZE_PROPERTY + " must be greater than or equal to zero");
        if (receiveBufferSize > 0) {
            socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, receiveBufferSize);
        }
        if (sendBufferSize > 0) {
            socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, sendBufferSize);
        }
        boolean tcpNoDelay = Boolean.parseBoolean(transportConfiguration.get(ATALANTA_TRANSPORT_SOCKET_TCP_NO_DELAY_PROPERTY));
        socketChannel.socket().setTcpNoDelay(tcpNoDelay);
        socketChannel.connect(rsAddress);

        int timeout = Integer.parseInt(transportConfiguration.get(ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY));
        socketChannel.socket().setSoTimeout(timeout);
        while (!socketChannel.finishConnect()) {
        } // wait for connection to be established
        
            context = new ChannelContext(this::handleInvalidRegionMap,
                    error -> closeSocketChannelAndStaleFutures(CloseReason.NETWORK_FAILURE, error));
        
        this.nioSelectorLoop.register(socketChannel, context);
    }

    private void handleInvalidRegionMap(NameOfTable tableName) {
        refreshRegionMap(tableName);
    }

    @Override
    public CompletableFuture<Void> refreshRegionMap(NameOfTable tableName) {
        CompletableFuture<Void> marker = new CompletableFuture<>();
        CompletableFuture<Void> running = regionMapRefreshes.putIfAbsent(tableName, marker);
        if (running != null) {
            LOGGER.debug("Region map refresh is already running for table {}, core {}", tableName, location);
            return running;
        }

        CompletableFuture.supplyAsync(() -> requestRegionMapRefresh(tableName))
                .thenCompose(refresh -> refresh)
                .whenComplete((unused, error) -> {
                    if (error == null) {
                        marker.complete(null);
                    } else {
                        marker.completeExceptionally(error);
                        LOGGER.warn("Cannot refresh region map for table {}, core {}", tableName, location, error);
                    }
                    regionMapRefreshes.remove(tableName, marker);
                });
        return marker;
    }

    @Override
    public int getRegionPosition(NameOfTable tableName, byte[] rowKey) {
        TableRegionServerInfo tableInfo = info.get(tableName);
        return tableInfo == null ? RegionPosition.UNSET.getPosition() : tableInfo.getRegionPosition(rowKey);
    }

    private CompletableFuture<Void> requestRegionMapRefresh(NameOfTable tableName) {
        TableMapObject mapObject = tableMapRequests.get(tableName);
        if (mapObject == null) {
            return failedFuture(new IllegalStateException("Unknown table: " + tableName));
        }

        try {
            TransportMessage request = createRegionMapRequest(mapObject);
            LOGGER.debug("Send region map refresh for table {}, core {}", tableName, location);
            return sendMessage0(request, tableName, true).thenAccept(bytes -> {
                TableRegionMapResponse response = new TableRegionMapResponse(bytes);
                if (response.getMessageType() == MessageType.INVALID_REGION_MAP_ERROR) {
                    throw new CompletionException(new NotActualRegionMappingException(location));
                }
                if (response.getMessageType() != MessageType.CURRENT_REGION_MAP) {
                    throw new IllegalStateException("Unexpected region map refresh response: " +
                            response.getMessageType());
                }
                constructTableRegionServerInfo(mapObject, response, tableName);
                LOGGER.debug("Region map refreshed for table {}, core {}", tableName, location);
            });
        } catch (IOException | RuntimeException e) {
            return failedFuture(e);
        }
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable error) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(error);
        return future;
    }

    @Override
    public String getLocation() {
        return this.location;
    }

    @Override
    public CompletableFuture<byte[]> sendBytes(ByteBuffer bb, int length) throws IOException {
        return sendBytes(bb, length, null, false);
    }

    private synchronized CompletableFuture<byte[]> sendBytes(ByteBuffer bb, int length, NameOfTable tableName,
                                                              boolean regionMapRefresh) throws IOException {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        if (!closed.get() && bootstrapOwner != null && bootstrapOwner != Thread.currentThread()) {
            // Serialization buffers may be reused by the caller as soon as send returns
            ByteBuffer source = bb.duplicate();
            source.limit(length).position(0);
            ByteBuffer copy = ByteBuffer.allocate(length);
            copy.put(source);
            deferredSends.add(new DeferredSend(copy, tableName, regionMapRefresh, future));
            return future;
        }
        return writeBytes(bb, length, tableName, regionMapRefresh, future);
    }

    // Called only under the send monitor, including when draining deferred requests
    private CompletableFuture<byte[]> writeBytes(ByteBuffer bb, int length, NameOfTable tableName,
                                                 boolean regionMapRefresh, CompletableFuture<byte[]> future)
            throws IOException {
        if (closed.get() || !socketChannel.isOpen() || !socketChannel.isConnected()) {
            LOGGER.trace("Send rejected on closed transport: transportId={}, location={}, table={}, " +
                            "regionMapRefresh={}, open={}, connected={}, connectionId={}",
                    Integer.toHexString(System.identityHashCode(this)), location, tableName, regionMapRefresh,
                    socketChannel.isOpen(), socketChannel.isConnected(),
                    Integer.toHexString(System.identityHashCode(connectionNio)));
            closeSocketChannelAndStaleFutures(CloseReason.CLOSED_TRANSPORT_DETECTED, null);
            future.completeExceptionally(new AtalantaTransportRuntimeException("Connection closed: " + location));
            return future;
        }
        bb.limit(length);
        ByteBuffer bufferToSend = bb;

        bufferToSend.flip();
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(requestTimeoutMs);
        context.pendingRequests.add(new PendingRequest(future, tableName, regionMapRefresh, deadlineNanos));
        long startedNanos = System.nanoTime();
        int requestWriteCalls = 0;
        int requestZeroWrites = 0;
        try {
            while (bufferToSend.hasRemaining()) {
                int l = socketChannel.write(bufferToSend);
                requestWriteCalls++;
                if (l == 0) {
                    requestZeroWrites++;
                    if (System.nanoTime() >= deadlineNanos) {
                        throw new SocketTimeoutException("Sending request to core " + location
                                + " timed out after " + requestTimeoutMs + " ms");
                    }
                    LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(500));
                }
                if (isTrace) {
                    LOGGER.trace("sendBytes: {} bytes, to: {}", l, socketChannel.getRemoteAddress());
                }
            }
        } catch (Exception e) {
            LOGGER.error("sendBytes error!", e);
            closeSocketChannelAndStaleFutures(CloseReason.SEND_FAILURE, e);
        }
        if (isTrace) {
            LOGGER.trace("send to core {} bytes, msgType={}, to {}", length,
                    MessageType.fromByte(bb.array()[4]), socketChannel.getRemoteAddress());
        }
        return future;
    }

    @Override
    public void close() throws IOException {
        closeSocketChannelAndStaleFutures(CloseReason.CONNECTION_SHUTDOWN, null);
    }

    @Override
    public TableRegionServerInfo authenticateAndGetTableRegionServerInfo(
            List<String> tableRegions, NameOfTable tableName)
            throws IOException, NotActualRegionMappingException {
        // Only bootstrap callers wait here, never ordinary sends or selector completion callbacks
        synchronized (bootstrapLock) {
            synchronized (this) {
                if (closed.get()) {
                    throw new IOException("Connection closed: " + location);
                }
                bootstrapOwner = Thread.currentThread();
            }
            try {
                LOGGER.trace("Begin exclusive AUTH/MAP: table={}, core={}", tableName, location);
                return TRSClientConnection.super.authenticateAndGetTableRegionServerInfo(tableRegions, tableName);
            } catch (IOException | NotActualRegionMappingException | RuntimeException | Error error) {
                // After a failed/interrupted AUTH or MAP, core state is not safe for queued data
                closeSocketChannelAndStaleFutures(CloseReason.BOOTSTRAP_FAILURE, error);
                throw error;
            } finally {
                finishBootstrap();
            }
        }
    }

    private void finishBootstrap() {
        while (true) {
            // Keep the gate active until the FIFO is drained. Release the monitor between writes
            // so selector callbacks can enqueue their next request without waiting for the backlog.
            synchronized (this) {
                DeferredSend send = deferredSends.poll();
                if (send == null) {
                    bootstrapOwner = null;
                    LOGGER.trace("Finish exclusive AUTH/MAP: core={}, closed={}", location, closed.get());
                    return;
                }
                try {
                    writeBytes(send.buffer, send.buffer.position(), send.tableName, send.regionMapRefresh, send.future);
                } catch (IOException | RuntimeException error) {
                    closeSocketChannelAndStaleFutures(CloseReason.SEND_FAILURE, error);
                    send.future.completeExceptionally(error);
                }
            }
        }
    }

    @Override
    public TableRegionServerInfo getTableRegionServerInfo(List<String> tableRegions, NameOfTable tableName)
            throws IOException, NotActualRegionMappingException {
        synchronized (tablePosMap) {
            Integer tablePosition = tablePosMap.get(tableName);
            if (tablePosition == null) {
                tablePosition = curTablePos++;
                tablePosMap.put(tableName, tablePosition);
                LOGGER.trace("Assigned table position on shared transport: table={}, tablePosition={}, core={}",
                        tableName, tablePosition, location);
            } else {
                LOGGER.trace("Preserve table position during region map refresh: table={}, tablePosition={}, core={}",
                        tableName, tablePosition, location);
            }
        }
        TableMapObject mapObject = new TableMapObject(tableName, tableRegions);
        tableMapRequests.put(tableName, mapObject);
        TableRegionMapResponse regionMap = getRegionMapResponse(mapObject, tableName);
        if (regionMap.getMessageType() == MessageType.INVALID_REGION_MAP_ERROR) {
            throw new NotActualRegionMappingException(location);
        }
        return constructTableRegionServerInfo(mapObject, regionMap, tableName);
    }

    @Override
    public void sendBulkPut(BulkPutOperation bulkPut, NameOfTable tableName) throws IOException {
        throw new UnsupportedOperationException();
    }

    private TableRegionServerInfo constructTableRegionServerInfo(TableMapObject mapObject, TableRegionMapResponse regionMap, NameOfTable tableName) {
        List<RegionInfo> rInfoList = new ArrayList<>();
        for (int i = 0; i < regionMap.getRegionInfos().length; i++) {
            RegionName rName = mapObject.getRegionNames().get(i);
            RegionInfoResponseObject regionInfoResponseObject = regionMap.getRegionInfos()[i];
            RegionInfo rInfo = new RegionInfo(tableName, rName, new RegionPosition(regionInfoResponseObject.getRegionPosition()),
                    regionInfoResponseObject.getStartKey(), regionInfoResponseObject.getEndKey());
            rInfoList.add(rInfo);
        }

        TableRegionServerInfo tableRSInfo = new TableRegionServerInfo(tableName, rInfoList);
        info.put(tableName, tableRSInfo);
        LOGGER.debug("Table region server info created");
        return tableRSInfo;
    }

    TableRegionMapResponse getRegionMapResponse(TableMapObject mapObject, NameOfTable tableName) throws NotActualRegionMappingException, IOException {
        TransportMessage requestRegMap = createRegionMapRequest(mapObject);
        LOGGER.debug("Send regions map request to core {}", location);
        CompletableFuture<byte[]> response = sendMessage(requestRegMap, tableName);
        LOGGER.debug("Send regions map request to core complete {}", location);
        TableRegionMapResponse regionMap = (TableRegionMapResponse) receiveTableMap(requestRegMap, response);
        LOGGER.debug("Receive regions map from core: {}, core {}", regionMap, location);
        return regionMap;
    }

    private TransportMessage createRegionMapRequest(TableMapObject mapObject) {
        if (Boolean.parseBoolean(transportConfiguration.get(ATALANTA_TRANSPORT_TABLE_MAP_TIMEOUT_ENABLE))) {
            int timeout = Integer.parseInt(transportConfiguration.get(ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY));
            return new TableRegionMapRequestWithTimeout(mapObject, timeout);
        }
        return new TableRegionMapRequest(mapObject);
    }

    TransportMessage receiveTableMap(TransportMessage requestRegMap, CompletableFuture<byte[]> response) throws IOException, NotActualRegionMappingException {
        try {
            RepeatableExecutor repeatableExecutor = repeatableExecutorFactory.getExecutor(RepeatableExecutorFactory.ACTION_RECEIVE_MESSAGE);
            return repeatableExecutor
                    .skipException(NotActualRegionMappingException.class)
                    .skipException(SocketTimeoutException.class)
                    .skipException(SocketException.class)
                    .withExceptionalAction(UnavailableCoreException.class,
                            () -> closeSocketChannelAndStaleFutures(CloseReason.RECEIVE_FAILURE, null), false)
                    .execute(() -> {
                        try {
                            return receiveMessage0(response);
                        } catch (NotActualRegionMappingException | UnavailableCoreException | IOException e) {
                            LOGGER.warn("error receiveMessage 0, core: {}", location, e);
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            LOGGER.error("Cant execute 'receiveMessage' with retries: {}, core: {}", location, e.getMessage());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else if (e.getCause() instanceof NotActualRegionMappingException) {
                throw (NotActualRegionMappingException) e.getCause();
            }
            throw e;
        }
    }

    private TransportMessage receiveMessage0(CompletableFuture<byte[]> future) throws IOException,
            NotActualRegionMappingException, UnavailableCoreException {
        try {
            byte[] bytes = future.get();
            if (isTrace) {
                LOGGER.trace("Receive {} bytes: {}", bytes.length, bytes);
            }
            MessageType type = OperationHelper.getTransportMessageType(bytes);
            if (type == MessageType.CURRENT_REGION_MAP || type == MessageType.INVALID_REGION_MAP_ERROR) {
                TableRegionMapResponse response = new TableRegionMapResponse(bytes);
                if (isTrace) {
                    LOGGER.trace("Receive TableRegionMapResponse: {}", response);
                }
                return response;
            }
            TransportMessage tm = OperationFactory.fromBytes(transportConfiguration, type, bytes, location);
            if (isTrace) {
                LOGGER.trace("Receive transport message: {}", tm);
            }
            return tm;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        } catch (CancellationException | ExecutionException ex) {
            throw new NotActualRegionMappingException(location);
        }
    }

    public CompletableFuture<byte[]> sendMessage(TransportMessage m, NameOfTable tableName) throws IOException {
        try {
            return sendMessage0(m, tableName);
        } catch (IOException e) {
            LOGGER.warn("error sendMessage, core: {}", location, e);
            closeSocketChannelAndStaleFutures(CloseReason.SEND_FAILURE, e);
            throw new RuntimeException(e);
        }
    }

    private void closeSocketChannelAndStaleFutures(CloseReason reason, Throwable cause) {
        if (!closed.compareAndSet(false, true)) {
            LOGGER.trace("Core transport already closed: transportId={}, location={}, requestedReason={}, " +
                            "connectionId={}",
                    Integer.toHexString(System.identityHashCode(this)), location, reason,
                    Integer.toHexString(System.identityHashCode(connectionNio)));
            return;
        }
        ACTIVE_TRANSPORTS.remove(this);
        LOGGER.trace("Closing core transport: transportId={}, location={}, reason={}, causeType={}, " +
                        "causeMessage={}, open={}, connected={}, pendingRequests={}, thread={}, connectionId={}",
                Integer.toHexString(System.identityHashCode(this)), location, reason,
                cause == null ? null : cause.getClass().getName(), cause == null ? null : cause.getMessage(),
                socketChannel.isOpen(), socketChannel.isConnected(), context.pendingRequests.size(),
                Thread.currentThread().getName(),
                Integer.toHexString(System.identityHashCode(connectionNio)));
        try {
            socketChannel.close();
        } catch (IOException e) {
            LOGGER.warn("Cannot close socket channel", e);
        }
        connectionNio.removeConnectionToCore(this);
        List<DeferredSend> deferred;
        synchronized (this) {
            deferred = new ArrayList<>(deferredSends);
            deferredSends.clear();
        }
        PendingRequest staleRequest;
        while ((staleRequest = context.pendingRequests.poll()) != null) {
            staleRequest.future.completeExceptionally(new AtalantaTransportRuntimeException("Connection closed: " + location));
        }
        for (DeferredSend send : deferred) {
            send.future.completeExceptionally(new AtalantaTransportRuntimeException("Connection closed: " + location));
        }
    }

    private static void checkRequestTimeouts() {
        long now = System.nanoTime();
        for (TRSClientConnectionImpl transport : ACTIVE_TRANSPORTS) {
            try {
                transport.checkRequestTimeout(now);
            } catch (Throwable error) {
                LOGGER.error("Cannot check request timeout for core {}", transport.location, error);
            }
        }
    }

    private void checkRequestTimeout(long now) {
        PendingRequest request = context.pendingRequests.peek();
        if (request == null || now - request.deadlineNanos < 0) {
            return;
        }

        TimeoutException error = new TimeoutException(
                "Request to core " + location + " timed out after " + requestTimeoutMs + " ms");
        if (!request.future.isDone()) {
            LOGGER.warn(error.getMessage());
            closeSocketChannelAndStaleFutures(CloseReason.REQUEST_TIMEOUT, error);
        }
    }

    @Override
    public CompletableFuture<byte[]> sendMessage0(TransportMessage message, NameOfTable tableName) throws IOException {
        return sendMessage0(message, tableName, false);
    }

    private CompletableFuture<byte[]> sendMessage0(TransportMessage message, NameOfTable tableName,
                                                    boolean regionMapRefresh) throws IOException {
        if (message instanceof TablePositionedTransportMessage) {
            ((TablePositionedTransportMessage) message).setTablePosition(tablePosMap.get(tableName));
        }
        final TransportMessage send;
        final ByteBuffer buffer;
        try {
            boolean isBigMessage = message.getMessageSize() > SocketConnection.CHUNK_MESSAGE_SIZE
                    && message instanceof Operation;
            send = isBigMessage ? new BigResultOperation((Operation) message) : message;
            ByteArrayBuffer<TransportMessage> tb = send.getBuffer();
            buffer = tb.serializeMessage(send);
            if (send instanceof BulkGetOperation) {
                buffer.put(4, MessageType.BULK_GET_OPERATION_NIO.getTypeByte());
            } else if (send instanceof BulkPutOperation) {
                buffer.put(4, MessageType.BULK_PUT_OPERATION_NIO.getTypeByte());
            }
        } catch (AtalantaSerializationException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new AtalantaSerializationException("Cannot serialize transport message "
                    + message.getClass().getName(), e);
        }
        return sendBytes(buffer, buffer.position(), tableName, regionMapRefresh);
    }

    @Override
    public List<Pair<List<PutOperation>, CompletableFuture<byte[]>>> sendBulkPutAsync(BulkPutOperation bulkPut,
                                                                                       NameOfTable tableName) throws IOException {
        try {
            List<BulkPutOperation> puts = splitBulkByChunks(bulkPut, tableName);
            List<Pair<List<PutOperation>, CompletableFuture<byte[]>>> responses = new ArrayList<>(puts.size());
            for (BulkPutOperation p : puts) {
                CompletableFuture<byte[]> response = null;
                List<PutOperation> operations = (List<PutOperation>) p.getOperations();
                if (operations.size() == 1 && !fitsIntoBulkContainer(operations.get(0))) {
                    Operation op = p.getOperations().get(0);
                    response = sendMessage(op, tableName);
                    responses.add(new Pair<>(operations, response));
                } else {
                    response = sendMessage(p, tableName);
                    responses.add(new Pair<>(operations, response));
                }
            }
            return responses;
        } catch (Exception e) {
            LOGGER.error("Cant execute 'sendMessage' with retries: {}, core: {}", location, e.getMessage());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }

    @Override
    public void flushBulkPut(List<CompletableFuture<byte[]>> responses) throws NotActualRegionMappingException {
        for (CompletableFuture<byte[]> response : responses) {
            byte[] received = new byte[0];
            try {
                received = response.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                throw new NotActualRegionMappingException(location);
            }
            if (received.length < 16) {
                checkReturnedExceptions(transportConfiguration, OperationHelper.getTransportMessageType(received), location);
            }
        }
    }

    private <T extends BulkOperation> List<T> splitBulkByChunks(T bulkOperation, NameOfTable tableName) {
        int chunkSize = Integer.parseInt(transportConfiguration.get(ATALANTA_TRANSPORT_BULK_CHUNK_SIZE));
        chunkSize = chunkSize < 32768 ? chunkSize : 32768;
        // Every bulk request sent by this transport receives a table position
        // in sendMessage0 and therefore always uses the 13-byte NIO header
        int bulkHeaderSize = BulkGetOperation.TRANSPORT_MESSAGE_HEADER_LENGTH + Integer.BYTES;
        int serializedBulkSize = Math.addExact(bulkOperation.getMessageSize(), Integer.BYTES);
        if (serializedBulkSize <= chunkSize) {
            return Collections.singletonList(bulkOperation);
        }

        List<T> splitMessages = new ArrayList<>();
        int size = bulkHeaderSize;
        List<Operation> operations = new ArrayList<>();
        for (Operation op : bulkOperation.getOperations()) {
            int messageSize = op.getMessageSize();
            if (size + messageSize > chunkSize && !operations.isEmpty()) {
                splitMessages.add((T) createBulkOperation(bulkOperation, operations, true, tableName));
                operations.clear();
                size = bulkHeaderSize;
            }
            operations.add(op);
            size += messageSize;
        }
        if (!operations.isEmpty()) {
            splitMessages.add((T) createBulkOperation(bulkOperation, operations, false, tableName));
        }
        return splitMessages;
    }

    private boolean fitsIntoBulkContainer(Operation operation) {
        int chunkSize = Integer.parseInt(transportConfiguration.get(ATALANTA_TRANSPORT_BULK_CHUNK_SIZE));
        chunkSize = chunkSize < 32768 ? chunkSize : 32768;
        return fitsIntoBulkContainer(operation, chunkSize);
    }

    static boolean fitsIntoBulkContainer(Operation operation, int chunkSize) {
        // NIO bulk messages always contain the table position on the wire
        int bulkHeaderSize = BulkGetOperation.TRANSPORT_MESSAGE_HEADER_LENGTH + Integer.BYTES;
        return (long) bulkHeaderSize + operation.getMessageSize() <= chunkSize;
    }

    private BulkOperation createBulkOperation(BulkOperation bulkOperation, List<Operation> operations, boolean isNewList, NameOfTable tableName) {
        List<? extends Operation> list = isNewList ? new ArrayList<>(operations) : operations;
        return bulkOperation instanceof BulkGetOperation
                ? new BulkGetOperation(bulkOperation.getRegionPosition().getPosition(), tablePosMap.get(tableName), (List<GetOperation>) list)
                : new BulkPutOperation(bulkOperation.getRegionPosition().getPosition(), tablePosMap.get(tableName), (List<PutOperation>) list);
    }

    @Override
    public ResultOperation[] sendBulkGet(BulkGetOperation bulkGet, NameOfTable tableName) throws IOException, NotActualRegionMappingException {
        try {
            List<BulkGetOperation> gets = splitBulkByChunks(bulkGet, tableName);
            List<BulkResponseOperation> responseOperations = new ArrayList<>();
            List<CompletableFuture<byte[]>> futures = new ArrayList<>();
            for (BulkGetOperation g : gets) {
                try {
                    CompletableFuture<byte[]> op = sendMessage(g, tableName);
                    futures.add(op);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            for (CompletableFuture<byte[]> f : futures) {
                try {
                    byte[] bytes = f.get();
                    responseOperations.add(new BulkResponseOperation(bytes));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("The operation was interrupted", e);
                } catch (ExecutionException e) {
                    throw new IOException("Asynchronous task execution failed", e.getCause());
                }

            }
            List<byte[]> responses = waitAndGet(futures);
            for (byte[] bytes : responses) {
                responseOperations.add(new BulkResponseOperation(bytes));
            }
            return responseOperations.stream()
                    .map(BulkResponseOperation::getResultOperations)
                    .flatMap(List::stream)
                    .toArray(ResultOperation[]::new);
        } catch (Exception e) {
            LOGGER.error("Cant execute 'sendMessage' with retries: {}, core: {}", location, e.getMessage());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }
    public List<Pair<List<GetOperation>, CompletableFuture<byte[]>>> sendBulkGetAsync(
            BulkGetOperation bulkGet, NameOfTable tableName) throws IOException, NotActualRegionMappingException {
        try {
            List<BulkGetOperation> gets = splitBulkByChunks(bulkGet, tableName);
            List<Pair<List<GetOperation>, CompletableFuture<byte[]>>> responses = new ArrayList<>(gets.size());
            for (BulkGetOperation g : gets) {
                try {
                    CompletableFuture<byte[]> op = sendMessage(g, tableName);
                    responses.add(new Pair<>(g.getOperations(), op));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return responses;
        } catch (Exception e) {
            LOGGER.error("Cant execute 'sendMessage' with retries: {}, core: {}", location, e.getMessage());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }

    @Override
    public RowCountResponse requestCount(RowCountRequest rowCountRequest, NameOfTable tableName) throws IOException, NotActualRegionMappingException {
        try {
            return new RepeatableExecutor()
                    .withStaticTimeout(100)
                    .withMaxRetries(2)
                    .skipException(NotActualRegionMappingException.class)
                    .execute(() -> {
                        try {
                            return requestCount0(rowCountRequest, tableName);
                        } catch (NotActualRegionMappingException | UnavailableCoreException | IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            LOGGER.error("Cant execute 'requestCount' with retries: {}, core: {}", location, e.getMessage());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else if (e.getCause() instanceof NotActualRegionMappingException) {
                throw (NotActualRegionMappingException) e.getCause();
            }
            throw e;
        }
    }

    @Override
    public byte[] receiveBytes() {
        throw new NotImplementedException("not supported in nio version");
    }

    private RowCountResponse requestCount0(RowCountRequest rowCountRequest, NameOfTable tableName) throws IOException, UnavailableCoreException, NotActualRegionMappingException {
        if (!rowCountRequest.getRegionPosition().isRegionPositionSet()) {
            throw new UnknownRegionException();
        }
        byte[] received = new byte[0];
        try {
            CompletableFuture<byte[]> response = sendMessage0(rowCountRequest, tableName);
            LOGGER.debug("COUNT request: table={}, core={}, transport={}, tablePosition={}, region={}, bytes={}",
                    tableName, location, Integer.toHexString(System.identityHashCode(this)),
                    rowCountRequest.getTablePosition(), rowCountRequest.getRegionPosition().getPosition(), rowCountRequest.getMessageSize());
            received = response.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new NotActualRegionMappingException(location);
        }
        if (received.length < 16) {
            checkReturnedExceptions(transportConfiguration, OperationHelper.getTransportMessageType(received), location);
        }
        RowCountResponse result = new RowCountResponse(received);
        LOGGER.debug("COUNT response: table={}, core={}, transport={}, tablePosition={}, requestedRegion={}, responseRegion={}, count={}",
                tableName, location, Integer.toHexString(System.identityHashCode(this)), rowCountRequest.getTablePosition(),
                rowCountRequest.getRegionPosition().getPosition(), result.getRegionPosition().getPosition(), result.getCounts());
        return result;
    }

    @Override
    public ScanResultContainer makeScan(ScanOperation scan, NameOfTable tableName) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        try {
            return sendScan(scan, tableName);
        } catch (UnavailableCoreException e) {
            throw new UnavailableServerException(e);
        }
    }

    @Override
    public void sendBytes(ByteBuffer bb, int offset, int length) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        throw new NotImplementedException("not supported in nio version");
    }

    @Override
    public void sendRequest(Operation op, TableRegionServerInfo tableInfo) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        throw new NotImplementedException("not supported in nio version");
    }

    @Override
    public Operation getResponse(Operation requests, TableRegionServerInfo tableInfo) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        throw new NotImplementedException("not supported in nio version");
    }

    private ScanResultContainer sendScan(ScanOperation scan, NameOfTable tableName) throws IOException, NotActualRegionMappingException, UnavailableCoreException {
        TableRegionServerInfo tableRegionServerInfo = info.get(tableName);
        if (tableRegionServerInfo == null) {
            throw new UnknownRegionException();
        }
        return getResponseForScan(scan, tableName);
    }

    private ScanResultContainer getResponseForScan(ScanOperation scan, NameOfTable tableName) throws IOException, NotActualRegionMappingException, UnavailableCoreException {

        byte[] received = new byte[0];
        try {
            received = sendOperation(scan, tableName).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new NotActualRegionMappingException(location);
        }

        if (received.length < 16) {
            checkReturnedExceptions(transportConfiguration, OperationHelper.getTransportMessageType(received), location);
        }
        return Boolean.parseBoolean(transportConfiguration.get(ATALANTA_HBASE_SCAN_FILTER))
                ? ScanOperationV2.getResultsFromScan(received, location)
                : ScanOperationV1.getResultsFromScan(received, location);
    }

    private CompletableFuture<byte[]> sendOperation(PositionedTransportMessage m, NameOfTable tableName) throws IOException {
        if (!m.getRegionPosition().isRegionPositionSet()) {
            throw new UnknownRegionException();
        }
        return sendMessage0(m, tableName);
    }

    @Override
    public void sendEmptyAuthRequest() throws IOException {
        try {

            try {
                sendEmptyAuthRequest0();
            } catch (UnavailableCoreException e) {
                LOGGER.warn("error sendEmptyAuthRequest 0, core: {}", location, e);
                throw new RuntimeException(e);
            } catch (IOException e) {
                LOGGER.warn("error sendEmptyAuthRequest 1, core: {}", location, e);
                closeSocketChannelAndStaleFutures(CloseReason.SEND_FAILURE, e);
                throw new RuntimeException(e);
            }

        } catch (Exception e) {
            LOGGER.error("Cant execute 'sendEmptyAuthRequest' with retries: {}, core: {}", location, e.getMessage());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }

    private void sendEmptyAuthRequest0() throws IOException, UnavailableCoreException {
        TransportMessage requestRegMap = EmptyAuthRequest.getInstance();
        try {
            byte[] response = sendMessage0(requestRegMap, null).get();
            if (response == null || response.length == 0) {
                throw new UnavailableCoreException();
            }
            int expectedSize = ClientAuthResultResponse.LENGTH_MESSAGE_SIZE
                    + ClientAuthResultResponse.LENGTH_AUTH_TYPE + ClientAuthResultResponse.LENGTH_AUTH_RESULT;
            if (response.length != expectedSize || OperationHelper.getMessageSize(response) != expectedSize) {
                throw new IOException("Invalid AUTH_NONE response size from " + location);
            }
            ClientAuthResultResponse authResponse = new ClientAuthResultResponse(response);
            MessageType responseType = authResponse.getMessageType();
            if (responseType != MessageType.AUTH_NONE && responseType != MessageType.AUTH) {
                throw new IOException("Unexpected AUTH_NONE response type " + responseType + " from " + location);
            }
            // The core answers the handshake with AUTH_NONE or with an AUTH reply.
            // Only an AUTH reply with an error result is a rejection.
            if (responseType == MessageType.AUTH && authResponse.getResult() == AuthResult.AUTH_ERROR) {
                throw new IOException("Core rejected AUTH_NONE: " + location);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for AUTH from " + location, e);
        } catch (ExecutionException e) {
            throw new UnavailableCoreException();
        }
    }

    public ResultOperation sendGet(GetOperation getOperation, TableName tableName) throws IOException, ExecutionException,
            InterruptedException, NotActualRegionMappingException {
        byte[] response = sendMessage(getOperation, tableName).get();
        return OperationFactory.fromBytes(transportConfiguration, response, location);
    }
}
