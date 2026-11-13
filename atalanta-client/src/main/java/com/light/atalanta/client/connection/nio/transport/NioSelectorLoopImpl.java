/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.transport;

import com.light.atalanta.transport.exceptions.UnknownTypeException;
import com.light.atalanta.transport.messages.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class NioSelectorLoopImpl implements NioSelectorLoop, Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(NioSelectorLoopImpl.class);
    private final Selector selector;
    private final Thread thread;
    private final Queue<RegistrationTask> tasks = new ConcurrentLinkedQueue<>();
    private final Object lifecycleLock = new Object();
    private volatile boolean isRunning = true;
    private final boolean isTrace;
    private final NioShutdownLogging shutdownLogging;

    public NioSelectorLoopImpl() {
        this(new NioShutdownLogging());
    }

    NioSelectorLoopImpl(NioShutdownLogging shutdownLogging) {
        this.shutdownLogging = shutdownLogging;
        try {
            selector = Selector.open();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        thread = new Thread(this, "atalanta-selector-loop-" + Thread.currentThread().getName());
        thread.setDaemon(true);
        thread.start();
        isTrace = LOGGER.isTraceEnabled();
    }

    @Override
    public synchronized void shutdown() throws IOException, InterruptedException {
        synchronized (lifecycleLock) {
            isRunning = false;
            failPendingRegistrations(new IOException("NIO selector loop is closed"));
        }
        selector.wakeup();
        try {
            if (Thread.currentThread() != thread) {
                thread.join();
            }
        } finally {
            selector.close();
        }
    }

    @Override
    public CompletableFuture<ChannelContext> register(SocketChannel socketChannel, ChannelContext channelContext) {
        CompletableFuture<ChannelContext> regFuture = new CompletableFuture<>();
        synchronized (lifecycleLock) {
            if (!isRunning) {
                regFuture.completeExceptionally(new IOException("NIO selector loop is closed"));
                return regFuture;
            }
            channelContext.expectedShutdown = shutdownLogging.registerConnection();
            tasks.add(new RegistrationTask(socketChannel, channelContext, regFuture));
        }
        selector.wakeup();
        return regFuture;
    }

    public void run() {
        while (isRunning && !Thread.currentThread().isInterrupted()) {
            try {
                RegistrationTask task;
                while ((task = tasks.poll()) != null) task.run();

                selector.select();
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();
                    if (key.isValid()) {
                        if (key.isReadable()) {
                            handleRead(key);
                        }
                    } else {
                        LOGGER.warn("key is invalid {}", ((SocketChannel) key.channel()).getRemoteAddress().toString());
                    }
                }
            } catch (IOException e) {
                if (isRunning) {
                    LOGGER.error("Worker error:" + e);
                }
                ;
            }
        }
        failPendingRegistrations(new IOException("NIO selector loop is closed"));
    }

    private void failPendingRegistrations(IOException error) {
        RegistrationTask task;
        while ((task = tasks.poll()) != null) {
            task.future.completeExceptionally(error);
        }
    }

    private final class RegistrationTask implements Runnable {
        private final SocketChannel socketChannel;
        private final ChannelContext channelContext;
        private final CompletableFuture<ChannelContext> future;

        private RegistrationTask(SocketChannel socketChannel, ChannelContext channelContext,
                                 CompletableFuture<ChannelContext> future) {
            this.socketChannel = socketChannel;
            this.channelContext = channelContext;
            this.future = future;
        }

        @Override
        public void run() {
            if (!isRunning) {
                future.completeExceptionally(new IOException("NIO selector loop is closed"));
                return;
            }
            try {
                socketChannel.register(selector, SelectionKey.OP_READ, channelContext);
                future.complete(channelContext);
            } catch (Exception error) {
                future.completeExceptionally(error);
            }
        }
    }

    private void handleRead(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        ChannelContext context = (ChannelContext) key.attachment();
        ByteBuffer readBuf = context.netReadBuffer;

        try {
            int bytesRead = channel.read(readBuf);
            if (bytesRead == -1) {
                IOException ex = new IOException("lost connection");
                if (context.isExpectedIdleShutdown()) {
                    LOGGER.debug("Connection closed by peer during shutdown_rs: {}", channel.getRemoteAddress());
                } else if (context.pendingRequests.isEmpty()) {
                    // disable/drop can close a table's idle sockets without a shutdown_rs hint.
                    LOGGER.debug("Connection closed by peer with no pending requests: {}", channel.getRemoteAddress());
                } else {
                    LOGGER.warn("Connection lost to {}", channel.getRemoteAddress(), ex);
                }
                closeKey(key, context, ex);
                return;
            } else if (isTrace) {
                LOGGER.trace("read {} bytes from {}", bytesRead, channel.getRemoteAddress());
            }

            readBuf.flip();

                appendToAccumulator(context, readBuf);
                readBuf.clear();
            
            processPackets(context, channel);

        } catch (IOException e) {
            closeKey(key, context, e);
        }
    }

    private void appendToAccumulator(ChannelContext context, ByteBuffer source) {
        if (context.accumulator.remaining() < source.remaining()) {
            int newCapacity = (context.accumulator.capacity() + source.remaining()) * 2;
            if (isTrace) {
                LOGGER.debug("Resizing accumulator to {}, context: {}", newCapacity, context);
            }
            ByteBuffer newAcc = ByteBuffer.allocate(newCapacity);
            context.accumulator.flip();
            newAcc.put(context.accumulator);
            context.accumulator = newAcc;
        }
        context.accumulator.put(source);
    }

    private void processPackets(ChannelContext context, SocketChannel channel) throws IOException {
        ByteBuffer acc = context.accumulator.order(LITTLE_ENDIAN);
        acc.flip();

        while (acc.remaining() >= 4) { // While a length prefix is available
            acc.mark(); // Mark the position before the length prefix
            int totalMessageLength = acc.getInt();

            if (totalMessageLength < 5) {
                IOException ex = new IOException("Invalid response length: " + totalMessageLength);
                LOGGER.warn("Invalid response received from {}. Closing socket.", channel.getRemoteAddress(), ex);
                throw ex;
            }

            int bodyLength = totalMessageLength - 4;

            if (acc.remaining() < bodyLength) { // The response body is incomplete
                acc.reset(); // Rewind to the length prefix
                break;
            }

            // Read the message body together with its length prefix
            byte[] responseBody = new byte[bodyLength + Integer.BYTES];
            acc.position(acc.position() - Integer.BYTES);
            acc.get(responseBody);

            if (isTrace) {
                LOGGER.trace("Response size: {}, type: {}, host: {}",
                        totalMessageLength, getMessageTypeNoException(responseBody),
                        channel.getRemoteAddress());
            }
            PendingRequest pendingRequest = context.pendingRequests.poll();
            if (pendingRequest != null) {
                // The original operation must see the core response and decide on a retry at the upper level.
                // The map refresh below only prepares the connection for such a retry.
                pendingRequest.future.complete(responseBody);
            }
            if (responseBody[4] == MessageType.INVALID_REGION_MAP_ERROR.getTypeByte()) {
                LOGGER.warn("INVALID_REGION_MAP_ERROR received from {}.", channel.getRemoteAddress());
                if (pendingRequest != null && pendingRequest.regionMapRefresh) {
                    LOGGER.warn("Region map refresh rejected for table {} by {}. Further refresh will not be scheduled.",
                            pendingRequest.tableName, channel.getRemoteAddress());
                } else if (pendingRequest != null && pendingRequest.tableName != null &&
                        context.invalidRegionMapHandler != null) {
                    try {
                        context.invalidRegionMapHandler.accept(pendingRequest.tableName);
                    } catch (RuntimeException e) {
                        LOGGER.error("Cannot handle INVALID_REGION_MAP_ERROR for table {} from {}.",
                                pendingRequest.tableName, channel.getRemoteAddress(), e);
                    }
                }
            }
        }
        acc.compact();
    }

    private static String getMessageTypeNoException(byte[] responseBody) {
        try {
            return MessageType.fromByte(responseBody[4]).name();
        } catch (UnknownTypeException e) {
            return "type/scan block : " + responseBody[4];
        }
    }

    private void closeKey(SelectionKey key, ChannelContext context, Throwable ex) {
        try {
            key.channel().close();
        } catch (IOException ignored) {
            LOGGER.warn("Cannot to closed connection {}", key, ignored);
        }
        key.cancel();
        PendingRequest pendingRequest;
        while ((pendingRequest = context.pendingRequests.poll()) != null) {
            pendingRequest.future.completeExceptionally(ex);
        }
        try {
            context.terminalConnectionFailureHandler.accept(ex);
        } catch (RuntimeException handlerError) {
            LOGGER.warn("Cannot handle terminal connection failure for {}", key, handlerError);
        }
    }
}
