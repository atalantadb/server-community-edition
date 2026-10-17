/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.pool;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.light.atalanta.*;
import com.light.atalanta.client.connection.ConnectionPoolSize;
import com.light.atalanta.client.connection.nio.transport.NioSelectorLoopGroup;
import com.light.atalanta.config.AtalantaProperties;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.light.atalanta.config.AtalantaProperties.ATALANTA_ZOOKEEPER_CONNECTION_STRING;

public class ConnectionPoolNio implements Connection {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionPoolNio.class);
    private static final int CLOSE_THREADS = 16;
    private final ConnectionNio connection;
    private final NioSelectorLoopGroup selectorLoopGroup;
    private final Configuration atalantaConfiguration;
    private final MutationProcessorsInitializer mutationProcessorsInitializer;

    private volatile List<AsyncMutationProcNio> asyncMutationProcNioList = null;

    private final Map<TableName, TableAsyncMutationNio> tablesCache = new ConcurrentHashMap<>();

    private final AtomicBoolean isClose = new AtomicBoolean(false);
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private final Lock lifecycleLock = new ReentrantLock();

    public ConnectionPoolNio(Configuration configuration) throws IOException {
        LOGGER.info("Create a new nio connection pool to {}",
                configuration.getOrDefault(ATALANTA_ZOOKEEPER_CONNECTION_STRING, "unknown"));
        configuration.checkConfiguration();
        ConnectionPoolSize.resolve(configuration);
        NioSelectorLoopGroup group = new NioSelectorLoopGroup(configuration);
        ConnectionNio mainConnection;
        try {
            mainConnection = new ConnectionNio(configuration, group);
        } catch (Throwable constructionFailure) {
            try {
                group.close();
            } catch (Throwable cleanupFailure) {
                constructionFailure.addSuppressed(cleanupFailure);
            }
            rethrowConstructionFailure(constructionFailure);
            throw new AssertionError("Unreachable");
        }
        this.selectorLoopGroup = group;
        this.connection = mainConnection;
        atalantaConfiguration = configuration;
        this.mutationProcessorsInitializer = () -> initAsyncMutationProcs(configuration);
    }

    ConnectionPoolNio(Configuration configuration, ConnectionNio connection,
                      NioSelectorLoopGroup selectorLoopGroup,
                      MutationProcessorsInitializer mutationProcessorsInitializer) {
        this.atalantaConfiguration = configuration;
        this.connection = connection;
        this.selectorLoopGroup = selectorLoopGroup;
        this.mutationProcessorsInitializer = mutationProcessorsInitializer;
    }

    private List<AsyncMutationProcNio> initAsyncMutationProcs(Configuration configuration) throws IOException {
        final int configuredPoolSize = ConnectionPoolSize.resolve(atalantaConfiguration);
        final int connectionPoolSize = configuredPoolSize > 0
                ? configuredPoolSize : Runtime.getRuntime().availableProcessors();
        List<AsyncMutationProcNio> list = new ArrayList<>();
        try {
            for (int i = 0; i < connectionPoolSize; i++) {
                ConnectionNio processorConnection = new ConnectionNio(configuration,
                        selectorLoopGroup);
                try {
                    list.add(new AsyncMutationProcNioImpl(processorConnection,
                            atalantaConfiguration.getInt(AtalantaProperties.ATALANTA_CONNECTION_POOL_QUEUE_SIZE),
                            atalantaConfiguration.getBoolean(AtalantaProperties.ATALANTA_CLIENT_NIO_DIRECT_DISPATCH)));
                } catch (Throwable processorFailure) {
                    try {
                        processorConnection.close();
                    } catch (Throwable cleanupFailure) {
                        processorFailure.addSuppressed(cleanupFailure);
                    }
                    throw processorFailure;
                }
            }
        } catch (Throwable constructionFailure) {
            for (AsyncMutationProcNio processor : list) {
                try {
                    processor.close();
                } catch (Throwable cleanupFailure) {
                    constructionFailure.addSuppressed(cleanupFailure);
                }
            }
            rethrowConstructionFailure(constructionFailure);
        }
        return list;
    }

    @Override
    public Configuration getConfiguration() {
        return atalantaConfiguration;
    }

    @Override
    public Admin getAdmin() throws IOException {
        ensureOpen();
        return new Admin(connection);
    }

    @Override
    public Table getTable(TableName tableName) throws IOException {
        lifecycleLock.lock();
        try {
            ensureOpen();
            startPoolIfNeededLocked();
            return tablesCache.computeIfAbsent(tableName,
                    (t) -> {
                        try {
                            return new TableAsyncMutationNio(t, connection, asyncMutationProcNioList, tablesCache,
                                    atalantaConfiguration);
                        } catch (NotActualRegionMappingException | IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void startPoolIfNeededLocked() throws IOException {
        if (asyncMutationProcNioList == null) {
            asyncMutationProcNioList = mutationProcessorsInitializer.initialize();
        }
    }

    @FunctionalInterface
    interface MutationProcessorsInitializer {
        List<AsyncMutationProcNio> initialize() throws IOException;
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        ensureOpen();
        return connection.getRegionLocator(tableName);
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
        ensureOpen();
        return new BufferedMutatorAsyncNio(getTable(tableName));
    }

    @Override
    public boolean isClosed() {
        return isClose.get() || connection.isClosed();
    }

    @Override
    public void close() throws IOException {
        closeInternal();
    }

    @Deprecated
    public void stop() throws IOException {
        closeInternal();
    }

    private void closeInternal() throws IOException {
        if (!isClose.compareAndSet(false, true)) {
            awaitCloseCompletion();
            return;
        }

        IOException failure = null;
        lifecycleLock.lock();
        try {
            for (TableAsyncMutationNio table : tablesCache.values()) {
                try {
                    table.destroyTableConnection();
                } catch (Throwable error) {
                    failure = addFailure(failure, error,
                            "Cannot destroy NIO table connection " + table.getName());
                }
            }
            tablesCache.clear();
            if (asyncMutationProcNioList != null) {
                try {
                    IOException processorFailure = closeMutationProcessors();
                    if (processorFailure != null) {
                        failure = addFailure(failure, processorFailure, "Cannot close NIO mutation processors");
                    }
                } catch (Throwable error) {
                    failure = addFailure(failure, error, "Cannot close NIO mutation processors");
                }
            }
            try {
                connection.close();
            } catch (Throwable error) {
                failure = addFailure(failure, error, "Cannot close main NIO connection");
            }
            try {
                selectorLoopGroup.close();
            } catch (Throwable error) {
                failure = addFailure(failure, error, "Cannot close NIO selector loops");
            }
        } finally {
            lifecycleLock.unlock();
            if (failure == null) {
                closeFuture.complete(null);
            } else {
                closeFuture.completeExceptionally(failure);
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    private IOException closeMutationProcessors() {
        ExecutorService executor = Executors.newFixedThreadPool(CLOSE_THREADS,
                new ThreadFactoryBuilder().setNameFormat("atalanta-nio-close-%d").build());
        try {
            List<CompletableFuture<Throwable>> closes = asyncMutationProcNioList.stream()
                    .map(processor -> CompletableFuture.supplyAsync(() -> {
                        try {
                            processor.close();
                            return null;
                        } catch (Throwable error) {
                            return error;
                        }
                    }, executor))
                    .collect(Collectors.toList());
            CompletableFuture.allOf(closes.toArray(new CompletableFuture[0])).join();

            IOException failure = null;
            for (CompletableFuture<Throwable> close : closes) {
                Throwable error = close.join();
                if (error != null) {
                    failure = addFailure(failure, error, "Cannot close NIO mutation processor");
                }
            }
            return failure;
        } finally {
            executor.shutdown();
        }
    }

    private void awaitCloseCompletion() throws IOException {
        try {
            closeFuture.get();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for NIO connection pool to close", error);
        } catch (ExecutionException error) {
            Throwable cause = error.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Cannot close NIO connection pool", cause);
        }
    }

    private static IOException addFailure(IOException current, Throwable error, String message) {
        IOException next = error instanceof IOException
                ? (IOException) error
                : new IOException(message, error);
        if (current == null) {
            return next;
        }
        if (current != next) {
            current.addSuppressed(next);
        }
        return current;
    }

    private static void rethrowConstructionFailure(Throwable error) throws IOException {
        if (error instanceof IOException) {
            throw (IOException) error;
        }
        if (error instanceof RuntimeException) {
            throw (RuntimeException) error;
        }
        if (error instanceof Error) {
            throw (Error) error;
        }
        throw new IOException("Cannot create NIO connection pool", error);
    }

    private void ensureOpen() throws IOException {
        if (isClose.get() || connection.isClosed()) {
            throw new IOException("NIO connection pool is closed");
        }
    }

    public void flush() throws IOException {
        ensureOpen();
        for (Table t : tablesCache.values()) {
            t.flush();
        }
    }
}
