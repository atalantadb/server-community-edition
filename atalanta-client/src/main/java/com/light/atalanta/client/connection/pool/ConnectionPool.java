/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.pool;

import com.light.atalanta.*;
import com.light.atalanta.client.connection.ConnectionPoolSize;
import com.light.atalanta.config.AtalantaProperties;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.light.atalanta.config.AtalantaProperties.ATALANTA_CONNECTION_POOL_REUSABLE;

@Deprecated
public class ConnectionPool implements Connection {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionPool.class);
    private final ConnectionThreadLocal connection;
    private final Configuration atalantaConfiguration;

    private final List<AsyncMutationProc> asyncMutationProcList;

    private final Map<TableName, TableAsyncMutation> tablesCache = new ConcurrentHashMap<>();

    private final AtomicBoolean isClose = new AtomicBoolean(false);

    public ConnectionPool(Configuration configuration) throws IOException {
        ConnectionPoolSize.resolve(configuration);
        this.connection = ConnectionFactoryThreadLocal.createConnection(configuration);
        atalantaConfiguration = configuration;
        asyncMutationProcList = initAsyncMutationProcs();
    }

    ConnectionPool(Configuration configuration, ConnectionThreadLocal connection,
                   List<AsyncMutationProc> asyncMutationProcList) {
        this.connection = connection;
        this.atalantaConfiguration = configuration;
        this.asyncMutationProcList = asyncMutationProcList;
    }

    private List<AsyncMutationProc> initAsyncMutationProcs() {
        final int configuredPoolSize = ConnectionPoolSize.resolve(atalantaConfiguration);
        final int connectionPoolSize = configuredPoolSize > 0
                ? configuredPoolSize : Runtime.getRuntime().availableProcessors();
        return IntStream.range(0, connectionPoolSize)
                .mapToObj(i -> new AsyncMutationProcImpl(connection, atalantaConfiguration.getInt(AtalantaProperties.ATALANTA_CONNECTION_POOL_QUEUE_SIZE)))
                .collect(Collectors.toList());
    }

    @Override
    public Configuration getConfiguration() {
        return atalantaConfiguration;
    }

    @Override
    public Admin getAdmin() throws IOException {
        return connection.getAdmin();
    }

    @Override
    public Table getTable(TableName tableName) throws IOException {
        return tablesCache.computeIfAbsent(tableName,
                (t) -> {
                    try {
                        return new TableAsyncMutation(t, connection, asyncMutationProcList, tablesCache);
                    } catch (NotActualRegionMappingException | IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        return connection.getRegionLocator(tableName);
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
        return new BufferedMutatorAsync(getTable(tableName));
    }

    @Override
    public boolean isClosed() {
        return connection.isClosed();
    }

    @Override
    public void close() throws IOException {
        if (atalantaConfiguration.getBoolean(ATALANTA_CONNECTION_POOL_REUSABLE)) {
            for (AsyncMutationProc asyncMutationProc : asyncMutationProcList) {
                asyncMutationProc.waitAllOperation();
            }
            return;
        }
        stop();
    }

    public void stop() throws IOException {
        synchronized (this) {
            if (isClose.get()) {
                throw new RuntimeException("Connection already closed!");
            }
            if (asyncMutationProcList != null) {
                for (AsyncMutationProc asyncMutationProc : asyncMutationProcList) {
                    asyncMutationProc.close();
                }
            }
            isClose.set(true);
        }
        for (TableAsyncMutation t : tablesCache.values()) {
            t.destroyTableConnection();
        }
        connection.close();
    }
}
