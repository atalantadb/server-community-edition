/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.pool;

import com.light.atalanta.ClusterTopology;
import com.light.atalanta.ConnectionThreadLocal;
import com.light.atalanta.DbConfiguration;
import com.light.atalanta.Table;
import com.light.atalanta.TableName;
import com.light.atalanta.client.connection.nio.transport.TRSClientConnectionImpl;
import com.light.atalanta.client.connection.nio.transport.NioSelectorLoopGroup;
import com.light.atalanta.client.connection.nio.transport.NioShutdownLogging;
import com.light.atalanta.config.AtalantaProperties;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.*;
import com.light.atalanta.transport.TRSClientConnection;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConnectionNio extends ConnectionThreadLocal {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionNio.class);
    private final NioClusterConnector nioClusterConnector;
    private final NioSelectorLoopGroup ownedSelectorLoopGroup;
    private final NioSelectorLoopGroup selectorLoopGroup;

    // Physical NIO transports owned by this connection and shared by all its tables.
    // A table may keep a borrowed reference to an entry for routing, but does not own its lifecycle.
    // Transports are shut down and removed from this map by ConnectionNio
    // (or by the transport itself after a fatal I/O error).
    private final Map<String, TRSClientConnection> coreConnections = new ConcurrentHashMap<>();
    private final AtomicBoolean closing = new AtomicBoolean();

    protected final ConcurrentHashMap<TableName, Table> tables = new ConcurrentHashMap<>();

    public ConnectionNio(DbConfiguration conf
    ) throws IOException {
        this(conf,
                createOwnedSelectorLoopGroup(conf), true);
    }

    private static NioSelectorLoopGroup createOwnedSelectorLoopGroup(DbConfiguration conf) throws IOException {
        conf.checkConfiguration();
        return new NioSelectorLoopGroup(conf);
    }

    ConnectionNio(DbConfiguration conf,
                  NioSelectorLoopGroup selectorLoopGroup) throws IOException {
        this(conf,
                selectorLoopGroup, false);
    }

    private ConnectionNio(DbConfiguration conf,
                          NioSelectorLoopGroup selectorLoopGroup,
                          boolean ownsSelectorLoopGroup) throws IOException {
        conf.checkConfiguration();
        this.atalantaConfiguration = conf;
        this.conf = conf;
        this.ownedSelectorLoopGroup = ownsSelectorLoopGroup ? selectorLoopGroup : null;
        this.selectorLoopGroup = selectorLoopGroup;
        NioClusterConnector connector;
        try {
            ZkConfig zkConfig = new ZkConfig(
                    conf.get(AtalantaProperties.ATALANTA_ZOOKEEPER_CONNECTION_STRING));
            this.zooHelper = new ZookeeperHelperImpl(
                    atalantaConfiguration,
                    zkConfig,
                    conf.get(AtalantaProperties.ATALANTA_ZOOKEEPER_PATH),
                    conf.getInt(AtalantaProperties.ATALANTA_ZOOKEEPER_RETRY_COUNT),
                    conf.getInt(AtalantaProperties.ATALANTA_ZOOKEEPER_RETRY_DELAY_MS),
                    conf.getInt(AtalantaProperties.ATALANTA_ZOOKEEPER_SESSION_TIMEOUT_MS)
            );
            this.masterGrpcChannel = buildGrpcChannel();
            this.masterGrpcStub = buildMasterGrpcStub(conf);
            this.zooKeeperCoreContext = new ZooKeeperCoreContext(zooHelper, new ZookeeperHelperExtended(zooHelper));
            this.topology = new ClusterTopology(zooKeeperCoreContext, conf);
            connector = new NioClusterConnector(zooKeeperCoreContext, conf, selectorLoopGroup);
        } catch (Throwable error) {
            closePartiallyInitializedResources(error);
            if (ownedSelectorLoopGroup != null) {
                try {
                    ownedSelectorLoopGroup.close();
                } catch (Throwable cleanupFailure) {
                    error.addSuppressed(cleanupFailure);
                }
            }
            rethrowConstructionFailure(error);
            throw new AssertionError("Unreachable");
        }
        this.nioClusterConnector = connector;
    }

    public NioShutdownLogging.Scope beginShutdownLogging() {
        return selectorLoopGroup.beginShutdownLogging();
    }

    private void closePartiallyInitializedResources(Throwable constructionFailure) {
        if (masterGrpcChannel != null) {
            try {
                masterGrpcChannel.shutdownNow();
                boolean terminated = masterGrpcChannel.awaitTermination(
                        atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_MASTER_DISCONNECTION_TIMEOUT_MS),
                        TimeUnit.MILLISECONDS);
                if (!terminated) {
                    constructionFailure.addSuppressed(
                            new IOException("Partially initialized master channel did not terminate"));
                }
            } catch (InterruptedException cleanupFailure) {
                Thread.currentThread().interrupt();
                constructionFailure.addSuppressed(cleanupFailure);
            } catch (Throwable cleanupFailure) {
                constructionFailure.addSuppressed(cleanupFailure);
            }
        }
        if (zooHelper != null) {
            try {
                zooHelper.close();
            } catch (Throwable cleanupFailure) {
                constructionFailure.addSuppressed(cleanupFailure);
            }
        }
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
        throw new IOException("Cannot create NIO connection", error);
    }

    public synchronized TRSClientConnection getConnectionToCore(String regionServerName) throws IOException {
        if (closing.get() || isClosed()) {
            throw new IOException("NIO connection is closing or already closed");
        }
        TRSClientConnection cached = coreConnections.get(regionServerName);
        TRSClientConnection result = coreConnections.computeIfAbsent(regionServerName, k -> {
            try {
                return nioClusterConnector.getConnectionToCore(k, this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        LOGGER.trace("Resolved core transport: core={}, transportId={}, cacheHit={}, connectionId={}",
                regionServerName, Integer.toHexString(System.identityHashCode(result)), cached == result,
                Integer.toHexString(System.identityHashCode(this)));
        return result;
    }

    // Closes this connection and all physical transports it owns. Tables are cleaned up first,
    // which only detaches their borrowed NIO references. Then the owner closes a snapshot of
    // the distinct transports. Synchronization prevents concurrent transport creation.
    @Override
    public synchronized void close() {
        if (!closing.compareAndSet(false, true)) {
            return;
        }
        try {
            super.close();
        } finally {
            try {
                closeCoreConnections();
            } finally {
                closeOwnedSelectorLoopGroup();
            }
        }
    }

    private void closeOwnedSelectorLoopGroup() {
        if (ownedSelectorLoopGroup == null) {
            return;
        }
        try {
            ownedSelectorLoopGroup.close();
        } catch (IOException error) {
            LOGGER.warn("Cannot close selector loops owned by NIO connection", error);
        }
    }

    private void closeCoreConnections() {
        Set<TRSClientConnection> transports = Collections.newSetFromMap(new IdentityHashMap<>());
        transports.addAll(coreConnections.values());
        coreConnections.clear();

        for (TRSClientConnection transport : transports) {
            try {
                LOGGER.trace("Closing owned core transport: transportId={}, location={}, connectionId={}",
                        Integer.toHexString(System.identityHashCode(transport)), transport.getLocation(),
                        Integer.toHexString(System.identityHashCode(this)));
                transport.close();
            } catch (IOException e) {
                LOGGER.warn("Cannot close owned core transport {}", transport.getLocation(), e);
            }
        }
    }

    @Override
    protected Table buildTable(TableName tableName, Map<RegionServerModel, List<RegionModel>> regionsMapping)
            throws IOException,
            NotActualRegionMappingException {
        return new NioTable(this, tableName, regionsMapping, atalantaConfiguration);
    }

    void flush() throws IOException {
        for (Table t : tables.values()) {
            t.flush();
        }
    }

    public void removeConnectionToCore(TRSClientConnectionImpl connection) {
        String core = coreConnections.entrySet().stream()
                .filter(e -> e.getValue() == connection)
                .map(Map.Entry::getKey)
                .findAny()
                .orElse(null);
        boolean removed = core != null && coreConnections.remove(core, connection);
        LOGGER.trace("Evict closed core transport: core={}, transportId={}, location={}, removed={}, " +
                        "cachedEntryFound={}, connectionId={}",
                core, Integer.toHexString(System.identityHashCode(connection)), connection.getLocation(), removed,
                core != null, Integer.toHexString(System.identityHashCode(this)));
    }

    @Override
    public Table getTable(TableName tableName) throws IOException {
        try {
            // Publish only a fully initialized table, once per key. Do not lock
            // this connection: construction waits for bootstrap workers that
            // acquire its monitor in NioTable.initSingleConnection().
            return tables.computeIfAbsent(tableName, name -> {
                try {
                    Table table = constructNewTable(name);
                    final Thread curThread = Thread.currentThread();
                    LOGGER.info("New table: {}, thread: {}, threadId: {}", name, curThread, curThread.getId());
                    return table;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }
}
