/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.config;

public class AtalantaProperties {
    public static final String ATALANTA_ZOOKEEPER_CONNECTION_STRING = "atalanta.zookeeper.connection.string";
    public static final String ATALANTA_ZOOKEEPER_PATH = "atalanta.zookeeper.path";
    public static final String ATALANTA_ZOOKEEPER_RETRY_COUNT = "atalanta.zookeeper.retry.count";
    public static final String ATALANTA_ZOOKEEPER_RETRY_DELAY_MS = "atalanta.zookeeper.retry.delay.ms";
    public static final String ATALANTA_ZOOKEEPER_SESSION_TIMEOUT_MS = "atalanta.zookeeper.session.timeout.ms";
    public static final String ATALANTA_MASTER_CONNECTION_TIMEOUT_MS = "atalanta.master.connection.timeout.ms";
    public static final String ATALANTA_MASTER_DISCONNECTION_TIMEOUT_MS = "atalanta.master.disconnection.timeout.ms";
    public static final String ATALANTA_CREATE_NAMESPACE_TIMEOUT_MS = "atalanta.create.namespace.timeout.ms";
    public static final String ATALANTA_CREATE_TABLE_TIMEOUT_MS = "atalanta.create.table.timeout.ms";
    public static final String ATALANTA_ENABLE_TIMEOUT_MS = "atalanta.enable.table.timeout.ms";
    public static final String ATALANTA_DISABLE_TABLE_TIMEOUT_MS = "atalanta.disable.table.timeout.ms";
    public static final String ATALANTA_DROP_TABLE_TIMEOUT_MS = "atalanta.drop.table.timeout.ms";
    public static final String ATALANTA_MOVE_REGION_TIMEOUT_MS = "atalanta.move.region.timeout.ms";
    public static final String ATALANTA_SPLIT_REGION_TIMEOUT_MS = "atalanta.split.region.timeout.ms";
    public static final String ATALANTA_REBALANCE_REGION_START_TIMEOUT_MS = "atalanta.rebalance.region.start.timeout.ms";
    public static final String ATALANTA_STOP_REBALANCE_TASKS_TIMEOUT_MS = "atalanta.stop.rebalance.tasks.timeout.ms";

    public static final String ATALANTA_CONNECTION_POOL_SIZE = "atalanta.connection.pool.size";
    public static final String ATALANTA_CONNECTION_POOL_HASH_BY_THREAD_ID = "atalanta.connection.pool.hash.by.thread.id";
    public static final String ATALANTA_CONNECTION_POOL_QUEUE_SIZE = "atalanta.connection.pool.queue.size";
    public static final String ATALANTA_CLIENT_NIO_DIRECT_DISPATCH = "atalanta.client.nio.direct.dispatch";
    public static final String ATALANTA_CLIENT_NIO_SELECTOR_LOOP_SIZE = "atalanta.client.nio.selector.loop.size";
    public static final String ATALANTA_CONNECTION_READ_POOL_SIZE = "atalanta.connection.read.pool.size";

    public static final String ATALANTA_CLIENT_SCAN_ITERATION_TIMEOUT = "atalanta.client.scan.iteration.timeout";
    public static final String ATALANTA_CONNECTION_RETRY_COUNT = "atalanta.connection.retry.count";
    public static final String ATALANTA_CONNECTION_RETRY_DELAY_MS = "atalanta.connection.retry.delay.ms";

    @Deprecated
    public static final String ATALANTA_CONNECTION_POOL_REUSABLE = "atalanta.connection.pool.reusable"; // legacy for ConnectionNio
    public static final String ATALANTA_CONNECTION_POOL_NAME = "atalanta.connection.pool.name";
    public static final String ATALANTA_CONNECTION_POOL_DISCONNECT_TIMEOUT_MS = "atalanta.connection.pool.disconnect.timeout.ms";
    public static final String ATALANTA_STOP_REGION_TIMEOUT_MS = "atalanta.stop.region.timeout.ms";
    public static final String ATALANTA_SHUTDOWN_RS_TIMEOUT_MS = "atalanta.shutdown.rs.timeout.ms";
    public static final String ATALANTA_START_REGION_TIMEOUT_MS = "atalanta.start.region.timeout.ms";

    public static final String ATALANTA_CLIENT_GET_TABLE_RETRY = "atalanta.client.get.table.retry";
    public static final String ATALANTA_CLIENT_GET_TABLE_TIMEOUT_MS = "atalanta.client.get.table.timeout.ms";

    public static final String ATALANTA_CLIENT_TABLE_RETRIES_NUMBER = "atalanta.client.table.retries.number";
    public static final String ATALANTA_CLIENT_TABLE_RETRIES_TIMEOUT_MS = "atalanta.client.table.retries.timeout.ms";

    public static final String ATALANTA_CLIENT_ENABLE_BULK_API = "atalanta.client.enable.bulk.api";
    /**
     * @deprecated The transport now selects the bulk header from its connection type.
     */
    @Deprecated
    public static final String ATALANTA_CLIENT_BULK_HEADER_V2 = "atalanta.client.bulk.header.v2";

    /**
     * @deprecated Compatibility switch for the obsolete V1 scan protocol.
     * Keep enabled until the legacy protocol is removed.
     */
    @Deprecated
    public static final String ATALANTA_HBASE_SCAN_FILTER = "atalanta.hbase.scan.filter";
    public static final String ATALANTA_CLIENT_MIN_BATCH_SIZE = "atalanta.client.min.batch.size";
    public static final String ATALANTA_CLIENT_AUTO_FLUSH_SIZE = "atalanta.client.auto.flush.size";

}
