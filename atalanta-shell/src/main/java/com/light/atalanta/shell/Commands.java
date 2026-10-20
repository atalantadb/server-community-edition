/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell;

public final class Commands {

    public static final String CREATE_NAMESPACE = "create_namespace";

    public static final String CREATE_TABLE = "create";

    public static final String ALTER_TABLE = "alter";

    public static final String ENABLE_TABLE = "enable";

    public static final String DISABLE_TABLE = "disable";

    public static final String DROP_TABLE = "drop";

    public static final String IS_EXISTS = "is_exists";

    public static final String IS_ENABLED = "is_enabled";

    public static final String IS_ASSIGNED = "is_assigned";

    public static final String IS_DISABLED = "is_disabled";

    public static final String IS_CREATING = "is_creating";

    public static final String IS_CREATED = "is_created";

    public static final String IS_DISABLING = "is_disabling";

    public static final String IS_DROPPING = "is_dropping";

    public static final String IS_RECOVERING = "is_recovering";

    public static final String IS_SPLITTING = "is_splitting";

    public static final String IS_NAMESPACE_EXISTS = "is_namespace_exists";

    public static final String LIST = "list";

    public static final String LIST_NAMESPACE = "list_namespace";

    public static final String LIST_NAMESPACE_TABLES = "list_namespace_tables";

    public static final String LIST_REGIONS = "list_regions";

    public static final String LIST_REGION_SERVERS = "list_region_servers";

    public static final String PUT = "put";

    public static final String PUT_BYTES = "put_bytes";

    public static final String GET = "get";

    public static final String GET_BYTES = "get_bytes";

    public static final String DELETE = "delete";

    public static final String DELETE_BYTES = "delete_bytes";

    public static final String SCAN = "scan";

    public static final String COUNT = "count";

    public static final String MOVE = "move";

    public static final String SPLIT = "split";

    public static final String REBALANCE_REPLICAS = "rebalance_replicas";

    public static final String STOP_REBALANCE_REPLICAS = "stop_rebalance_replicas";

    public static final String REBALANCE_REGIONS = "rebalance_regions";

    public static final String STOP_REBALANCE_REGIONS = "stop_rebalance_regions";

    public static final String SYNC_REPLICAS = "sync_replicas";

    public static final String GRANT = "grant";

    public static final String REVOKE = "revoke";

    public static final String USER_PERMISSION = "user_permission";

    public static final String EXIT = "exit";

    public static final String QUIT = "quit";

    public static final String SNAPSHOT = "snapshot";

    public static final String LIST_SNAPSHOTS = "list_snapshots";

    public static final String CLONE_SNAPSHOT = "clone_snapshot";

    public static final String DELETE_SNAPSHOT = "delete_snapshot";

    public static final String SALT = "salt";

    public static final String GET_SALT = "get_salt";

    public static final String PUT_SALT = "put_salt";

    public static final String DELETE_SALT = "delete_salt";

    public static final String GET_BINARY = "get_binary";

    public static final String GET_BINARY_SALT = "get_binary_salt";

    public static final String STOP_REGION_SERVER = "stop_region_server";
    public static final String SHUTDOWN_RS = "shutdown_rs";
    public static final String START_REGION_SERVER = "start_region_server";

    private Commands() {
    }

}
