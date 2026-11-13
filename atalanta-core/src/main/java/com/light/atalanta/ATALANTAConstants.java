/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

public final class ATALANTAConstants {

    public static final String STATUS_PREFIX = "status=";

    public static final String DEFAULT_NAMESPACE = "default";

    public static final String ZK_SEPARATOR = "/";

    public static final byte[] EMPTY_BYTES = new byte[0];

    public static final byte[][] EMPTY_SPLIT_KEYS_BYTES = new byte[0][0];

    public static final byte[] MIN_SPLIT_KEY_BYTES = {(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00};

    public static final byte[] MAX_SPLIT_KEY_BYTES = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};

    public static final String CSV_NEWLINE = "\r\n";

    public static final String CSV_SEPARATOR = ":";

    public static final String HOSTNAME_SEPARATOR = ":";

    public static final String F_MASTER_NODE = "fmaster";

    public static final String REGION_SERVER_INFO_SUBPATH = "rs_info";

    public static final String REGION_SERVER_EPHEMERAL_SUBPATH = "rs_ephemeral";

    public static final String REGION_SERVER_INITIALIZING = "INITIALIZING";

    public static final String REGION_SERVER_RUNNING = "RUNNING";

    public static final String REGION_SERVER_DISABLING = "DISABLING";

    public static final String REGION_SERVER_DISABLED = "DISABLED";

    public static final String COM_LIGHT_ATALANTA_METRICS = "com.light.atalanta:metrics=";

    public static final int DEFAULT_DATA_BLOCK_SIZE = 2; // Default block size: 2 x 4096 bytes
    public static final String ATALANTA_METADATA = "_md";

    public static final String SNAPSHOT_ZNODE = ATALANTA_METADATA + ZK_SEPARATOR + "snapshots";

    public static final String SNAPSHOT_DIR = "_snapshots_dir";
    public static final String ATALANTA_MODIFY = ATALANTA_METADATA + ZK_SEPARATOR + "modify";
    public static final String ATALANTA_NS = ATALANTA_METADATA + ZK_SEPARATOR +"ns";

    public static final String ATALANTA_REGIONS_INFO = "regions";

    private ATALANTAConstants() { }
}
