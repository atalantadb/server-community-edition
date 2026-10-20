/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.tool;

/** Client transport used by the usage example. */
public enum ConnectionType {
    NIO("NIO"),
    LOCAL_THREAD("LocalThread");

    private final String displayName;

    ConnectionType(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }
}
