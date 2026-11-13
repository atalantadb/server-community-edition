/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.services;

public class ZkConfig {
    private final String connString;

    public ZkConfig(String connString) { this.connString = connString; }
    public String getConnString() { return connString; }
}
