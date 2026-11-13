/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.api.DbTable;

import java.io.Closeable;
import java.io.IOException;

public interface Connection extends AutoCloseable, Closeable {
    Configuration getConfiguration();

    Admin getAdmin() throws IOException;

    Table getTable(TableName tableName) throws IOException;

    RegionLocator getRegionLocator(TableName tableName) throws IOException;

    BufferedMutator getBufferedMutator(TableName tableName) throws IOException;

    boolean isClosed();
}
