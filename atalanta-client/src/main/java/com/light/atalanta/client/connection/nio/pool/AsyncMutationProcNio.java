/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.pool;

import com.light.atalanta.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface AsyncMutationProcNio extends Closeable {
    <T> CompletableFuture<Object> operation(List<T> get, TableName tableName) throws IOException;

    ResultScanner getScanner(ScanRows scan, TableName tableName) throws IOException;

    List<RegionLocation> getAllRegionLocations(TableName tableName);

}
