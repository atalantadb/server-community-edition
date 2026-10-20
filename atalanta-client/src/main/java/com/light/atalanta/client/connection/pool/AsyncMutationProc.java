/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.pool;

import com.light.atalanta.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

@Deprecated
public interface AsyncMutationProc extends Closeable {
    CompletableFuture<Result> get(GetRows get, TableName tableName) throws IOException;

    ResultScanner getScanner(ScanRows scan, TableName tableName) throws IOException;

    CompletableFuture<Result> mutate(DbMutation put, TableName tableName);

    void waitAllOperation() throws IOException;

    CompletableFuture<Result> close(TableName tableName);
}
