/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.api;

import com.light.atalanta.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface DbTable extends Closeable {
    Result get(Get get) throws IOException;

    Result[] get(List<Get> listGet) throws IOException;

    ResultScanner getScanner(Scan scan) throws IOException;

    TableName getName();

    void put(List<Put> putList) throws IOException;

    void put(Put put) throws IOException;

    void delete(List<Delete> deletes) throws IOException;

    void mutateRow(RowMutations mutation) throws IOException;

    void mutateNoFlush(DbMutation mutation) throws IOException;

    void mutateNoFlush(List<DbMutation> mutations) throws IOException;

    void flush() throws IOException;

    void delete(Delete delete) throws IOException;

    default long count() throws IOException {
        throw new UnsupportedOperationException("Could not supported by " + this.getClass());
    }
}
