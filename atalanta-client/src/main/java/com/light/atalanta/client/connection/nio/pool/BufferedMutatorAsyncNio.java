/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.pool;

import com.light.atalanta.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class BufferedMutatorAsyncNio implements BufferedMutator {

    private final Table table;

    public BufferedMutatorAsyncNio(Table table) {
        this.table = table;
    }

    @Override
    public void mutate(DbMutation mutation) throws IOException {
        table.mutateNoFlush(mutation);
    }

    @Override
    public void mutate(List<DbMutation> mutations) throws IOException {
        table.mutateNoFlush(mutations);
    }

    @Override
    public void flush() throws IOException {
        table.flush();
    }

    @Override
    public NameOfTable getName() {
        return table.getName();
    }

    @Override
    public void close() throws IOException {
        table.close();
    }
}
