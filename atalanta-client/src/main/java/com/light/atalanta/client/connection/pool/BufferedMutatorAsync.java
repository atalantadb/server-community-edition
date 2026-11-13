/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.pool;

import com.light.atalanta.*;
import org.apache.commons.lang3.NotImplementedException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Deprecated
public class BufferedMutatorAsync implements BufferedMutator {

    private final Table table;

    public BufferedMutatorAsync(Table table) {
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
