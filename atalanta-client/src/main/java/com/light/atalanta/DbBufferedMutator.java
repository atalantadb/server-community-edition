/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface DbBufferedMutator extends Closeable {
    void mutate(DbMutation mutation) throws IOException;

    void mutate(List<DbMutation> mutations) throws IOException;

    void flush() throws IOException;

    NameOfTable getName();
}
