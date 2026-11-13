/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;


import java.io.IOException;

public interface ResultScanner extends AutoCloseable, Iterable<Result> {
    Result next() throws IOException;

    Result[] next(int var1) throws IOException;

    void close();
}
