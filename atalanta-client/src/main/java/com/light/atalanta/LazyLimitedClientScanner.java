/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import java.io.IOException;

public class LazyLimitedClientScanner extends ClientScanner {
    private final int limit;
    private int currentIteration = 0;

    LazyLimitedClientScanner(Scan scan, LazyTableImpl table) {
        super(scan, table);
        this.limit =
                scan.getCacheSize();
    }

    @Override
    public Result next() throws IOException {
        if (currentIteration >= limit) {
            return null;
        }
        Result r = super.next();
        currentIteration++;
        return r;
    }
}