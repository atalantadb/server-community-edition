/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

public interface ScanRows {
    void setFilter(DbSingleColumnValueFilter singleColumnValueFilter);

    void setBatch(int batch);

    default void setCacheBlocks(boolean cacheBlocks) {
    }

    void setCaching(int caching);

    void setLimit(int limit);

    ScanRows addColumn(byte[] family, byte[] qualifier);

    void setMaxResultSize(int maxResultSize);

    default void setStartRow(byte[] startRow) {
        withStartRow(startRow);
    }

    ScanRows withStartRow(byte[] startRow);

    default void setStopRow(byte[] startRow) {
        withStopRow(startRow);
    }

    ScanRows withStopRow(byte[] stopRow);
}
