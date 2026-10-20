/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client;

import com.light.atalanta.Put;
import com.light.atalanta.Pair;
import com.light.atalanta.RowKey;
import com.light.atalanta.RowMutations;

import java.util.NavigableMap;

public class MutationValidator {
    public static void validatePut(Put put) {
        validateRow(put);
        NavigableMap<byte[], NavigableMap<byte[], Pair<Long, byte[]>>> familyMap = put.getFamilyMap();
        if (familyMap == null || familyMap.isEmpty()) {
            throw new IllegalArgumentException("put object is empty");
        }
    }

    public static void validateRowMutations(RowMutations rowMutations) {
        validateRow(rowMutations);
        if (!rowMutations.isFullDelete() && rowMutations.getFamilyCellMap().isEmpty()) {
            throw new IllegalArgumentException("row mutation object is empty");
        }
    }

    public static void validateRow(RowKey rowKey) {
        byte[] key = rowKey.getRow();
        validateBytes(key);
    }

    public static void validateBytes(byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException("rowKey is null");
        }
        if (key.length == 0) {
            throw new IllegalArgumentException("rowKey is empty");
        }
    }
}
