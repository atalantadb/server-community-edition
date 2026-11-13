/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.tool;

import java.nio.charset.StandardCharsets;

/** Fixed schema used by the data generator and verifier. */
public final class DataToolSchema {

    public static final String NAMESPACE = "data_tool";
    public static final String TABLE = "records";
    public static final String COLUMN_FAMILY = "cf";
    public static final int QUALIFIER_COUNT = 10;

    private DataToolSchema() {
    }

    public static String qualifiedTableName() {
        return NAMESPACE + ":" + TABLE;
    }

    public static long totalQualifierBytes() {
        long total = 0;
        for (int index = 0; index < QUALIFIER_COUNT; index++) {
            total += qualifier(index).getBytes(StandardCharsets.UTF_8).length;
        }
        return total;
    }

    public static String qualifier(int index) {
        if (index < 0 || index >= QUALIFIER_COUNT) {
            throw new IllegalArgumentException("Qualifier index is out of range: " + index);
        }
        return "field" + index;
    }
}
