/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

public interface NameOfTable {
    String SEPARATOR = ":";

    default String getNameAsString() {
        return getNamespace() + SEPARATOR + getTableName();
    }

    String getNamespace();

    String getTableName();

    default String getQualifierAsString() {
        return getTableName();
    }
}
