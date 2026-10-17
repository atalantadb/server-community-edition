/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import java.util.Collections;
import java.util.List;

public interface DbResult {
    byte[] getValue(byte[] family, byte[] qualifier);

    DbCellScanner cellScanner();

    Cell current();

    boolean isEmpty();

    byte[] getRow();

    Boolean getExists();

    List<Cell>	listCells();

    default List<Cell> getColumnCells(byte[] family, byte[] qualifier) {
        return Collections.singletonList(getColumnLatestCell(family, qualifier));
    }

    Cell getColumnLatestCell(byte[] family, byte[] qualifier);

    boolean containsColumn(byte[] family, byte[] qualifier);
}
