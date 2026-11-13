/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import java.util.List;
import java.util.NavigableMap;

public interface DbMutation extends RowKey {
    NavigableMap<byte[], List<Cell>> getFamilyCellMap();

}
