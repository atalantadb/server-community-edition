/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.exceptions;

import com.light.atalanta.TableName;

public class TableBlockedException extends NoRetryException {
    public TableBlockedException(TableName tableName) {
        super("table '" + tableName + "' is blocked by recovery");
    }
}
