/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.exceptions;

import com.light.atalanta.TableName;

public class TableNotExistsException extends Exception {

    public TableNotExistsException(TableName tableName) {
        super("table '" + tableName + "' not exists");
    }
}
