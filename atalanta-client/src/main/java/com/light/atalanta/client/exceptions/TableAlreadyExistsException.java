/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.exceptions;

import com.light.atalanta.TableName;

public class TableAlreadyExistsException extends Exception {

    public TableAlreadyExistsException(TableName tableName) {
        super("table '" + tableName + "' already exists");
    }
}
