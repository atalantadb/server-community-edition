/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.api;

import com.light.atalanta.DbColumnDescriptor;
import com.light.atalanta.TableName;

import java.util.Collection;

public interface DbTableDescriptor {

    TableName getTableName();

    DbTableDescriptor addFamily(DbColumnDescriptor columnDescriptor);

    Collection<DbColumnDescriptor> getFamilies();
}
