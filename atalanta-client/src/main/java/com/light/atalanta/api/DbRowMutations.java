/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.api;

import com.light.atalanta.DbMutation;
import com.light.atalanta.Mutation;
import com.light.atalanta.RowKey;

import java.io.IOException;

public interface DbRowMutations extends RowKey, DbMutation {

    void add(Mutation mutation) throws IOException;
}
