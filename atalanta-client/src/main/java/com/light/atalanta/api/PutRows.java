/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.api;

import com.light.atalanta.DbMutation;
import com.light.atalanta.Mutation;

public interface PutRows extends Mutation {
    PutRows addColumn(byte[] family, byte[] qualifier, byte[] value);

    PutRows addImmutable(byte[] family, byte[] qualifier, byte[] value);

    PutRows addColumn(byte[] cf, byte[] qualifier, long ts, byte[] value);
}
