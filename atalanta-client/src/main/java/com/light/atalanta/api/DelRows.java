/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.api;

import com.light.atalanta.Cell;
import com.light.atalanta.Mutation;

public interface DelRows extends Mutation {
    default DelRows add(Cell cell) {
        return addColumn(cell.getFamilyArray(), cell.cloneQualifier());
    }

    default DelRows addColumns(byte[] family, byte[] qualifier) {
        return addColumns(family, qualifier, Long.MAX_VALUE);
    }

    default DelRows addColumns(byte[] family, byte[] qualifier, long timestamp) {
        return addColumn(family, qualifier, timestamp);
    }

    /** Legacy deletion; prefer addColumns for deletion priority within RowMutations. */
    @Deprecated
    default DelRows addColumn(byte[] family, byte[] qualifier) {
        return addColumn(family, qualifier, Long.MAX_VALUE);
    }

    /** Legacy deletion; prefer addColumns for deletion priority within RowMutations. */
    @Deprecated
    DelRows addColumn(byte[] family, byte[] qualifier, long timestamp);


    default DelRows addFamily(byte[] family) {
        return addFamily(family, Long.MAX_VALUE);
    }

    DelRows addFamily(byte[] family, long timestamp);
}
