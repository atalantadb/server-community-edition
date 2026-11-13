/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

/** Shared folding rules for cells in Delete and RowMutations. */
final class MutationCellOrder {
    private MutationCellOrder() {
    }

    static boolean shouldReplace(long currentTimestamp, boolean currentDeleteColumns,
                                 long incomingTimestamp, boolean incomingDeleteColumns) {
        // Resolve LATEST only for comparison. Keep the original timestamp on the wire
        // so the server can resolve it, and never substitute a constructor timestamp.
        if (isLatest(currentTimestamp) || isLatest(incomingTimestamp)) {
            long now = System.currentTimeMillis();
            currentTimestamp = isLatest(currentTimestamp) ? now : currentTimestamp;
            incomingTimestamp = isLatest(incomingTimestamp) ? now : incomingTimestamp;
        }
        if (incomingTimestamp != currentTimestamp) {
            return incomingTimestamp > currentTimestamp;
        }
        // A legacy delete must not erase range-delete priority before a subsequent put.
        // Otherwise, preserve the existing last-operation-wins behavior at equal time.
        return !currentDeleteColumns || incomingDeleteColumns;
    }

    private static boolean isLatest(long timestamp) {
        return timestamp == Long.MAX_VALUE || timestamp == -1;
    }
}
