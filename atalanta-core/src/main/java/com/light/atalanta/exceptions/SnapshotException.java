/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.exceptions;

public class SnapshotException extends RuntimeException {
    public SnapshotException(String msg) {
        super(msg);
    }

    public SnapshotException(Throwable e) {
        super(e);
    }
}
