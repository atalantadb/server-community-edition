/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations;

public class OperationException extends RuntimeException {
    public OperationException(String message, Throwable cause) {
        super(message, cause);
    }
    public OperationException(Throwable cause) {
        super(cause);
    }
}
