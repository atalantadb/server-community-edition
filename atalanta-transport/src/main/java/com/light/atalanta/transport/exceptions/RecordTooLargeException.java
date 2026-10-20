/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.exceptions;

import com.light.atalanta.exceptions.NoRetryException;

public class RecordTooLargeException extends NoRetryException {
    public static final String MESSAGE = "An attempt was made to process a record exceeding 60,648 bytes. " +
            "This operation is only supported in the Enterprise edition of the DB";

    public RecordTooLargeException() {
        super(MESSAGE);
    }
}
