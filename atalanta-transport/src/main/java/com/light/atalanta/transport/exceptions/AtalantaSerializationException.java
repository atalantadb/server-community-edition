/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.exceptions;

import com.light.atalanta.exceptions.AtalantaTransportRuntimeException;

/**
 * A deterministic failure while converting a transport message to bytes.
 * Such failures must not be retried or treated as connection/topology errors.
 */
public class AtalantaSerializationException extends AtalantaTransportRuntimeException {
    public AtalantaSerializationException(String message, Throwable cause) {
        super(message);
        initCause(cause);
    }
}
