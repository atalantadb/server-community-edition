/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.exceptions;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

public class NoRetryException extends RuntimeException {
    public NoRetryException(String msg) {
        super(msg);
    }
    public NoRetryException(Throwable cause) {
        super(cause);
    }
    public NoRetryException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /** Finds a terminal failure even when wrapped by futures or another retry layer. */
    public static NoRetryException findCause(Throwable error) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        for (Throwable cause = error; cause != null && visited.add(cause); cause = cause.getCause()) {
            if (cause instanceof NoRetryException) {
                return (NoRetryException) cause;
            }
        }
        return null;
    }
}
