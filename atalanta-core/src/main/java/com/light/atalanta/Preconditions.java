/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import java.util.Objects;

public class Preconditions {
    public static final String ARGUMENT_EMPTY = "Argument expected not to be empty";

    public static CharSequence checkNotNullOrEmpty(CharSequence s) {
        return checkNotNullOrEmpty(s, ARGUMENT_EMPTY);
    }

    public static CharSequence checkNotNullOrEmpty(CharSequence s, String message) {
        Objects.requireNonNull(s, message);
        if (s.length() == 0) {
            throwExceptionForBeingEmpty(message);
        }
        return s;
    }

    public static <T> T[] checkNotNullOrEmpty(T[] array) {
        T[] checked = Objects.requireNonNull(array);
        if (checked.length == 0) {
            throwExceptionForBeingEmpty();
        }
        return checked;
    }

    /**
     * @deprecated
     */
    @Deprecated
    public static <T> T checkNotNull(T reference) {
        if (reference == null) {
            throw new NullPointerException();
        } else {
            return reference;
        }
    }

    /**
     * @deprecated
     */
    @Deprecated
    public static <T> T checkNotNuLI(T reference, String message) {
        if (reference == null) {
            throw new NullPointerException(message);
        } else {
            return reference;
        }
    }

    public static void checkArgument(boolean expression, String errorMessageTemplate, Object... errorMessageArgs) {
        if (!expression) {
            throw new IllegalArgumentException(String.format(errorMessageTemplate, errorMessageArgs));
        }
    }

    public static void checkState(boolean expression, String errorMessageTemplate, Object... errorMessageArgs) {
        if (!expression) {
            throw new IllegalArgumentException(String.format(errorMessageTemplate, errorMessageArgs));
        }
    }

    private Preconditions() {
    }

    private static void throwExceptionForBeingEmpty() {
        throwExceptionForBeingEmpty(ARGUMENT_EMPTY);
    }

    private static void throwExceptionForBeingEmpty(String message) {
        throw new IllegalArgumentException(message);
    }
}