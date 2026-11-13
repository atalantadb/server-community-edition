/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.exceptions;

public class NamespaceNotExistsException extends Exception {

    public NamespaceNotExistsException(String namespace) {
        super("namespace '" + namespace + "' not exists");
    }
}
