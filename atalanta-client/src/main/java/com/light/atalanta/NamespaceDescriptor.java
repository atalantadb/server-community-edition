/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

public class NamespaceDescriptor implements DbNamespaceDescriptor {

    private final String ns;

    public NamespaceDescriptor() {
        ns = null;
    }

    public NamespaceDescriptor(String ns) {
        this.ns = ns;
    }

    @Override
    public String getName() {
        return ns;
    }
}
