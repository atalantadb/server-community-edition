/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

public class ColumnFamilyDescriptor implements DbColumnFamilyDescriptor {

    private final byte[] name;

    private ColumnFamilyDescriptor(byte[] familyName) {
        this.name = familyName;
    }

    public static DbColumnFamilyDescriptor valueOf(byte[] family) {
        return new ColumnFamilyDescriptor(family);
    }


    @Override
    public byte[] getName() {
        return name;
    }
}
