/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

public interface Cell {
    byte[] cloneValue();

    byte[] cloneQualifier();

    byte[] cloneRow();

    byte[] getRowArray();

    int getRowLength();

    byte[] getFamilyArray();

    byte getFamilyLength();

    byte[] getQualifierArray();

    int getQualifierLength();

    long getTimestamp();

    byte[] getValueArray();

    int getValueLength();

    int getValueOffset();

    int getQualifierOffset();
}
