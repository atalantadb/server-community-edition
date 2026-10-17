/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

public class CellUtil {

    public static byte[] cloneRow(Cell cell) {
        byte[] output = new byte[cell.getRowLength()];
        copyRowTo(cell, output, 0);
        return output;
    }

    public static byte[] cloneFamily(Cell cell) {
        byte[] output = new byte[cell.getFamilyLength()];
        copyFamilyTo(cell, output, 0);
        return output;
    }

    public static byte[] cloneQualifier(Cell cell) {
        byte[] output = new byte[cell.getQualifierLength()];
        copyQualifierTo(cell, output, 0);
        return output;
    }

    public static byte[] cloneValue(Cell cell) {
        byte[] output = new byte[cell.getValueLength()];
        copyValueTo(cell, output, 0);
        return output;
    }

    public static void copyRowTo(Cell cell, byte[] destination, int destinationOffset) {
        System.arraycopy(cell.getRowArray(), 0, destination, destinationOffset,
                cell.getRowLength());
    }

    public static void copyFamilyTo(Cell cell, byte[] destination, int destinationOffset) {
        System.arraycopy(cell.getFamilyArray(), 0, destination, destinationOffset,
                cell.getFamilyLength());
    }

    public static void copyQualifierTo(Cell cell, byte[] destination, int destinationOffset) {
        System.arraycopy(cell.getQualifierArray(), cell.getQualifierOffset(), destination,
                destinationOffset, cell.getQualifierLength());
    }

    public static void copyValueTo(Cell cell, byte[] destination, int destinationOffset) {
        System.arraycopy(cell.getValueArray(), cell.getValueOffset(), destination, destinationOffset,
                cell.getValueLength());
    }
}
