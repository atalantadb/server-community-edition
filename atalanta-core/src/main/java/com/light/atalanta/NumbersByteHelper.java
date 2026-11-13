/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

public class NumbersByteHelper {
    private NumbersByteHelper() {
    }

    public static int fromBytes_int(byte[] bytes, int pos) {
        return bytes[pos] & 0xff |
                (bytes[pos + 1] & 0xff) << 8 |
                (bytes[pos + 2] & 0xff) << 16 |
                (bytes[pos + 3] & 0xff) << 24;
    }

    public static short fromBytes_short(byte[] bytes, int pos) {
        return (short) (bytes[pos] & 0xff |
                (bytes[pos + 1] & 0xff) << 8);
    }

    public static byte[] toBytes_int(int value) {
        return new byte[]{
                (byte) (value & 0xff),
                (byte) ((value >> 8) & 0xff),
                (byte) ((value >> 16) & 0xff),
                (byte) ((value >> 24) & 0xff)
        };
    }

    public static byte[] toBytes_short(short value) {
        byte[] ret = new byte[2];
        ret[0] = (byte) (value & 0xff);
        ret[1] = (byte) ((value >> 8) & 0xff);
        return ret;
    }

    public static byte[] toBytes_long(long value) {
        return new byte[]{
                (byte) value,
                (byte) (value >> 8),
                (byte) (value >> 16),
                (byte) (value >> 24),
                (byte) (value >> 32),
                (byte) (value >> 40),
                (byte) (value >> 48),
                (byte) (value >> 56)};
    }

    public static long fromBytes_long(byte[] bytes, int pos) {
        return ((long) bytes[pos + 7] << 56)
                | ((long) bytes[pos + 6] & 0xff) << 48
                | ((long) bytes[pos + 5] & 0xff) << 40
                | ((long) bytes[pos + 4] & 0xff) << 32
                | ((long) bytes[pos + 3] & 0xff) << 24
                | ((long) bytes[pos + 2] & 0xff) << 16
                | ((long) bytes[pos + 1] & 0xff) << 8
                | ((long) bytes[pos] & 0xff);
    }

}
