/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.utils;

import java.nio.ByteBuffer;

/**
 * CRC-16/CCITT-FALSE: polynomial 0x1021, initial value 0xFFFF, no final XOR.
 * Table-driven, MSB first.
 */
public final class Crc16 {
    private static final int POLY  = 0x1021;          // CCITT
    private static final int[] TABLE = new int[256];

    /* MSB-first table, as in the C implementation */
    static {
        for (int i = 0; i < 256; i++) {
            int crc = i << 8;                         // high byte
            for (int j = 0; j < 8; j++) {
                crc = (crc & 0x8000) != 0
                        ? ((crc << 1) ^ POLY) & 0xFFFF
                        : (crc << 1) & 0xFFFF;
            }
            TABLE[i] = crc;
        }
    }

    private Crc16() {}  // util-class

    /* ---------- API ---------- */

    /** CRC-16/CCITT (init=0xFFFF, no final XOR) for a whole array. */
    public static int calc(byte[] bytes) {
        return calc(ByteBuffer.wrap(bytes), 0, bytes.length);
    }

    /** CRC-16/CCITT (init=0xFFFF, no final XOR) for a buffer fragment. */
    public static int calc(ByteBuffer buf, int pos, int size) {
        int crc = 0xFFFF;
        for (int i = 0; i < size; i++) {
            int b   = buf.get(pos + i) & 0xFF;
            int idx = ((crc >>> 8) ^ b) & 0xFF;       // high byte
            crc = ((crc << 8) ^ TABLE[idx]) & 0xFFFF;
        }
        return crc;
    }
}

