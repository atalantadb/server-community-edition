/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.util;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.util.Arrays;

/** Prefixes a row key with its four-byte, big-endian xxHash32 (seed 0). */
public final class XxHash32KeySalter {
    private static final int SALT_SIZE = Integer.BYTES;
    private static final XXHash32 HASH = XXHashFactory.safeInstance().hash32();

    private XxHash32KeySalter() {
    }

    public static byte[] saltKey(byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException("Key must not be null");
        }

        int hash = HASH.hash(key, 0, key.length, 0);
        byte[] salted = new byte[SALT_SIZE + key.length];
        salted[0] = (byte) (hash >>> 24);
        salted[1] = (byte) (hash >>> 16);
        salted[2] = (byte) (hash >>> 8);
        salted[3] = (byte) hash;
        System.arraycopy(key, 0, salted, SALT_SIZE, key.length);
        return salted;
    }

    public static byte[] unsaltKey(byte[] saltedKey) {
        if (saltedKey == null || saltedKey.length < SALT_SIZE) {
            throw new IllegalArgumentException("Salted key must contain a four-byte prefix");
        }
        return Arrays.copyOfRange(saltedKey, SALT_SIZE, saltedKey.length);
    }
}
