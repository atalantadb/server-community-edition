/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

public enum AuthResult {

    AUTH_OK((byte) 1), AUTH_ERROR((byte) 0);

    private final byte code;

    AuthResult(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static AuthResult fromByte(byte b) {
        switch (b) {
            case 0:
                return AUTH_ERROR;
            case 1:
                return AUTH_OK;
        }
        throw new IllegalArgumentException("Incorrect byte value");
    }
}
