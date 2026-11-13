/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.decoding;

import com.light.atalanta.transport.messages.OperationFactory;
import com.light.atalanta.transport.messages.TransportMessage;

import java.io.IOException;

public class NoOpDecompressor implements Decompressor {

    @Override
    public byte[] decompressBytes(byte[] bytes) throws IOException {
        return bytes;
    }

    @Override
    public TransportMessage decompressMessage(byte[] bytes) throws IOException {
        try {
            return OperationFactory.fromBytes(decompressBytes(bytes), null, 60_000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
