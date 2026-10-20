/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.decoding;

import com.light.atalanta.transport.messages.OperationFactory;
import com.light.atalanta.transport.messages.TransportMessage;
import org.tukaani.xz.LZMAInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static com.light.atalanta.transport.decoding.DecodingFactory.BLOCK_SIZE;

public class LZMADecompressService implements Decompressor{
    @Override
    public byte[] decompressBytes(byte[] bytes) throws IOException {
        Arrays.fill(bytes, 5, 13, (byte) 0xFF);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             // LZMAInputStream reads the header itself: 5 property bytes and 8 bytes of the original size
             LZMAInputStream lzmaIn = new LZMAInputStream(bais);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[BLOCK_SIZE];
            int count;
            while ((count = lzmaIn.read(buffer)) != -1) {
                baos.write(buffer, 0, count);
            }
            return baos.toByteArray();
        }
    }

    @Override
    public TransportMessage decompressMessage(byte[] bytes) {
        try {
            return OperationFactory.fromBytes(decompressBytes(bytes), null, 60_000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
