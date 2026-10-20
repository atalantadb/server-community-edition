/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.decoding;

import com.light.atalanta.transport.messages.TransportMessage;
import org.apache.commons.compress.compressors.lzma.LZMACompressorOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class LZMACompressService implements Compressor {
    @Override
    public byte[] compressBytes(byte[] bytes) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (LZMACompressorOutputStream lzmaOut = new LZMACompressorOutputStream(outputStream)) {
            lzmaOut.write(bytes);
        }
        byte[] compressed = outputStream.toByteArray();
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(bytes.length);
        byte[] sizeBytes = buffer.array();
        System.arraycopy(sizeBytes, 0, compressed, 5, 8);
        return compressed;
    }

    @Override
    public byte[] compressMessage(TransportMessage message) throws IOException {
        ByteBuffer buffer = message.serializeMessage(ByteBuffer.allocate(message.getMessageSize())
                .order(ByteOrder.LITTLE_ENDIAN));
        byte[] serialized = buffer.position() == buffer.array().length
                ? buffer.array()
                : Arrays.copyOfRange(buffer.array(), buffer.arrayOffset(),
                buffer.arrayOffset() + buffer.position());
        return compressBytes(serialized);
    }
}
