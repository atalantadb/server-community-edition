/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.decoding;

import java.io.IOException;

public class DecodingFactory {
    static final int BLOCK_SIZE = 64 * 1_024;

    private static Compressor compressor;
    private static Decompressor decompressor;

    static {
        String compress = "noop";
        String decompress = "lzma";
        setCompressType(compress, decompress);
    }

    public static void setCompressType(String compress, String decompress) {
        // TODO: Implement switching of the compression type
        Compress c = Compress.fromString(compress);
        compressor = c.getCompressor();
        if (decompress.equals("lzma"))
            decompressor = new LZMADecompressService();
        else
            decompressor = c.getDecompressor();

    }

    public static byte[] compress(byte[] b) throws IOException {
        return compressor.compressBytes(b);
    }

    public static byte[] decompress(byte[] b) throws IOException {
        return decompressor.decompressBytes(b);
    }
}
