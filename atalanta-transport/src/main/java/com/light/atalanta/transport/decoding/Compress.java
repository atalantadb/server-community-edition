/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.decoding;

public enum Compress {
    NOOP {
        @Override
        public Compressor getCompressor() {
            return new NoOpCompressor();
        }

        @Override
        public Decompressor getDecompressor() {
            return new NoOpDecompressor();
        }
    }, LZMA {
        @Override
        public Compressor getCompressor() {
            return new LZMACompressService();
        }

        @Override
        public Decompressor getDecompressor() {
            return new LZMADecompressService();
        }
    };

    public static Compress fromString(String compress) {
        switch (compress) {
            case "lzma":
            case "LZMA": {
                return LZMA;
            }
            case "noop":
            case "NOOP":
            default: {
                return NOOP;
            }
        }
    }

    public abstract Compressor getCompressor();

    public abstract Decompressor getDecompressor();
}
