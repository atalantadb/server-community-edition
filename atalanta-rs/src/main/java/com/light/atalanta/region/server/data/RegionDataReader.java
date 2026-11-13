/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.data;

import com.google.protobuf.ByteString;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.region.server.data.files.AtalantaLobChunk;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Iterator;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;

public class RegionDataReader implements Iterator<ByteString>, AutoCloseable, Cloneable {

    public static final Logger LOGGER = LoggerFactory.getLogger(RegionDataReader.class.getName());
    public static final int BUFFER_SIZE = 1024 * 1024 * 1;

    private final FileChannel inCh;

    private ByteBuffer buffer;

    public RegionDataReader(String pathToFile, Long pos) {
        try {
            inCh= FileChannel.open(Paths.get(pathToFile));
            inCh.position(pos);
            buffer = ByteBuffer.allocate(BUFFER_SIZE);
        } catch (IOException ex) {
            LOGGER.error("Cannot open file: {}", pathToFile, ex);
            throw new RuntimeException(ex);
        }
    }

    public ByteString read() throws IOException {
        if ( inCh.read(buffer) > 0 ) {
            buffer.flip();
            ByteString res = ByteString.copyFrom(buffer.array(), 0, buffer.limit());
            buffer.clear();
            return res;
        } else {
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        inCh.close();
    }


    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public ByteString next() {
        return null;
    }
}
