/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RegionDataWriter implements AutoCloseable, Cloneable {

    public static final Logger LOGGER = LoggerFactory.getLogger(RegionDataWriter.class.getName());
    private final FileChannel outCh;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public RegionDataWriter(String pathToFile) {
        try {
            outCh = FileChannel.open(Paths.get(pathToFile),
                    EnumSet.of(StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING,
                            StandardOpenOption.WRITE)
            );
        } catch (IOException ex) {
            LOGGER.error("Cannot open file: {}", pathToFile, ex);
            throw new RuntimeException(ex);
        }
    }

    public void write(ByteBuffer data) throws IOException {
        executor.submit(() -> outCh.write(data));
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
        executor.awaitTermination(1000, TimeUnit.SECONDS);
        outCh.close();
    }


}
