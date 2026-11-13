/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.tracers;

import com.light.atalanta.collections.LimitedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Writer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Writer.class);

    private static final Map<Thread, LimitedQueue<String>> logs = new ConcurrentHashMap<>();
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public static void addLog(byte[] bytes, int size) {
        LimitedQueue<String> queue = logs.get(Thread.currentThread());
        if (queue == null) {
            queue = new LimitedQueue<>(30_000);
            logs.put(Thread.currentThread(), queue);
        }
        String line = FORMATTER.format(LocalDateTime.now()) + ";" + Arrays.toString(bytes) + ";" + size;
        queue.add(line);
    }

    public static void save() {
        try {
            for (Map.Entry<Thread, LimitedQueue<String>> entry : logs.entrySet()) {
                String file = getFileName(entry.getKey().getName(), System.currentTimeMillis());
                Path path = Paths.get(file);
                Files.createFile(path);
                Files.write(path, entry.getValue());
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    static String getFileName(String threadName, long time) {
        return "log-" + threadName + "-" + time;
    }
}
