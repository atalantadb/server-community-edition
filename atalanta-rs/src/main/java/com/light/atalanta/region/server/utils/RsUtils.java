/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.utils;

import com.light.atalanta.region.server.services.metrics.JmxRsMetricsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public final class RsUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(RsUtils.class);

    private static List<String> rsDirs = null;

    private RsUtils() {
    }

    public static List<String> rsDirs(List<String> list) {
        if (rsDirs == null) {
            rsDirs = new ArrayList<>();
            list.forEach(s -> rsDirs.addAll(Arrays.asList(s.split(","))));
        }
        return rsDirs;
    }

    public static int align(int number, int roundFactor) {
        long result = ((long) number + roundFactor - 1) / roundFactor * roundFactor;
        if (result > Integer.MAX_VALUE) {
            throw new ArithmeticException("Aligned value overflows int: " + result +
                    ", number=" + number + ", roundFactor=" + roundFactor);
        }
        return (int) result;
    }

    public static Set<FileStore> getStorages(List<String> disks) {
        return disks.stream()
                .map(d -> {
                    try {
                        return Files.getFileStore(Paths.get(d));
                    } catch (IOException e) {
                        LOGGER.error("cannot get store for path " + d, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }
}
