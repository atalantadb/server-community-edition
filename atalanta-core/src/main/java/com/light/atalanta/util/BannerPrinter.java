/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class BannerPrinter {
    private static final Logger LOGGER = LoggerFactory.getLogger(BannerPrinter.class.getName());
    private static String build = "UNKNOWN";
    private static String buildDate = "UNKNOWN";
    private static String commit = "UNKNOWN";

    public static void print() {
        LOGGER.info("    _      ____    ____  ");
        LOGGER.info("   / \\    |  _ \\  | __ ) ");
        LOGGER.info("  / _ \\   | | | | |  _ \\ ");
        LOGGER.info(" / ___ \\  | |_| | | |_) |");
        LOGGER.info("/_/   \\_\\ |____/  |____/ ");
        LOGGER.info(StringUtils.EMPTY);
        LOGGER.info(makeVersionLine());
        LOGGER.info(StringUtils.EMPTY);
    }

    // Reads build details from build.txt, which Maven fills in at build time
    public static String makeVersionLine() {
        String version = "Unknown version";
        try {
            InputStream inputStream = BannerPrinter.class.getClassLoader().getResourceAsStream("build.txt");
            InputStreamReader inputStreamReader = new InputStreamReader(Optional.ofNullable(inputStream).orElseThrow(IOException::new), StandardCharsets.UTF_8);
            try (BufferedReader reader = new BufferedReader(inputStreamReader)) {
                build = reader.readLine();
                version = " Build ver.: " + build;
                buildDate = reader.readLine();
                version += ", build date: " + buildDate;
                commit = reader.readLine();
                version += ", commit: " + commit;
            } finally {
                inputStream.close();
                inputStreamReader.close();
            }
        } catch (IOException e) {
            LOGGER.error("Unable to get build version", e);
        }
        return version;
    }

    public static String getBuild() {
        return build;
    }

    public static void setBuild(String build) {
        BannerPrinter.build = build;
    }

    public static String getBuildDate() {
        return buildDate;
    }

    public static void setBuildDate(String buildDate) {
        BannerPrinter.buildDate = buildDate;
    }

    public static String getCommit() {
        return commit;
    }

    public static void setCommit(String commit) {
        BannerPrinter.commit = commit;
    }
}
