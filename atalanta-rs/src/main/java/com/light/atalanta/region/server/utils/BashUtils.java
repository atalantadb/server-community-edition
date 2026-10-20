/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class BashUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(BashUtils.class);

    private static final ProcessBuilder PROCESS_BUILDER = new ProcessBuilder();

    private BashUtils() {
    }

    public static Process runShellCommand(String command) throws IOException, InterruptedException {
        LOGGER.debug("Run command: '{}'", command);
        PROCESS_BUILDER.command("bash", "-c", command);
        Process exec = PROCESS_BUILDER.start();
        int exitVal = exec.waitFor();
        if (exitVal != 0) {
            throw new IllegalStateException("Error running command: " + command + "\nExit code = " + exitVal);
        }
        return exec;
    }

    public static List<String> getCommandResponse(Process exec) throws IOException {
        try (Reader is = new InputStreamReader(exec.getInputStream(), UTF_8);
             BufferedReader reader = new BufferedReader(is)) {
            List<String> lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            return lines;
        }
    }
}
