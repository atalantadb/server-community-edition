/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.util;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CmdUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CmdUtils.class);

    private CmdUtils() {

    }

    public static List<String> runCommand(String command, boolean isEmptyError, ThreadFactory commandThreadFactory) {
        ProcessBuilder processBuilder = new ProcessBuilder(command.split(" "));
        processBuilder.environment(); // Initializes the inherited environment of the child process
        ScheduledExecutorService extractor = Executors.newSingleThreadScheduledExecutor(commandThreadFactory);
        try {
            Process process = processBuilder.start();
            AtomicReference<List<String>> result = new AtomicReference<>(new ArrayList<>());
            extractor.scheduleWithFixedDelay(() -> {
                try {
                    result.get().addAll(IOUtils.readLines(process.getInputStream(), StandardCharsets.UTF_8));
                } catch (Exception ex) {
                    LOGGER.info("Unable to read cmd result", ex);
                }
            }, 0, 5000, TimeUnit.MILLISECONDS);
            if (!process.waitFor(60_000, TimeUnit.MILLISECONDS)) {
                LOGGER.error("Command '{}' timeout error", command);
                throw new Exception("command" + command + "timeout");
            }
            extractor.shutdownNow();

            List<String> content = IOUtils.readLines(process.getInputStream(), StandardCharsets.UTF_8);
            if (content.size() > 0) {
                result.get().addAll(content);
            }
            int exitValue = process.exitValue();
            if (exitValue != 0) {
                String err = IOUtils.toString(process.getErrorStream(), StandardCharsets.UTF_8);
                LOGGER.error("Command '{}' return errCode: {}, error msg: {}", command, exitValue, err);
                throw new Exception("wrong exit status for command " + command);
            }
            if (isEmptyError && result.get().isEmpty()) {

                LOGGER.error("Command '{}' return empty response", command);
                throw new Exception("command " + command + "response is empty");
            }
            return result.get();
        } catch (Exception e) {
            LOGGER.error("Command '{}' error", command, e);
            throw new RuntimeException(e);
        } finally {
            if (!extractor.isShutdown()) {
                extractor.shutdownNow();
            }
        }
    }
}
