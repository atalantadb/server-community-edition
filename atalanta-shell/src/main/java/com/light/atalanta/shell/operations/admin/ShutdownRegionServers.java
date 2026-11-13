/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.admin;

import com.light.atalanta.Admin;
import com.light.atalanta.api.DbAdmin;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.light.atalanta.shell.Commands.SHUTDOWN_RS;

public final class ShutdownRegionServers {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownRegionServers.class);

    public static void action(DbAdmin admin, String line, String command, List<Object> argv, Terminal terminal) {
        if (!argv.isEmpty() || !line.trim().equals(command)) {
            terminal.writer().println("Usage: " + SHUTDOWN_RS + " (shuts down all running RS and the master)");
            return;
        }
        long start = System.nanoTime();
        terminal.writer().println("Shutting down all running RS and the master; waiting for the final result...");
        terminal.writer().flush();
        try {
            ((Admin) admin).shutdownRs();
            terminal.writer().println("All RS confirmed shutdown. Master is exiting. Time: "
                    + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " ms.");
        } catch (Exception e) {
            terminal.writer().println("Unable to complete shutdown_rs: " + e.getMessage());
            LOGGER.debug("shutdown_rs failed", e);
        }
        terminal.writer().flush();
    }

    private ShutdownRegionServers() {
    }
}
