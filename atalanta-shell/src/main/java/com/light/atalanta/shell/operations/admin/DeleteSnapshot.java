/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.admin;


import com.light.atalanta.api.DbAdmin;
import com.light.atalanta.shell.ShellUtils;
import org.apache.commons.lang3.StringUtils;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.light.atalanta.shell.Commands.DELETE_SNAPSHOT;

public final class DeleteSnapshot {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteSnapshot.class);

    public static void action(DbAdmin admin, String line, String command, List<Object> argv, Terminal terminal) {
        if (argv.size() == 1 && argv.get(0) instanceof String && ShellUtils.isValidSingleArgString(line, command)) {
            String name = (String) argv.get(0);
            try {
                long start = System.currentTimeMillis();
                admin.deleteSnapshot(name);
                long end = System.currentTimeMillis();
                terminal.writer().println("Snapshot " + name + " delete successfully. Time: + (end start) + ms.");
            } catch (Exception ex) {
                terminal.writer().println("Unable to delete snapshot " + name + ":" + ex.getMessage());
                LOGGER.debug(StringUtils.EMPTY, ex);
            }
        } else {
            terminal.writer().println("Usage: " + DELETE_SNAPSHOT + " 'namespace:table'");
        }
    }

    private DeleteSnapshot() {
    }
}