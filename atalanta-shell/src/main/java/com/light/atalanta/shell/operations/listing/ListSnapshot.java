/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.listing;


import com.light.atalanta.api.DbAdmin;
import com.light.atalanta.Admin;
import com.light.atalanta.shell.ShellUtils;

import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.regex.Pattern;
import static com.light.atalanta.shell.Commands.LIST_SNAPSHOTS;
public final class ListSnapshot {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListSnapshot.class);

    public static void action(DbAdmin admin, String line, String command, List<Object> argv, Terminal terminal) {
        Pattern patternNamespace;
        if (argv.size() == 0 && ShellUtils.isValidEmptyArgString(line, command)) {
            patternNamespace = Pattern.compile(".*");
        } else if (argv.size() == 1 && argv.get(0) instanceof String
                && ShellUtils.isValidSingleArgString(line, command)) {
            patternNamespace = Pattern.compile((String) argv.get(0));
        } else {
            terminal.writer().println("Usage: " + LIST_SNAPSHOTS + " or " + LIST_SNAPSHOTS + "'pattern'");
            return;
        }

        try {
            long start = System.currentTimeMillis();
            List<String> snapshots = ((Admin) admin).listSnapshot(patternNamespace);
            long end = System.currentTimeMillis();
            terminal.writer().println(String.format(
                    "%s Found %s snapshots (s). Time: %s ms.",
                    ShellUtils.listToStringsAsc(snapshots), snapshots.size(), end - start));
        } catch (Exception ex) {
            terminal.writer().println("Unable to list snapshots': " + ex.getMessage());
            LOGGER.debug("error!", ex);
        }
    }

    private ListSnapshot() {
    }
}