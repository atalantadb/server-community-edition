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

import static com.light.atalanta.shell.Commands.CLONE_SNAPSHOT;
import static com.light.atalanta.shell.ATALANTAShell.FACTORY;

public final class CloneSnapshot {
    private static final Logger LOGGER = LoggerFactory.getLogger(CloneSnapshot.class);
    private CloneSnapshot() {
    }
    public static void action (DbAdmin admin, String line, String command, List<Object> argv, Terminal terminal) {
        if (argv.size() == 2 && !argv.stream().filter(a -> !(a instanceof String)).findAny().isPresent()
                && ShellUtils.isValidDualArgString(line, command)) {
            String snapshotName = (String) argv.get(0);
            String tableName = (String) argv.get(1);
            try {
                long start = System.currentTimeMillis();
                admin.cloneSnapshotAsync(snapshotName, FACTORY.tableName(tableName));
                long end = System.currentTimeMillis();
                terminal.writer().println(String.format(
                        "Starting clone snapshot '%s' for table '%s'. Time: %s ms.", snapshotName, tableName, end - start));
            } catch (Exception ex) {
                terminal.writer().println(String.format(
                        "Unable to clone snapshot '%s' for table '%s'. Error: %s", snapshotName, tableName, ex.getMessage()));
                LOGGER.debug(StringUtils.EMPTY, ex);
            }
        } else {
            terminal.writer().println("Usage: " + CLONE_SNAPSHOT + " 'snapshot_name', 'namespace:table'");
        }
    }
}