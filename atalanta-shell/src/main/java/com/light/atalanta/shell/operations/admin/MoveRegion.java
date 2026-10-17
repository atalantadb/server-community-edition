/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.admin;

import com.light.atalanta.api.DbAdmin;
import com.light.atalanta.Admin;
import com.light.atalanta.shell.ShellUtils;
import org.apache.commons.lang3.StringUtils;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.light.atalanta.shell.Commands.MOVE;

public final class MoveRegion {
    private static final Logger LOGGER = LoggerFactory.getLogger(MoveRegion.class);


    public static void action(DbAdmin admin, String line, String command, List<Object> argv, Terminal terminal) {
        if (argv.size() == 2 && argv.get(0) instanceof String && argv.get(1) instanceof String
                && ShellUtils.isValidDualArgString(line, command)) {
            String regionName = (String) argv.get(0);
            String regionServerName = (String) argv.get(1);
            try {
                long start = System.currentTimeMillis();
                ((Admin) admin).move(regionName, regionServerName);
                long end = System.currentTimeMillis();
                terminal.writer().println(String.format(
                        "Region '%s' moved to '%s' successfully. Time: %s ms.",
                        regionName, regionServerName, end - start));
            } catch (Exception ex) {
                terminal.writer().println(String.format(
                        "Unable to move region '%s' to '%s': %s",
                        regionName, regionServerName, ex.getMessage()));
                LOGGER.debug(StringUtils.EMPTY, ex);
            }
        } else {
            terminal.writer().println("Usage: " + MOVE + " 'regionName_replicaNum', 'regionServerName'");
        }
    }

    private MoveRegion() {
    }
}
