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

import static com.light.atalanta.shell.Commands.START_REGION_SERVER;
import static com.light.atalanta.shell.Commands.STOP_REGION_SERVER;

public final class StopStartRegionServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(StopStartRegionServer.class);

    public static void action(DbAdmin admin, String line, String command, List<Object> argv, Terminal terminal, Boolean isStart) {
        if (argv.size() == 1 && argv.get(0) instanceof String && ShellUtils.isValidSingleArgString(line, command)) {
            String rs = (String) argv.get(0);
            try {
                long start = System.currentTimeMillis();
                if (isStart) {
                    ((Admin) admin).startRegionServer(rs);
                } else {
                    ((Admin) admin).stopRegionServer(rs);
                }
                long end = System.currentTimeMillis();
                terminal.writer().println("Region server '" + rs + "' " + ( isStart ? "enabled" : "disabled") + " successfully. Time: " + (end - start) + " ms.");
            } catch (Exception ex) {
                String disable = ( isStart ? "enable" : "disable");
                terminal.writer().println("Unable to " + disable + " region server '" + rs + "': " + ex.getMessage());
                LOGGER.debug(StringUtils.EMPTY, ex);
            }
        } else {
            terminal.writer().println("Usage: " + (isStart? START_REGION_SERVER : STOP_REGION_SERVER) + " '<RS_ID>'");
        }
    }

    private StopStartRegionServer() {
    }
}
