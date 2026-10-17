/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.admin;

import com.light.atalanta.api.DbAdmin;
import com.light.atalanta.Admin;
import com.light.atalanta.TableName;
import com.light.atalanta.shell.ShellUtils;
import org.apache.commons.lang3.StringUtils;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.light.atalanta.shell.Commands.SPLIT;

public final class SplitRegion {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitRegion.class);

    public static void action(DbAdmin admin, String line, String command, List<Object> argv, Terminal terminal) {
        if (argv.size() == 1 && argv.get(0) instanceof String && ShellUtils.isValidSingleArgString(line, command)) {
            String target = (String) argv.get(0);
            String targetType = "Region or table";
            try {
                long start = System.currentTimeMillis();
                TableName tableName = TableName.fromString(target);
                if (target.contains(":") || admin.tableExists(tableName)) {
                    targetType = "Table";
                    ((Admin) admin).split(tableName);
                } else {
                    targetType = "Region";
                    ((Admin) admin).split(target);
                }
                long end = System.currentTimeMillis();
                terminal.writer().println(String.format(
                        "%s '%s' split was successful. Time: %s ms.",
                        targetType, target, end - start));
            } catch (Exception ex) {
                terminal.writer().println(String.format(
                        "Unable to split %s '%s': %s",
                        targetType, target, ex.getMessage()));
                LOGGER.debug(StringUtils.EMPTY, ex);
            }
        } else {
            terminal.writer().println("Usage: " + SPLIT + " 'regionName' | " + SPLIT + " 'namespace:table' | " + SPLIT + " 'table'");
        }
    }

    private SplitRegion() {
    }
}
