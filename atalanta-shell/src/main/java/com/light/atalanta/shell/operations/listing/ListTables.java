/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.listing;

import com.light.atalanta.api.DbAdmin;
import com.light.atalanta.NameOfTable;
import com.light.atalanta.shell.ShellUtils;
import org.apache.commons.lang3.StringUtils;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static com.light.atalanta.shell.Commands.LIST;

public final class ListTables {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListTables.class);

    public static void action(DbAdmin admin, String line, String command, List<Object> argv, Terminal terminal) {
        Pattern patternTable;
        if (argv.size() == 0 && ShellUtils.isValidEmptyArgString(line, command)) {
            patternTable = Pattern.compile(".*");
        } else if (argv.size() == 1 && ShellUtils.isValidSingleArgString(line, command)) {
            patternTable = Pattern.compile((String) argv.get(0));
        } else {
            terminal.writer().println("Usage: " + LIST + " or " + LIST + " 'pattern'");
            return;
        }
        try {
            long start = System.currentTimeMillis();
            java.util.List<NameOfTable> tableNames = Arrays.asList(admin.listTableNames(patternTable));
            long end = System.currentTimeMillis();
            terminal.writer().println(String.format(
                    "%sFound %s table(s). Time: %s ms.",
                    ShellUtils.listToStringsAsc(tableNames),
                    tableNames.size(), end - start));
        } catch (Exception ex) {
            terminal.writer().println("Unable to list tables': " + ex.getMessage());
            LOGGER.debug(StringUtils.EMPTY, ex);
        }
    }

    private ListTables() {
    }
}
