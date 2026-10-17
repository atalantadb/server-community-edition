/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.data;

import com.light.atalanta.TableName;
import com.light.atalanta.Connection;
import com.light.atalanta.Delete;
import com.light.atalanta.shell.ShellUtils;
import org.apache.commons.lang3.StringUtils;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.light.atalanta.shell.Commands.DELETE;
import static com.light.atalanta.shell.Commands.DELETE_BYTES;
import static com.light.atalanta.shell.ShellUtils.getByteValue;

public final class DelRows {
    private static final Logger LOGGER = LoggerFactory.getLogger(Delete.class);

    public static void action(Connection connection, String line, String command, List<Object> argv, Terminal terminal, boolean bytes, boolean slat) {
        if ((argv.size() == 2 || argv.size() == 3) && argv.get(0) instanceof String && ShellUtils.isValidDualArgString(line, command)) {
            String tableName = (String) argv.get(0);
            try {
                long start = System.currentTimeMillis();
                Delete del = new Delete(getByteValue(argv.get(1), slat));
                if (argv.size() == 3) {
                    String[] column = argv.get(2).toString().split(":");
                    del.addColumn(column[0].getBytes(), column[1].getBytes());
                }
                connection.getTable(TableName.fromString(tableName)).delete(del);
                long end = System.currentTimeMillis();
                terminal.writer().println(String.format(
                        "Table '%s' delete data successfully. Time: %s ms.",
                        tableName, end - start));
            } catch (Exception ex) {
                terminal.writer().println("Unable to get data from table '" + tableName + "': " + ex.getMessage());
                LOGGER.debug(StringUtils.EMPTY, ex);
            }
        } else {
            if (bytes) {
                terminal.writer().println("Usage: " + DELETE_BYTES + " 'namespace:table', 'row_key', <'family:qualifier'>");
            } else {
                terminal.writer().println("Usage: " + DELETE + " 'namespace:table', 'row_key', <'family:qualifier'>");
            }
        }
    }

    public static void action(Connection connection, String line, String command, List<Object> argv, Terminal terminal, boolean bytes) {
        action(connection, line, command, argv, terminal, bytes, false);
    }

    private DelRows() {
    }
}
