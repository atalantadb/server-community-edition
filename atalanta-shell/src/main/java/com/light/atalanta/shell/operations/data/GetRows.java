/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.data;


import com.light.atalanta.TableName;
import com.light.atalanta.Connection;
import com.light.atalanta.DbResult;
import com.light.atalanta.Get;
import com.light.atalanta.Result;
import com.light.atalanta.shell.ShellUtils;
import com.light.atalanta.transport.values.KeyValues;
import com.light.atalanta.transport.values.QualifierValue;
import com.light.atalanta.util.Bytes;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static com.light.atalanta.shell.Commands.GET;
import static com.light.atalanta.shell.Commands.GET_BYTES;
import static com.light.atalanta.shell.ShellUtils.*;

public final class GetRows {
    private static final Logger LOGGER = LoggerFactory.getLogger(Get.class);


    public static void action(Connection connection, String line, String command, List<Object> argv, Terminal terminal, boolean bytes, boolean salt, boolean isBynary) {
        if (argv.size() == 2 && argv.get(0) instanceof String && ShellUtils.isValidDualArgString(line, command)) {
            String tableName = (String) argv.get(0);
            try {
                long start = System.currentTimeMillis();
                Get getModel = new Get(getByteValue(argv.get(1), salt));
                DbResult result = connection.getTable(TableName.fromString(tableName)).get(getModel);
                long end = System.currentTimeMillis();
                String resultStr = !result.isEmpty() ? resultToString((Result) result, bytes, terminal.getSize().getColumns(), isBynary) : "\nNo results found.";
                terminal.writer().println(String.format("%-" + (1 + terminal.getSize().getColumns() / 4) + "s CELL"
                                + "%s%n Table '%s' get data successfully. Time: %s ms.",
                        "COLUMN", resultStr, tableName, end - start));
            } catch (Exception ex) {
                terminal.writer().println("Unable to get data from table '" + tableName + "': " + ex.getMessage());
                LOGGER.debug(StringUtils.EMPTY, ex);
            }
        } else {
            if (bytes) {
                terminal.writer().println("Usage: " + GET_BYTES + " 'namespace:table', 'row_key");
            } else {
                terminal.writer().println("Usage: " + GET + "'namespace:table', 'row_key");
            }
        }
    }

    public static String resultToString(Result result, boolean bytes, int columns, boolean isBinary) {
        SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd 'T'hh:mm:ss.SSS");
        StringBuilder sb = new StringBuilder();
        if (result != null && result.getKeyValues() != null) {
            for (KeyValues kv : sortCfAsc(result.getKeyValues())) {
                if (kv.getValues().length == 0) {
                    sb.append(String.format("%nkey=%s\tno value!", Bytes.toStringBinary(kv.getRowKey())));
                }
                for (QualifierValue qv : sortQualifiersAsc(kv.getValues())) {
                    String value;
                    if (bytes) {
                        value = Hex.encodeHexString(qv.getValue());
                    } else {
                        value = isBinary ? Bytes.toStringBinary(qv.getValue()) : new String(qv.getValue(), StandardCharsets.UTF_8);
                    }
                    sb.append(String.format("%n %-" + (1 + columns / 4) + "s timestamp=%s, value=%s",
                            " " + new String(kv.getCfName(), StandardCharsets.UTF_8) + ":" + new String(qv.getQualifier(), StandardCharsets.UTF_8), dt.format(new Date(qv.getTimestamp())),
                            value));
                }
            }
        }

        sb.append(String.format("%n %d row(s)", result.getKeyValues().length));
        if (sb.toString().length() > 0) {
            return sb.toString();
        }
        return "\nNo results found." + sb;
    }

    private GetRows() {
    }

    public static void action(Connection connection, String line, String command, List<Object> parseArgs, Terminal terminal, boolean b) {
        action(connection, line, command, parseArgs, terminal, b, false, false);
    }

    public static void action(Connection connection, String line, String command, List<Object> parseArgs, Terminal terminal, boolean b, boolean salt) {
        action(connection, line, command, parseArgs, terminal, b, salt, false);
    }
}
