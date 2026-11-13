/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.data;

import com.light.atalanta.*;
import com.light.atalanta.shell.ShellUtils;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.light.atalanta.shell.Commands.PUT;
import static com.light.atalanta.shell.Commands.PUT_BYTES;

public final class PutRows {
    private static final Logger LOGGER = LoggerFactory.getLogger(Put.class);

    public static void action(Connection connection, String line, String command, List<Object> argv, Terminal terminal, boolean bytes, boolean salt) {
        final int indexTableName = 0;
        final int indexRowKey = 1;
        final int indexCf = 2;
        final int indexValue = 3;
        // jline doesn't treat an empty value as a parameter,
        // so add a separate empty parameter
        if (argv.size() == 3) {
            argv.add(StringUtils.EMPTY);
        }
        if ((argv.size() == 4 || argv.size() == 5)  && argv.get(indexTableName) instanceof String
                && argv.get(indexCf) instanceof String
                && (argv.size() == 5 || ShellUtils.isValidQuadrupleArgString(line, command)) ) {
            // Prepare the arguments
            String tableName = (String) argv.get(indexTableName);
            byte[] rowKey = ShellUtils.getByteValue(argv.get(indexRowKey), salt);
            String cfCq = (String) argv.get(indexCf);
            Object value = argv.get(indexValue);
            try {
                // Additional preparation
                Preconditions.checkArgument(cfCq.split(":").length == 2, "damaged column family and/or qualifier");
                String columnFamily = cfCq.split(":")[indexTableName];
                String columnQualifier = cfCq.split(":")[indexRowKey];
                // The main part of the operation
                long start = System.currentTimeMillis();
                Put put = argv.size() == 5 ?  new Put(rowKey, Long.valueOf((String) argv.get(4))): new Put(rowKey);
                byte[] data;
                if (bytes && value instanceof String) {
                    data = Hex.decodeHex(((String) value).toCharArray());
                } else {
                    data = value instanceof String ? ((String) value).getBytes(StandardCharsets.UTF_8) : (byte[]) value;
                }
                put.addColumn(columnFamily.getBytes(StandardCharsets.UTF_8),
                        columnQualifier.getBytes(StandardCharsets.UTF_8),
                        data);

                Table table = connection.getTable(TableName.fromString(tableName));
                // putAndGet is experimental and not ready for use; use ordinary put
                table.put(put);
                long end = System.currentTimeMillis();
                terminal.writer().println("Table '" + tableName + "' put data complete. Time: " + (end - start) + " ms.");
                terminal.flush();
            } catch (Exception ex) {
                String msg = "Unable put data for table '" + tableName ;
                terminal.writer().println(msg + "': " + ex.getMessage());
                LOGGER.error(msg, ex);
            }
        } else {
            if (bytes) {
                terminal.writer().println("Usage: " + PUT_BYTES + " 'namespace:table', 'row_key', 'col_family:col_qualifier', 'hex_string'");
            } else {
                terminal.writer().println("Usage: " + PUT + " 'namespace:table', 'row_key', 'col_family:col_qualifier', 'value'");
            }
        }
    }

    public static void action(Connection connection, String line, String command, List<Object> argv, Terminal terminal, boolean bytes) {
        action(connection, line, command, argv, terminal, bytes, false);
    }

    private PutRows() {
    }
}
