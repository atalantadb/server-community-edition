/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.data;

import com.light.atalanta.*;
import com.light.atalanta.Scan;
import com.light.atalanta.api.DbTable;
import com.light.atalanta.util.Bytes;
import org.apache.commons.lang3.StringUtils;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.light.atalanta.shell.Commands.COUNT;

public final class Count {
    private static final Logger LOGGER = LoggerFactory.getLogger(Count.class);

    private Count() {
    }


    public static void action(Connection connection, String line, String command, List<Object> argv, Terminal terminal) {
        if (!argv.isEmpty()) {
            String tableName = (String) argv.get(0);
            try {
                long start = System.currentTimeMillis();
                Scan scanModel = new Scan();
                if (argv.size() > 1) {
                    Object startRow = argv.get(1);
                    if (startRow instanceof byte[]) {
                        if (((byte[]) startRow).length > 0) {
                            scanModel.withStartRow((byte[]) startRow);
                        }
                    } else {
                        scanModel.withStartRow(((String) startRow).getBytes(StandardCharsets.UTF_8));
                    }
                }
                if (argv.size() > 2) {
                    Object stopRow = argv.get(2);
                    if (stopRow instanceof byte[]) {
                        if (((byte[]) stopRow).length > 0) {
                            scanModel.withStopRow((byte[]) stopRow);
                        }
                    } else {
                        scanModel.withStopRow(((String) stopRow).getBytes(StandardCharsets.UTF_8));
                    }
                }

                long count = makeCountNew(connection, tableName);
                long end = System.currentTimeMillis();

                terminal.writer().println(String.format("Total count: %s record(s). Time: %s ms.", count, end - start));
                terminal.writer().println(String.format("Table '%s' counting successfully. Found %s record(s). Time: %s ms.", tableName, count, end - start));
            } catch (Exception ex) {
                terminal.writer().println("Unable to get data from table '" + tableName + " : " + ex.getMessage());
                LOGGER.debug(StringUtils.EMPTY, ex);
            }
        } else {
            terminal.writer().println("Usage: " + COUNT + "'namespace:table'");
            terminal.writer().println("Usage: " + COUNT + "'namespace:table', <StartRow>, <StopRow>");
            terminal.writer().println("Example: " + COUNT + " 'namespace:table', 'a','Z");
            terminal.writer().println("Example: " + COUNT + " \"\\x00\\x00\", \"0xFFFF\" ");
        }
    }

    private static int makeCount(Connection connection, String tableName, Scan scanModel, long start) throws IOException {
        long printTime = start;
        int count = 0;
        DbTable table = connection.getTable(TableName.fromString(tableName));
        ResultScanner scanner = table.getScanner(scanModel);
        DbResult r;
        while ((r = scanner.next()) != null) {
            if (System.currentTimeMillis() - printTime > 1_000) {
                System.out.printf("count: %s. time: %s ms. row: %s%n", count, System.currentTimeMillis() - start, Bytes.toHex(r.getRow()));
                printTime = System.currentTimeMillis();
            }
            ++count;
        }
        return count;
    }

    private static long makeCountNew(Connection connection, String tableName) throws IOException {
        DbTable table = connection.getTable(TableName.fromString(tableName));
        return table.count();
    }

}
