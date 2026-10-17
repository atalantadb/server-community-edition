/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.data;

import com.light.atalanta.TableName;
import com.light.atalanta.DbResult;
import com.light.atalanta.ResultScanner;
import com.light.atalanta.api.DbTable;
import com.light.atalanta.Connection;
import com.light.atalanta.Result;
import com.light.atalanta.Scan;
import com.light.atalanta.shell.JsonLogging;
import com.light.atalanta.shell.ShellUtils;
import org.apache.commons.lang3.StringUtils;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.light.atalanta.shell.Commands.SCAN;

public final class ScanRows {
    private static final Logger LOGGER = LoggerFactory.getLogger(Scan.class);

    private ScanRows() {
    }

    public static void action(Connection connection, String line, String command, List<Object> argv, Terminal terminal) {
        if (argv.size() > 0) {
            Map<String, String> addProps = ShellUtils.getPropertiesFromArguments(argv);
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
                if (addProps.containsKey("LIMIT")) {
                    scanModel.setCacheSize(Integer.parseInt(addProps.get("LIMIT")));
                }
                DbTable table = connection.getTable(TableName.fromString(tableName));
                ResultScanner scanner = table.getScanner(scanModel);

                DbResult result;
                int count = 0;
                JsonLogging jsonLogging = new JsonLogging(argv.get(argv.size() - 1));
                while ((result = scanner.next()) != null) {
                    Map<String, Map<String, String>> values = new HashMap<>();
                    String resultStr = ShellUtils.resultToString((Result) result, false, values);
                    terminal.writer().println(resultStr);
                    count++;
                    jsonLogging.log(line, tableName, values);
                }
                jsonLogging.close();
                long end = System.currentTimeMillis();
                terminal.writer().println(String.format("Table '%s' scan successfully. Found %s record(s). Time: %s ms.", tableName, count, end - start));
            } catch (Exception ex) {
                terminal.writer().println("Unable to get data from table '" + tableName + "': " + ex.getMessage());
                LOGGER.debug(StringUtils.EMPTY, ex);
            }
        } else {
            terminal.writer().println("Usage: " + SCAN + " 'namespace:table'");
            terminal.writer().println("Usage: " + SCAN + " 'namespace:table', 'a', 'b'");
            terminal.writer().println("Usage: " + SCAN + " 'namespace:table', \"\\x00\\x01\", \"\\xFF\\xFF\"");
        }
    }
}
