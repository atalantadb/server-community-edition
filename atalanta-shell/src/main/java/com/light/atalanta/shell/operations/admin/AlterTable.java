/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.admin;


import com.light.atalanta.ColumnDescriptor;
import com.light.atalanta.TableDescriptor;
import com.light.atalanta.TableName;
import com.light.atalanta.api.DbAdmin;
import com.light.atalanta.model.CompressType;
import com.light.atalanta.model.RecordCompressionType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.light.atalanta.shell.operations.admin.CreateTable.*;

@SuppressFBWarnings("UC_USELESS_OBJECT")
public final class AlterTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AlterTable.class);

    public static void action(DbAdmin admin, String line, String command, List<Object> argv, Terminal terminal) {
        if (CreateTable.isHbaseFormat(argv)) {
            Map<String, String> params = CreateTable.addAllHbaseParam(argv, terminal);
            String tableName = (String) argv.get(0);
            String columnFamily = params.computeIfAbsent(CF_NAME, k -> (String) argv.get(1));

            try {
                long start = System.currentTimeMillis();
                TableName tableNameImpl = TableName.fromString(tableName);
                TableDescriptor tableDescriptor = new TableDescriptor(tableNameImpl);
                ColumnDescriptor columnDescriptor = new ColumnDescriptor(columnFamily.getBytes(StandardCharsets.UTF_8));
                columnDescriptor.setBlocksize(2);
                if (params.get(COMPRESSION) != null) {
                    columnDescriptor.setCompressionType(CompressType.valueOf(params.get(COMPRESSION)));
                }
                if (params.get(RECORD_COMPRESSION) != null) {
                    columnDescriptor.setRecordCompressionType(RecordCompressionType.valueOf(params.get(RECORD_COMPRESSION)));
                }
                if(params.get(DICTIONARY) != null) {
                    columnDescriptor.setDictionary(Boolean.valueOf(params.get(DICTIONARY)));
                }
                if(params.get(RECORD_TARGET_COUNT) != null) {
                    columnDescriptor.setRecordTargetCount(Long.valueOf(params.get(RECORD_TARGET_COUNT)));
                }
                tableDescriptor.addFamily(columnDescriptor);
                admin.modifyTable(tableDescriptor);
                long end = System.currentTimeMillis();
                terminal.writer().println(String.format(
                        "Table '%s', '%s' modify successfully. Time: %s ms.", tableName, columnFamily, end - start));
            } catch (Exception ex) {
                terminal.writer().println(String.format(
                        "Unable to modify table '%s', '%s': %s", tableName, columnFamily, ex.getMessage()));
                LOGGER.debug(StringUtils.EMPTY, ex);
            }
        } else {
            terminal.writer().println("Usage: " + "alter 'namespace:table', {NAME => 'cf', COMPRESSION => 'SNAPPY', RECORD_COMPRESSION => 'LZ'}");
        }
    }
}
