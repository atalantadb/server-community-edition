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
import com.light.atalanta.shell.HbaseStyleArgs;
import com.light.atalanta.shell.ShellUtils;
import com.light.atalanta.util.HexStringSplit;
import com.light.atalanta.util.UniformSplit;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.light.atalanta.shell.Commands.CREATE_TABLE;

@SuppressFBWarnings("UC_USELESS_OBJECT")
public final class CreateTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateTable.class);
    public static final String BLOCKSIZE_PRM = "blocksize=";
    public static final String NUMREGIONS = "NUMREGIONS";
    public static final String COMPRESSION = "COMPRESSION";
    public static final String RECORD_COMPRESSION = "RECORD_COMPRESSION";
    public static final String DICTIONARY = "DICTIONARY";
    public static final String SPLITALGO = "SPLITALGO";
    public static final String RECORD_TARGET_COUNT = "RECORD_TARGET_COUNT";
    public static final String CF_NAME = "NAME";
    public static final List<String> TABLE_PRM_LIST = Arrays.asList(CF_NAME, NUMREGIONS, COMPRESSION, RECORD_COMPRESSION,
            DICTIONARY, SPLITALGO, RECORD_TARGET_COUNT);

    public static void action(DbAdmin admin, String line, String command, List<Object> argv, Terminal terminal) {
        if ((argv.size() == 2 && argv.get(0) instanceof String && argv.get(1) instanceof String
                && ShellUtils.isValidDualArgString(line, command))
                || (argv.size() == 3 && argv.get(2) instanceof String && checkRegionCount(argv)) ||
                isHbaseFormat(argv)) {
            Map<String, String> params = addAllHbaseParam(argv, terminal);
            String tableName = (String) argv.get(0);
            String columnFamily = params.computeIfAbsent(CF_NAME, k -> (String) argv.get(1));
            Integer regionCount = argv.size() > 2 && argv.get(2) instanceof String ?
                    Integer.valueOf((String) argv.get(2)) : null;
            String regionCountPrm = params.get(NUMREGIONS);
            regionCount = regionCountPrm != null ? Integer.valueOf(regionCountPrm) : regionCount;

            // RECORD_TARGET_COUNT, an optional parameter
            long recordTargetCount = 0L;
            String recordTargetCountPrm = params.get(RECORD_TARGET_COUNT);
            if (recordTargetCountPrm != null) {
                try {
                    recordTargetCount = Long.valueOf(recordTargetCountPrm);
                    if (regionCount != null)
                        if (regionCount != 0 && recordTargetCount != 0)
                            recordTargetCount = recordTargetCount / (long) regionCount;
                } catch (NumberFormatException ex) {
                    terminal.writer().println(
                            "WARNING: Ignore invalid RECORD_TARGET_COUNT value: " + recordTargetCountPrm);
                }
            }

            try {
                long start = System.currentTimeMillis();
                TableName tableNameImpl = TableName.fromString(tableName);
                TableDescriptor tableDescriptor = new TableDescriptor(tableNameImpl);
                ColumnDescriptor columnDescriptor = new ColumnDescriptor(columnFamily.getBytes(StandardCharsets.UTF_8));
                columnDescriptor.setBlocksize(2);
                columnDescriptor.setCompressionType(CompressType.valueOf(params.getOrDefault(COMPRESSION, "none")));
                columnDescriptor.setRecordCompressionType(RecordCompressionType.valueOf(params.getOrDefault(RECORD_COMPRESSION, "lz")));
                columnDescriptor.setDictionary(Boolean.valueOf(params.getOrDefault(DICTIONARY, "false")));
                if(params.get(RECORD_TARGET_COUNT) != null) {
                    columnDescriptor.setRecordTargetCount(Long.valueOf(params.get(RECORD_TARGET_COUNT)));
                }
                tableDescriptor.addFamily(columnDescriptor);
                if (regionCount == null || regionCount == 1) {
                    admin.createTable(tableDescriptor, new byte[0][0]);
                } else {
                    byte[][] split;
                    if (params.getOrDefault(SPLITALGO, "uniformsplit").equals("hexstringsplit")) {
                        split = (new HexStringSplit()).split(regionCount);
                    } else {
                        split = (new UniformSplit()).split(regionCount);
                    }
                    admin.createTable(tableDescriptor, split);
                }

                long end = System.currentTimeMillis();
                terminal.writer().println(String.format(
                        "Table '%s', '%s' created successfully. Time: %s ms.", tableName, columnFamily, end - start));
            } catch (Exception ex) {
                terminal.writer().println(String.format(
                        "Unable to create table '%s', '%s': %s", tableName, columnFamily, ex.getMessage()));
                LOGGER.debug(StringUtils.EMPTY, ex);
            }
        } else {
            terminal.writer().println("Usage: " + CREATE_TABLE + " 'namespace:table', 'cf'");
            terminal.writer().println("Usage: " + CREATE_TABLE + " 'namespace:table', 'cf', <REGION COUNT>");
            terminal.writer().println("Usage: "
                    + "create 'namespace:table', {NAME => 'cf', DICTIONARY => 'TRUE'}");
            terminal.writer().println("Usage: "
                    + "create 'namespace:table', {NAME => 'cf', DICTIONARY => 'TRUE', RECORD_COMPRESSION => 'LZ'}");
            terminal.writer().println("Usage: "
                    + "create 'namespace:table', {NAME => 'cf', DICTIONARY => 'TRUE', COMPRESSION => 'SNAPPY'}");
            terminal.writer().println("Usage: "
                    + "create 'namespace:table', {NAME => 'cf', COMPRESSION => 'LZ', DICTIONARY => 'TRUE'}, "
                    + "{NUMREGIONS => <REGION COUNT>, SPLITALGO => 'UniformSplit'}");
            terminal.writer().println("Usage: "
                    + "create 'namespace:table', {NAME => 'cf', DICTIONARY => 'TRUE'}, "
                    + "{NUMREGIONS => <REGION COUNT>, "
                    + "SPLITALGO => 'UniformSplit', RECORD_TARGET_COUNT => <FORECASTED KEYS COUNT PER REGION>}");
            terminal.writer().println("Notice1: Please note the following behavior regarding compression parameters:");
            terminal.writer().println("If a value other than `NONE` is specified for the general `COMPRESSION`");
            terminal.writer().println("  the `RECORD_COMPRESSION` parameter will be ignored, regardless of the value set for it.");
            terminal.writer().println("  The `RECORD_COMPRESSION` setting is only effective when `COMPRESSION = NONE`.");
            terminal.writer().println("Notice2: Specifying the `RECORD_TARGET_COUNT`parameter helps to prevent ");
            terminal.writer().println("  performance degradation during index auto resize.");
            terminal.writer().println("  However, high value inherently increases immediate RAM consumption.");
        }
    }

    static Map<String, String> addAllHbaseParam(List<Object> argv, Terminal terminal) {
        Map<String, String> res = new HashMap<>();
        for (Object arg : argv) {
            if (arg instanceof HbaseStyleArgs) {
                res.putAll(((HbaseStyleArgs) arg).getHbaseArgs());
            }
        }
        res.keySet().forEach( k -> {
            if (!TABLE_PRM_LIST.contains(k)) {
                terminal.writer().println("WARNING: Ignore unknown param: " + k);
            }
        });
        return res;
    }

    static boolean isHbaseFormat(List<Object> argv) {
        if (argv.size() > 1 && argv.get(0) instanceof String) {
            for (int i = 1; i < argv.size(); ++i) {
                if (!(argv.get(i) instanceof HbaseStyleArgs)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    private static boolean checkRegionCount(List<Object> argv) {
        try {
            return Integer.parseInt((String) argv.get(2)) > 0;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    private static Integer parseBlocksize(String argv) {
        return Integer.valueOf(argv.substring(BLOCKSIZE_PRM.length()));
    }

    private static boolean checkBlockSize(String s) {
        String normStr = s.toLowerCase(Locale.ROOT).replace(" ", "");
        try {
            return normStr.startsWith(BLOCKSIZE_PRM) || parseBlocksize(normStr) != null;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    private CreateTable() {
    }
}
