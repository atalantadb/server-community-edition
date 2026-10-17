/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell;

import org.jline.terminal.Terminal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.EMPTY;

public class HbaseStyleArgs {

    public static final HbaseStyleArgs HBASE_STYLE_ARGS_EMPTY = new HbaseStyleArgs();
    private Map<String, String> hbaseArgs = new HashMap<>();

    public static HbaseStyleArgs parse(String arg, Terminal terminal) {
        String[] hbaseArgs = arg.replace(" ", EMPTY)
                .replace("'", EMPTY)
                .replace("}", EMPTY)
                .replace("{", EMPTY)
                .replace("\"", EMPTY)
                .split(",");
        HbaseStyleArgs res = new HbaseStyleArgs();
        for (String hbaseArg : hbaseArgs) {
            String[] split = hbaseArg.split("=>");
            if ( split.length != 2 ) {
                String msg = "wrong arg format: '" + arg + "'";
                terminal.writer().println(msg);
                return HBASE_STYLE_ARGS_EMPTY;
            }
            res.hbaseArgs.put(split[0].toUpperCase(), split[1].toLowerCase());
        }
        return res;
    }

    public static List<String> merge(List<String> argv) {
        List<String> res = new ArrayList<>();
        int stage = 0;
        String mergedStr = "";
        for (String a : argv) {
            // Find the opening element '{...'
            if (stage == 0) {
                if (a.trim().startsWith("{")) {
                    mergedStr = a;
                    stage = 1;
                } else {
                    res.add(a);
                }
            } else if (stage == 1) {
                // Find the closing element '}'

                mergedStr =  mergedStr + a;
                if (a.trim().contains("}")) {
                    res.add(mergedStr);
                    stage = 0;
                    mergedStr = "";
                }
            }
        }
        return res;
    }

    public Map<String, String> getHbaseArgs() {
        return hbaseArgs;
    }
}
