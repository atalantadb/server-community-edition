/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.data;


import com.light.atalanta.Connection;
import com.light.atalanta.util.XxHash32KeySalter;
import com.light.atalanta.util.Bytes;
import org.jline.terminal. Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import static com.light.atalanta.shell.Commands.SALT;
import static com.light.atalanta.shell.ShellUtils.getByteValue;
public final class Salt {
    private static final Logger LOGGER = LoggerFactory.getLogger(Salt.class);
    private Salt() {
    }

    public static void action (Connection connection, String line, String command, List<Object> argv, Terminal terminal) {
        if (!argv.isEmpty()) {
            byte[] value = getByteValue(argv.get(0), false);
            long start = System.currentTimeMillis();
            byte[] res = XxHash32KeySalter.saltKey(value);
            long end = System.currentTimeMillis();
            System.out.println(String.format("%s' (0x%s)", Bytes.toStringBinary(res), Bytes.toHex(res))); terminal.writer().println(String.format("Time: %s ms.", end - start));
        } else {
            terminal.writer().println("Usage: " + SALT + " <rowkey>");
            terminal.writer().println("Example: " + SALT + " \"\\x00\\x00\" ");
            terminal.writer().println("Example: " + SALT + " \"0xFFFF\" ");
            terminal.writer().println("Example: " + SALT + " 'key' ");
        }
    }
    }
