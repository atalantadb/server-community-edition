/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.admin;

import com.light.atalanta.api.DbAdmin;
import com.light.atalanta.Admin;
import com.light.atalanta.shell.ShellUtils;
import org.apache.commons.lang3.StringUtils;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.light.atalanta.shell.Commands.SYNC_REPLICAS;

/**
 * Synchronizes the reserve replicas of a region with the leader replicas.
 */
public final class SyncReplicasAsync {
    private static final Logger LOGGER = LoggerFactory.getLogger(SyncReplicasAsync.class);

    public static void action(DbAdmin admin, String line, String command, List<Object> argv, Terminal terminal) {
        if (argv.size() == 1 && argv.get(0) instanceof String && ShellUtils.isValidSingleArgString(line, command)) {
            String syncEntity = (String) argv.get(0);
            try {
                long start = System.currentTimeMillis();
                ((Admin) admin).syncReplicasAsync(syncEntity);
                long end = System.currentTimeMillis();
                if (syncEntity.equals("!")) {
                    terminal.writer().println(String.format(
                            "Sync of all DB table replicas was started. Time: %s ms.",
                            end - start));
                } else if (syncEntity.startsWith("@")) {
                    terminal.writer().println(String.format(
                            "Sync of namespace '%s' table replicas was started. Time: %s ms.",
                            syncEntity.replaceFirst("@", StringUtils.EMPTY), end - start));
                } else {
                    terminal.writer().println(String.format(
                            "Sync of table '%s' replicas was started. Time: %s ms.",
                            syncEntity, end - start));
                }
            } catch (Exception ex) {
                terminal.writer().println(String.format(
                        "Unable to start replicas sync: %s", ex.getMessage()));
                LOGGER.debug(StringUtils.EMPTY, ex);
            }
        } else {
            terminal.writer().println(String.format(
                    "Usage:%n"
                            + "%s '!' – database root level%n"
                            + "%s '@namespace' – namespace level%n"
                            + "%s 'namespace:table' – table level%n"
                            + "Note: wildcard is supported",
                    SYNC_REPLICAS, SYNC_REPLICAS, SYNC_REPLICAS));
        }
    }

    private SyncReplicasAsync() {
    }
}
