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

import static com.light.atalanta.shell.Commands.STOP_REBALANCE_REPLICAS;

public final class StopReplicaRebalanceTasks {
    private static final Logger LOGGER = LoggerFactory.getLogger(StopReplicaRebalanceTasks.class);

    public static void action(DbAdmin admin, String line, String command, List<Object> argv, Terminal terminal) {
        if (argv.size() == 1 && argv.get(0) instanceof String && ShellUtils.isValidSingleArgString(line, command)) {
            String rebalanceEntity = (String) argv.get(0);
            try {
                long start = System.currentTimeMillis();
                ((Admin) admin).markReplicasRebalanceStop(rebalanceEntity);
                long end = System.currentTimeMillis();
                if (rebalanceEntity.equals("!")) {
                    terminal.writer().println(String.format(
                            "Rebalancing of all DB replicas was marked to stop. Time: %s ms.",
                            end - start));
                } else if (rebalanceEntity.startsWith("@")) {
                    terminal.writer().println(String.format(
                            "Rebalancing of namespace '%s' table replicas was marked to stop. Time: %s ms.",
                            rebalanceEntity.replaceFirst("@", StringUtils.EMPTY), end - start));
                } else {
                    terminal.writer().println(String.format(
                            "Rebalancing of table '%s' replicas was marked to stop. Time: %s ms.",
                            rebalanceEntity, end - start));
                }
            } catch (Exception ex) {
                terminal.writer().println(String.format(
                        "Unable to mark replicas rebalancing to stop: %s", ex.getMessage()));
                LOGGER.debug(StringUtils.EMPTY, ex);
            }
        } else {
            terminal.writer().println(String.format(
                    "Usage:%n"
                            + "%s '!' – database root level%n"
                            + "%s '@namespace' – namespace level%n"
                            + "%s 'namespace:table' – table level%n"
                            + "Note: wildcard is supported",
                    STOP_REBALANCE_REPLICAS, STOP_REBALANCE_REPLICAS, STOP_REBALANCE_REPLICAS));
        }
    }

    private StopReplicaRebalanceTasks() {
    }
}
