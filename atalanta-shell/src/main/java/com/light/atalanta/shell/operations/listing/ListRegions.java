/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.listing;

import com.light.atalanta.TableName;
import com.light.atalanta.api.DbAdmin;
import com.light.atalanta.Admin;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.shell.ShellUtils;
import org.apache.commons.lang3.StringUtils;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static com.light.atalanta.shell.Commands.LIST_REGIONS;

public final class ListRegions {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListRegions.class);

    public static void action(DbAdmin admin, String line, String command, List<Object> argv, Terminal terminal) {
        if (argv.size() == 1 && argv.get(0) instanceof String &&  ShellUtils.isValidSingleArgString(line, command)) {
            String tableName = (String) argv.get(0);
            try {
                long start = System.currentTimeMillis();
                List<RegionDistributionModel> regions = ((Admin) admin).listTableRegions(TableName.fromString(tableName));
                long end = System.currentTimeMillis();
                terminal.writer().println(String.format(
                        "%sFound %s region replica(s). Time: %s ms.",
                        listToStrings(regions),
                        regions.size(), end - start));
            } catch (Exception ex) {
                terminal.writer().println("Unable to list regions for table '" + tableName + "': " + ex.getMessage());
                LOGGER.debug(StringUtils.EMPTY, ex);
            }
        } else {
            terminal.writer().println("Usage: " + LIST_REGIONS + " 'namespace:table'");
        }
    }

    private static String listToStrings(List<RegionDistributionModel> distribution) {
        String list = distribution.stream().map(RegionDistributionModel::toHumanString)
                .sorted()
                .collect(Collectors.joining("\n"));
        if (distribution.size() > 0) {
            list = list + "\n";
        }
        return list;
    }

    private ListRegions() {
    }
}
