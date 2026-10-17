/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.helpers;

import com.light.atalanta.TableName;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.util.ZookeeperStatus;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import static com.light.atalanta.ATALANTAConstants.DEFAULT_NAMESPACE;

/**
 * Selects tables by a pattern string at one of three levels:
 * 1. All tables in the database: "!"
 * 2. All tables in a namespace: "@namespace_name"
 * 3. A single table: "namespace_name:table_name"
 *
 * Wildcards are supported.
 */
public class TableCandidateSelector {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableCandidateSelector.class);

    public static final String ALL_DB_REGIONS = "!";

    public static final String NAMESPACE_REGIONS = "@";

    public static List<TableName> select(ZooKeeperMasterContext zooContext, ZookeeperHelper zooHelper, String rebalanceEntity, boolean checkStatus) throws Exception {
        List<TableName> tableNames = new ArrayList<>();
        try {
            if (rebalanceEntity.equals(ALL_DB_REGIONS)) {
                tableNames.addAll(zooContext.listTables());
            } else if (rebalanceEntity.startsWith(NAMESPACE_REGIONS)) {
                rebalanceEntity = rebalanceEntity.replaceFirst(NAMESPACE_REGIONS, StringUtils.EMPTY);
                for (String ns : zooContext.listNamespace()) {
                    if (isMatch(ns, rebalanceEntity, "namespace")) {
                        tableNames.addAll(zooContext.listNamespaceTables(ns));
                    }
                }
            } else {
                String rebalanceTableWildcard = rebalanceEntity;
                if ( !rebalanceTableWildcard.contains(":") && !rebalanceTableWildcard.startsWith("*")) {
                    rebalanceTableWildcard = DEFAULT_NAMESPACE + ":" + rebalanceEntity;
                }
                for (TableName tableName : zooContext.listTables()) {
                    if (isMatch(tableName.toString(), rebalanceTableWildcard, "table")) {
                        tableNames.add(tableName);
                    }
                }
            }
            if (checkStatus) {
                Iterator<TableName> tableNameIterator = tableNames.iterator();
                while (tableNameIterator.hasNext()) {
                    TableName tableName = tableNameIterator.next();
                    try {
                        checkTableStatus(zooHelper, tableName);
                    } catch (InappropriateTableStatusException ex) {
                        LOGGER.info("Skipping table {} rebalance because of status", tableName);
                        tableNameIterator.remove();
                    }
                }
            }
        } catch (Exception ex) {
            throw new Exception("unable to compute table list for rebalance", ex);
        }
        return tableNames;
    }

    private static boolean isMatch(String rebalanceEntity, String wildcard, String entityType) {
        if (rebalanceEntity.equals(wildcard)) {
            LOGGER.debug("Marking {} '{}' for rebalancing because wildcard is: {}", entityType, rebalanceEntity, wildcard);
            return true;
        }
        String regEx = wildcardToRegEx(wildcard);
        boolean res = Pattern.compile(regEx)
                .matcher(rebalanceEntity)
                .matches();
        if (res) {
            LOGGER.debug("Marking {} '{}' for rebalancing because wildcard is: {}", entityType, rebalanceEntity, wildcard);
        } else {
            LOGGER.debug("Not marking {} '{}' for rebalancing because wildcard is: {}", entityType, rebalanceEntity, wildcard);
        }
        return res;
    }

    public static void checkTableStatus(ZookeeperHelper zooHelper, TableName tableName) throws Exception {
        ZookeeperStatus tableStatus;
        try {
            String tablePath = ZooKeeperContextHelper.getTablePath(tableName);
            tableStatus = ZookeeperStatus.fromString(zooHelper.getNotNullString(tablePath));
        } catch (Exception ex) {
            throw new Exception("error while table status check", ex);
        }
        switch (tableStatus) {
            case CREATING:
                throw new InappropriateTableStatusException("table is creating for now");
            case SPLITTING:
                throw new InappropriateTableStatusException("table is splitting for now");
            case MODIFYING:
                throw new InappropriateTableStatusException("table is modifying for now");
            case ENABLING:
                throw new InappropriateTableStatusException("table is enabling for now");
            case DISABLING:
                throw new InappropriateTableStatusException("table is disabling for now");
            case DISABLED:
                throw new InappropriateTableStatusException("table disabled. Enable it first");
            case DROPPING:
                throw new InappropriateTableStatusException("table is dropping for now");
            case RECOVERING:
                throw new InappropriateTableStatusException("table is recovering for now");
            case ASSIGNING:
                throw new InappropriateTableStatusException("table regions is moving for now");
            case ASSIGNED:
            case CREATED:
            case ENABLED:
                break;
            default:
                throw new Exception("unknown table status: " + tableStatus);
        }
    }

    private static class InappropriateTableStatusException extends Exception {
        public InappropriateTableStatusException(String msg) {
            super(msg);
        }
    }

    private static String wildcardToRegEx(String t) {
        if (StringUtils.isEmpty(t)) {
            return StringUtils.EMPTY;
        }
        return t.replaceAll("\\*", ".+").replaceAll("\\?", ".").trim();
    }
}
