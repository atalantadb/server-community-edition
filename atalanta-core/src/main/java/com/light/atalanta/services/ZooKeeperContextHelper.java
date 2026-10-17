/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.services;

import com.google.common.base.Preconditions;
import com.light.atalanta.ATALANTAConstants;
import com.light.atalanta.TableName;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.RegionServerModel;
import org.apache.commons.lang3.StringUtils;

import static com.light.atalanta.ATALANTAConstants.CSV_NEWLINE;
import static com.light.atalanta.ATALANTAConstants.ZK_SEPARATOR;

/**
 * Methods and constants for working with the configuration stored in ZooKeeper.
 */
public final class ZooKeeperContextHelper {

    public static final String REGION_SERVER_REGIONS_NODE = "/regions";

    public static final String REGION_SERVER_API_LOCATION_NODE = "/api_location";

    public static final int CREATING_REGION_STATUS = 1;

    public static final int CREATED_REGION_STATUS = 2;

    public static final int DISABLING_REGION_STATUS = 3;

    public static final int DISABLED_REGION_STATUS = 4;

    public static final int DROPPING_REGION_STATUS = 5;

    public static final int DROPPED_REGION_STATUS = 6;

    public static final int UNASSIGNING_REGION_STATUS = 7;

    public static final int ASSIGNING_REGION_STATUS = 8;

    public static final int ASSIGNED_REGION_STATUS = 9;

    public static final int ENABLING_REGION_STATUS = 10;

    public static final int ENABLED_REGION_STATUS = 11;

    public static final int RECOVERING_REGION_STATUS = 12;

    public static final int FORCE_RECOVERING_REGION_STATUS = 13;

    public static final int SPLITTING_REGION_STATUS = 14;

    public static final int SPLITTED_REGION_STATUS = 15;

    public static final int SYNCING_REGION_STATUS = 16;

    public static final int MODIFYING_REGION_STATUS = 17;

    public static final int MODIFY_REGION_STATUS = 18;

    public static final int MINIMUM_REGION_STATUS_VALUE = CREATING_REGION_STATUS;

    public static final int MAXIMUM_REGION_STATUS_VALUE = SYNCING_REGION_STATUS;

    private ZooKeeperContextHelper() {

    }

    /**
     * Methods for the dynamic referencing status.
     * Referencing status = original region status + 100.
     */

    private static final int REFERENCING_REGION_STATUS_ADDITION = 100;

    public static int getReferencingRegionStatus(RegionModel region) {
        return region.getStatusId() + REFERENCING_REGION_STATUS_ADDITION;
    }

    public static int getReferencedRegionStatus(RegionModel region) {
        Preconditions.checkArgument(region.getStatusId() > REFERENCING_REGION_STATUS_ADDITION,
                "Referencing region status must be greater than " + REFERENCING_REGION_STATUS_ADDITION);
        return region.getStatusId() - REFERENCING_REGION_STATUS_ADDITION;
    }

    public static boolean isReferencingRegionStatus(RegionModel region) {
        return (region.getStatusId() >= (MINIMUM_REGION_STATUS_VALUE + REFERENCING_REGION_STATUS_ADDITION))
                && (region.getStatusId() <= (MAXIMUM_REGION_STATUS_VALUE + REFERENCING_REGION_STATUS_ADDITION));
    }

    public static boolean isReferencingRegionStatus(int statusId) {
        return (statusId >= (MINIMUM_REGION_STATUS_VALUE + REFERENCING_REGION_STATUS_ADDITION))
                && (statusId <= (MAXIMUM_REGION_STATUS_VALUE + REFERENCING_REGION_STATUS_ADDITION));
    }

    /**
     * Splits a CSV representation into lines.
     * Unlike String::split, returns an empty array for empty input.
     */
    public static String[] splitCsvByLines(String data) {
        if (StringUtils.isBlank(data)) {
            return new String[0];
        } else {
            return data.split(CSV_NEWLINE);
        }
    }

    /**
     * Returns the table configuration path.
     */
    public static String getTablePath(TableName tableName) {
        return String.join(ZK_SEPARATOR, tableName.getNamespace(), tableName.getTableName());
    }

    /**
     * Returns the ZooKeeper path of a region server.
     */
    public static String getRegionServerPath(TableName tableName, RegionServerModel regionServer) {
        return String.join(ZK_SEPARATOR, getTablePath(tableName), regionServer.getName());
    }

    /**
     * Returns the ZooKeeper path of the region list node of a region server.
     */
    public static String getRegionServerRegionsPath(TableName tableName, RegionServerModel regionServer) {
        return getRegionServerPath(tableName, regionServer) + REGION_SERVER_REGIONS_NODE;
    }

    /**
     * Returns the ZooKeeper path of the node with the region server API location.
     */
    public static String getRegionServerApiLocationPath(TableName tableName, RegionServerModel regionServer) {
        return getRegionServerPath(tableName, regionServer) + REGION_SERVER_API_LOCATION_NODE;
    }
}
