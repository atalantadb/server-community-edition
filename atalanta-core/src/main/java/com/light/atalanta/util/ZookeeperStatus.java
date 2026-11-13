/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.util;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import static com.light.atalanta.ATALANTAConstants.STATUS_PREFIX;

public enum ZookeeperStatus {
    CREATING("creating"),
    CREATED("created"),
    ENABLING("enabling"),
    ENABLED("enabled"),
    DISABLING("disabling"),
    DISABLED("disabled"),
    DROPPING("dropping"),
    ASSIGNING("assigning"),
    ASSIGNED("assigned"),
    SPLITTING("splitting"),
    MODIFYING("modifying"),
    // Dynamic status: use with care
    RECOVERING("recovering"),
    FAILED("failed");
;

    private final String value;

    ZookeeperStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static ZookeeperStatus fromString(String statusData) {
        if (StringUtils.isEmpty(statusData) || !statusData.startsWith(STATUS_PREFIX)) {
            throw new IllegalArgumentException("Status is invalid: " + statusData);
        }

        ZookeeperStatus result;
        try {
            String statusStr = statusData.substring(STATUS_PREFIX.length());
            if (statusStr.endsWith(RECOVERING_POSTFIX)) {
                result = RECOVERING;
            } else {
                result = valueOf(statusStr.toUpperCase());
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Status is invalid: " + statusData);
        }
        return result;
    }

    /**
     * Methods for the dynamic RECOVERING status.
     */

    private static final String RECOVERING_POSTFIX = "_recovering";

    public static String getRecoveringStatus(String status) {
        return status + RECOVERING_POSTFIX;
    }

    public static String getRecoveredStatus(String status) {
        Preconditions.checkArgument(status.endsWith(RECOVERING_POSTFIX), "Table recovering status must be ends with: " + RECOVERING_POSTFIX);
        return status.replace(RECOVERING_POSTFIX, StringUtils.EMPTY);
    }
}
