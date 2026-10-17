/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Map;

public interface DbConfiguration extends Iterable<Map.Entry<String, String>> {
    String get(String key);

    default String getOrDefault(String key, String defValue) {
        String value = get(key);
        if (StringUtils.isBlank(value)) {
            value = defValue;
        }
        return value;
    }

    void set(String key, String value);

    void addResource(File resource) throws Exception;

    void addResource(String resource) throws Exception;

    Integer getInt(String key);

    Long getLong(String key);

    Boolean getBoolean(String key);


    // TODO: Rework and remove from this interface
    @Deprecated
    void checkConfiguration();

    Map<String, Object> getSubsystemOptions(String subsystemName);
}
