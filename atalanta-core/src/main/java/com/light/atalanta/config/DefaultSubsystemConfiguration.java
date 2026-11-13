/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation of subsystem settings.
 * It needs two things to work:
 * <ul>
 * <li>the properties to override (in most cases a Map or Properties);</li>
 * <li>the settings path, used to identify the source of the settings.</li>
 * </ul>
 */
public class DefaultSubsystemConfiguration implements SubsystemConfiguration {
    private final String path;
    private final Map<String, Object> options = new HashMap<>();

    public DefaultSubsystemConfiguration(String path) {
        this.path = path;
    }

    @Override
    public String getSubsystemPath() {
        return path;
    }

    @Override
    public Map<String, Object> getOptions() {
        return options;
    }

    @Override
    public void putOption(String key, Object value) {
        options.put(key, value);
    }

    @Override
    public Object get(String key) {
        return options.get(key);
    }

    @Override
    public String getAsString(String key) {
        return String.valueOf(options.get(key));
    }

    @Override
    public int getAsInt(String key) {
        return Integer.parseInt(getAsString(key));
    }
}
