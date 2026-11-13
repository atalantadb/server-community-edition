/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.config;

import java.util.Map;

/**
 * Complete configuration of a subsystem.
 * Allows overriding all settings of a given subsystem.
 * <p>
 * With such a configuration the user is fully responsible for its validity.
 */
public interface SubsystemConfiguration {
    String getSubsystemPath();

    Map<String, Object> getOptions();

    void putOption(String key, Object value);

    Object get(String key);

    String getAsString(String key);

    int getAsInt(String key);
}
