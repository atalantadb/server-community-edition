/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ShutdownManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownManager.class);

    public static void exitWithError(String message) {
        LOGGER.error(message);
        Runtime.getRuntime().halt(1);
    }

    public static void exitWithError(String message, Throwable cause) {
        LOGGER.error(message, cause);
        Runtime.getRuntime().halt(1);
    }

    private ShutdownManager() { }
}
