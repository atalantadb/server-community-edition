/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.util.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SleepHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(SleepHelper.class);

    public static void sleep(int sec) {
        LOGGER.warn("SLEEPING {} SEC...", sec);
        try {
            Thread.sleep(sec * 1000L);
        } catch (InterruptedException ignore) { }
        LOGGER.warn("CONTINUING AFTER SLEEP");
    }

    private SleepHelper() {
    }
}
