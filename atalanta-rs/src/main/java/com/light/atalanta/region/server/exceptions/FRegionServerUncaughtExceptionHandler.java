/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.exceptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FRegionServerUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(FRegionServerUncaughtExceptionHandler.class);

    @Override
    public void uncaughtException(Thread thread, Throwable th) {
        LOGGER.error("Uncaught exception", th);
        throw new RuntimeException(th);
    }
}
