/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class UniqueNameThreadFactory implements ThreadFactory {

    private final String baseName;
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    public UniqueNameThreadFactory(String baseName) {
        this.baseName = baseName;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        String threadName = baseName + "-" + threadNumber.getAndIncrement();
        return new Thread(runnable, threadName);
    }
}