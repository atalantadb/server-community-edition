/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.exceptions;

public class NotEnoughRegionServerInstances extends Exception {

    private final int currentCount;

    public NotEnoughRegionServerInstances(int currentCount) {
        super("not enough region server instances. Current count: " + currentCount);
        this.currentCount = currentCount;
    }

    public NotEnoughRegionServerInstances(String message, int currentCount) {
        super(message);
        this.currentCount = currentCount;
    }

    public int getCurrentCount() {
        return currentCount;
    }
}
