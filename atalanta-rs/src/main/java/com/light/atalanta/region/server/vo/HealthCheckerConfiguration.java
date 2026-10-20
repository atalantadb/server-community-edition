/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.vo;

import javax.validation.constraints.NotNull;

public class HealthCheckerConfiguration {
    private int initialDelaySecs = 10;
    private int delaySecs = 1;

    @NotNull
    private String coreName;
    private long coreCheckIntervalMs = 60_000L;
    private long coreTimeoutMs = 60_000L;

    public int getInitialDelaySecs() {
        return initialDelaySecs;
    }

    public void setInitialDelaySecs(int initialDelaySecs) {
        this.initialDelaySecs = initialDelaySecs;
    }

    public int getDelaySecs() {
        return delaySecs;
    }

    public void setDelaySecs(int delaySecs) {
        this.delaySecs = delaySecs;
    }

    public String getCoreName() {
        return coreName;
    }

    public void setCoreName(String coreName) {
        this.coreName = coreName;
    }

    public long getCoreCheckIntervalMs() {
        return coreCheckIntervalMs;
    }

    public void setCoreCheckIntervalMs(long coreCheckIntervalMs) {
        this.coreCheckIntervalMs = coreCheckIntervalMs;
    }

    public long getCoreTimeoutMs() {
        return coreTimeoutMs;
    }

    public void setCoreTimeoutMs(long coreTimeoutMs) {
        this.coreTimeoutMs = coreTimeoutMs;
    }

    @Override
    public String toString() {
        return "HealthCheckerConfiguration{" +
                "\n\tinitialDelaySecs=" + initialDelaySecs +
                ", \n\tdelaySecs=" + delaySecs +
                ", \n\tcoreName='" + coreName + '\'' +
                ", \n\tcoreCheckIntervalMs=" + coreCheckIntervalMs +
                ", \n\tcoreTimeoutMs=" + coreTimeoutMs +
                '}';
    }
}
