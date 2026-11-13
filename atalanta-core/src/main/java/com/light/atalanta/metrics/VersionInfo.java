/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.metrics;

public class VersionInfo {

    protected static final String UNKNOWN = "unknown";
    private String build;
    private String date;
    private String commit;

    public VersionInfo() {
        build = UNKNOWN;
        date = UNKNOWN;
        commit = UNKNOWN;
    }

    public String getBuild() {
        return build;
    }

    public String getDate() {
        return date;
    }

    public String getCommit() {
        return commit;
    }

    public void setBuild(String build) {
        this.build = build;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void setCommit(String commit) {
        this.commit = commit;
    }
}
