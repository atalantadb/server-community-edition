/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.vo;

public class ClientConfiguration {

    private Integer masterConnectionTimeoutMs = null;

    private Long masterDisconnectionTimeoutMs = null;

    private Long createNamespaceTimeoutMs = null;

    private Long createTableTimeoutMs = null;

    private Long disableTableTimeoutMs = null;

    private Long dropTableTimeoutMs = null;

    public Integer getMasterConnectionTimeoutMs() {
        return masterConnectionTimeoutMs;
    }

    public void setMasterConnectionTimeoutMs(Integer masterConnectionTimeoutMs) {
        this.masterConnectionTimeoutMs = masterConnectionTimeoutMs;
    }

    public Long getMasterDisconnectionTimeoutMs() {
        return masterDisconnectionTimeoutMs;
    }

    public void setMasterDisconnectionTimeoutMs(Long masterDisconnectionTimeoutMs) {
        this.masterDisconnectionTimeoutMs = masterDisconnectionTimeoutMs;
    }

    public Long getCreateNamespaceTimeoutMs() {
        return createNamespaceTimeoutMs;
    }

    public void setCreateNamespaceTimeoutMs(Long createNamespaceTimeoutMs) {
        this.createNamespaceTimeoutMs = createNamespaceTimeoutMs;
    }

    public Long getCreateTableTimeoutMs() {
        return createTableTimeoutMs;
    }

    public void setCreateTableTimeoutMs(Long createTableTimeoutMs) {
        this.createTableTimeoutMs = createTableTimeoutMs;
    }

    public Long getDisableTableTimeoutMs() {
        return disableTableTimeoutMs;
    }

    public void setDisableTableTimeoutMs(Long disableTableTimeoutMs) {
        this.disableTableTimeoutMs = disableTableTimeoutMs;
    }

    public Long getDropTableTimeoutMs() {
        return dropTableTimeoutMs;
    }

    public void setDropTableTimeoutMs(Long dropTableTimeoutMs) {
        this.dropTableTimeoutMs = dropTableTimeoutMs;
    }

    @Override
    public String toString() {
        return "ClientConfiguration{" +
                "\n\tmasterConnectionTimeoutMs=" + masterConnectionTimeoutMs +
                ", \n\tmasterDisconnectionTimeoutMs=" + masterDisconnectionTimeoutMs +
                ", \n\tcreateNamespaceTimeoutMs=" + createNamespaceTimeoutMs +
                ", \n\tcreateTableTimeoutMs=" + createTableTimeoutMs +
                ", \n\tdisableTableTimeoutMs=" + disableTableTimeoutMs +
                ", \n\tdropTableTimeoutMs=" + dropTableTimeoutMs +
                '}';
    }
}
