/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.model;

public class FMasterFollowerInfo {

    private String znode;

    private String znodeNbr;

    private String url;

    private Boolean isLeader;

    public Boolean getLeader() {
        return isLeader;
    }

    public void setLeader(Boolean leader) {
        isLeader = leader;
    }


    public String getZnode() {
        return znode;
    }

    public void setZnode(String znode) {
        this.znode = znode;
    }

    public String getZnodeNbr() {
        return znodeNbr;
    }

    public void setZnodeNbr(String znodeNbr) {
        this.znodeNbr = znodeNbr;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
