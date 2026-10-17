/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.services;

import com.light.atalanta.web.ui.model.*;

import java.util.List;

public interface ZookeeperDaoService {

    boolean checkConnection();

    String getZkConnectionString();

    String getLeaderInfo();

    String getMasterUrl() throws Exception;

    List<FMasterFollowerInfo> getFollowerInfo();

    List<RegionServerModelExt> getAllRegionServers();

    List<RegionServerModelExt> getRegionServerModelExts() throws Exception;

    List<NsModel> getNsList();

    List<SnModel> getSnList();

    List<AtalantaTableModel> getTableList(String ns);

    List<AtalantaTableModel> getTableList(int pg, int pgSize);

    int getTableCount(String ns);

    int getTableCount();

    AtalantaTableModel getAtalantaTableModel(String ns, String tableName);

    RegionServerModelExt getRegionServerWithMetrics(String s);
}
