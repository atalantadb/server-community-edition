/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.services;

import com.light.atalanta.web.ui.model.AtalantaCellModel;
import com.light.atalanta.web.ui.model.AtalantaTableModel;
import com.light.atalanta.web.ui.model.NsModel;

public interface AtalantaClientService {

    void createNs(NsModel nsModel);

    void createTable(AtalantaTableModel atalantaTableModel);

    String getData(AtalantaCellModel atalantaGetModel);

    String putData(AtalantaCellModel atalantaGetModel);

    void dropTable(AtalantaTableModel atalantaTableModel);

    void enableTable(AtalantaTableModel atalantaTableModel);

    void disableTable(AtalantaTableModel atalantaTableModel);

    void moveRegion(String regionIdentity, String rsName);
}
