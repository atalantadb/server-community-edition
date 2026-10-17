/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.services;

import com.light.atalanta.*;
import com.light.atalanta.util.ClientUtil;
import com.light.atalanta.util.UniformSplit;
import com.light.atalanta.web.ui.model.AtalantaCellModel;
import com.light.atalanta.web.ui.model.AtalantaTableModel;
import com.light.atalanta.web.ui.model.NsModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;

import static java.nio.charset.StandardCharsets.UTF_8;

@Service
public class AtalantaClientServiceImpl implements AtalantaClientService, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtalantaClientServiceImpl.class.getName());
    private ConnectionThreadLocal connection = null;
    @Value("${atalanta.client.config:config/atalanta-site.xml}")
    private String atalantaClientConfig;
    @Value("${web.ui.data.operation.enabled:false}")
    private boolean isDataOperationEnable;
    @Value("${web.ui.drop.operation.enabled:false}")
    private boolean isDropOperationEnable;
    @Value("${web.ui.enable-disable.operation.enabled:false}")
    private boolean isEnableDisableOperationEnable;
    @Value("${web.ui.move.region.operation.enabled:false}")
    private boolean isMoveRegionOperationEnable;

    @Override
    public void createNs(NsModel nsModel) {
        if (!initConnection()) {
            throw new RuntimeException("Failed to initialize connection");
        }
        try {
            (connection.getAdmin()).createNamespace(nsModel.getName());
        } catch (Exception e) {
            LOGGER.error("Cannot create namespace '{}'", nsModel.getName(), e);
            throw new RuntimeException("Failed to create namespace: " + e.getMessage(), e);
        }
    }

    @Override
    public void createTable(AtalantaTableModel atalantaTableModel) {
        if (!initConnection()) {
            throw new RuntimeException("Failed to initialize connection");
        }
        try {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(atalantaTableModel.getNs(), atalantaTableModel.getName());

            if (atalantaTableModel.getRegionCount() > 1) {
                // Create splits for the specified number of regions
                byte[][] splits = new UniformSplit().split(atalantaTableModel.getRegionCount());
                admin.createTable(tableName, splits);
            } else {
                admin.createTable(tableName);
            }
        } catch (Exception e) {
            LOGGER.error("Cannot create table '{}'", atalantaTableModel, e);
            throw new RuntimeException("Failed to create table: " + e.getMessage(), e);
        }
    }

    @Override
    public String getData(AtalantaCellModel atalantaGetModel) {
        if (!isDataOperationEnable) {
            return "";
        }
        if (initConnection()) {
            try {
                Table table = connection.getTable(TableName.valueOf(atalantaGetModel.getNs(), atalantaGetModel.getTable()));
                Get get = new Get(atalantaGetModel.getKey().getBytes(UTF_8));
                Result result = table.get(get);
                return ClientUtil.resultToString(result, false);
            } catch (Exception e) {
                LOGGER.error("Cannot create table '{}'", atalantaGetModel, e);
                return "ERROR: " + e.getMessage();
            }
        } else {
            return "ERROR: Can't connect to ATALANTA";
        }
    }

    @Override
    public String putData(AtalantaCellModel atalantaGetModel) {
        if (!isDataOperationEnable) {
            return "";
        }
        if (initConnection()) {
            try {
                Table table = connection.getTable(TableName.valueOf(atalantaGetModel.getNs(), atalantaGetModel.getTable()));
                Put put = new Put(atalantaGetModel.getKey().getBytes(UTF_8));
                String[] colName = atalantaGetModel.getColName().split(":");
                put.addColumn(colName.length > 1 ? colName[0].getBytes(UTF_8) : new byte[0],
                        colName.length > 1 ? colName[1].getBytes(UTF_8) : colName[0].getBytes(UTF_8),
                        atalantaGetModel.getValue().getBytes(UTF_8));
                table.put(put);
                return "success";
            } catch (Exception e) {
                LOGGER.error("Cannot create table '{}'", atalantaGetModel, e);
                return "ERROR: " + e.getMessage();
            }
        } else {
            return "ERROR: Can't connect to ATALANTA";
        }
    }

    @Override
    public void dropTable(AtalantaTableModel atalantaTableModel) {
        if (!isDropOperationEnable) {
            return;
        }
        if (!initConnection()) {
            throw new RuntimeException("Failed to initialize connection");
        }
        try {
            (connection.getAdmin()).deleteTable(TableName.valueOf(atalantaTableModel.getNs(), atalantaTableModel.getName()));
        } catch (Exception e) {
            LOGGER.error("Cannot drop table '{}'", atalantaTableModel, e);
            throw new RuntimeException("Failed to drop table: " + e.getMessage(), e);
        }
    }

    @Override
    public void enableTable(AtalantaTableModel atalantaTableModel) {
        if (!isEnableDisableOperationEnable) {
            return;
        }
        if (!initConnection()) {
            throw new RuntimeException("Failed to initialize connection");
        }
        try {
            (connection.getAdmin()).enableTable(TableName.valueOf(atalantaTableModel.getNs(), atalantaTableModel.getName()));
        } catch (Exception e) {
            LOGGER.error("Cannot enable table '{}'", atalantaTableModel, e);
            throw new RuntimeException("Failed to enable table: " + e.getMessage(), e);
        }
    }

    @Override
    public void disableTable(AtalantaTableModel atalantaTableModel) {
        if (!isEnableDisableOperationEnable) {
            return;
        }
        if (!initConnection()) {
            throw new RuntimeException("Failed to initialize connection");
        }
        try {
            (connection.getAdmin()).disableTable(TableName.valueOf(atalantaTableModel.getNs(), atalantaTableModel.getName()));
        } catch (Exception e) {
            LOGGER.error("Cannot disable table '{}'", atalantaTableModel, e);
            throw new RuntimeException("Failed to disable table: " + e.getMessage(), e);
        }
    }

    @Override
    public void moveRegion(String regionIdentity, String rsName) {
        if (!isMoveRegionOperationEnable) {
            return;
        }
        if (!initConnection()) {
            throw new RuntimeException("Failed to initialize connection");
        }
        try {
            (connection.getAdmin()).move(regionIdentity, rsName);
        } catch (Exception e) {
            LOGGER.error("Cannot move region '{}' to RS '{}'", regionIdentity, rsName, e);
            throw new RuntimeException("Failed to move region '" + regionIdentity + "' to RS '" + rsName + "':" + e.getMessage(), e);
        }
    }

    private synchronized boolean initConnection() {
        if (connection != null) {
            return true;
        }
        try {
            LOGGER.info("Create atalanta client ... ");
            Configuration atalantaConf = Configuration.create();
            atalantaConf.addResource(new File(atalantaClientConfig));
            connection = ConnectionFactoryThreadLocal.createConnection(atalantaConf);
            LOGGER.info("ATALANTA client has been created");
            return true;
        } catch (Exception e) {
            LOGGER.info("Cannot create ATALANTA client", e);
            return false;
        }
    }


    @Override
    public void close() {
        connection.close();
    }
}
