/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.api;

import com.light.atalanta.Connection;
import com.light.atalanta.DbNamespaceDescriptor;
import com.light.atalanta.RegionInfo;
import com.light.atalanta.TableName;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

public interface DbAdmin extends Closeable {
    boolean tableExists(TableName tableName) throws IOException;

    boolean isNamespaceExists(String namespace) throws IOException;

    TableName[] listTableNames(String s) throws IOException;

    List<TableName> listTableNamesByNamespace(String s) throws IOException;

    List<RegionInfo> getRegions(TableName tableName) throws IOException;

    void disableTable(TableName tableName) throws IOException;

    void deleteTable(TableName tableName) throws IOException;

    void createNamespace(String namespace) throws IOException;

    void createTable(TableName tableName, byte[][] splitKeys) throws IOException;

    void createTable(DbTableDescriptor tDesc) throws IOException;

    TableName[] listTableNames(Pattern compile) throws IOException;

    boolean isTableAvailable(TableName tn) throws IOException;

    boolean isTableEnabled(TableName tableName) throws IOException;

    void enableTable(TableName tableName) throws IOException;

    void createTable(DbTableDescriptor tableDescriptor, byte[][] split) throws IOException;

    DbNamespaceDescriptor[] listNamespaceDescriptors() throws IOException;

    void createNamespace(DbNamespaceDescriptor nd) throws IOException;

    Future<Void> snapshotAsync(DbSnapshotDescription snapshotDescription) throws IOException;

    void snapshot(DbSnapshotDescription snapshotDescription) throws IOException;

    Future<Void> cloneSnapshotAsync(String snapshotName, TableName tableName) throws IOException;

    Future<Void> deleteSnapshot(String name) throws IOException;

    void truncateTable(TableName tableName) throws IOException;

    void truncateTablePreserve(TableName tableName)throws IOException ;

    Connection getConnection();

    void modifyTable(DbTableDescriptor td) throws IOException;
}
