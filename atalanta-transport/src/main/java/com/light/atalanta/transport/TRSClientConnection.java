/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport;

import com.light.atalanta.NameOfTable;
import com.light.atalanta.TableName;
import com.light.atalanta.transport.exceptions.*;
import com.light.atalanta.transport.messages.*;
import com.light.atalanta.transport.values.TableRegionServerInfo;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public interface TRSClientConnection extends Closeable {

    @Override
    void close() throws IOException;

    TableRegionServerInfo getTableRegionServerInfo(List<String> tableRegions, NameOfTable tableName) throws IOException, NotActualRegionMappingException;

    /** Initialize table bindings; AUTH_NONE is required by the core before the region map. */
    default TableRegionServerInfo authenticateAndGetTableRegionServerInfo(
            List<String> tableRegions, NameOfTable tableName)
            throws IOException, NotActualRegionMappingException {
            sendEmptyAuthRequest();
        
        return getTableRegionServerInfo(tableRegions, tableName);
    }

    void sendBulkPut(BulkPutOperation bulkPut, NameOfTable tableName) throws IOException;

    ResultOperation[] sendBulkGet(BulkGetOperation bulkGet, NameOfTable tableName) throws IOException, NotActualRegionMappingException;

    RowCountResponse requestCount(RowCountRequest rowCountRequest, NameOfTable tableName) throws IOException, NotActualRegionMappingException;

    byte[] receiveBytes() throws IOException, UnavailableCoreException;

    ScanResultContainer makeScan(ScanOperation scan, NameOfTable tableName) throws IOException, NotActualRegionMappingException, UnavailableServerException;

    void sendBytes(ByteBuffer bb, int offset, int length) throws IOException, NotActualRegionMappingException, UnavailableServerException;

    void sendRequest(Operation op, TableRegionServerInfo tableInfo) throws IOException, NotActualRegionMappingException, UnavailableServerException;

    Operation getResponse(Operation requests, TableRegionServerInfo tableInfo) throws IOException, NotActualRegionMappingException, UnavailableServerException;

    void sendEmptyAuthRequest() throws IOException;

    String getLocation();
}
