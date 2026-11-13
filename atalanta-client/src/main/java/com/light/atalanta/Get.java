/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.transport.messages.MessageType;
import com.light.atalanta.util.Bytes;
import com.light.atalanta.util.BytesComparator;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.NavigableMap;
import java.util.TreeMap;

public class Get implements GetRows, RowKey, RowKeyMessage {

    private final byte[] rowkey;

    // Column family -> requested qualifiers
    private NavigableMap<byte[], NavigableMap<byte[], Long>> familyMap = null;

    private DbCompareFilter filter;
    private boolean checkExistenceOnly; // Not implemented

    public Get() {
        rowkey = null;
    }

    public Get(byte[] rowkey) {
        this.rowkey = Bytes.cloneBytesArray(rowkey);
    }

    @Override
    public byte[] getRow() {
        return rowkey;
    }

    @Override
    public MessageType getType() {
        return MessageType.GET;
    }

    public Get addColumn(byte[] family, byte[] qualifier) {
        checkFamilyMap();

        familyMap.computeIfAbsent(family, k -> new TreeMap<>(BytesComparator.BYTES_COMPARATOR));
        familyMap.get(family).put((qualifier == null ? StringUtils.EMPTY : new String(qualifier, StandardCharsets.UTF_8)).getBytes(), System.currentTimeMillis());
        return this;
    }

    @Override
    public Get setCheckExistenceOnly(boolean b) {
        this.checkExistenceOnly = b;
        return this;
    }

    public DbCompareFilter getFilter() {
        return filter;
    }

    public void setFilter(DbCompareFilter filter) {
        this.filter = filter;
    }

    @Override
    public Get setTimestamp(long timestamp) {
        // Not implemented: the timestamp is ignored
        return this;
    }

    @Override
    public NavigableMap<byte[], NavigableMap<byte[], Long>> getFamilyMap() {
        return familyMap;
    }

    private void checkFamilyMap() {
        if (this.familyMap == null) {
            this.familyMap = new TreeMap<>(BytesComparator.BYTES_COMPARATOR);
        }
    }
}

