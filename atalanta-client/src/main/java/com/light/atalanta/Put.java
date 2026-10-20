/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;


import com.light.atalanta.api.PutRows;
import com.light.atalanta.client.serializers.PutData;
import com.light.atalanta.client.serializers.PutDataHolder;
import com.light.atalanta.transport.messages.MessageType;
import com.light.atalanta.transport.values.QualifierValue;
import com.light.atalanta.util.Bytes;
import com.light.atalanta.util.BytesComparator;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class Put implements PutRows, Append, RowKey, RowKeyMessage {
    private static final Long DEFAULT_TIMESTAMP = Long.MAX_VALUE;

    private boolean isReusable = false;

    private final byte[] rowkey;
    private long timestamp;
    private Long boxedTimestamp;

    protected NavigableMap<byte[], NavigableMap<byte[], Pair<Long, byte[]>>> familyMap = null;
    private PutData putData = null;

    private MessageType responseType = null;
    private MessageType putType = MessageType.PUT;

    protected Put() {
        rowkey = null;
    }

    public Put(byte[] rowkey) {
        this.rowkey = setRowkey(rowkey);
        this.timestamp = Long.MAX_VALUE;
    }

    public Put(byte[] rowkey, long timestamp) {
        this.rowkey = setRowkey(rowkey);
        this.timestamp = timestamp;
    }

    public Put(byte[] rowkey, PutOptions... options) {
        for (PutOptions po : options) {
            po.setOption(this);
        }
        this.rowkey = setRowkey(rowkey);
        this.timestamp = Long.MAX_VALUE;
    }

    public Put(byte[] rowkey, long timestamp, PutOptions... options) {
        for (PutOptions po : options) {
            po.setOption(this);
        }
        this.rowkey = setRowkey(rowkey);
        this.timestamp = timestamp;
    }

    @Override
    public Put addColumn(byte[] cf, byte[] qualifier, byte[] value) {
        return addColumn(cf, qualifier, timestamp, value);
    }

    @Override
    public Put addImmutable(byte[] family, byte[] qualifier, byte[] value) {
        return addColumn(family, qualifier, value);
    }

    @Override
    public Put addColumn(byte[] cf, byte[] qualifier, long ts, byte[] value) {
        if (isReusable) {
            if (putData != null) {
                putData.addQualifier(cf, ts, qualifier, value);
            }
        } else {
            checkFamilyMap();

            NavigableMap<byte[], Pair<Long, byte[]>> qv = familyMap.computeIfAbsent(cf, k -> new TreeMap<>(BytesComparator.BYTES_COMPARATOR));
            Pair<Long, byte[]> currentValue = qv.get(qualifier);
            if (currentValue == null || currentValue.getFirst() <= ts) {
                byte[] v = new byte[value.length];
                System.arraycopy(value, 0, v, 0, v.length);
                qv.put(qualifier, new Pair<>(boxTimestamp(ts), v));
            }
        }
        return this;
    }

    private void checkFamilyMap() {
        if (familyMap == null) {
            familyMap = new TreeMap<>(BytesComparator.BYTES_COMPARATOR);
        }
    }

    @Override
    public NavigableMap<byte[], List<Cell>> getFamilyCellMap() {
        checkFamilyMap();
        return familyMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> toCellList(e.getKey(), e.getValue()),
                        (o, n) -> n,
                        () -> new TreeMap<>(BytesComparator.BYTES_COMPARATOR)));
    }

    private List<Cell> toCellList(byte[] cf, NavigableMap<byte[], Pair<Long, byte[]>> value) {
        return value.entrySet().stream()
                .map(e -> new KeyValue(rowkey, cf,
                        new QualifierValue(e.getValue().getFirst(), e.getKey(), e.getValue().getSecond())))
                .collect(Collectors.toList());
    }

    @Override
    public byte[] getRow() {
        return rowkey;
    }

    public byte[] setRowkey(byte[] rowkey) {
        if (isReusable) {
            putData.setRowkey(rowkey);
        }
        return Bytes.cloneBytesArray(rowkey);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        this.boxedTimestamp = null;
    }

    private Long boxTimestamp(long timestamp) {
        if (timestamp != this.timestamp) {
            return timestamp;
        }
        if (boxedTimestamp == null) {
            if (timestamp == Long.MAX_VALUE) {
                boxedTimestamp = DEFAULT_TIMESTAMP;
            } else {
                boxedTimestamp = timestamp;
            }
        }
        return boxedTimestamp;
    }

    public NavigableMap<byte[], NavigableMap<byte[], Pair<Long, byte[]>>> getFamilyMap() {
        return familyMap;
    }

    @Override
    public MessageType getType() {
        return putType;
    }

    public boolean isReusable() {
        return isReusable;
    }

    public PutData getPutData() {
        return putData;
    }

    public void setReusable(boolean reusable) {
        isReusable = reusable;
    }

    public MessageType getResponseType() {
        return responseType;
    }

    public void setResponseType(MessageType responseType) {
        this.responseType = responseType;
    }

    @Override
    public void add(byte[] family, byte[] qualifier, byte[] value) {
        addColumn(family, qualifier, timestamp, value);
    }

    MessageType getPutType() {
        return putType;
    }

    public void setPutType(MessageType putType) {
        // Experimental feature: not ready for use, including manual selection via Put
        if (putType == MessageType.PUT_AND_GET) {
            throw new UnsupportedOperationException("putAndGet/mutateAndGet are experimental and not ready for use");
        }
        this.putType = putType;
    }

    public enum PutOptions {
        REUSABLE {
            @Override
            public void setOption(Put put) {
                put.setReusable(true);
                put.putData = PutDataHolder.getPutData();
            }
        };

        public abstract void setOption(Put put);
    }
}
