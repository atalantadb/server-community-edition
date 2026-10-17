/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.serializers;

import com.light.atalanta.NumbersByteHelper;
import com.light.atalanta.transport.messages.Operation;
import com.light.atalanta.transport.values.KeyValues;
import com.light.atalanta.transport.values.QualifierValue;
import com.light.atalanta.util.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class PutData {

    private static final int MAX_CF_COUNT = 16;
    private static final int MAX_QUALIFIERS_PER_CF_COUNT = 256;

    private byte[] rowkey = null;
    private final ColumnFamilyHolder[] cfHolders;

    private int cfSize;
    private boolean isEmpty = true;

    PutData() {
        cfHolders = new ColumnFamilyHolder[MAX_CF_COUNT];
        for (int i = 0; i < MAX_CF_COUNT; i++) {
            cfHolders[i] = new ColumnFamilyHolder();
        }
    }

    public int getDataSize() {
        int size = 0;
        size += Operation.TRANSPORT_MESSAGE_HEADER_LENGTH;
        for (int i = 0; i < cfSize; i++) {
            size += cfHolders[i].getColumnFamilySize();
        }
        return size;
    }

    public byte[] getRowkey() {
        return rowkey;
    }

    public void setRowkey(byte[] rowkey) {
        this.rowkey = Bytes.cloneBytesArray(rowkey);
        markNonEmpty();
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    private void markEmpty() {
        this.isEmpty = true;
    }

    private void markNonEmpty() {
        this.isEmpty = false;
    }

    public ColumnFamilyHolder[] getColumnFamilies() {
        return cfHolders;
    }

    public void addQualifier(byte[] cf, long timestamp, byte[] qualifier, byte[] value) {
        ColumnFamilyHolder cfh = getCF(cf);
        if (cfh.getRowkey() == null) {
            cfh.setRowkey(rowkey);
        }
        if (cfh.getCf() == null) {
            cfh.setCf(cf);
        }
        cfh.addQualifier(timestamp, qualifier, value);
        markNonEmpty();
    }

    private ColumnFamilyHolder getCF(byte[] cf) {
        if (cfSize == 0) {
            cfSize++;
            return cfHolders[0];
        }
        for (int i = 0; i < cfSize; i++) {
            ColumnFamilyHolder holder = cfHolders[i];
            if (Arrays.equals(holder.cf, cf)) {
                return holder;
            }
        }
        if (cfSize > MAX_QUALIFIERS_PER_CF_COUNT) {
            throw new RuntimeException("A lot of CFs! Its not supported yet.");
        }
        cfSize++;
        return cfHolders[cfSize - 1];
    }

    public void cleanup() {
        this.rowkey = null;
        for (int i = 0; i < cfSize; i++) {
            cfHolders[i].cleanup();
        }
        cfSize = 0;
        markEmpty();
    }

    public int getElementsCount() {
        return cfSize;
    }

    static class ColumnFamilyHolder {
        private byte[] rowkey;
        private byte[] cf;
        private final QualifierHolder[] qualifiers;

        ColumnFamilyHolder() {
            qualifiers = new QualifierHolder[MAX_QUALIFIERS_PER_CF_COUNT];
            for (int i = 0; i < MAX_QUALIFIERS_PER_CF_COUNT; i++) {
                qualifiers[i] = new QualifierHolder();
            }
        }

        public int getColumnFamilySize() {
            int size = 0;
            size += rowkey.length;
            size += cf.length;
            size += KeyValues.KEY_VALUES_OBJECT_HEADER_LENGTH;
            for (int i = 0; i < qualifiersSize; i++) {
                size += qualifiers[i].getQualifierSize();
            }
            return size;
        }

        public void serializeColumnFamily(ByteBuffer bb, byte[] rowkey) {
            int size = getColumnFamilySize();
            bb.put(NumbersByteHelper.toBytes_int(rowkey.length));
            bb.put(NumbersByteHelper.toBytes_int(cf.length));
            bb.put(NumbersByteHelper.toBytes_int(size - KeyValues.KEY_VALUES_OBJECT_HEADER_LENGTH - rowkey.length - cf.length));

            bb.put(rowkey);
            bb.put(cf);

            for (int i = 0; i < qualifiersSize; i++) {
                qualifiers[i].serializeQualifier(bb);
            }
        }

        private int qualifiersSize;

        public byte[] getCf() {
            return cf;
        }

        public void setCf(byte[] cf) {
            this.cf = Bytes.cloneBytesArray(cf);
        }

        public byte[] getRowkey() {
            return rowkey;
        }

        public void setRowkey(byte[] rowkey) {
            this.rowkey = Bytes.cloneBytesArray(rowkey);
        }

        public QualifierHolder[] getQualifiers() {
            return qualifiers;
        }

        public void cleanup() {
            this.rowkey = null;
            this.cf = null;
            for (int i = 0; i < qualifiersSize; i++) {
                qualifiers[i].clean();
            }
            qualifiersSize = 0;
        }

        public void addQualifier(long timestamp, byte[] qualifier, byte[] value) {
            if (qualifiersSize > MAX_QUALIFIERS_PER_CF_COUNT) {
                throw new RuntimeException("A lot of qualifiers! Its not supported yet.");
            }
            QualifierHolder currentPosition = this.qualifiers[qualifiersSize];
            currentPosition.setTimestamp(timestamp);
            currentPosition.setQualifier(qualifier);
            currentPosition.setValue(value);
            qualifiersSize++;
        }

        public int getElementsCount() {
            return qualifiersSize;
        }

    }

    static class QualifierHolder {
        private long timestamp = Long.MAX_VALUE;
        private byte[] qualifier;
        private byte[] value;

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public byte[] getQualifier() {
            return qualifier;
        }

        public void setQualifier(byte[] qualifier) {
            this.qualifier = Bytes.cloneBytesArray(qualifier);
        }

        public byte[] getValue() {
            return value;
        }

        public void setValue(byte[] value) {
            this.value = Bytes.cloneBytesArray(value);
        }

        public void clean() {
            this.timestamp = Long.MAX_VALUE;
            this.qualifier = null;
            this.value = null;
        }

        public void serializeQualifier(ByteBuffer bb) {
            bb.put(NumbersByteHelper.toBytes_long(timestamp));
            bb.put(NumbersByteHelper.toBytes_int(qualifier.length));
            bb.put(NumbersByteHelper.toBytes_int(value.length));

            bb.put(qualifier);
            bb.put(value);
        }

        public int getQualifierSize() {
            int size = 0;
            size += QualifierValue.QUALIFIER_VALUE_HEADER_LENGTH;
            size += qualifier.length;
            size += value.length;
            return size;
        }
    }
}
