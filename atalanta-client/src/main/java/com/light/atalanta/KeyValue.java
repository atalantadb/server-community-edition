/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.transport.values.QualifierValue;
import com.light.atalanta.util.Bytes;

public class KeyValue implements Cell {
    private final byte[] rowkey;
    private final byte[] cf;
    private final QualifierValue qv;

    public KeyValue(byte[] rowkey, byte[] cf, QualifierValue qv) {
        this.rowkey = Bytes.cloneBytesArray(rowkey);
        this.cf = Bytes.cloneBytesArray(cf);
        this.qv = qv;
    }

    @Override
    public byte[] cloneValue() {
        return qv.getValue().clone();
    }

    @Override
    public byte[] cloneQualifier() {
        return qv.getQualifier().clone();
    }

    @Override
    public byte[] cloneRow() {
        return rowkey.clone();
    }

    @Override
    public byte[] getRowArray() {
        return rowkey;
    }

    @Override
    public int getRowLength() {
        return rowkey.length;
    }

    @Override
    public byte[] getFamilyArray() {
        return cf;
    }

    @Override
    public byte getFamilyLength() {
        return (byte) cf.length;
    }

    @Override
    public byte[] getQualifierArray() {
        return qv.getQualifier();
    }

    @Override
    public int getQualifierLength() {
        return qv.getQualifier().length;
    }

    @Override
    public long getTimestamp() {
        return qv.getTimestamp();
    }

    @Override
    public byte[] getValueArray() {
        return qv.getValue();
    }

    @Override
    public int getValueLength() {
        return qv.getValue().length;
    }

    @Override
    public int getQualifierOffset() {
        return 0;
    }

    @Override
    public int getValueOffset() {
        return 0;
    }
}
