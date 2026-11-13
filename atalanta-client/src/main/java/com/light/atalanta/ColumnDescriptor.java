/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.model.CompressType;
import com.light.atalanta.model.RecordCompressionType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public class ColumnDescriptor implements DbColumnDescriptor {

    public static final ColumnDescriptor DEFAULT_COLUMN_DESCRIPTOR =
            new ColumnDescriptor("cf".getBytes(StandardCharsets.UTF_8));

    static {
        DEFAULT_COLUMN_DESCRIPTOR.compressType = CompressType.none;
        DEFAULT_COLUMN_DESCRIPTOR.recordCompressionType = RecordCompressionType.lz;
        DEFAULT_COLUMN_DESCRIPTOR.dictionary = false;
    }

    private final byte[] familyName;

    private int dataBlockSize;

    private CompressType compressType;

    private RecordCompressionType recordCompressionType;

    private Boolean dictionary;

    private Long recordTargetCount;

    @Override
    public RecordCompressionType getRecordCompressionType() {
        return recordCompressionType;
    }

    @Override
    public void setRecordCompressionType(Object recordCompressionType) {
        if (recordCompressionType instanceof RecordCompressionType) {
            this.recordCompressionType = (RecordCompressionType) recordCompressionType;
        } else {
            throw new RuntimeException("Unknown record type class: " + recordCompressionType.getClass());
        }
    }

    public ColumnDescriptor(byte[] familyName) {
        this.familyName = familyName;
    }

    @Override
    public DbColumnDescriptor setBloomFilterType(Object bloomFilterType) {
        // Not implemented: the value is ignored
        return this;
    }

    @Override
    public DbColumnDescriptor setCompressionType(Object compressType) {
        this.compressType = compressType instanceof CompressType ? (CompressType) compressType : CompressType.none;
        return this;
    }

    @Override
    public DbColumnDescriptor setDataBlockEncoding(Object fastDiff) {
        // Not implemented: the value is ignored
        return this;
    }

    @Override
    public Object getCompression() {
        return compressType;
    }

    @Override
    public DbColumnDescriptor setBlocksize(int dataBlockSize) {
        this.dataBlockSize = dataBlockSize;
        return this;
    }

    @Override
    public int getBlockSize() {
        return dataBlockSize;
    }

    @Override
    public byte[] getName() {
        return familyName;
    }

    @Override
    public String getNameAsString() {
        return new String(familyName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnDescriptor that = (ColumnDescriptor) o;
        return dataBlockSize == that.dataBlockSize && Arrays.equals(familyName, that.familyName) && compressType == that.compressType;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(dataBlockSize, compressType);
        result = 31 * result + Arrays.hashCode(familyName);
        return result;
    }

    @Override
    public void setDictionary(Boolean dictionary) {
        this.dictionary = dictionary;
    }

    @Override
    public Boolean getDictionary() {
        return dictionary;
    }

    @Override
    public Long getRecordTargetCount() {
        return recordTargetCount;
    }

    @Override
    public void setRecordTargetCount(Long recordTargetCount) {
        this.recordTargetCount = recordTargetCount;
    }
}
