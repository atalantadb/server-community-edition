/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;


public interface DbColumnDescriptor {

    Object getRecordCompressionType();

    void setRecordCompressionType(Object recordCompressionType);

    // TODO: Use an enum
    DbColumnDescriptor setBloomFilterType(Object bloomFilterType);

    DbColumnDescriptor setCompressionType(Object compressionType);

    DbColumnDescriptor setDataBlockEncoding(Object fastDiff);

    Object getCompression();

    DbColumnDescriptor setBlocksize(int s);

    int getBlockSize();

    byte[] getName();

    String getNameAsString();

    void setDictionary(Boolean dictionary);

    Boolean getDictionary();

    Long getRecordTargetCount();

    void setRecordTargetCount(Long recordTargetCount);
}
