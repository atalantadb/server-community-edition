/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.util.Bytes;
import com.light.atalanta.util.BytesComparator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

@SuppressFBWarnings("EI_EXPOSE_REP2")
public class SingleColumnValueFilter implements DbSingleColumnValueFilter, DbCompareFilter {

    private final byte[] columnFamily;
    private final byte[] columnQualifier;
    private final DbCompareFilter.CompareOp compareOp;
    private final byte[] value;

    public SingleColumnValueFilter(byte[] family, byte[] qualifier, DbCompareFilter.CompareOp compareOp, byte[] value) {
        this.columnFamily = Bytes.cloneBytesArray(family);
        this.columnQualifier = Bytes.cloneBytesArray(qualifier);
        this.compareOp = compareOp;
        this.value = Bytes.cloneBytesArray(value);
    }

    public boolean isMatch(DbResult r) {
        return Arrays.stream(((Result)r).getKeyValues())
                .filter(kv -> BytesComparator.compareTo(kv.getCfName(), columnFamily) == 0)
                .flatMap(kv -> Arrays.stream(kv.getValues()))
                .anyMatch(qv -> BytesComparator.compareTo(qv.getQualifier(), columnQualifier) == 0
                        && filterColumnValue(value, qv.getValue()));
    }

    private boolean filterColumnValue(byte[] value, byte[] data) {
        int compareResult = BytesComparator.compareTo(value, data);
        switch (this.compareOp) {
            case LESS:
                return compareResult > 0;
            case LESS_OR_EQUAL:
                return compareResult >= 0;
            case EQUAL:
                return compareResult == 0;
            case NOT_EQUAL:
                return compareResult != 0;
            case GREATER_OR_EQUAL:
                return compareResult <= 0;
            case GREATER:
                return compareResult < 0;
            default:
                throw new RuntimeException("Unknown Compare op " + this.compareOp.name());
        }
    }

}
