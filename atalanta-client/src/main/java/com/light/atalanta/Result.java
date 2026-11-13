/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.transport.messages.EmptyResultOperation;
import com.light.atalanta.transport.messages.MessageType;
import com.light.atalanta.transport.messages.ResultOperation;
import com.light.atalanta.transport.values.KeyValues;
import com.light.atalanta.transport.values.QualifierValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Result implements DbResult, DbCellScannable, CellScanner, RowKey, RowKeyMessage {
    public static final Result EMPTY_RESULT = new Result(EmptyResultOperation.EMPTY_OPERATION);

    private final ResultOperation resultOperation;
    private int scannerKeyValuesIndex;
    private int scannerQualifierValueIndex;
    private Cell scannerCurrent;

    public Result() {
        resultOperation = null;
    }

    @Override
    public byte[] getRow() {
        return resultOperation.getRowkey();
    }

    @Override
    public Boolean getExists() {
        return resultOperation != null && !resultOperation.isEmpty();
    }

    @Override
    public Cell getColumnLatestCell(byte[] family, byte[] qualifier) {
        // TODO: Simplified implementation
        return getCell(family, qualifier);
    }

    @Override
    public boolean containsColumn(byte[] family, byte[] qualifier) {
        // TODO: Simplified implementation
        return isCellExists(family, qualifier);
    }

    @Override
    public MessageType getType() {
        return MessageType.RESULT;
    }

    @Override
    public CellScanner cellScanner() {
        return resultOperation == null || isEmptyResult() ? null : this;
    }

    // Visible for testing
    public CellScanner getCellScanner() {
        return cellScanner();
    }

    @Override
    public Cell current() {
        return scannerCurrent;
    }

    @Override
    public boolean advance() {
        if (resultOperation == null || isEmptyResult()) {
            scannerCurrent = null;
            return false;
        }
        KeyValues[] keyValues = resultOperation.getValues();
        while (scannerKeyValuesIndex < keyValues.length) {
            KeyValues currentKeyValues = keyValues[scannerKeyValuesIndex];
            QualifierValue[] qualifierValues = currentKeyValues.getValues();
            if (scannerQualifierValueIndex < qualifierValues.length) {
                scannerCurrent = new KeyValue(currentKeyValues.getRowKey(), currentKeyValues.getCfName(),
                        qualifierValues[scannerQualifierValueIndex++]);
                return true;
            }
            scannerKeyValuesIndex++;
            scannerQualifierValueIndex = 0;
        }
        scannerCurrent = null;
        return false;
    }

    public boolean isEmpty() {
        return resultOperation.isEmpty();
    }

    public boolean isEmptyResult() {
        return resultOperation == EmptyResultOperation.EMPTY_OPERATION;
    }

    public Result(ResultOperation resultOperation) {
        this.resultOperation = resultOperation;
    }

    public byte[] getValue(byte[] cf, byte[] qualifier) {
        for (KeyValues kvs : resultOperation.getValues()) {
            if (Arrays.equals(cf, kvs.getCfName())) {
                for (QualifierValue qv : kvs.getValues()) {
                    if (Arrays.equals(qualifier, qv.getQualifier()))
                        return qv.getValue();
                }
            }
        }
        return null;
    }

    private Cell getCell(byte[] cf, byte[] qualifier) {
        for (KeyValues kvs : resultOperation.getValues()) {
            if (Arrays.equals(cf, kvs.getCfName())) {
                for (QualifierValue qv : kvs.getValues()) {
                    if (Arrays.equals(qualifier, qv.getQualifier()))
                        return new KeyValue(kvs.getRowKey(), kvs.getCfName(), qv);
                }
            }
        }
        return null;
    }

    private boolean isCellExists(byte[] cf, byte[] qualifier) {
        for (KeyValues kvs : resultOperation.getValues()) {
            if (Arrays.equals(cf, kvs.getCfName())) {
                for (QualifierValue qv : kvs.getValues()) {
                    if (Arrays.equals(qualifier, qv.getQualifier())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public KeyValues[] getKeyValues() {
        return resultOperation.getValues();
    }

    public List<Cell> getColumnCells(byte[] cf) {
        for (KeyValues kvs : resultOperation.getValues()) {
            if (Arrays.equals(cf, kvs.getCfName())) {
                List<Cell> cells = new ArrayList<>();
                for (QualifierValue qv : kvs.getValues()) {
                    cells.add(new KeyValue(kvs.getRowKey(), kvs.getCfName(), qv));
                }
                return cells;
            }
        }
        return null;
    }

    public List<Cell> listCells() {
        List<Cell> cells = new ArrayList<>();
        for (KeyValues kvs : resultOperation.getValues()) {
            for (QualifierValue qv : kvs.getValues()) {
                cells.add(new KeyValue(kvs.getRowKey(), kvs.getCfName(), qv));
            }
        }
        return cells;
    }

    @Override
    public String toString() {
        return "Result:" + resultOperation;
    }

    public ResultOperation getResultOperation() {
        return resultOperation;
    }
}
