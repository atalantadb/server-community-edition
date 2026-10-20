/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.api.PutRows;
import com.light.atalanta.client.serializers.PutSerializer;
import com.light.atalanta.client.serializers.PutToByteSerializer;
import com.light.atalanta.transport.RegionPosition;
import com.light.atalanta.transport.messages.*;
import com.light.atalanta.transport.messages.ScanOperationV2;
import com.light.atalanta.transport.values.DeletedQualifierValue;
import com.light.atalanta.transport.values.KeyValues;
import com.light.atalanta.transport.values.QualifierValue;
import com.light.atalanta.util.Bytes;
import com.light.atalanta.util.BytesComparator;

import java.io.ByteArrayOutputStream;
import java.util.*;

public class OperationConverter {

    private static final short DEFAULT_TRY_NUM = 1;

    @SuppressWarnings("unchecked")
    public static <T extends Operation> T toOperation(RowKeyMessage rowKey) {
        MessageType type = rowKey.getType();
        T operation = null;
        switch (type) {
            case GET: {
                operation = (T) createGet((Get) rowKey);
                break;
            }
            case PUT:
            case PUT_AND_GET: {
                operation = (T) createPut((Put) rowKey);
                break;
            }
            case DELETE: {
                operation = (T) createDelete((Delete) rowKey);
                break;
            }
        }
        if (operation == null) {
            throw new NullPointerException();
        }

        return operation;
    }

    @SuppressWarnings("unchecked")
    public static <T extends RowKey> T fromOperation(Operation operation) {
        MessageType type = operation.getMessageType();
        T rowKey = null;
        switch (type) {
            case EMPTY_MESSAGE: {
                rowKey = (T) Result.EMPTY_RESULT;
                break;
            }
            case RESULT:
            case PUT:
            case DELETE: {
                rowKey = operation instanceof BigResultOperation
                        ? (T) createBigResult((BigResultOperation) operation)
                        : (T) createResult((ResultOperation) operation);
                break;
            }
        }
        if (rowKey == null) {
            throw new NullPointerException();
        }
        return rowKey;
    }

    private static Result createBigResult(BigResultOperation operation) {
        return new Result(new ResultOperation(operation.getValues()));
    }

    public static <T extends Operation> List<T> toOperations(List<? extends RowKeyMessage> rowKeys) {
        List<T> results = new ArrayList<>();
        rowKeys.forEach(r -> results.add(toOperation(r)));
        return results;
    }

    public static <T extends RowKey> List<T> fromOperations(List<? extends Operation> operations) {
        List<T> results = new ArrayList<>();
        operations.forEach(r -> results.add(fromOperation(r)));

        return results;
    }

    private static GetOperation createGet(GetRows get) {
        byte[] rowkey = get.getRow();
        if (get.getFamilyMap() == null) {
            return new GetOperation(new KeyValues(rowkey, null, (QualifierValue) null), get);
        } else {
            KeyValues[] kv = new KeyValues[get.getFamilyMap().size()];
            int i = 0;
            for (Map.Entry<byte[], NavigableMap<byte[], Long>> entry : get.getFamilyMap().entrySet()) {
                QualifierValue[] qv = new QualifierValue[entry.getValue().size()];
                int j = 0;
                for (Map.Entry<byte[], Long> qualifiers : entry.getValue().entrySet()) {
                    qv[j] = new QualifierValue(qualifiers.getValue(), qualifiers.getKey(), null);
                    j++;
                }
                kv[i] = new KeyValues(rowkey, entry.getKey(), qv);
                i++;
            }

            return new GetOperation(kv, get);
        }
    }

    private static Result createResult(ResultOperation operation) {
        return new Result(operation);
    }

    private static PutOperation createPut(Put put) {
        NavigableMap<byte[], NavigableMap<byte[], Pair<Long, byte[]>>> values = put.getFamilyMap();
        if (values == null) {
            if (put.isReusable()) {
                return createFromReusablePut(put);
            } else {
                throw new RuntimeException("Put is empty");
            }
        } else {
            return createFromPut(put);
        }
    }

    private static PutOperation createFromReusablePut(PutRows put) {
        PutToByteSerializer.ByteCollector collector = PutSerializer.getSerializer().serializePut((Put) put, -1);

        byte[] b = collector.getBytes();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(b, 0, collector.getSize());

        return new PutOperation(baos.toByteArray(), put);
    }

    public static Operation createMutation(RowMutations rowMutation) {
        if (rowMutation.isFullDelete()) {
            return new DeleteOperation(new KeyValues(rowMutation.getRow(), null, (QualifierValue) null), rowMutation);
        }
        NavigableMap<byte[], List<QualifierValue>> qualValues = new TreeMap<>(BytesComparator.BYTES_COMPARATOR);
        for (Pair<Cell, RowMutations.Type> entry : rowMutation.getCells()) {
            QualifierValue qv;
            Cell cell = entry.getFirst();
            qualValues.putIfAbsent(cell.getFamilyArray(), new ArrayList<>());
            if (entry.getSecond() == RowMutations.Type.PUT) {
                qv = new QualifierValue(cell.getTimestamp(), cell.cloneQualifier(), cell.cloneValue());
            } else {
                qv = new DeletedQualifierValue(cell.getTimestamp(), cell.cloneQualifier());
            }
            qualValues.get(cell.getFamilyArray()).add(qv);
        }
        List<KeyValues> kv = new ArrayList<>();
        qualValues.forEach((k, v) -> kv.add(new KeyValues(rowMutation.getRow(), k, v)));
        return new PutOperation(kv.toArray(new KeyValues[0]), rowMutation);
    }

    private static PutOperation createFromPut(Put put) {
        KeyValues[] kv = new KeyValues[put.getFamilyMap().size()];
        int i = 0;
        for (Map.Entry<byte[], NavigableMap<byte[], Pair<Long, byte[]>>> entry : put.getFamilyMap().entrySet()) {
            QualifierValue[] qv = new QualifierValue[entry.getValue().size()];
            int j = 0;
            for (Map.Entry<byte[], Pair<Long, byte[]>> qualifiers : entry.getValue().entrySet()) {
                qv[j] = QualifierValue.withSharedValue(qualifiers.getValue().getFirst(), qualifiers.getKey(), qualifiers.getValue().getSecond());
                j++;
            }
            kv[i] = new KeyValues(put.getRow(), entry.getKey(), qv);
            i++;
        }
        return new PutOperation(kv, put.getPutType(), put);
    }

    private static PutRows createPut(PutOperation operation) {
        PutRows put = new Put(operation.getRowkey());
        for (KeyValues kv : operation.getValues()) {
            for (QualifierValue qv : kv.getValues()) {
                put.addColumn(kv.getCfName(), qv.getQualifier(), qv.getValue());
            }
        }
        return put;
    }

    private static DeleteOperation createDelete(Delete delete) {
        byte[] rowkey = delete.getRow();
        return delete.getFamilyCellMap().isEmpty()
                ? new DeleteOperation(new KeyValues(rowkey, null, (QualifierValue) null), delete)
                : createPartialDelete(delete);

    }

    private static DeleteOperation createPartialDelete(Delete delete) {
        byte[] rowkey = delete.getRow();
        List<KeyValues> keyValues = new ArrayList<>();
        for (Map.Entry<byte[], List<Cell>> entry : delete.getFamilyCellMap().entrySet()) {
            List<Cell> values = entry.getValue();
            if (values.isEmpty()) {
                keyValues.add(new KeyValues(rowkey, entry.getKey(), (QualifierValue) null));
            } else {
                List<QualifierValue> list = new ArrayList<>();
                for (Cell cell : values) {
                    list.add(new QualifierValue(cell.getTimestamp(), cell.cloneQualifier(), Bytes.START_BYTES_UNIFORM));
                }
                KeyValues kv = new KeyValues(rowkey, entry.getKey(), list);
                keyValues.add(kv);
            }
        }
        return new DeleteOperation(keyValues.toArray(new KeyValues[0]), delete);
    }

    private static Delete createDelete(DeleteOperation operation) {
        return new Delete(operation.getRowkey());
    }

    public static ScanOperation fromScanV1(Scan scan, int position, int dataBlockSize) {
        return new ScanOperationV1(new RegionPosition(position),
                scan.getBlockNumber(),
                DEFAULT_TRY_NUM,
                scan.getStartKey(),
                scan.getStopKey(),
                dataBlockSize);
    }

    public static ScanOperation fromScanV2(Scan scan, int position, int dataBlockSize) {
        return new ScanOperationV2(new RegionPosition(position),
                scan.getBlockNumber(),
                DEFAULT_TRY_NUM,
                (short) scan.getBs(),
                scan.getStartKey(),
                scan.getStopKey(),
                dataBlockSize);
    }

    public static ScanOperation createScanV1(Scan scan, int position, int blockNumber, short tryNumber, int dataBlockSize) {
        return new ScanOperationV1(new RegionPosition(position),
                blockNumber,
                tryNumber,
                scan.getStartKey(),
                scan.getStopKey(),
                dataBlockSize);
    }

    public static ScanOperation createScanV2(Scan scan, int position, int blockNumber, short tryNumber, int dataBlockSize) {
        return new ScanOperationV2(new RegionPosition(position),
                blockNumber,
                tryNumber,
                (short) scan.getBs(),
                scan.getStartKey(),
                scan.getStopKey(),
                dataBlockSize);
    }
}
