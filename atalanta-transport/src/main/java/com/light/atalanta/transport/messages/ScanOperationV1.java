/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.NumbersByteHelper;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;
import com.light.atalanta.transport.RegionPosition;
import com.light.atalanta.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.light.atalanta.transport.messages.BulkHelper.NOT_USE_TABLE_POSITION;

public class ScanOperationV1 implements ScanOperation {
    private static final int SCAN_ALIGN_BORDER_PER_BLOCK = 16;

    public static final int LENGTH_MESSAGE_SIZE = 4;
    public static final int LENGTH_MESSAGE_TYPE = 1;
    public static final int LENGTH_MESSAGE_REGION_POS = 4;
    public static final int LENGTH_MESSAGE_BLOCK_NUM = 4;
    public static final int LENGTH_MESSAGE_TRY_NUM = 2;
    public static final int LENGTH_MESSAGE_FLAG_RESERVE = 2;
    public static final int LENGTH_MESSAGE_BYTES_ARRAY = 8;
    public static final int LENGTH_MESSAGE_BLOCK_SIZE_WITH_RESERVED = 4;

    public static final int TRANSPORT_MESSAGE_HEADER_LENGTH = LENGTH_MESSAGE_SIZE +
            LENGTH_MESSAGE_TYPE +
            LENGTH_MESSAGE_REGION_POS +
            LENGTH_MESSAGE_BLOCK_NUM +
            LENGTH_MESSAGE_TRY_NUM +
            LENGTH_MESSAGE_BYTES_ARRAY +
            LENGTH_MESSAGE_BYTES_ARRAY +
            LENGTH_MESSAGE_FLAG_RESERVE;

    private final int msgSize;
    private int tablePosition = NOT_USE_TABLE_POSITION;
    private final RegionPosition regionPos;
    private int blockNum;
    private short tryNum;
    private final short reserve;
    private final byte[] startKey;
    private final byte[] endKey;

    private final int dataBlockSize;

    static ScanOperationV1 createNextScan(ScanOperationV1 scanOperation) {
        ScanOperationV1 next = new ScanOperationV1(scanOperation.getRegionPosition(),
                scanOperation.blockNum + 1,
                (short) 1, scanOperation.startKey, scanOperation.endKey,
                scanOperation.dataBlockSize);
        next.setTablePosition(scanOperation.getTablePosition());
        return next;
    }

    public ScanOperationV1(RegionPosition position) {
        this(position, 1, (short) 1, Bytes.START_BYTES_UNIFORM, Bytes.END_BYTES_UNIFORM, 1);
    }

    public ScanOperationV1(RegionPosition position, int blockNum, short tryNum, byte[] startKey, byte[] endKey, int dataBlockSize) {
        this.msgSize = TRANSPORT_MESSAGE_HEADER_LENGTH;

        this.regionPos = position;
        this.blockNum = blockNum;
        this.tryNum = tryNum;
        this.reserve = 0;
        this.startKey = startKey;
        this.endKey = endKey;
        this.dataBlockSize = dataBlockSize;
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer bb) throws IOException {
        ByteBuffer res = bb.order(ByteOrder.LITTLE_ENDIAN);
        res.putInt(getMessageSize());
        res.put(getMessageType().getTypeByte());
        res.putInt(regionPos.getPosition());
        if (tablePosition >= 0) {
            res.putInt(tablePosition);
        }
        res.putInt(blockNum);
        res.putShort(tryNum);
        res.putShort(reserve);
        res.put(startKey);
        res.put(endKey);

        return res;
    }

    @Override
    public int getMessageSize() {
        return msgSize + (tablePosition >= 0 ? Integer.BYTES : 0);
    }

    @Override
    public MessageType getMessageType() {
        return tablePosition >= 0 ? MessageType.SCAN_NIO : MessageType.SCAN;
    }

    @Override
    public void setTablePosition(int tablePosition) {
        if (tablePosition < 0 && tablePosition != NOT_USE_TABLE_POSITION) {
            throw new IllegalArgumentException("Invalid table position: " + tablePosition);
        }
        this.tablePosition = tablePosition;
    }

    @Override
    public int getTablePosition() {
        return tablePosition;
    }

    @Override
    public RegionPosition getRegionPosition() {
        return regionPos;
    }

    public void setBlockNumber(int blockNumber) {
        this.blockNum = blockNumber;
    }

    public int getBlockNumber() {
        return this.blockNum;
    }

    public long getTryNumber() {
        return this.tryNum;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public int getDataBlockSize() {
        return dataBlockSize;
    }

    public void setTryNumber(long tryNum) {
        this.tryNum = (short) tryNum;
    }

    public static ScanResultContainer getResultsFromScan(byte[] scanResult, String location) throws NotActualRegionMappingException {
        int pos = Operation.LENGTH_MESSAGE_SIZE;
        int blockNumber = NumbersByteHelper.fromBytes_int(scanResult, pos);
        pos += LENGTH_MESSAGE_BLOCK_NUM;
        short tryNumber = NumbersByteHelper.fromBytes_short(scanResult, pos);
        pos += LENGTH_MESSAGE_TRY_NUM;

        short reserve = NumbersByteHelper.fromBytes_short(scanResult, pos);
        pos += LENGTH_MESSAGE_FLAG_RESERVE;

        int curDataBlockSize = NumbersByteHelper.fromBytes_int(scanResult, pos);
        pos += LENGTH_MESSAGE_BLOCK_SIZE_WITH_RESERVED;

        ScanResultContainer container = new ScanResultContainerV1(blockNumber, tryNumber);
        if (scanResult.length < (pos + LENGTH_MESSAGE_SIZE)) {
            container.setFinished();
            return container;
        }
        while (pos < scanResult.length) {
            int size = OperationHelper.getMessageSize(scanResult, pos);
            byte[] op = new byte[size];
            System.arraycopy(scanResult, pos, op, 0, size);

            ResultOperation ro = OperationFactory.fromBytes(op, location, 60_000);
            if (ro == EmptyResultOperation.EMPTY_OPERATION) {
                container.setFinished();
                return container;
            }
            container.addResultOperation(ro);
            if (pos == Operation.LENGTH_MESSAGE_SIZE) {
                container.setFirstRowkey(ro.getRowkey());
            }
            pos = alignBytesForNextBorder(pos, size, curDataBlockSize);
        }
        return container;
    }

    private static int alignBytesForNextBorder(int pos, int size, int dataBlockSize) {
        int align = SCAN_ALIGN_BORDER_PER_BLOCK * (dataBlockSize + 1);
        int alignSize = size;
        while (alignSize % align != 0) {
            alignSize += 1;
        }
        return pos + alignSize;
    }

}
