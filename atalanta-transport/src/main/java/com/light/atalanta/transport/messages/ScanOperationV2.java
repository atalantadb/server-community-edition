/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.NumbersByteHelper;
import com.light.atalanta.transport.RegionPosition;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;
import com.light.atalanta.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static com.light.atalanta.transport.messages.BulkHelper.NOT_USE_TABLE_POSITION;

public class ScanOperationV2 implements ScanOperation {
    private static final int SCAN_ALIGN_BORDER_PER_BLOCK = 16;

    public static final int LENGTH_MESSAGE_SIZE = 4;
    public static final int LENGTH_MESSAGE_TYPE = 1;
    public static final int LENGTH_MESSAGE_REGION_POS = 4;
    public static final int LENGTH_MESSAGE_BLOCK_NUM = 4;
    public static final int LENGTH_MESSAGE_TRY_NUM = 2;
    public static final int LENGTH_MESSAGE_FLAG_RESERVE = 2;
    public static final int LENGTH_MESSAGE_BYTES_ARRAY_NUM = 4;

    public static final int TRANSPORT_MESSAGE_HEADER_LENGTH = LENGTH_MESSAGE_SIZE +
            LENGTH_MESSAGE_TYPE +
            LENGTH_MESSAGE_REGION_POS +
            LENGTH_MESSAGE_BLOCK_NUM +
            LENGTH_MESSAGE_TRY_NUM +
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

    static ScanOperationV2 createNextScan(ScanOperationV2 scanOperation) {
        ScanOperationV2 next = new ScanOperationV2(scanOperation.getRegionPosition(),
                scanOperation.blockNum + 1,
                (short) 1,
                (short) 0xFFFF,
                scanOperation.startKey,
                scanOperation.endKey,
                scanOperation.dataBlockSize);
        next.setTablePosition(scanOperation.getTablePosition());
        return next;
    }

    public ScanOperationV2(RegionPosition position) {
        this(position, 1, (short) 1, (short) 0xFF, Bytes.START_BYTES_UNIFORM, Bytes.END_BYTES_UNIFORM, 1);
    }

    public ScanOperationV2(RegionPosition position, int blockNum, short tryNum, short reserve, byte[] startKey, byte[] endKey, int dataBlockSize) {
        this.msgSize = TRANSPORT_MESSAGE_HEADER_LENGTH +
                LENGTH_MESSAGE_BYTES_ARRAY_NUM +
                startKey.length +
                LENGTH_MESSAGE_BYTES_ARRAY_NUM +
                endKey.length;


        this.regionPos = position;
        this.blockNum = blockNum;
        this.tryNum = tryNum;
        this.reserve = reserve;
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

        res.put(NumbersByteHelper.toBytes_int(startKey.length));
        if (startKey.length > 0) {
            res.put(startKey);
        }

        res.put(NumbersByteHelper.toBytes_int(endKey.length));
        if (endKey.length > 0) {
            res.put(endKey);
        }
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

    public static ScanResultContainerV2 getResultsFromScan(byte[] scanResult, String location) throws NotActualRegionMappingException {
        int pos = 0;

        // Total size
        int fullSize = NumbersByteHelper.fromBytes_int(scanResult, pos);
        if (fullSize == 17) {
            return new ScanResultContainerV2(0, (short) 0, 0, 0);
        }
        pos = Operation.LENGTH_MESSAGE_SIZE;

        // Block
        int clientBlockNumber = NumbersByteHelper.fromBytes_int(scanResult, pos);
        pos += LENGTH_MESSAGE_BLOCK_NUM;

        // Attempt number (short)
        short tryNumber = NumbersByteHelper.fromBytes_short(scanResult, pos);
        pos += 2;
        // Reserved (short)
        short reserve = NumbersByteHelper.fromBytes_short(scanResult, pos);
        pos += 2;

        // Block size
        int dataBlockSize = NumbersByteHelper.fromBytes_int(scanResult, pos);
        pos += 4;

        // Current block number
        int realBlockNumber = NumbersByteHelper.fromBytes_int(scanResult, pos);
        pos += 4;

        // Decode the data
        ScanResultContainerV2 container = new ScanResultContainerV2(clientBlockNumber, tryNumber, dataBlockSize, realBlockNumber);
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
            container.setFirstRowkey(ro.getRowkey());
            pos = alignBytesForNextBorder(pos, size, dataBlockSize);
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

    @Override
    public String toString() {
        return "ScanOperationV2{" +
                "msgSize=" + getMessageSize() +
                ", msgType=" + getMessageType() +
                ", tablePosition=" + tablePosition +
                ", regionPos=" + regionPos +
                ", blockNum=" + blockNum +
                ", tryNum=" + tryNum +
                ", reserve=" + reserve +
                ", startKey=" + Arrays.toString(startKey) +
                ", endKey=" + Arrays.toString(endKey) +
                ", dataBlockSize=" + dataBlockSize +
                '}';
    }

}
