/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.data.files;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.light.atalanta.region.server.data.files.AtalantaLobChunk.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class AtalantaRecord {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtalantaRecord.class);
    public static final int ALIGN_SIZE = 16;
    private ByteBuffer record;
    private int msgSize; //uint32
    private byte type; //uint8
    private int regionPos; //uint32
    private int recCrc; //uint32
    private byte lobInfo; //uint8
    private byte zipInfo; //uint8
    private byte newVersion; //uint8
    private int keySize; //uint32
    private int cfSize; //uint32
    private int qvSize; //uint32
    private byte[] key;
    private byte[] cf;
    private byte[] values;
    private long lobOffset; //uint64
    private int lobSize; //uint32
    private byte alignSize; //uint8
    private long lobId; //uint64
    private final long avgCompressSize;
    private final int numberOfBytesToAlign;
    private final int zipInfoPosInBuffer;


    public AtalantaRecord(int msgSize, ByteBuffer buffer, long avgCompressSize) {
        try {
            buffer.position(MSG_SIZE_SIZE);
            this.record = buffer;
            int a = record.limit() % ALIGN_SIZE;
            numberOfBytesToAlign = a == 0 ? 0 : ALIGN_SIZE - a;
            this.msgSize = msgSize;
            this.type = buffer.get();
            this.regionPos = buffer.order(LITTLE_ENDIAN).getInt();
            this.recCrc = buffer.order(LITTLE_ENDIAN).getInt();
            this.lobInfo = buffer.get();
            zipInfoPosInBuffer = buffer.position();
            this.zipInfo = buffer.get();
            this.newVersion = buffer.get();
            this.keySize = buffer.order(LITTLE_ENDIAN).getInt();
            this.cfSize = buffer.order(LITTLE_ENDIAN).getInt();
            this.qvSize = buffer.order(LITTLE_ENDIAN).getInt();
            this.key = new byte[(int) keySize];
            buffer.get(key);
            this.cf = new byte[cfSize];
            buffer.get(cf);
            if (isLobRecord()) {
                buffer.position(msgSize - LOB_INFO_SIZE);
                this.lobOffset = buffer.order(LITTLE_ENDIAN).getLong();
                this.lobSize = buffer.order(LITTLE_ENDIAN).getInt();
                this.alignSize = buffer.get();
                this.lobId = buffer.order(LITTLE_ENDIAN).getLong();
            } else {
                lobOffset = 0;
                lobSize = -1;
                alignSize = 0;
                lobId = -1;
            }
            this.avgCompressSize = avgCompressSize;
        } catch (BufferUnderflowException ex) {
            LOGGER.error("BufferUnderflowException: Cannot parse record: msgSize: {}, bufferPos: {},  bufferLimit: {} ",
                    msgSize, buffer.position(), buffer.limit());
            LOGGER.error("BufferUnderflowException: Cannot parse record: buffer: {} ", buffer.array());
            throw new RuntimeException(ex);
        }
    }

    public void uncompressValues(int recordAlignSize) {
        if (isCompressedRecord()) {
            int position = record.position();
            byte[] compressData = new byte[msgSize - position];
            record.get(compressData);
            LZ4Factory factory = LZ4Factory.fastestInstance();
            LZ4FastDecompressor uncompress = factory.fastDecompressor();
            values = new byte[qvSize];
            uncompress.decompress(compressData, values);
            int msgSize = position + values.length;
            int a = msgSize % recordAlignSize;
            int numberOfBytesToAlign = a == 0 ? 0 : recordAlignSize - a;
            ByteBuffer newRecord = ByteBuffer.allocate(position + values.length + numberOfBytesToAlign);
            newRecord.order(LITTLE_ENDIAN).putInt(msgSize);
            newRecord.put(record.array(), MSG_SIZE_SIZE, position - MSG_SIZE_SIZE);
            newRecord.put(values);
            zipInfo = (byte) (zipInfo & 0xFE);
            newRecord.order(LITTLE_ENDIAN).put(zipInfoPosInBuffer, zipInfo);
            record = newRecord;
            this.msgSize = msgSize;
        }
    }

    public void compressValues(int recordAlignSize) {
        if (!isCompressedRecord() && lobInfo == 0) {
            int position = record.position();
            byte[] uncompressData = new byte[msgSize - position];
            record.get(uncompressData);
            LZ4Factory factory = LZ4Factory.fastestInstance();
            LZ4Compressor compressor = factory.fastCompressor();
            values = compressor.compress(uncompressData);
            msgSize = position + values.length;
            int a = msgSize % recordAlignSize;
            int numberOfBytesToAlign = a == 0 ? 0 : recordAlignSize - a;
            ByteBuffer newRecord = ByteBuffer.allocate(msgSize + numberOfBytesToAlign);
            newRecord.order(LITTLE_ENDIAN).putInt(0, msgSize);
            newRecord.position(MSG_SIZE_SIZE);
            newRecord.put(record.array(), MSG_SIZE_SIZE, position - MSG_SIZE_SIZE);
            newRecord.put(values);
            zipInfo = (byte) (zipInfo | 0x01);
            newRecord.order(LITTLE_ENDIAN).put(zipInfoPosInBuffer, zipInfo);
            record = newRecord;
        }
    }

    private boolean isCompressedRecord() {
        return (zipInfo & 0x01) == 1;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public byte getType() {
        return type;
    }

    public int getRegionPos() {
        return regionPos;
    }

    public int getRecCrc() {
        return recCrc;
    }

    public byte getLobInfo() {
        return lobInfo;
    }

    public byte getZipInfo() {
        return zipInfo;
    }

    public byte getNewVersion() {
        return newVersion;
    }

    public long getKeySize() {
        return keySize;
    }

    public long getCfSize() {
        return cfSize;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValues() {
        return values;
    }

    public long getLobOffset() {
        return lobOffset;
    }

    public void setLobOffset(long lobOffset) {
        this.lobOffset = lobOffset;
        record.order(LITTLE_ENDIAN).putLong(msgSize - LOB_INFO_SIZE, lobOffset);
    }

    public int getLobSize() {
        return lobSize;
    }

    public byte getAlignSize() {
        return alignSize;
    }

    public long getLobId() {
        return lobId;
    }

    public byte[] array() {
        return record.array();
    }

    public long getAvgCompressSize() {
        return avgCompressSize;
    }

    public ByteBuffer getRecord() {
        return record;
    }


    public int getNumberOfBytesToAlign() {
        return numberOfBytesToAlign;
    }

    @Override
    public String toString() {
        return "AtalantaRecord{" +
                "record=" + record +
                ", msgSize=" + msgSize +
                ", type=" + type +
                ", regionPos=" + regionPos +
                ", recCrc=" + recCrc +
                ", lobInfo=" + lobInfo +
                ", zipInfo=" + zipInfo +
                ", newVersion=" + newVersion +
                ", keySize=" + keySize +
                ", cfSize=" + cfSize +
                ", qvSize=" + qvSize +
                ", key=" + Arrays.toString(key) +
                ", values=" + Arrays.toString(values) +
                ", lobOffset=" + lobOffset +
                ", lobSize=" + lobSize +
                ", alignSize=" + alignSize +
                ", lobId=" + lobId +
                '}';
    }

    public int getSizeWithAlign() {
        return record.limit() + numberOfBytesToAlign;
    }

    public boolean isLobRecord() {
        long calculatedMsgSize = KEY_RECORD_OFFSET + getKeySize() + getCfSize() + LOB_INFO_SIZE;
        if (getMsgSize() != calculatedMsgSize) {
            LOGGER.trace("It is not LOB record size: calculated: {}, actual: {}", calculatedMsgSize, getMsgSize());
            return false;
        }

        return lobInfo == 1;
    }

    public void resize(int recSize) {
        if (record.limit() < recSize) {
            ByteBuffer newBuffer = ByteBuffer.allocate(recSize);
            ByteBuffer oldRecord = record.duplicate();
            oldRecord.position(0);
            oldRecord.limit(record.limit());
            newBuffer.put(oldRecord);
            record = newBuffer;
        }
    }
}
