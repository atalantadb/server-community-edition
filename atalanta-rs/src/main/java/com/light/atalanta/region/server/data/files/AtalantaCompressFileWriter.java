/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.data.files;

import com.light.atalanta.model.CompressType;
import com.light.atalanta.model.RecordCompressionType;
import com.light.atalanta.region.server.utils.RsUtils;
import com.light.atalanta.util.Bytes;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.xerial.snappy.Snappy;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import static com.light.atalanta.region.server.data.files.AtalantaLobChunk.MSG_SIZE_SIZE;
import static com.light.atalanta.region.server.services.operations.progressed.SplitRegion1.LOB_FILE_NAME;


public class AtalantaCompressFileWriter implements AtalantaFileWriter, AutoCloseable, Closeable {

    public static final int UNCOMPRESSED_SIZE_SIZE = 6;
    public static final int INIT_SNAPPY_BLOCK_FACTOR = 8;
    public static final int COUNT_SIZE = 2;
    public static final int CRC_SIZE = 4;
    public static final int BLOCK_INFO_SIZE = MSG_SIZE_SIZE + UNCOMPRESSED_SIZE_SIZE + COUNT_SIZE + CRC_SIZE;
    public static final int MAX_BLOCK_SIZE = 65536;
    public static final int COMPRESS_MSG_ALIGN_SIZE = 16;
    public static final int ROUND_FACTOR = 16;
    private final FileOutputStream fileOutputStream;
    private final FileChannel outCh;
    private final int blockSize;
    private final int tryToCompressLevel;
    private final int alignSize;
    private int maxBlockSize;
    private int curBlockCompressSize = 0;
    private int curBlockUncompressSize = 0;
    private List<AtalantaRecord> recordBuffer = new ArrayList<>();
    private byte[] curCompressData = null;
    private int curCompressDataUncompressedSize = 0;
    private int curBlockMsgCount = 0;
    private final CompressType compressType;
    private final AtalantaLobFileCopier lobFileCopier;

    public AtalantaCompressFileWriter(String path, AtalantaMeta meta, int tryToCompressLevel, CompressType compressType,
                                 Boolean isLobPresent, String srcLobFilePath, String targetPath, RecordCompressionType recordCompressType) throws IOException {
        fileOutputStream = new FileOutputStream(path, false);
        outCh = fileOutputStream.getChannel();
        blockSize = MAX_BLOCK_SIZE;
        alignSize = 256 * MAX_BLOCK_SIZE / blockSize;
        maxBlockSize = blockSize - INIT_SNAPPY_BLOCK_FACTOR * alignSize;
        this.tryToCompressLevel = tryToCompressLevel;
        this.compressType = compressType;
        if (meta.getMeta().limit() == MAX_BLOCK_SIZE) {
            meta.writeMeta(outCh);
        } else {
            outCh.truncate(MAX_BLOCK_SIZE);
            outCh.position(MAX_BLOCK_SIZE);
        }
        if (isLobPresent) {
            lobFileCopier = new AtalantaLobFileCopier(srcLobFilePath, targetPath);
        } else {
            lobFileCopier = null;
        }
    }

    public void write(AtalantaRecord atalantaRecord) throws IOException {
        if (atalantaRecord.isLobRecord()) {
            final long lobOffset = lobFileCopier.write(atalantaRecord);
            atalantaRecord.setLobOffset(lobOffset);
        }
        atalantaRecord.uncompressValues(COMPRESS_MSG_ALIGN_SIZE);
        curBlockCompressSize += atalantaRecord.getAvgCompressSize();
        int recSize = RsUtils.align(atalantaRecord.getMsgSize(), ROUND_FACTOR);
        atalantaRecord.resize(recSize);
        recordBuffer.add(atalantaRecord);
        curBlockUncompressSize += atalantaRecord.getRecord().array().length;
        updateMaxBlockSize();
        if (curBlockCompressSize > blockSize * tryToCompressLevel / 100) {
            tryToCompressAndSaveNextBlock(false);
        }
    }

    private void updateMaxBlockSize() {
        int recordsCount = recordBuffer.size();
        if (recordsCount >= 12 && recordsCount < COMPRESS_MSG_ALIGN_SIZE) {
            maxBlockSize = blockSize - (INIT_SNAPPY_BLOCK_FACTOR - 3) * alignSize;
            return;
        }
        if (recordsCount >= COMPRESS_MSG_ALIGN_SIZE && recordsCount < 20) {
            maxBlockSize = blockSize - (INIT_SNAPPY_BLOCK_FACTOR - 4) * alignSize;
            return;
        }
        if (recordsCount >= 20 && recordsCount < 24) {
            maxBlockSize = blockSize - (INIT_SNAPPY_BLOCK_FACTOR - 5) * alignSize;
            return;
        }
        maxBlockSize = blockSize - (INIT_SNAPPY_BLOCK_FACTOR - 6) * alignSize;
    }

    @Override
    public void close() throws IOException {
        while ( !recordBuffer.isEmpty() ) {
            tryToCompressAndSaveNextBlock(true);
        }
        if (fileOutputStream != null) {
            fileOutputStream.close();
            outCh.close();
        }
        if (lobFileCopier != null) {
            lobFileCopier.close();
        }
    }


    private void tryToCompressAndSaveNextBlock(boolean forceSave) throws IOException {
        ByteBuffer data = ByteBuffer.allocate(curBlockUncompressSize);
        recordBuffer.forEach(r -> {
            ByteBuffer record = r.getRecord();
            record.position(0);
            final int alignSize = RsUtils.align(r.getMsgSize(), COMPRESS_MSG_ALIGN_SIZE);
            final int length = alignSize <= record.array().length ? record.array().length : alignSize;
            data.put(record.array(), 0, Math.min(length, record.array().length));
        });
        data.position(0);
        byte[] arr = new byte[data.remaining()];
        data.get(arr);
        byte[] newCompressData = compressData(arr);
        curBlockMsgCount = recordBuffer.size();
        if (newCompressData.length > maxBlockSize) {
            // If recordBuffer already holds more records than fit into a block after compression
            if (curCompressData != null) {
                // If we already have compressed data without the last record, write it to a new block,
                // and move the last record to a new buffer
                writeBlock(curCompressData, curCompressDataUncompressedSize, curBlockMsgCount - 1);
                ArrayList<AtalantaRecord> newRecordBuffer = new ArrayList<>();
                AtalantaRecord atalantaRecord = recordBuffer.get(recordBuffer.size() - 1);
                newRecordBuffer.add(atalantaRecord);
                recordBuffer = newRecordBuffer;
                curBlockCompressSize += atalantaRecord.getAvgCompressSize();
                curBlockUncompressSize += atalantaRecord.getRecord().limit();
                updateMaxBlockSize();
            } else {
                // If we have no compressed data, try to compress without the last record
                removeLastRecordAndRecompress(data, recordBuffer.size() - 1);
            }
        } else {
            // If more records can be added to recordBuffer and they still fit into a block after compression
            if (forceSave) {
                // No more records will come, save the current data to a new block
                writeBlock(newCompressData, arr.length, recordBuffer.size());
                recordBuffer.clear();
                updateMaxBlockSize();
            } else {
                // Keep the compressed data and wait for the next record
                curCompressData = newCompressData;
                curCompressDataUncompressedSize = arr.length;
            }
        }
    }

    private byte[] compressData(byte[] arr) throws IOException {
        if (compressType == CompressType.snappy) {
            return Snappy.compress(arr);
        } else if (compressType == CompressType.lz) {
            return compressLz(arr);
        } else {
            throw new RuntimeException("Unknown compress type: " + compressType);
        }
    }

    private byte[] compressLz(byte[] arr) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = factory.fastCompressor();
        return compressor.compress(arr);
    }

    private void removeLastRecordAndRecompress(ByteBuffer data, int removeFrom) throws IOException {
        int sizeToRemove = 0;
        for (int i = removeFrom; i < recordBuffer.size(); ++i) {
            AtalantaRecord record = recordBuffer.get(i);
            sizeToRemove += record.getRecord().limit();
        }
        data.position(0);
        byte[] arr = new byte[data.remaining() - sizeToRemove];
        data.get(arr);
        byte[] newCompressData = compressData(arr);
        if (newCompressData.length <= maxBlockSize) {
            writeBlock(newCompressData, arr.length, removeFrom);
            ArrayList<AtalantaRecord> newRecordBuffer = new ArrayList<>();
            for (int i = removeFrom; i < recordBuffer.size(); ++i) {
                AtalantaRecord atalantaRecord = recordBuffer.get(i);
                curBlockCompressSize += atalantaRecord.getAvgCompressSize();
                curBlockUncompressSize += atalantaRecord.getRecord().limit();
                newRecordBuffer.add(atalantaRecord);
            }
            recordBuffer = newRecordBuffer;
            updateMaxBlockSize();
        } else {
            removeLastRecordAndRecompress(data, removeFrom - 1);
        }
    }

    private void writeBlock(byte[] curCompressData, long uncompressSize, long msgCount) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(blockSize);
        int blockSize = curCompressData.length + BLOCK_INFO_SIZE;
        buffer.order(ByteOrder.LITTLE_ENDIAN).putInt(blockSize);
        byte[] uncompressSizeArr = Bytes.toBytesRevers(uncompressSize);
        buffer.put(uncompressSizeArr, 0, UNCOMPRESSED_SIZE_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN).putShort((short) msgCount);
        buffer.put(curCompressData);
        CRC32 crc32 = new CRC32();
        int alignCrcPos = ((curCompressData.length + BLOCK_INFO_SIZE - 1) / this.alignSize + 1) * this.alignSize;
        crc32.update(buffer.array(), 0, alignCrcPos - CRC_SIZE);
        buffer.position(alignCrcPos - CRC_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN).putInt((int) crc32.getValue());
        buffer.position(0);
        outCh.write(buffer);

        // Reset the counters
        this.curCompressData = null;
        curCompressDataUncompressedSize = 0;
        curBlockMsgCount = 0;
        curBlockCompressSize = 0;
        curBlockUncompressSize = 0;
    }

}
