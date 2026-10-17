/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.data.files;

import com.light.atalanta.model.CompressType;
import com.light.atalanta.region.server.utils.RsUtils;
import com.light.atalanta.util.Bytes;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

import static com.light.atalanta.region.server.data.files.AtalantaCompressFileWriter.*;

public class AtalantaReader implements AutoCloseable, Closeable {
    public static final Logger LOGGER = LoggerFactory.getLogger(AtalantaReader.class.getName());
    public static final int BLOCK_SIZE = 65536;

    static {
    }

    private final FileInputStream fileInputStream;
    private final FileChannel fileChannelIn;
    private final int dataBlockSize;
    private FileInputStream fileInputStreamLob;
    private FileChannel fileChannelInLob;
    private final long fileSize;
    private final AtalantaMeta dataFileHeader;
    private final int fileIndex;
    private long lobFileSize = -1;
    private int roundFactor;
    private ByteBuffer curBlock;
    private CompressType compressType = CompressType.none;
    private boolean isNeedDecompress;
    private long curBlockDecompressSize = 1;
    private long curBlockCompressSize = 1;
    private short curBlockMsgCount = 0;
    private long endMarkerOffset = -1;

    public AtalantaReader(String path, String pathToLob, CompressType compressType) throws IOException {
        this(path, pathToLob);
        this.compressType = compressType;
        if (compressType != CompressType.none) {
            roundFactor = 16;
        }
        isNeedDecompress = compressType != CompressType.none;
    }

    public AtalantaReader(String path, CompressType compressType) throws IOException {
        this(path);
        this.compressType = compressType;
        if (compressType != CompressType.none) {
            roundFactor = 16;
        }
        isNeedDecompress = compressType != CompressType.none;
    }

    public AtalantaReader(String path, String pathToLob) throws IOException {
        this(path);
        fileInputStreamLob = new FileInputStream(pathToLob);
        fileChannelInLob = fileInputStreamLob.getChannel();
        lobFileSize = fileChannelInLob.size();
    }

    public AtalantaReader(String path) throws IOException {
        fileInputStream = new FileInputStream(path);
        fileChannelIn = fileInputStream.getChannel();
        fileSize = fileChannelIn.size();
        fileIndex = getFileIndex(path);
        roundFactor = 16 * (fileIndex + 1);
        dataBlockSize = calculateBlockSize(fileIndex);
        dataFileHeader = readHeader();
        curBlock = readNextBlock();
    }

    @Override
    public void close() throws IOException {
        fileChannelIn.close();
        fileInputStream.close();
        if (fileInputStreamLob != null) {
            fileChannelInLob.close();
            fileInputStreamLob.close();
        }
    }

    public long getDataBlockSize() {
        return dataBlockSize;
    }

    public AtalantaMeta getHeader() {
        return dataFileHeader;
    }

    public AtalantaRecord next() throws IOException {
        int msgSize = readNextMsgSize();
        byte[] record = new byte[0];
        try {
            while ((compressType == CompressType.none && msgSize == 0)
                    || ((compressType == CompressType.snappy || compressType == CompressType.lz) && msgSize < 18)) {
                curBlock = readNextBlock();
                isNeedDecompress = compressType != CompressType.none;
                if (curBlock == null) {
                    return null;
                }
                msgSize = readNextMsgSize();
            }
            if (isNeedDecompress) {
                LOGGER.trace("Read and decompress the next record: msgSize: {}, buffer pos: {},  buffer limit: {} ",
                        msgSize, curBlock.position(), curBlock.limit());

                curBlockCompressSize = msgSize;
                byte[] curBlockDecompressSizeByte = new byte[8];
                curBlock.get(curBlockDecompressSizeByte, 0, UNCOMPRESSED_SIZE_SIZE);
                curBlockDecompressSize = Bytes.toLong(curBlockDecompressSizeByte);
                curBlockMsgCount = curBlock.order(ByteOrder.LITTLE_ENDIAN).getShort();
                LOGGER.trace("curBlockDecompressSize: {}, curBlockMsgCount: {}", curBlockDecompressSize, curBlockMsgCount);

                record = new byte[msgSize - BLOCK_INFO_SIZE];
                curBlock.get(record);
                int alignSize = 256 * BLOCK_SIZE / dataBlockSize;
                int crcPos = ((msgSize - 1) / alignSize + 1) * alignSize - CRC_SIZE;
                curBlock.position(crcPos);
                int crc32 = curBlock.order(ByteOrder.LITTLE_ENDIAN).getInt();
                curBlock = ByteBuffer.wrap(uncompress(record, (int)curBlockDecompressSize));
                msgSize = readNextMsgSize();
                LOGGER.trace("crc32: {}, msgSize: {}", crc32, msgSize);
                isNeedDecompress = false;
            }

            int recSize = RsUtils.align(msgSize, roundFactor);
            curBlock.position(curBlock.position() - 4);
            record = new byte[Math.min(recSize, curBlock.limit() - curBlock.position())];
            curBlock.get(record);
            ByteBuffer blockData = ByteBuffer.wrap(record);

            return new AtalantaRecord(msgSize, blockData,
                    msgSize * curBlockCompressSize / curBlockDecompressSize);

        } catch (
                BufferUnderflowException ex) {
            LOGGER.error("BufferUnderflowException: Cannot read next record: " +
                            "msgSize: {}, alignSize: {},  buffer pos: {},  buffer limit: {} ",
                    msgSize, record.length, curBlock.position(), curBlock.limit());
            throw new RuntimeException(ex);
        }
    }

    private byte[] uncompress(byte[] record, int curBlockDecompressSize) throws IOException {
        if (compressType == CompressType.snappy) {
            return Snappy.uncompress(record);
        } else if (compressType == CompressType.lz) {
            return uncompressLz(record, curBlockDecompressSize);
        } else {
            throw new RuntimeException("Unknown compress type: " + compressType);
        }
    }

    private byte[] uncompressLz(byte[] record, int curBlockDecompressSize) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        byte[] restored = new byte[curBlockDecompressSize];
        decompressor.decompress(record, restored);
        return restored;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getEndMarkerOffset() {
        return endMarkerOffset;
    }

    public AtalantaMeta getDataFileHeader() {
        return dataFileHeader;
    }

    public int getFileIndex() {
        return fileIndex;
    }

    public long getLobFileSize() {
        return lobFileSize;
    }

    private int getFileIndex(String path) {
        int lastSlashPos = path.lastIndexOf("/");
        final String fileName = lastSlashPos > 0 ? path.substring(lastSlashPos + 1) : path;
        try {
            return Integer.parseInt(fileName.substring(0, Math.min(3, fileName.length())));
        } catch (NumberFormatException ex) {
            return -1;
        }
    }

    private AtalantaMeta readHeader() throws IOException {
        ByteBuffer meta = ByteBuffer.allocate((int) Math.min(fileSize, dataBlockSize));
        fileChannelIn.read(meta);
        return new AtalantaMeta(meta);
    }

    /**
     * 000.data -> block = meta = 4096 bytes
     * 001.data -> block = meta = 8192 bytes
     * 002.data -> block = meta = 12288 bytes
     */
    private int calculateBlockSize(long fileIndex) {
        return (int) ((fileIndex + 1) * 4 * 1024);
    }

    private int readNextMsgSize() {
        return (curBlock == null || (curBlock.limit() - curBlock.position()) <= 4) ? 0 :
                curBlock.order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private ByteBuffer readNextBlock() throws IOException {
        if (endMarkerOffset >= 0) {
            return null;
        }
        long blockOffset = fileChannelIn.position();
        ByteBuffer res = ByteBuffer.allocate(dataBlockSize);
        int size = fileChannelIn.read(res);
        // Check the raw block header, never bytes inside compressed record data
        if (size >= 16 && res.getInt(0) == -1 && res.getLong(4) == 0 && res.getInt(12) == 0) {
            endMarkerOffset = blockOffset;
            return null;
        }
        return size > 0 ? (ByteBuffer) res.flip() : null;
    }
}
