/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.data.files;

import com.light.atalanta.region.server.utils.Crc16;
import com.light.atalanta.util.BytesComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;


public class AtalantaLobChunk {
    public static final Logger LOGGER = LoggerFactory.getLogger(AtalantaLobChunk.class.getName());
    public static final int CHUNK_BLOCK_SIZE = 16384;
    public static final int MSG_SIZE_SIZE = 4;
    public static final int HEADER_SIZE = 2 + 2 + 4 + 8 + 8;
    public static final int CRC_HEADER_START_POS = 2 + 2;
    public static final int CRC_HEADER_SIZE = 4 + 8 + 8;
    public static final int LOB_INFO_SIZE = 21;
    public static final int KEY_RECORD_OFFSET = 28;
    private int crcHeaderFromLob; //uint16
    private int crcRecordFromLob; //uint16
    private long mark; //uint32
    private long prevChunkOffset; //uint64
    private long nextChunkOffset; //uint64
    // isCrcHeaderOk == null means the check was not performed
    private Boolean isCrcHeaderOk;
    // isCrcRecordOk == null means the check was not performed
    private Boolean isCrcRecordOk;
    private int crcHeaderCalc;
    private int crcRecordCalc;
    private ByteBuffer curChunk;
    private final AtalantaRecord atalantaRecord;
    private boolean isFirtChunk;
    private boolean isEndChunk;
    private final FileChannel lobCh;
    private final long chunkOffset;

    public static AtalantaLobChunk build(AtalantaRecord atalantaRecord, FileChannel lobCh) throws IOException {
        AtalantaLobChunk res = new AtalantaLobChunk(atalantaRecord, lobCh, atalantaRecord.getLobOffset() * CHUNK_BLOCK_SIZE);
        if (res.isFirtChunk) {
            if (res.mark == atalantaRecord.getLobId()) {
                return res;
            } else {
                LOGGER.error("Wrong chunk mark. mark: {}, lobId in record: {}", res.mark, atalantaRecord.getLobId());
                throw new RuntimeException("Wrong chunk mark: chunk:" + res.mark + " record: " + atalantaRecord.getLobId());
            }
        } else {
            res = res.prevChunk();
            while ( !res.isFirtChunk ){
                res = res.prevChunk();
            }
            return res;
        }
    }

    public static boolean checkIsLobRecords(AtalantaRecord r, FileChannel lobCh) throws IOException {
        if (r.getLobInfo() != 1) {
            return false;
        }
        long calculatedMsgSize = KEY_RECORD_OFFSET + r.getKeySize() + r.getCfSize() + LOB_INFO_SIZE;
        if (r.getMsgSize() != calculatedMsgSize) {
            LOGGER.trace("It is not LOB record size: calculated: {}, actual: {}", calculatedMsgSize, r.getMsgSize());
            return false;
        }
        long offset = r.getLobOffset() * CHUNK_BLOCK_SIZE;
        if (offset > lobCh.size()) {
            LOGGER.warn("LOB offset too big : offset: {}, file size: {}", offset, lobCh.size());
            return false;
        }
        ByteBuffer buff = ByteBuffer.allocate((int) r.getKeySize());
        lobCh.position(offset + HEADER_SIZE + KEY_RECORD_OFFSET);
        lobCh.read(buff);
        if (BytesComparator.compareTo(r.getKey(), buff.array()) != 0) {
            LOGGER.warn("LOB msg compare key error : keySize: {}, record key: {}, lob key: {} ", r.getKeySize(),
                    r.getKey(), buff.array());
            return false;
        }

        return true;
    }

    private AtalantaLobChunk(AtalantaRecord atalantaRecord, FileChannel lobCh, long offset) throws IOException {
        this.atalantaRecord = atalantaRecord;
        this.lobCh = lobCh;
        chunkOffset = offset;
        init(offset);
    }

    private ByteBuffer init(long offset) throws IOException {
        lobCh.position(offset);
        curChunk = ByteBuffer.allocate(CHUNK_BLOCK_SIZE);
        lobCh.read(curChunk);
        curChunk.flip();
        crcHeaderFromLob = curChunk.order(java.nio.ByteOrder.LITTLE_ENDIAN).getShort();
        crcRecordFromLob = curChunk.order(java.nio.ByteOrder.LITTLE_ENDIAN).getShort();
        mark = curChunk.order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
        prevChunkOffset = curChunk.order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
        nextChunkOffset = curChunk.order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
        crcHeaderCalc = Crc16.calc(curChunk, CRC_HEADER_START_POS, CRC_HEADER_SIZE);
        crcRecordCalc = Crc16.calc(curChunk, HEADER_SIZE, CHUNK_BLOCK_SIZE - HEADER_SIZE);
        isCrcHeaderOk = (crcHeaderFromLob & 0xFFFF) == 0xFFFF ? null : crcHeaderCalc == crcHeaderFromLob;
        isCrcRecordOk = (crcRecordFromLob & 0xFFFF) == 0xFFFF ? null : crcRecordCalc == crcRecordFromLob;
        isFirtChunk = prevChunkOffset == 0;
        isEndChunk = nextChunkOffset == 0;
        return curChunk;
    }

    public AtalantaLobChunk nextChunk() throws IOException {
        if (isEndChunk) {
            return null;
        } else {
            return new AtalantaLobChunk(atalantaRecord, lobCh, nextChunkOffset);
        }
    }

    public AtalantaLobChunk prevChunk() throws IOException {
        if (isFirtChunk) {
            return null;
        } else {
            return new AtalantaLobChunk(atalantaRecord, lobCh, prevChunkOffset);
        }
    }

    public int getCrcRecordFromLob() {
        return crcRecordFromLob;
    }

    public long getMark() {
        return mark;
    }

    public long getPrevChunkOffset() {
        return prevChunkOffset;
    }

    public long getNextChunkOffset() {
        return nextChunkOffset;
    }

    public Boolean getCrcHeaderOk() {
        return isCrcHeaderOk;
    }

    public Boolean getCrcRecordOk() {
        return isCrcRecordOk;
    }

    public int getCrcHeaderCalc() {
        return crcHeaderCalc;
    }

    public int getCrcRecordCalc() {
        return crcRecordCalc;
    }

    public ByteBuffer getCurChunk() {
        return curChunk;
    }

    public AtalantaRecord getAtalantaRecord() {
        return atalantaRecord;
    }

    public boolean isFirtChunk() {
        return isFirtChunk;
    }

    public boolean isEndChunk() {
        return isEndChunk;
    }

    public long getChunkOffset() {
        return chunkOffset;
    }

    public static boolean isLobNotEmpty(String srcLobFilePath) throws Exception {
        return Files.exists(Paths.get(srcLobFilePath)) ?
                (new File(srcLobFilePath)).length() > CHUNK_BLOCK_SIZE : false;
    }
}
