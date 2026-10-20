/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.data.files;

import com.light.atalanta.region.server.utils.Crc16;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import static com.light.atalanta.region.server.data.files.AtalantaLobChunk.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class AtalantaLobFileCopier implements AutoCloseable, Closeable {
    private final FileOutputStream fileOutputStream;
    private final FileChannel outLobCh;

    private FileInputStream fileInputStream;
    private final FileChannel inLobCh;
    public String pathToTargetLob;

    private final ByteBuffer buffer = ByteBuffer.allocate(CHUNK_BLOCK_SIZE - HEADER_SIZE);

    public AtalantaLobFileCopier(String pathToScrLob, String pathToTargetLob) throws IOException {
        try {
            this.pathToTargetLob = pathToTargetLob;
            fileInputStream = new FileInputStream(pathToScrLob);
        } catch (FileNotFoundException ex) { // If the source file doesn't exist, do nothing
            fileInputStream = null;
        }
        if (fileInputStream == null) {
            fileOutputStream = null;
            outLobCh = null;
            inLobCh = null;
        } else {
            inLobCh = fileInputStream.getChannel();
            fileOutputStream = new FileOutputStream(pathToTargetLob, false);
            outLobCh = fileOutputStream.getChannel();
            copyHeader();
        }
    }

    public boolean isSrcLobExist() {
        return fileInputStream != null;
    }

    private void copyHeader() throws IOException {
        ByteBuffer headerBuffer = ByteBuffer.allocate(CHUNK_BLOCK_SIZE);
        inLobCh.read(headerBuffer);
        headerBuffer.position(0);
        outLobCh.write(headerBuffer);
    }

    @Override
    public void close() throws IOException {
        if (fileOutputStream != null) {
            fileOutputStream.close();
            outLobCh.close();
        }
    }

    // Returns the offset of the first chunk
    public long write(AtalantaRecord atalantaRecord) throws IOException {
        long res = outLobCh.position();
        AtalantaLobChunk chunk = AtalantaLobChunk.build(atalantaRecord, inLobCh);
        writeNextChunk(chunk);
        return res / CHUNK_BLOCK_SIZE;
    }

    private void writeNextChunk(AtalantaLobChunk chunk) throws IOException {
        long startPos = outLobCh.position();
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE).order(LITTLE_ENDIAN);
        header.putShort((short) 0);                        // hdr_crc16: computed below
        header.putShort((short) chunk.getCrcRecordCalc()); // rec_crc16 (the payload doesn't change, so compute it now)
        header.putInt((int) chunk.getMark());
        header.putLong(chunk.isFirtChunk() ? 0 : startPos - CHUNK_BLOCK_SIZE);     // prev_chunk_offset
        header.putLong(chunk.isEndChunk() ? 0 : startPos + CHUNK_BLOCK_SIZE);     // next_chunk_offset
        int hdrCrc = Crc16.calc(header, CRC_HEADER_START_POS, CRC_HEADER_SIZE); // Actual value
        header.putShort(0, (short) hdrCrc);                // Write to the first 2 bytes
        header.flip();
        outLobCh.write(header);
        inLobCh.position(chunk.getChunkOffset() + HEADER_SIZE);
        buffer.clear();                                    // Buffer of CHUNK_BLOCK_SIZE - HEADER_SIZE bytes
        inLobCh.read(buffer);
        buffer.flip();
        outLobCh.write(buffer);
        if (!chunk.isEndChunk()) {
            writeNextChunk(chunk.nextChunk());
        }
    }

    public AtalantaRecord copyIfNeeded(int msgSize, byte[] recordBytes) throws IOException {
        if (outLobCh == null) {
            return null;
        }
        ByteBuffer data = ByteBuffer.allocate(recordBytes.length + MSG_SIZE_SIZE);
        data.order(LITTLE_ENDIAN).putInt(msgSize);
        data.put(recordBytes);
        AtalantaRecord r = new AtalantaRecord(msgSize, data, msgSize);
        if (checkIsLobRecord(r)) {
            long lobOffset = write(r);
            r.setLobOffset(lobOffset);
            return r;
        } else {
            return null;
        }

    }

    public boolean checkIsLobRecord(AtalantaRecord r) throws IOException {
        return AtalantaLobChunk.checkIsLobRecords(r, inLobCh);
    }

    public byte[] copyIfNeededWithMsgSize(int msgSize, byte[] recordBytes) throws IOException {
        if (outLobCh == null) {
            return recordBytes;
        }
        AtalantaRecord rec = copyIfNeeded(msgSize, Arrays.copyOfRange(recordBytes, MSG_SIZE_SIZE, recordBytes.length));
        if (rec != null) {
            System.arraycopy(rec.array(), 0, recordBytes, 0, rec.array().length);
        }
        return recordBytes;
    }


}
