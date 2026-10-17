/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.data.files;

import com.light.atalanta.region.server.utils.RsUtils;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class AtalantaDataFile implements AtalantaFileWriter, AutoCloseable, Closeable {

    public static final String DATA_FILE_015 = "015.data";
    private final FileOutputStream fileOutputStream;
    private final FileChannel outCh;
    private int alignSize;
    private final int blockReservSize;
    private final int blockSize;
    private ByteBuffer data;
    private boolean isRecordAdded;

    private final File file;

    public AtalantaDataFile(int blockSize, String targetPath, AtalantaMeta header) throws IOException {
        this.blockSize = blockSize;
        int fileIndex = getFileNameByBlockSize(blockSize);
        String[] fileNames = {"000.data", "001.data", "003.data", "007.data", DATA_FILE_015};
        String fileName = fileNames[fileIndex];
        if (fileIndex < 0 || fileIndex > 4) {
            throw new IllegalArgumentException("fileIndex must be in range [0, 4]: " + fileIndex);
        }
        blockReservSize = alignSize = 16 << fileIndex;
        file =  new File(targetPath + "/" + fileName);
        fileOutputStream = new FileOutputStream(file, false);
        outCh = fileOutputStream.getChannel();
        if (fileName.equals(DATA_FILE_015) && header.getMeta().limit() == blockSize) {
            header.writeMeta(outCh);
        } else {
            outCh.truncate(blockSize);
            outCh.position(blockSize);
        }
        data = ByteBuffer.allocate(blockSize);
    }

    private int getFileNameByBlockSize(int blockSize) {
        int i = 0;
        for (int size = 4096; size <= 65536; size *= 2) {
            if (blockSize <= size) {
                return i;
            }
            i++;
        }
        throw new RuntimeException("Block size is too big: " + blockSize);
    }

    @Override
    public void close() throws IOException {
        if (isRecordAdded) {
            writeNextBlock();
            outCh.close();
            fileOutputStream.close();
        } else {
            outCh.close();
            fileOutputStream.close();
            if ( !file.getName().equals("015.data") ) {
                file.delete();
            }
        }
    }

    @Override
    public void write(AtalantaRecord atalantaRecord) throws IOException {
        int size = RsUtils.align(atalantaRecord.getMsgSize(), alignSize);
        ByteBuffer record = ByteBuffer.allocate(size);
        record.put(atalantaRecord.array(), 0, Math.min(size, atalantaRecord.array().length));
        int sizeToSave = record.limit() + alignSize;
        if (sizeToSave > data.remaining()) {
            writeNextBlock();
        }
        record.position(0);
        data.put(record);
        isRecordAdded =true;
    }

    public boolean isMyRecord(AtalantaRecord atalantaRecord) {
        return atalantaRecord.getRecord().limit() + blockReservSize <= data.limit();
    }

    public int getAlignSize() {
        return alignSize;
    }

    private void writeNextBlock() throws IOException {
        data.position(0);
        outCh.write(data);
        data = ByteBuffer.allocate(blockSize);
        isRecordAdded = false;
    }
}
