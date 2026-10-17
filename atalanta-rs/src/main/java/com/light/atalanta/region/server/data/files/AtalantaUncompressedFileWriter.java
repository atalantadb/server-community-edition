/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.data.files;

import com.light.atalanta.model.RecordCompressionType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.light.atalanta.region.server.services.operations.progressed.SplitRegion1.LOB_FILE_NAME;

public class AtalantaUncompressedFileWriter implements AtalantaFileWriter {

    private final List<AtalantaDataFile> atalantaDataFileList = new ArrayList<>(5);
    private final AtalantaLobFileCopier lobFileCopier;
    private final RecordCompressionType recordCompressType;

    public AtalantaUncompressedFileWriter(String targetPath, AtalantaMeta header, Boolean isLobPresent, String srcLobFilePath,
                                     RecordCompressionType recordCompressType) throws IOException {
        this.recordCompressType = recordCompressType;
        for(int blockSize = 4096; blockSize <= 65536; blockSize *= 2) {
            atalantaDataFileList.add(new AtalantaDataFile(blockSize, targetPath, header));
        }
        if (isLobPresent) {
            lobFileCopier = new AtalantaLobFileCopier(srcLobFilePath, targetPath + "/" + LOB_FILE_NAME);
        } else {
            lobFileCopier = null;
        }
    }

    @Override
    public void write(AtalantaRecord atalantaRecord) throws IOException {
        if (atalantaRecord.isLobRecord()) {
            final long lobOffset = lobFileCopier.write(atalantaRecord);
            atalantaRecord.setLobOffset(lobOffset);
        }
        for(AtalantaDataFile atalantaDataFile : atalantaDataFileList) {
            if (recordCompressType == RecordCompressionType.lz) {
                atalantaRecord.compressValues(atalantaDataFile.getAlignSize());
            } else {
                atalantaRecord.uncompressValues(atalantaDataFile.getAlignSize());
            }
            if (atalantaDataFile.isMyRecord(atalantaRecord)) {
                atalantaDataFile.write(atalantaRecord);
                return;
            }
        }
        throw new RuntimeException("Record is too big!. Size: " + atalantaRecord.getRecord().limit());
    }

    @Override
    public void close() throws IOException {
        for(AtalantaDataFile atalantaDataFile : atalantaDataFileList) {
            atalantaDataFile.close();
        }
        if (lobFileCopier != null) {
            lobFileCopier.close();
        }
    }
}
