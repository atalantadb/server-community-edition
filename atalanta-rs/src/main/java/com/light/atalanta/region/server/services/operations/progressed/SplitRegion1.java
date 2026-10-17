/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.operations.progressed;

import com.light.atalanta.Pair;
import com.light.atalanta.Preconditions;
import com.light.atalanta.TableName;
import com.light.atalanta.model.CompressType;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.region.server.data.files.*;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.region.server.services.ReplicaMapperProcessor;
import com.light.atalanta.region.server.services.grpc.RegionTaskFunctionalized;
import com.light.atalanta.region.server.services.operations.RegionOperation;
import com.light.atalanta.util.Bytes;
import com.light.atalanta.util.BytesComparator;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SplitRegion1 implements RegionOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitRegion1.class.getName());

    public static final String SPLITTED_DELIMITER = "-";

    public static final String DATA_EXTENSION = ".data";
    public static final String BKP_POSTFIX = "_bkp.data";
    public static final String HIGH_POSTFIX = "_high.data";
    public static final String LOW_POSTFIX = "_low.data";
    public static final String LOB_FILE_NAME = "lob.data";
    public static final String LOB_LOW_FILE_NAME = "low_lob.data";
    public static final String LOB_HIGH_FILE_NAME = "high_lob.data";
    public static final String LOB_BKP_FILE_NAME = "bkp_lob.data";

    private final ReplicaMapperProcessor replicaMapperProcessor;
    private final DisableRegion disableRegion;
    private final FileSystem fileSystem;
    private final int tryToCompressLevel;

    public SplitRegion1(ReplicaMapperProcessor replicaMapperProcessor, DisableRegion disableRegion, FileSystem fileSystem, int tryToCompressLevel) {
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.disableRegion = disableRegion;
        this.fileSystem = fileSystem;
        this.tryToCompressLevel = tryToCompressLevel;
    }

    @Override
    public void progressedAction(TableName tableName, RegionModel replicaToSplit, AtomicReference<String> taskProgress, AtomicBoolean interrupted, String splittedBase64) throws Exception {
        LOGGER.info("Start splitting1 region '{}:{}'", tableName, replicaToSplit.toShortString());

        updateTaskProgress(taskProgress, tableName, replicaToSplit, "disabling splitting region");
        disableRegion.progressedAction(tableName, replicaToSplit, new AtomicReference<>(), interrupted, StringUtils.EMPTY);

        RegionModel replicaSplitted = RegionModel.fromBase64String(splittedBase64);
        String targetSplittedId = getTargetSplittedName(replicaSplitted) + "_" + replicaSplitted.getReplicaNum();

        // data/ns/tbl/rs1/splittingRegion_0
        String commonPathToLow = null;
        // data/ns/tbl/rs1/splittedRegion_0
        String commonPathToHigh = null;

        final String srcRegionDir = replicaMapperProcessor.getReplicaDir(replicaToSplit);
        final String srcTableDir = srcRegionDir.substring(0, srcRegionDir.lastIndexOf("/"));
        String srcLobFilePath = srcRegionDir + "/" + LOB_FILE_NAME;
        String srcLobBkpFilePath = srcRegionDir + "/" + LOB_BKP_FILE_NAME;
        String pathToLowLob = srcRegionDir + "/" + LOB_LOW_FILE_NAME;
        String pathToHighLob = srcRegionDir + "/" + LOB_HIGH_FILE_NAME;
        boolean isLobDataPresent = AtalantaLobChunk.isLobNotEmpty(srcLobFilePath);
        List<Pair<String, String>> lowFiles = new ArrayList<>();
        List<Pair<String, String>> highFiles = new ArrayList<>();
        List<Pair<String, String>> srcFiles = new ArrayList<>();
        if (isLobDataPresent && fileSystem.isPathExist(srcLobBkpFilePath)) {
            renameBkpToOriginal(taskProgress, tableName, replicaToSplit, srcLobBkpFilePath, srcLobFilePath);
        }

        try (AtalantaLobFileCopier lowLobCopier = new AtalantaLobFileCopier(srcLobFilePath, pathToLowLob);
             AtalantaLobFileCopier highLobCopier = new AtalantaLobFileCopier(srcLobFilePath, pathToHighLob)) {
            // data/ns/tbl/rs1/splittingRegion_0/000.data: no retry
            // data/ns/tbl/rs1/splittingRegion_0/000_bkp.data: retry
            for (String srcFilePath : replicaMapperProcessor.getReplicaFiles(replicaToSplit)) {
                // 000.data: no retry
                // 000_bkp.data: retry
                String srcFileName = srcFilePath.substring(srcFilePath.lastIndexOf("/") + 1);

                // data/ns/tbl/rs1/splittingRegion_0
                String pathToLow = srcFilePath.replace("/" + srcFileName, StringUtils.EMPTY);
                // data/ns/tbl/rs1/splittedRegion_0
                String pathToHigh = pathToLow.substring(0, pathToLow.lastIndexOf("/"));
                pathToHigh = pathToHigh + "/" + targetSplittedId;

                // data/ns/tbl/rs1/splittingRegion_0/000_low.data
                String lowFilePath = pathToLow + "/" + srcFileName
                        .replace(BKP_POSTFIX, DATA_EXTENSION)
                        .replace(DATA_EXTENSION, LOW_POSTFIX);
                // data/ns/tbl/rs1/splittedRegion_0/000_high.data
                String highFilePath = pathToHigh + "/" + srcFileName
                        .replace(BKP_POSTFIX, DATA_EXTENSION)
                        .replace(DATA_EXTENSION, HIGH_POSTFIX);

                // data/ns/tbl/rs1/splittingRegion_0/000.data
                String targetLowFilePath = lowFilePath.replace(LOW_POSTFIX, DATA_EXTENSION);
                // data/ns/tbl/rs1/splittedRegion_0/000.data
                String targetHighFilePath = highFilePath.replace(HIGH_POSTFIX, DATA_EXTENSION);

                // If there is a backup:
                // - delete the other files
                // - restore the file from the backup
                // - continue
                if (srcFileName.endsWith(BKP_POSTFIX)) {
                    updateTaskProgress(taskProgress, tableName, replicaToSplit, "bkp file for: " + srcFileName + " already exists, cleaning other artifacts");
                    fileSystem.makeSureFileRemoved(targetLowFilePath);
                    fileSystem.makeSureFileRemoved(targetHighFilePath);
                    fileSystem.makeSureFileRemoved(highFilePath);
                    fileSystem.makeSureFileRemoved(lowFilePath);
                    renameBkpToOriginal(taskProgress, tableName, replicaToSplit, srcFilePath, srcFilePath.replace(BKP_POSTFIX, DATA_EXTENSION));
                    srcFilePath = srcFilePath.replace(BKP_POSTFIX, DATA_EXTENSION);
                    srcFileName = srcFileName.replace(BKP_POSTFIX, DATA_EXTENSION);
                }

                if (commonPathToHigh == null) {
                    commonPathToHigh = pathToHigh;
                    // Create the folder for high
                    fileSystem.makeSureDirExists(commonPathToHigh);
                    replicaToSplit.writeRegionInfoFile(commonPathToHigh);
                }
                if (commonPathToLow == null) {
                    commonPathToLow = pathToLow;
                    // The folder for low already exists
                }

                Preconditions.checkArgument(commonPathToHigh.equals(pathToHigh), "found unexpected splitted path");
                Preconditions.checkArgument(commonPathToLow.equals(pathToLow), "found unexpected splitting path");

                // data/ns/tbl/rs1/splittingRegion_0/000_bkp.data
                String bkpFilePath = srcFilePath.replace(DATA_EXTENSION, BKP_POSTFIX);

                fileSystem.makeSureFileRemoved(lowFilePath);
                fileSystem.makeSureFileRemoved(highFilePath);
                Preconditions.checkArgument(!new File(lowFilePath).exists(), "low file already exists for " + srcFileName);
                Preconditions.checkArgument(!new File(highFilePath).exists(), "high file already exists for " + splittedBase64);

                if (replicaToSplit.getCompressType() == CompressType.none) {
                    splitFile(tableName, replicaToSplit, replicaSplitted, taskProgress, srcFileName, srcFilePath,
                            lowFilePath, highFilePath, lowLobCopier, highLobCopier);
                } else if (replicaToSplit.getCompressType() == CompressType.snappy || replicaToSplit.getCompressType() == CompressType.lz) {
                    splitCompressFile(tableName, replicaToSplit, replicaSplitted, taskProgress, srcFileName, srcFilePath,
                            lowFilePath, highFilePath, lowLobCopier, highLobCopier, srcLobFilePath);
                } else {
                    throw new NotImplementedException("Unknown compress type: " + replicaToSplit.getCompressType().name());
                }

                // TODO: Validate the split file sizes
                printFileSizes(srcFilePath, lowFilePath, highFilePath, tableName, replicaToSplit);

                if (isLobDataPresent) { // If there is at least one lob record, update the src region files after all other files
                    srcFiles.add(new Pair<>(srcFilePath, bkpFilePath));
                    lowFiles.add(new Pair<>(lowFilePath, targetLowFilePath));
                    highFiles.add(new Pair<>(highFilePath, targetHighFilePath));
                } else {
                    renameOriginalToBkp(taskProgress, tableName, replicaToSplit, srcFilePath, bkpFilePath);
                    renameLowToTarget(taskProgress, tableName, replicaToSplit, lowFilePath, targetLowFilePath);
                    renameHighToTarget(taskProgress, tableName, replicaToSplit, highFilePath, targetHighFilePath);
                    replicaMapperProcessor.upsertNewReplica(replicaToSplit.getName() + "-" + targetSplittedId, pathToHigh, tableName);
                }
            }
        }
        if (isLobDataPresent) {
            srcFiles.forEach(p -> renameOriginalToBkp(taskProgress, tableName, replicaToSplit, p.getFirst(), p.getSecond()));
            highFiles.forEach(p -> renameLowToTarget(taskProgress, tableName, replicaToSplit, p.getFirst(), p.getSecond()));
            lowFiles.forEach(p -> renameHighToTarget(taskProgress, tableName, replicaToSplit, p.getFirst(), p.getSecond()));
            String pathToHigh = srcTableDir + "/" + targetSplittedId;
            replicaMapperProcessor.upsertNewReplica(replicaToSplit.getName() + "-" + targetSplittedId, pathToHigh, tableName);
            String bkpLobFilePath = srcLobFilePath.replace(LOB_FILE_NAME, LOB_BKP_FILE_NAME);
            renameOriginalLowToBkp(taskProgress, tableName, replicaToSplit, srcLobFilePath, bkpLobFilePath);
            String targetLowFilePath = pathToLowLob.replace(LOB_LOW_FILE_NAME, LOB_FILE_NAME);
            if (AtalantaLobChunk.isLobNotEmpty(pathToLowLob)) {
                renameLowLobToTarget(taskProgress, tableName, replicaToSplit, pathToLowLob, targetLowFilePath);
            } else {
                Files.delete(Paths.get(pathToLowLob));
            }
            String targetHighFilePath = srcTableDir + "/" + targetSplittedId + "/" + LOB_FILE_NAME;
            if (AtalantaLobChunk.isLobNotEmpty(pathToHighLob)) {
                renameHighLobToTarget(taskProgress, tableName, replicaToSplit, pathToHighLob, targetHighFilePath);
            } else {
                Files.delete(Paths.get(pathToHighLob));
            }
        }
        LOGGER.info("Done splitting1 region '{}:{}'", tableName, replicaToSplit.toShortString());

    }


    private void splitFile(TableName tableName, RegionModel replicaToSplit, RegionModel replicaSplitted,
                           AtomicReference<String> taskProgress, String srcFileName, String srcFilePath,
                           String lowFilePath, String highFilePath, AtalantaLobFileCopier lowLobCopier,
                           AtalantaLobFileCopier highLobCopier) throws Exception {
        updateTaskProgress(taskProgress, tableName, replicaToSplit, "splitting file: " + srcFileName);

        LOGGER.trace("Splitting1 region '{}:{}': low region borders are [{}, {}]",
                tableName, replicaToSplit.toShortString(), Bytes.toHex(replicaToSplit.getBeginKey()), Bytes.toHex(replicaToSplit.getEndKey()));
        LOGGER.trace("Splitting1 region '{}:{}': high region borders are [{}, {}]",
                tableName, replicaToSplit.toShortString(), Bytes.toHex(replicaSplitted.getBeginKey()), Bytes.toHex(replicaSplitted.getEndKey()));

        final int fileIndex = getFileIndex(srcFileName);
        final long blockSize = calculateBlockSize(fileIndex);
        File dataFile = new File(srcFilePath);
        long dataFileSize = dataFile.length();
        // The meta may be smaller than the block size
        long metaSize = Math.min(dataFileSize, blockSize);
        long offset = metaSize;

        LOGGER.info("Splitting1 region '{}:{}': block size: {}, meta size: {}, file size: {}, file name: {}", tableName, replicaToSplit.toShortString(), blockSize, metaSize, dataFileSize, srcFileName);

        long lowBytesWrote = 0;
        long highBytesWrote = 0;
        long totalKeyCounter = 0;
        long lowKeyCounter = 0;
        long highKeyCounter = 0;

        // The core index addresses blocks: keep states of one full key in different blocks.
        // Each set owns only the keys of its current output block.
        Set<ByteBuffer> lowBlockKeys = new HashSet<>();
        Set<ByteBuffer> highBlockKeys = new HashSet<>();

        try (BufferedOutputStream lowFile = new BufferedOutputStream(Files.newOutputStream(Paths.get(lowFilePath)))) {
            try (BufferedOutputStream highFile = new BufferedOutputStream(Files.newOutputStream(Paths.get(highFilePath)))) {
                try (RandomAccessFile sourceFile = new RandomAccessFile(dataFile, "r")) {
                    // Write the meta to both files if the source file had it
                    if (dataFileSize > 0) {
                        Preconditions.checkArgument(dataFileSize >= offset, "unexpected data file size, offset = " + offset + ", size = " + dataFileSize);
                        if (dataFileSize == offset) {
                            LOGGER.trace("Splitting1 region '{}:{}': source file contains meta only", tableName, replicaToSplit.toShortString());
                        }
                        byte[] metadata = readFileBytes(sourceFile, 0, (int) offset);
                        lowFile.write(metadata);
                        highFile.write(metadata);
                        lowBytesWrote += metadata.length;
                        highBytesWrote += metadata.length;
                        // The meta may be smaller than the block size, so adjust the offset
                        offset = blockSize;
                    } else {
                        LOGGER.trace("Splitting1 region '{}:{}': source file is empty", tableName, replicaToSplit.toShortString());
                    }
                    while (dataFileSize > 0 && offset < dataFileSize) {
                        LOGGER.trace("Splitting1 region '{}:{}': current source offset is {}", tableName, replicaToSplit.toShortString(), offset);
                        // The first 4 bytes of a block: size of the whole record
                        byte[] msgSizeBytes = readFileBytes(sourceFile, offset, 4);
                        if (msgSizeBytes.length == 0) {
                            LOGGER.warn("Splitting1 region '{}:{}': array size is zero. offset is {}", tableName, replicaToSplit.toShortString(), offset);
                        }

                        // On zeros, move on to the next block, blockSize bytes ahead
                        if (consistOfZeroes(msgSizeBytes)) {
                            msgSizeBytes = readFileBytes(sourceFile, offset, (int) blockSize - 4);
                            if (consistOfZeroes(msgSizeBytes)) {
                                offset = offset + (blockSize - offset % blockSize);
                                continue;
                            }
                            Preconditions.checkArgument(offset % blockSize != 0, "unexpected zeroes in data file, offset = " + offset);
                            offset = offset + (blockSize - offset % blockSize);
                            if (offset > dataFileSize) {
                                LOGGER.trace("Splitting1 region '{}:{}': shifting offset to the next block, but found end of file. shifted offset is: {}", tableName, replicaToSplit.toShortString(), offset);
                                break;
                            }
                            LOGGER.trace("Splitting1 region '{}:{}': shifting offset to the next block. new value is {}", tableName, replicaToSplit.toShortString(), offset);
                            // Re-read the first 4 bytes of the block at the adjusted offset
                            msgSizeBytes = readFileBytes(sourceFile, offset, 4);
                        }

                        int msgSize = bytesToIntLittleEndian(msgSizeBytes);
                        if (msgSize == -1 && dataFileSize - offset >= 16) {
                            byte[] markerTail = new byte[12];
                            sourceFile.seek(offset + 4);
                            sourceFile.readFully(markerTail);
                            if (consistOfZeroes(markerTail)) {
                                truncateAfterEndMarker(srcFilePath, offset);
                                break;
                            }
                        }
                        if (msgSize == 0) {
                            LOGGER.trace("Splitting1 region '{}:{}': msgSize is zero, assuming end of file. offset is: {}", tableName, replicaToSplit.toShortString(), offset);
                            break;
                        }
                        // Record size alignment
                        int roundedMsgSize = roundMessageSizeUp(fileIndex, msgSize);

                        // Bytes 17 to 20: key size
                        byte[] keySizeBytes = readFileBytes(sourceFile, offset + 17 - 1, 4);
                        int keySize = bytesToIntLittleEndian(keySizeBytes);

                        // The key starts at byte 29
                        byte[] keyBytes = readFileBytes(sourceFile, offset + 29 - 1, keySize);
                        String key = new String(keyBytes, StandardCharsets.UTF_8);
                        String keyHex = Bytes.toHex(keyBytes);
                        LOGGER.trace("Splitting1 region '{}:{}': found new key {}. string value is: {}. record size is: {}", tableName, replicaToSplit.toShortString(), keyHex, key, msgSize);
                        ++totalKeyCounter;

                        // Read the whole record now that its size is known
                        byte[] recordBytes = readFileBytes(sourceFile, offset, roundedMsgSize);
                        // keyBytes is a fresh array and is not modified; do not use the routing prefix
                        ByteBuffer recordKey = ByteBuffer.wrap(keyBytes);

                        // To compare the key with the region bounds,
                        // make their lengths equal by truncating the longer one on the right
                        byte[] processedKey = keyBytes;
                        byte[] lowBeginKey = replicaToSplit.getBeginKey();
                        byte[] lowEndKey = replicaToSplit.getEndKey();
                        byte[] highBeginKey = replicaSplitted.getBeginKey();
                        byte[] highEndKey = replicaSplitted.getEndKey();

                        if (keySize < lowBeginKey.length) {
                            lowBeginKey = Arrays.copyOf(lowBeginKey, keySize);
                            lowEndKey = Arrays.copyOf(lowEndKey, keySize);
                            highBeginKey = Arrays.copyOf(highBeginKey, keySize);
                            highEndKey = Arrays.copyOf(highEndKey, keySize);
                        } else if (keySize > lowBeginKey.length) {
                            processedKey = Arrays.copyOf(keyBytes, lowBeginKey.length);
                        }

                        // Find which file the current key goes to
                        int lowBeginRes = BytesComparator.compareTo(processedKey, lowBeginKey);
                        int lowEndRes = BytesComparator.compareTo(processedKey, lowEndKey);
                        int highBeginRes = BytesComparator.compareTo(processedKey, highBeginKey);
                        int highEndRes = BytesComparator.compareTo(processedKey, highEndKey);

                        String membership = String.format("%s, %s, %s, %s", lowBeginRes, lowEndRes, highBeginRes, highEndRes);
                        String infoStr = String.format("offset='%d', key_base64='%s', key_hex='%s', low=[%s:%s], high=[%s:%s], membership=[%s]",
                                offset,
                                Base64.getEncoder().encodeToString(processedKey), keyHex,
                                Base64.getEncoder().encodeToString(lowBeginKey),
                                Base64.getEncoder().encodeToString(lowEndKey),
                                Base64.getEncoder().encodeToString(highBeginKey),
                                Base64.getEncoder().encodeToString(highEndKey), membership
                        );
                        if ((lowBeginRes >= 0) && (lowEndRes < 0)) {
                            // Write the record to the first file
                            LOGGER.trace("Splitting1 region '{}:{}': key is going to LOW file. info: {}", tableName, replicaToSplit.toShortString(), infoStr);
                            long currentBlockIdx = lowBytesWrote / blockSize;
                            long predictedBlock = (lowBytesWrote + recordBytes.length) / blockSize;
                            if (lowBytesWrote % blockSize == 0) {
                                lowBlockKeys.clear();
                            }
                            // Also start a new block on a repeated key, keeping both records
                            if (currentBlockIdx != predictedBlock || lowBlockKeys.contains(recordKey)) {
                                long zeroesCount = blockSize - (lowBytesWrote % blockSize);
                                LOGGER.trace("Splitting1 region '{}:{}': starting a new LOW block. adding {} zeroes", tableName, replicaToSplit.toShortString(), zeroesCount);
                                byte[] zeroes = new byte[(int) zeroesCount];
                                lowFile.write(zeroes);
                                lowBytesWrote += zeroesCount;
                                lowBlockKeys.clear();
                            }
                            recordBytes = lowLobCopier.copyIfNeededWithMsgSize(msgSize, recordBytes);
                            lowFile.write(recordBytes);
                            lowBytesWrote += recordBytes.length;
                            lowBlockKeys.add(recordKey);
                            ++lowKeyCounter;
                            LOGGER.trace("Splitting1 region '{}:{}': current low offset is {}", tableName, replicaToSplit.toShortString(), lowBytesWrote);
                        } else if ((highBeginRes >= 0) && (highEndRes <= 0)) {
                            // Write the record to the second file
                            LOGGER.trace("Splitting1 region '{}:{}': key is going to HIGH file. info: {}", tableName, replicaToSplit.toShortString(), infoStr);
                            long currentBlockIdx = highBytesWrote / blockSize;
                            long predictedBlock = (highBytesWrote + recordBytes.length) / blockSize;
                            if (highBytesWrote % blockSize == 0) {
                                highBlockKeys.clear();
                            }
                            // Also start a new block on a repeated key, keeping both records
                            if (currentBlockIdx != predictedBlock || highBlockKeys.contains(recordKey)) {
                                long zeroesCount = blockSize - (highBytesWrote % blockSize);
                                LOGGER.trace("Splitting1 region '{}:{}': starting a new HIGH block. adding {} zeroes", tableName, replicaToSplit.toShortString(), zeroesCount);
                                byte[] zeroes = new byte[(int) zeroesCount];
                                highFile.write(zeroes);
                                highBytesWrote += zeroesCount;
                                highBlockKeys.clear();
                            }
                            recordBytes = highLobCopier.copyIfNeededWithMsgSize(msgSize, recordBytes);
                            highFile.write(recordBytes);
                            highBytesWrote += recordBytes.length;
                            highBlockKeys.add(recordKey);
                            ++highKeyCounter;
                            LOGGER.trace("Splitting1 region '{}:{}': current high offset is {}", tableName, replicaToSplit.toShortString(), highBytesWrote);
                        } else {
                            // Error: the key is outside the bounds of the low and end regions
                            throw new Exception("key is not lying inside low or high region borders. info: " + infoStr);
                        }

                        // Move the offset by the message size
                        offset += roundedMsgSize;

                        // Check for a zero message size
                        Preconditions.checkArgument(roundedMsgSize != 0, "roundedMsgSize is zero");
                    }
                    LOGGER.debug("Splitting1 region '{}:{}': low bytes wrote count is {}, high bytes wrote count is {}", tableName, replicaToSplit.toShortString(), lowBytesWrote, highBytesWrote);
                    updateTaskProgress(taskProgress, tableName, replicaToSplit, "done splitting file: " + srcFileName);
                } catch (Exception ex) {
                    throw new Exception("error splitting file: " + srcFileName, ex);
                }
            }
        }

        LOGGER.info("Splitting completed for file '{}'. region '{}:{}', " +
                        "total key: {}, key to low: {}, ket to high: {}", tableName, tableName, replicaToSplit.toShortString(),
                totalKeyCounter, lowKeyCounter, highKeyCounter);

    }

    private void splitCompressFile(TableName tableName, RegionModel replicaToSplit, RegionModel replicaSplitted,
                                   AtomicReference<String> taskProgress, String srcFileName, String srcFilePath,
                                   String lowFilePath, String highFilePath, AtalantaLobFileCopier lowLobCopier,
                                   AtalantaLobFileCopier highLobCopier, String srcLobFilePath) throws Exception {
        updateTaskProgress(taskProgress, tableName, replicaToSplit, "splitting compressed file: " + srcFileName);

        LOGGER.trace("Splitting1 compressed region '{}:{}': low region borders are [{}, {}]",
                tableName, replicaToSplit.toShortString(), Bytes.toHex(replicaToSplit.getBeginKey()), Bytes.toHex(replicaToSplit.getEndKey()));
        LOGGER.trace("Splitting1 compressed  region '{}:{}': high region borders are [{}, {}]",
                tableName, replicaToSplit.toShortString(), Bytes.toHex(replicaSplitted.getBeginKey()), Bytes.toHex(replicaSplitted.getEndKey()));

        long lowBytesWrote = 0;
        long highBytesWrote = 0;
        long totalKeyCounter = 0;
        long lowKeyCounter = 0;
        long highKeyCounter = 0;

        int recordNbr = 0;
        try (AtalantaReader atalantaReader = AtalantaLobChunk.isLobNotEmpty(srcLobFilePath) ?
                new AtalantaReader(srcFilePath, srcLobFilePath, replicaToSplit.getCompressType())
                : new AtalantaReader(srcFilePath, replicaToSplit.getCompressType());
             AtalantaCompressFileWriter lowFile = new AtalantaCompressFileWriter(lowFilePath, atalantaReader.getHeader(),
                     tryToCompressLevel, replicaToSplit.getCompressType(),  AtalantaLobChunk.isLobNotEmpty(srcLobFilePath) , srcLobFilePath,
                     lowLobCopier.pathToTargetLob, replicaToSplit.getRecordCompressionType());
             AtalantaCompressFileWriter highFile = new AtalantaCompressFileWriter(highFilePath, atalantaReader.getHeader(),
                     tryToCompressLevel, replicaToSplit.getCompressType(),  AtalantaLobChunk.isLobNotEmpty(srcLobFilePath) , srcLobFilePath,
                     highLobCopier.pathToTargetLob, replicaToSplit.getRecordCompressionType())) {

            LOGGER.info("Splitting1 compressed region '{}:{}': block size:{}, file size: {}, file name: {}",
                    tableName, replicaToSplit.toShortString(), atalantaReader.getHeader().getMeta().limit(),
                    atalantaReader.getFileSize(), srcFileName);


            for (AtalantaRecord record = atalantaReader.next(); record != null; record = atalantaReader.next()) {
                recordNbr++;
                // To compare the key with the region bounds,
                // make their lengths equal by truncating the longer one on the right
                byte[] processedKey = record.getKey();
                byte[] lowBeginKey = replicaToSplit.getBeginKey();
                byte[] lowEndKey = replicaToSplit.getEndKey();
                byte[] highBeginKey = replicaSplitted.getBeginKey();
                byte[] highEndKey = replicaSplitted.getEndKey();

                int keySize = processedKey.length;
                if (keySize < lowBeginKey.length) {
                    lowBeginKey = Arrays.copyOf(lowBeginKey, keySize);
                    lowEndKey = Arrays.copyOf(lowEndKey, keySize);
                    highBeginKey = Arrays.copyOf(highBeginKey, keySize);
                    highEndKey = Arrays.copyOf(highEndKey, keySize);
                } else if (keySize > lowBeginKey.length) {
                    processedKey = Arrays.copyOf(processedKey, lowBeginKey.length);
                }

                // Find which file the current key goes to
                int lowBeginRes = BytesComparator.compareTo(processedKey, lowBeginKey);
                int lowEndRes = BytesComparator.compareTo(processedKey, lowEndKey);
                int highBeginRes = BytesComparator.compareTo(processedKey, highBeginKey);
                int highEndRes = BytesComparator.compareTo(processedKey, highEndKey);

                String membership = String.format("%s, %s, %s, %s", lowBeginRes, lowEndRes, highBeginRes, highEndRes);
                String infoStr = String.format("recordNbr='%d', key_base64='%s', key_hex='%s', low=[%s:%s], high=[%s:%s], membership=[%s]",
                        recordNbr,
                        Base64.getEncoder().encodeToString(processedKey), Bytes.toHex(processedKey),
                        Base64.getEncoder().encodeToString(lowBeginKey),
                        Base64.getEncoder().encodeToString(lowEndKey),
                        Base64.getEncoder().encodeToString(highBeginKey),
                        Base64.getEncoder().encodeToString(highEndKey), membership
                );
                if ((lowBeginRes >= 0) && (lowEndRes < 0)) {
                    // Write the record to the first file
                    LOGGER.trace("Splitting1 region '{}:{}': key is going to LOW file. info: {}", tableName, replicaToSplit.toShortString(), infoStr);
                    lowFile.write(record);
                    LOGGER.trace("Splitting1 region '{}:{}': current low offset is {}", tableName, replicaToSplit.toShortString(), lowBytesWrote);
                } else if ((highBeginRes >= 0) && (highEndRes <= 0)) {
                    // Write the record to the second file
                    LOGGER.trace("Splitting1 region '{}:{}': key is going to HIGH file. info: {}", tableName, replicaToSplit.toShortString(), infoStr);
                    highFile.write(record);
                    LOGGER.trace("Splitting1 region '{}:{}': current high offset is {}", tableName, replicaToSplit.toShortString(), highBytesWrote);
                } else {
                    // Error: the key is outside the bounds of the low and end regions
                    throw new Exception("key is not lying inside low or high region borders. info: " + infoStr);
                }
            }
            if (atalantaReader.getEndMarkerOffset() >= 0) {
                truncateAfterEndMarker(srcFilePath, atalantaReader.getEndMarkerOffset());
            }
            LOGGER.debug("Splitting1 region '{}:{}': low bytes wrote count is {}, high bytes wrote count is {}", tableName, replicaToSplit.toShortString(), lowBytesWrote, highBytesWrote);
            updateTaskProgress(taskProgress, tableName, replicaToSplit, "done splitting file: " + srcFileName);
        } catch (Exception ex) {
            throw new Exception("error splitting file on record " + recordNbr + " in " + srcFileName, ex);
        }

        LOGGER.info("Splitting completed for file '{}'. region '{}:{}', " +
                        "total key: {}, key to low: {}, ket to high: {}", tableName, tableName, replicaToSplit.toShortString(),
                totalKeyCounter, lowKeyCounter, highKeyCounter);

    }

    /**
     * 000.data -> block = meta = 4096 bytes
     * 001.data -> block = meta = 8192 bytes
     * 002.data -> block = meta = 12288 bytes
     */
    private long calculateBlockSize(long fileIndex) {
        return (fileIndex + 1) * 4 * 1024;
    }

    /**
     * Record size alignment:
     * 000.data -> the size must be a multiple of 16
     * 001.data -> the size must be a multiple of 32
     * 002.data -> the size must be a multiple of 48
     */
    private int roundMessageSizeUp(int fileIndex, int number) {
        int roundFactor = 16 * (fileIndex + 1);
        if (number % roundFactor == 0) {
            return number;
        }
        return number + (roundFactor - number % roundFactor);
    }

    /**
     * 000.data -> 0
     * 000_bkp.data -> 0
     * 001.data -> 1
     * 001_bkp.data -> 1
     */
    private int getFileIndex(String fileName) {
        return Integer.parseInt(
                fileName
                        .replace(BKP_POSTFIX, StringUtils.EMPTY)
                        .replace(DATA_EXTENSION, StringUtils.EMPTY)
        );
    }

    private void truncateAfterEndMarker(String srcFilePath, long markerOffset) throws Exception {
        // Write the end-of-data marker 0xFFFFFFFF
        long endOffset = markerOffset + 16;
        try (RandomAccessFile sourceFile = new RandomAccessFile(srcFilePath, "rw")) {
            long originalSize = sourceFile.length();
            sourceFile.setLength(endOffset);
            LOGGER.info("Splitting1 file '{}': end marker at offset {}, file truncated from {} to {} bytes",
                    srcFilePath, markerOffset, originalSize, endOffset);
        }
    }

    private boolean consistOfZeroes(byte[] array) {
        for (byte b : array) {
            if (b != (byte) 0) {
                return false;
            }
        }
        return true;
    }

    private int bytesToIntLittleEndian(byte[] bytes) {
        return java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private byte[] readFileBytes(RandomAccessFile randomAccessFile, long offset, int length) throws Exception {
        byte[] result = new byte[length];
        randomAccessFile.seek(offset);
        randomAccessFile.read(result, 0, length);
        return result;
    }

    private void renameOriginalToBkp(AtomicReference<String> taskProgress, TableName tableName, RegionModel replicaToSplit, String srcFilePath, String bkpFilePath) {
        updateTaskProgress(taskProgress, tableName, replicaToSplit, "renaming original file to bkp");
        boolean renameResult = fileSystem.renameFile(srcFilePath, bkpFilePath);
        Preconditions.checkArgument(renameResult, "unable to rename original file to bkp");
    }

    private void renameBkpToOriginal(AtomicReference<String> taskProgress, TableName tableName, RegionModel replicaToSplit, String bkpFilePath, String srcFilePath) {
        updateTaskProgress(taskProgress, tableName, replicaToSplit, "renaming bkp file to original");
        LOGGER.info("rename bkp {} to {}", bkpFilePath, srcFilePath);
        boolean renameResult = fileSystem.renameFile(bkpFilePath, srcFilePath);
        Preconditions.checkArgument(renameResult, "unable to rename bkp file to original");
    }

    private void renameLowToTarget(AtomicReference<String> taskProgress, TableName tableName, RegionModel replicaToSplit, String lowFilePath, String targetLowFilePath) {
        updateTaskProgress(taskProgress, tableName, replicaToSplit, "renaming low file to original");
        boolean renameResult = fileSystem.renameFile(lowFilePath, targetLowFilePath);
        Preconditions.checkArgument(renameResult, "unable to rename low file to original");
    }

    private void renameHighToTarget(AtomicReference<String> taskProgress, TableName tableName, RegionModel replicaToSplit, String highFilePath, String targetHighFilePath) {
        updateTaskProgress(taskProgress, tableName, replicaToSplit, "renaming high file to original");
        boolean renameResult = fileSystem.renameFile(highFilePath, targetHighFilePath);
        Preconditions.checkArgument(renameResult, "unable to rename high file to original");
    }

    private void renameOriginalLowToBkp(AtomicReference<String> taskProgress, TableName tableName, RegionModel replicaToSplit, String srcLobFilePath, String bkpLobFilePath) {
        updateTaskProgress(taskProgress, tableName, replicaToSplit, "renaming lob file to backup");
        boolean renameResult = fileSystem.renameFile(srcLobFilePath, bkpLobFilePath);
        Preconditions.checkArgument(renameResult, "unable to rename lob file to backup");
    }

    private void renameLowLobToTarget(AtomicReference<String> taskProgress, TableName tableName, RegionModel replicaToSplit, String srcLowLobFilePath, String targetLobFilePath) {
        updateTaskProgress(taskProgress, tableName, replicaToSplit, "renaming low lob file to backup");
        boolean renameResult = fileSystem.renameFile(srcLowLobFilePath, targetLobFilePath);
        Preconditions.checkArgument(renameResult, "unable to rename low lob file to backup");
    }

    private void renameHighLobToTarget(AtomicReference<String> taskProgress, TableName tableName, RegionModel replicaToSplit, String srcHighLobFilePath, String targetLobFilePath) {
        updateTaskProgress(taskProgress, tableName, replicaToSplit, "renaming high lob file to backup");
        boolean renameResult = fileSystem.renameFile(srcHighLobFilePath, targetLobFilePath);
        Preconditions.checkArgument(renameResult, "unable to rename high lob file to backup");
    }

    private void printFileSizes(String srcFilePath, String lowFilePath, String highFilePath, TableName tableName, RegionModel replicaToSplit) {
        long srcFileSize = new File(srcFilePath).length();
        long lowFileSize = new File(lowFilePath).length();
        long highFileSize = new File(highFilePath).length();
        String message = String.format("original: %s, low: %s, high: %s", srcFileSize, lowFileSize, highFileSize);
        LOGGER.info("Splitting1 region '{}:{}': file info for: {} is: {}", tableName, replicaToSplit.toShortString(), srcFilePath, message);
        // TODO: Check that srcFileSize == lowFileSize + highFileSize - additionalSize and,
        // if additionalSize > 0 (the 4 KB meta added to the second file), that the low file is 4 KB aligned
    }

    @Override
    public void functionalizedAction(TableName tableName, RegionModel regionModel, RegionTaskFunctionalized task, String reservedReplicas) throws Exception {
        throw new NotImplementedException("unable to run progressed task as functionalized");
    }

    private void updateTaskProgress(AtomicReference<String> taskProgress, TableName tableName, RegionModel regionModel, String text) {
        taskProgress.set(text);
        LOGGER.info("Splitting1 region '{}:{}': {}", tableName, regionModel.toShortString(), text);
    }

    // Returns the target name of a splitted copy based on the temporary one
    private String getTargetSplittedName(RegionModel splitted) {
        String tmpSplittedName = splitted.getName();
        com.google.common.base.Preconditions.checkArgument(tmpSplittedName.contains(SPLITTED_DELIMITER), "splitted region name must contain delimeter \"-\"");
        return tmpSplittedName.substring(tmpSplittedName.indexOf(SPLITTED_DELIMITER) + 1);
    }
}
