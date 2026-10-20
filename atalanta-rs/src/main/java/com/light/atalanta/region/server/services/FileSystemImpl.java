/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services;

import com.google.common.base.Preconditions;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import com.light.atalanta.util.ShutdownManager;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.light.atalanta.region.server.services.operations.progressed.MoveRegion.TEMP_REGION_BATCH_POSTFIX;

@SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
public class FileSystemImpl implements FileSystem {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemImpl.class);

    private final RegionServerConfiguration conf;

    public FileSystemImpl(RegionServerConfiguration conf) {
        this.conf = conf;
    }

    /**
     * Returns the names of the files and folders in the given directory.
     *
     * @param path directory to list
     */
    @Override
    public List<String> getDirFilesAndDirs(String path) throws IOException {
        List<String> elements;
        try (Stream<Path> pathStream = Files.list(Paths.get(path))) {
            elements = pathStream.map(Path::getFileName)
                    .map(Path::toString)
                    .collect(Collectors.toList());
        }
        return elements;
    }

    /**
     * Returns the files in the given directory.
     *
     * @param path directory to list
     */
    @Override
    public List<File> getDirFileDetails(String path) throws IOException {
        List<File> files;
        try (Stream<Path> pathStream = Files.list(Paths.get(path))) {
            files = pathStream.map(Path::toFile)
                    .filter(File::isFile)
                    .collect(Collectors.toList());
        }
        return files;
    }

    @Override
    @SuppressFBWarnings("RR_NOT_CHECKED")
    public byte[] readFileBytes(String filePath, long offset, long length) throws Exception {
        Preconditions.checkArgument(length < Integer.MAX_VALUE, "Region file batch length can't be greater " + Integer.MAX_VALUE);
        int intLength = (int) length;
        File file = new File(filePath);
        byte[] result = new byte[intLength];
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            randomAccessFile.seek(offset);
            randomAccessFile.read(result, 0, intLength);
        }
        return result;
    }

    /**
     * Deletes temporary replica folders. Their names end with _temp.
     */
    @Override
    public void removeTmp() {
        LOGGER.info("Start cleaning temporary files");
        try {
            for (String dataDir : conf.getRsDirs()) {
                File file = new File(dataDir);
                if (!file.exists()) {
                    LOGGER.error("Data directory doesn't exist: {}", file.getAbsolutePath());
                }
                for (String namespace : getDirFilesAndDirs(dataDir)) {
                    String namespaceDir = String.join("/", dataDir, namespace);
                    for (String table : getDirFilesAndDirs(namespaceDir)) {
                        String tableDir = String.join("/", namespaceDir, table);
                        for (String replica : getDirFilesAndDirs(tableDir)) {
                            String replicaDir = String.join("/", tableDir, replica);
                            if (replicaDir.endsWith(TEMP_REGION_BATCH_POSTFIX)) {
                                makeSureDirRemoved(replicaDir);
                            }
                        }
                    }
                }
            }
        } catch (Exception ex) {
            ShutdownManager.exitWithError("Unable to clean temporary files", ex);
        }
        LOGGER.info("Done cleaning temporary files");
    }

    @Override
    public boolean makeSureDirExists(String path) {
        return new File(path).mkdirs();
    }

    @Override
    public boolean renameDir(String source, String target) {
        return new File(source).renameTo(new File(target));
    }

    @Override
    public boolean renameFile(String source, String target) {
        return new File(source).renameTo(new File(target));
    }

    @Override
    public void writeFileBytes(String filePath, byte[] data) throws Exception {
        try (FileOutputStream output = new FileOutputStream(filePath, true)) {
            output.write(data);
        }
    }

    @Override
    public void makeSureDirRemoved(String path) throws IOException {
        FileUtils.deleteDirectory(new File(path));
    }

    @Override
    public void makeSureFileRemoved(String filePath) throws IOException {
        File file = new File(filePath);
        if (file.exists()) {
            FileUtils.delete(file);
        }
    }

    @Override
    public void copyFile(String src, String dst) throws IOException {
        Files.copy(Paths.get(src), Paths.get(dst), StandardCopyOption.COPY_ATTRIBUTES);
    }

    @Override
    public String calculateFileHash(String filePath) {
        // TODO: File hashing is disabled
        return "TEMPORARY_DISABLED";
    }

    @Override
    public long calculateFileSize(String filePath) {
        return new File(filePath).length();
    }

    @Override
    public boolean isPathExist(String filePath) {
        return Files.exists(Paths.get(filePath));
    }

    @Override
    public void removeDir(String dirPath) {
        try {
            FileUtils.deleteDirectory(new File(dirPath));
        } catch (IOException e) {
            LOGGER.error("Can't remove dir: " + e.getMessage());
        }
    }

}
