/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.IOException;
import java.util.List;

public interface FileSystem {

    List<String> getDirFilesAndDirs(String path) throws IOException;

    List<File> getDirFileDetails(String path) throws IOException;

    @SuppressFBWarnings("RR_NOT_CHECKED")
    byte[] readFileBytes(String filePath, long offset, long length) throws Exception;

    void removeTmp();

    boolean makeSureDirExists(String path);

    boolean renameDir(String source, String target);

    boolean renameFile(String source, String target);

    void writeFileBytes(String filePath, byte[] data) throws Exception;

    void makeSureDirRemoved(String path) throws IOException;

    void makeSureFileRemoved(String filePath) throws IOException;

    void copyFile(String src, String dst) throws IOException;

    String calculateFileHash(String filePath) throws Exception;

    long calculateFileSize(String filePath) throws Exception;

    boolean isPathExist(String filePath);

    public void removeDir(String dirPath);
}
