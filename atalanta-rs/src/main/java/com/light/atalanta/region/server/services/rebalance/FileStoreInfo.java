/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.rebalance;

import java.io.IOException;
import java.nio.file.FileStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class FileStoreInfo {

    private final String name;

    private long freeDiskSpace;

    private final List<String> dirs = new ArrayList<>();

    public FileStoreInfo(String name, long freeDiskSpace, String dir) {
        this.name = name;
        this.freeDiskSpace = freeDiskSpace;
        this.dirs.add(dir);
    }

    public FileStoreInfo(FileStore fileStore, String dir) throws IOException {
        this(fileStore.name(), fileStore.getUsableSpace(), dir);
    }

    public String getName() {
        return name;
    }

    public long getFreeDiskSpace() {
        return freeDiskSpace;
    }

    public void setFreeDiskSpace(long freeDiskSpace) {
        this.freeDiskSpace = freeDiskSpace;
    }

    public List<String> getDirs() {
        return dirs;
    }

    public String getRandomDir() {
        Collections.shuffle(dirs);
        return dirs.stream().findAny().get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileStoreInfo that = (FileStoreInfo) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
