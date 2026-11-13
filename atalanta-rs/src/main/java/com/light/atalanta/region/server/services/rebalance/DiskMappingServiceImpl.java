/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.rebalance;

import com.light.atalanta.ATALANTAConstants;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.region.server.services.ReplicaMapperProcessor;
import com.light.atalanta.util.ShutdownManager;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static com.light.atalanta.region.server.services.ReplicaMapperProcessor.ORPHAN_REPLICA_POSTFIX;
import static com.light.atalanta.region.server.services.ReplicaMapperProcessor.REBALANCING_REPLICA_POSTFIX;

public class DiskMappingServiceImpl implements DiskMappingService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DiskMappingServiceImpl.class);

    private final FileSystem fileSystem;

    private final ReplicaMapperProcessor replicaMapperProcessor;

    public DiskMappingServiceImpl(FileSystem fileSystem, ReplicaMapperProcessor replicaMapperProcessor) {
        this.fileSystem = fileSystem;
        this.replicaMapperProcessor = replicaMapperProcessor;
    }

    @Override
    public Map<FileStoreInfo, List<RegionDirModel>> getDiskMapping(List<String> dirs) {
        Map<FileStoreInfo, List<RegionDirModel>> res = new HashMap<>();
        checkDouble(dirs);
        for (String dataDir : dirs) {
            try {
                FileStoreInfo fileStoreInfoDir = new FileStoreInfo(Files.getFileStore(Paths.get(dataDir)), dataDir);
                res.putIfAbsent(fileStoreInfoDir, new ArrayList<>());
                for (String namespace : fileSystem.getDirFilesAndDirs(dataDir)) {
                    if (!ATALANTAConstants.SNAPSHOT_DIR.equals(namespace)
                            && !ATALANTAConstants.ATALANTA_METADATA.equals(namespace)) {
                        String namespaceDir = String.join("/", dataDir, namespace);
                        for (String table : fileSystem.getDirFilesAndDirs(namespaceDir)) {
                            String tableDir = String.join("/", namespaceDir, table);
                            for (String replica : fileSystem.getDirFilesAndDirs(tableDir)) {
                                String replicaDirStr = String.join("/", tableDir, replica);
                                File replicaDir = new File(replicaDirStr);

                                if (!(replica.contains(ORPHAN_REPLICA_POSTFIX) ||
                                        replica.contains(REBALANCING_REPLICA_POSTFIX))) {
                                    FileStoreInfo fileStoreInfo = new FileStoreInfo(Files.getFileStore(replicaDir.toPath()), dataDir);
                                    RegionDirModel regionDirModel = new RegionDirModel(replica, namespace, table, replicaDirStr,
                                            FileUtils.sizeOfDirectory(replicaDir));
                                    List<RegionDirModel> regionDirList = res.getOrDefault(fileStoreInfo, new ArrayList<>());
                                    regionDirList.add(regionDirModel);
                                    res.put(fileStoreInfo, regionDirList);
                                } else if (replica.endsWith(REBALANCING_REPLICA_POSTFIX)) {
                                    FileUtils.deleteDirectory(replicaDir);
                                }
                            }
                        }
                    }
                }
            } catch (IOException ex) {
                LOGGER.error("Cannot mapping regions for dir: {}", dataDir, ex);
            }
        }

        return res;
    }

    private void checkDouble(List<String> dirs) {
        LOGGER.info("Start check double...");
        HashMap<String, String> replicasDirMap = new HashMap<>();
        Set<String> duplicateNames = new HashSet<>();
        boolean isDoubleFound = false;
        for (String dataDir : dirs) {
            LOGGER.info("Start check double for path {}", dataDir);
            try {
                for (String namespace : fileSystem.getDirFilesAndDirs(dataDir)) {
                    if (!ATALANTAConstants.SNAPSHOT_DIR.equals(namespace)
                            && !ATALANTAConstants.ATALANTA_METADATA.equals(namespace)) {
                        String namespaceDir = String.join("/", dataDir, namespace);
                        for (String table : fileSystem.getDirFilesAndDirs(namespaceDir)) {
                            String tableDir = String.join("/", namespaceDir, table);
                            for (String replica : fileSystem.getDirFilesAndDirs(tableDir)) {
                                String replicaDirStr = String.join("/", tableDir, replica);
                                if (!replica.endsWith(ORPHAN_REPLICA_POSTFIX)) {
                                    if (duplicateNames.contains(replica)) {
                                        removeSingleReplica(replicaDirStr);
                                    } else if (replicasDirMap.containsKey(replica)) {
                                        String existingPath = replicasDirMap.get(replica);
                                        isDoubleFound |= clearDouble(existingPath, replicaDirStr);
                                        duplicateNames.add(replica);
                                        replicasDirMap.remove(replica);
                                    } else {
                                        replicasDirMap.put(replica, replicaDirStr);
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (IOException ex) {
                LOGGER.error("Cannot mapping regions for dir: {}", dataDir, ex);
            }
        }

        if (isDoubleFound) {
            String msg = "double reg found!";
            ShutdownManager.exitWithError(msg, new RuntimeException(msg));
        }
        LOGGER.info("Double check complete");
    }

    private void removeSingleReplica(String replicaPath) {
        try {
            FileUtils.deleteDirectory(new File(replicaPath));
            LOGGER.info("Removed orphan replica: {}", replicaPath);
        } catch (IOException e) {
            LOGGER.error("Failed to remove replica: {}", replicaPath, e);
        }
    }
    private boolean clearDouble(String replicaDirPath1, String replicaDirPath2) throws IOException {
        LOGGER.info("found double: r1: {}, r2: {}", replicaDirPath1, replicaDirPath2);
        File r1 = new File(replicaDirPath1);
        File r2 = new File(replicaDirPath2);
        if (isShaEquals(r1, r2)) {
            FileStore fileStore1 = Files.getFileStore(r1.toPath());
            FileStore fileStore2 = Files.getFileStore(r2.toPath());
            long freeStore1 = fileStore1.getUsableSpace();
            long freeStore2 = fileStore2.getUsableSpace();
            if (freeStore2 > freeStore1) {
                FileUtils.deleteDirectory(r1);

                LOGGER.info("Delete double {}. free size 1: {}, free size 2: {}", replicaDirPath1, freeStore1, freeStore2);
            } else {
                FileUtils.deleteDirectory(r2);
                LOGGER.info("Delete double {}. free size 1: {}, free size 2: {}", replicaDirPath2, freeStore1, freeStore2);
            }
        } else {
            String message = "Region double found, not equals: path1: " + replicaDirPath1 + " path2: " + replicaDirPath2;
            LOGGER.error(message);
            return true;
        }
        return false;
    }

    private boolean isShaEquals(File r1, File r2) {
        return Arrays.equals(getSha(r1), getSha(r2));
    }

    private byte[] getSha(File r1) {
        byte[] buffer = new byte[1024 * 1024];
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("cannot calc SHA-256", e);
            throw new RuntimeException(e);
        }

        String path = "";

        for (File f : Objects.requireNonNull(r1.listFiles())) {
            path = f.getAbsolutePath();
            FileInputStream is = null;
            try {
                is = new FileInputStream(f);
                int read;
                while ((read = is.read(buffer)) > 0) {
                    digest.update(buffer, 0, read);
                }
            } catch (IOException e) {
                LOGGER.error("Cannot read file {}", f.getAbsolutePath(), e);
                throw new RuntimeException(e);
            } finally {
                if (is != null) {
                    try {
                        is.close();
                    } catch (IOException e) {
                        LOGGER.error("Cannot close file {}", f.getAbsolutePath(), e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        LOGGER.error("SHA256 for {}: {:02x}", path, new BigInteger(1, digest.digest()));
        return digest.digest();
    }

}
