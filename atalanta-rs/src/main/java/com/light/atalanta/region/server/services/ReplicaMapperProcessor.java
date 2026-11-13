/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services;

import com.light.atalanta.ATALANTAConstants;
import com.light.atalanta.Preconditions;
import com.light.atalanta.TableName;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.region.server.utils.UniqueNameThreadFactory;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.light.atalanta.region.server.services.operations.progressed.SplitRegion1.*;

/**
 * Maps replicas to their paths on disk.
 * Initialized on RS start by scanning the data directories.
 * Kept up to date by the region operations: DistributeRegion, DisableRegion, DropRegion.
 */
public class ReplicaMapperProcessor implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaMapperProcessor.class);

    private static final String REGION_REPLICA_SEPARATOR = "_";
    public static final String ORPHAN_REPLICA_POSTFIX = "_orphan";

    public static final String REBALANCING_REPLICA_POSTFIX = "_rebalancing";

    private final RegionServerConfiguration regionServerConfiguration;
    private final FileSystem fileSystem;

    public Map<ReplicaDescription, String> getReplicasMapping() {
        return replicasMapping;
    }

    private final Map<ReplicaDescription, String> replicasMapping;
    private final Map<String, TableName> replicasByTable;

    private final ScheduledExecutorService mappingMonitoring;

    public ReplicaMapperProcessor(RegionServerConfiguration regionServerConfiguration,
                                 FileSystem fileSystem) {
        this.regionServerConfiguration = regionServerConfiguration;
        this.fileSystem = fileSystem;
        this.replicasMapping = new ConcurrentHashMap<>();
        ThreadFactory statsThreadFactory = new UniqueNameThreadFactory("atalanta-mapping-stats-pool");
        this.mappingMonitoring = Executors.newSingleThreadScheduledExecutor(statsThreadFactory);
        replicasByTable = new ConcurrentHashMap<>();
    }

    /**
     * Adds (updates) a replica in the replica -> disk path mapping.
     * Returns the previous path if the replica was already mapped (which means something may have gone wrong).
     */
    public synchronized String upsertNewReplica(RegionModel regionModel, String replicaDir, TableName tableName) {
        String name = regionModel.getName();
        if (name.contains("-")) {
            name = name.substring(name.indexOf("-") + 1);
        }
        ReplicaDescription replicaDescription = new ReplicaDescription(name + "_" + regionModel.getReplicaNum());
        String previousDir = replicasMapping.get(replicaDescription);
        replicasMapping.put(replicaDescription, replicaDir);
        replicasByTable.put(name, tableName);
        return previousDir;
    }

    public synchronized String upsertNewReplica(String regionModelIdentity, String replicaDir, TableName tableName) {
        if (regionModelIdentity.contains("-")) {
            regionModelIdentity = regionModelIdentity.substring(regionModelIdentity.indexOf("-") + 1);
        }
        ReplicaDescription replicaDescription = new ReplicaDescription(regionModelIdentity);
        String  previousDir = replicasMapping.get(replicaDescription);
        replicasMapping.put(replicaDescription, replicaDir);
        replicasByTable.put(regionModelIdentity, tableName);
        return previousDir;
    }

    /**
     * Returns the path of the replica on disk.
     * For a splitted replica named sourceName-destName_id, takes destName_id.
     */
    public String getReplicaDir(RegionModel regionModel) {
        String name = regionModel.getName();
        if (name.contains("-")) {
            name = name.substring(name.indexOf("-") + 1);
        }
        String replicaDir = replicasMapping.get(new ReplicaDescription(name + "_" + regionModel.getReplicaNum()));
        if (StringUtils.isBlank(replicaDir)) {
            throw new RuntimeException("Region directory was not found");
        }
        return replicaDir;
    }

    /**
     * Removes the replica path from the mapping.
     */
    public synchronized boolean removeReplicaDir(RegionModel regionModel) {
        String name = regionModel.getName();
        if (name.contains("-")) {
            name = name.substring(name.indexOf("-") + 1);
        }
        ReplicaDescription replicaDescription = new ReplicaDescription(name + "_" + regionModel.getReplicaNum());
        boolean deleted = replicasMapping.containsKey(replicaDescription);
        replicasMapping.remove(replicaDescription);
        return deleted;
    }

    /**
     * Checks whether the replica is in the cache.
     */
    public boolean contains(RegionModel regionModel) {
        String name = regionModel.getName();
        if (name.contains("-")) {
            name = name.substring(name.indexOf("-") + 1);
        }
        ReplicaDescription replicaDescription = new ReplicaDescription(name + "_" + regionModel.getReplicaNum());
        return replicasMapping.containsKey(replicaDescription);
    }

    /**
     * Keeps only the given replicas. The others are renamed and later deleted.
     */
    public synchronized void retain(List<RegionDistributionModel> regionsToRetain) {
        List<ReplicaDescription> retainDescriptions = regionsToRetain.stream()
                .map(rdm -> {
                    String name = rdm.getRegionModel().getName();
                    if (name.contains("-")) {
                        name = name.substring(name.indexOf("-") + 1);
                    }
                    return new ReplicaDescription(name + "_" + rdm.getRegionModel().getReplicaNum());
                })
                .collect(Collectors.toList());
        Map<ReplicaDescription, String> replicasMappingCopy = new HashMap<>(replicasMapping);
        for (Map.Entry<ReplicaDescription, String > curReplica : replicasMappingCopy.entrySet()) {
            if (!retainDescriptions.contains(curReplica.getKey())) {
                LOGGER.info("Processing orphan replica: {}", curReplica.getKey());
                String orphanReplicaPath =
                        curReplica.getValue();
                boolean renameResult = fileSystem.renameDir(orphanReplicaPath, orphanReplicaPath + "_" + System.currentTimeMillis() + ORPHAN_REPLICA_POSTFIX);
                Preconditions.checkArgument(renameResult,  "Unable to change replica directory to orphan");
                replicasMapping.remove(curReplica.getKey());
                replicasByTable.remove(curReplica.getKey());
            }
        }


        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Runnable task = () -> {
            LOGGER.info("Start orphans cleaner");
            for (String dataDir : regionServerConfiguration.getRsDirs()) {
                try {
                    for (String namespace : fileSystem.getDirFilesAndDirs(dataDir)) {
                        if (!ATALANTAConstants.SNAPSHOT_DIR.equals(namespace)
                                && !ATALANTAConstants.ATALANTA_METADATA.equals(namespace)) {
                            String namespaceDir = String.join("/", dataDir, namespace);
                            try {
                                for (String table : fileSystem.getDirFilesAndDirs(namespaceDir)) {
                                    String tableDir = String.join("/", namespaceDir, table);
                                    try {
                                        for (String replica : fileSystem.getDirFilesAndDirs(tableDir)) {
                                            String fullReplicaPath = String.join("/", tableDir, replica);
                                            if (replica.endsWith(ORPHAN_REPLICA_POSTFIX)) {
                                                Pattern pattern = Pattern.compile("_(\\d+)_orphan");
                                                Matcher matcher = pattern.matcher(replica);
                                                if (matcher.find()) {
                                                    String timestampStr = matcher.group(1);
                                                    long timestamp = Long.parseLong(timestampStr);
                                                    long hours = timestamp / (1000 * 60 * 60);
                                                    long currHours = System.currentTimeMillis() / (1000 * 60 * 60);
                                                    LOGGER.info("Hours {} is {}", fullReplicaPath, hours);
                                                    if (currHours - hours > regionServerConfiguration.getDataConfiguration().getKeepOrphanHours()) {
                                                        LOGGER.info("Replica {} deleted, {} hours", fullReplicaPath, hours);
                                                        fileSystem.removeDir(fullReplicaPath);
                                                    }
                                                } else {
                                                    LOGGER.warn("Can't find timestamp in " + fullReplicaPath);
                                                }
                                            }
                                        }
                                    } catch (IOException ex) {
                                        LOGGER.error("Unable to list elements for table directory: {}", tableDir, ex);
                                    }
                                }
                            } catch (IOException ex) {
                                LOGGER.error("Unable to list elements for namespace directory: {}", namespaceDir, ex);
                            }
                        }
                    }
                } catch (IOException ex) {
                    LOGGER.error("Unable to list elements for data directory: {}", dataDir, ex);
                }
            }
        };
        scheduler.scheduleAtFixedRate(task, 0, 60, TimeUnit.MINUTES);

    }

    /**
     * Counts the regions placed in the given directory.
     */
    public long getReplicasCount(String dataDir) {
        return replicasMapping.values().stream()
                .filter(dd -> dd.startsWith(dataDir + "/"))
                .count();
    }

    /**
     * Reads the replica paths from the file system on start.
     * Format: /data/namespace/table/regionName_replicaNum
     */
    public void initMapping() {
        LOGGER.info("Start region replicas mapping initialization");
        for (String dataDir : regionServerConfiguration.getRsDirs()) {
            try {
                for (String namespace : fileSystem.getDirFilesAndDirs(dataDir)) {
                    if (!ATALANTAConstants.SNAPSHOT_DIR.equals(namespace)
                            && !ATALANTAConstants.ATALANTA_METADATA.equals(namespace)) {
                        String namespaceDir = String.join("/", dataDir, namespace);
                        try {
                            for (String table : fileSystem.getDirFilesAndDirs(namespaceDir)) {
                                String tableDir = String.join("/", namespaceDir, table);
                                try {
                                    for (String replica : fileSystem.getDirFilesAndDirs(tableDir)) {
                                        // An obsolete replica is not loaded into the mapping
                                        if (replica.endsWith(ORPHAN_REPLICA_POSTFIX) ||
                                            replica.endsWith(REBALANCING_REPLICA_POSTFIX)) {
                                            LOGGER.info("Skipping orphan replica: {}", replica);
                                            continue;
                                        }
                                        String replicaDir = String.join("/", tableDir, replica);
                                        try {
                                            TableName tableName = TableName.valueOf(namespace, table);
                                            ReplicaDescription replicaDescription = new ReplicaDescription(replica);
                                            replicasMapping.put(replicaDescription, replicaDir);
                                            int beginIndex = replica.indexOf("_");
                                            replicasByTable.put(beginIndex > 0 ? replica.substring(0, beginIndex) : replica, tableName);
                                        } catch (Exception ex) {
                                            LOGGER.error("Unable to add replica to mapping: {}", replicaDir, ex);
                                        }
                                    }
                                } catch (IOException ex) {
                                    LOGGER.error("Unable to list elements for table directory: {}", tableDir, ex);
                                }
                            }
                        } catch (IOException ex) {
                            LOGGER.error("Unable to list elements for namespace directory: {}", namespaceDir, ex);
                        }
                    }
                }
            } catch (IOException ex) {
                LOGGER.error("Unable to list elements for data directory: {}", dataDir, ex);
            }
        }
        replicasMapping.forEach((rep, path) -> LOGGER.debug("Found new replica: {}, path: {}", rep, path));
        LOGGER.info("Finish region replicas mapping initialization. Found: {} replicas", replicasMapping.size());

        startMappingMonitoring();
    }

    private void startMappingMonitoring() {
        mappingMonitoring.scheduleWithFixedDelay(() ->
                LOGGER.trace("Current replicas mapping count: {}, content: {}", replicasMapping.size(),
                        replicasMapping.entrySet().stream()
                                .map(e -> String.format("\t%nreplica: %s, path: %s", e.getKey(), e.getValue()))
                                .collect(Collectors.toList())),
                15, 15, TimeUnit.SECONDS);
    }

    @Override
    public void close()  {
        mappingMonitoring.shutdownNow();
    }

    public List<String> getReplicaFiles(RegionModel regionModel) throws Exception {
        List<String> fileList = new ArrayList<>();
        String replicaPath = getReplicaDir(regionModel);
        List<File> allFiles = fileSystem.getDirFileDetails(replicaPath);
        // Analyze all bkp files
        List<File> bkpFiles = allFiles.stream()
                .filter(f -> f.getName().endsWith(BKP_POSTFIX))
                .collect(Collectors.toList());
        // Model the target names without the bkp suffix
        List<String> fileNamesToExclude = bkpFiles.stream()
                .map(f -> f.getName().replace(BKP_POSTFIX, DATA_EXTENSION))
                .collect(Collectors.toList());
        // A file must be split if:
        // - it has no backup
        // - it is neither low nor high
        allFiles.stream()
                .filter(f -> f.getName().endsWith(DATA_EXTENSION))
                .filter(f -> !fileNamesToExclude.contains(f.getName()))
                .filter(f -> !f.getName().endsWith(LOW_POSTFIX))
                .filter(f -> !f.getName().endsWith(HIGH_POSTFIX))
                .filter(f -> !f.getName().endsWith(LOB_FILE_NAME))
                .forEach(f -> fileList.add(f.getAbsolutePath()));
        return fileList;
    }

    public TableName getReplicaTableName(String name) {
        int index = name.indexOf("_");
        return replicasByTable.get(index > 0 ? name.substring(0, index) : name);
    }

    /**
     * Replica information. Used to navigate the mapping.
     */
    public static class ReplicaDescription {
        private final String regionName;
        private final String replicaNum;

        // Can be created from a directory name or from an identity
        public ReplicaDescription(String regionDirName) {
            Preconditions.checkArgument(StringUtils.isNotEmpty(regionDirName)
                    && regionDirName.contains(REGION_REPLICA_SEPARATOR)
                    && regionDirName.split(REGION_REPLICA_SEPARATOR).length >= 2, "Region dir/identity is damaged: " + regionDirName);
            // Parse a directory named regionName_replicaNum
            String[] parts = regionDirName.split(REGION_REPLICA_SEPARATOR);
            this.regionName = parts[0];
            this.replicaNum = parts[1];
        }

        public ReplicaDescription(RegionModel regionModel) {
            Preconditions.checkArgument(regionModel != null, "Region model can't be null");
            this.regionName = regionModel.getName();
            this.replicaNum = String.valueOf(regionModel.getReplicaNum());
        }

        public String getRegionName() {
            return regionName;
        }

        public String getReplicaNum() {
            return replicaNum;
        }


        @Override
        public String toString() {
            return regionName + "_" + replicaNum;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ReplicaDescription that = (ReplicaDescription) o;
            return regionName.equals(that.regionName) && replicaNum.equals(that.replicaNum);
        }

        @Override
        public int hashCode() {
            return Objects.hash(regionName, replicaNum);
        }
    }

    public long getSize(String s) {
        File file = getRegDir(s);
        return file == null ? -1 : FileUtils.sizeOfDirectory(file);
    }

    public File getRegDir(String regionDirName) {
        ReplicaMapperProcessor.ReplicaDescription reg = new ReplicaMapperProcessor.ReplicaDescription(regionDirName);
        String fileName = getReplicasMapping().get(reg);
        if (fileName != null) {
            File imageFile = new File(fileName);
            return imageFile;
        } else {
            return null;
        }
    }

}
