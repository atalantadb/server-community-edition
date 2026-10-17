/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.operations.progressed;

import com.google.common.base.Preconditions;
import com.light.atalanta.TableName;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.region.server.services.ReplicaMapperProcessor;
import com.light.atalanta.region.server.services.grpc.RegionTaskFunctionalized;
import com.light.atalanta.region.server.services.operations.RegionOperation;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import com.light.atalanta.transport.TransportConfiguration;
import com.light.atalanta.transport.TransportConnectionFactory;
import com.light.atalanta.transport.messages.RsResponse;
import com.light.atalanta.transport.TRSConnectionStub;
import com.light.atalanta.transport.messages.TableRegionActionRequest;
import com.light.atalanta.transport.messages.TransportMessage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Distributes a region.
 * Sends the ASSIGN request to the core.
 */
public class DistributeRegion implements RegionOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributeRegion.class.getName());

    private final RegionServerConfiguration regionServerConfiguration;

    private final ReplicaMapperProcessor replicaMapperProcessor;

    private final FileSystem fileSystem;

    private static final Random RANDOM = new Random();

    public DistributeRegion(RegionServerConfiguration regionServerConfiguration, ReplicaMapperProcessor replicaMapperProcessor, FileSystem fileSystem) {
        this.regionServerConfiguration = regionServerConfiguration;
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.fileSystem = fileSystem;
    }

    @Override
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public void progressedAction(TableName tableName, RegionModel regionModel, AtomicReference<String> taskProgress, AtomicBoolean interrupted, String reservedReplicas) throws Exception {
        boolean contains = replicaMapperProcessor.contains(regionModel);
        // "@" means that the region must already exist
        if (reservedReplicas.contains("@")) {
            Preconditions.checkArgument(contains, "Region " + regionModel + " is not exists in mapping!");
            reservedReplicas = reservedReplicas.replace("@", StringUtils.EMPTY);
        }
        // Check whether the region is in the internal structure. If it is, just enable it.
        // If not, pick a data dir, distribute the region and add it to the internal structure.
        if (contains) {
            // This is a region enable operation
            String replicaDir = replicaMapperProcessor.getReplicaDir(regionModel);
            distributeRegionInternal(tableName, regionModel, taskProgress, reservedReplicas, replicaDir, false);
        } else {
            // This is a region distribute operation
            distributeRegionInternal(tableName, regionModel, taskProgress, reservedReplicas, null, true);
        }
    }

    /**
     * For a new region, leave replicaDir empty.
     * If the region data already exists, no separate folder is needed: set replicaDir.
     * The second case happens, for example, when a region is moved.
     */
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public void distributeRegionInternal(TableName tableName, RegionModel regionModel, AtomicReference<String> taskProgress, String reservedReplicas, String replicaDir, boolean errorIfExists) throws Exception {
        LOGGER.info("Start distributing region '{}:{}'", tableName, regionModel.toShortString());
        TransportConfiguration transportConfiguration = TransportConfiguration.defaultConfiguration().set(TransportConfiguration.ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY, String.valueOf(regionServerConfiguration.getCoreConfiguration().getSocketTimeoutMs()));
        updateTaskProgress(taskProgress, tableName, regionModel, "establishing core connection");
        try (TRSConnectionStub connectionStub = TransportConnectionFactory.createConnectionStub(
                transportConfiguration,
                regionServerConfiguration.getCoreConfiguration().getHost(),
                regionServerConfiguration.getCoreConfiguration().getRsPort())) {

            if (replicaDir == null) {
                updateTaskProgress(taskProgress, tableName, regionModel, "choosing data directory");
                replicaDir = chooseReplicaDir(tableName, regionModel);
                fileSystem.makeSureDirExists(replicaDir);
                regionModel.writeRegionInfoFile(replicaDir);
            } else {
                LOGGER.info("Distributing region '{}:{}': {}", tableName, regionModel.toShortString(), "known region, just enabling");
            }

            int retry = 0;
            while (true) {
                RsResponse response = null;
                try (TRSConnectionStub unassignConnectionStub = TransportConnectionFactory.createConnectionStub(
                        transportConfiguration,
                        regionServerConfiguration.getCoreConfiguration().getHost(),
                        regionServerConfiguration.getCoreConfiguration().getRsPort())) {
                    updateTaskProgress(taskProgress, tableName, regionModel, "sending unassign rollback core request");
                    TransportMessage unassignRegionRequest = new TableRegionActionRequest(tableName,
                            regionModel, TableRegionActionRequest.Command.UNASSIGN,
                            "/asd", null);
                    response = unassignConnectionStub.sendRequestToRs(unassignRegionRequest);

                    updateTaskProgress(taskProgress, tableName, regionModel, "waiting & checking unassign rollback core response");
                }
                if (response == null) {
                    throw new Exception("Error unassign region: '" + tableName + ":" + regionModel.toShortString() + "': null core UNASSIGN response");
                }
                if (response.isNegativeAnswer()) {
                    LOGGER.error("Core UNASSIGN rollback operation error. Response: " + response);
                    Thread.sleep(500);
                } else {
                    break;
                }
            }
            // If we were interrupted, don't continue
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }

            updateTaskProgress(taskProgress, tableName, regionModel, "sending core request");

            TransportMessage regionRequest = new TableRegionActionRequest(tableName,
                    regionModel, TableRegionActionRequest.Command.ASSIGN,
                    replicaDir, reservedReplicas);
            RsResponse response = connectionStub.sendRequestToRs(regionRequest);

            updateTaskProgress(taskProgress, tableName, regionModel, "waiting & checking core response");
            if (response.isNegativeAnswer()) {
                throw new Exception("Core ASSIGN operation error. Response: " + response);
            }

            // If we were interrupted, don't continue
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }

            updateTaskProgress(taskProgress, tableName, regionModel, "updating regions mapping");
            // Add the replica to the mapping (save the path it was distributed to)
            String previousDir = replicaMapperProcessor.upsertNewReplica(regionModel, replicaDir, tableName);
            // If the path was already in the mapping, something may have gone wrong. Log a warning.
            if (errorIfExists && previousDir != null) {
                LOGGER.warn("Distributing region '{}:{}' region already exists in mapping. Previous dir: {}", tableName, regionModel.toShortString(), previousDir);
                // TODO: Decide whether a warning is enough
                throw new Exception("Replica path already exists in mapping. See the logs for details");
            }
            updateTaskProgress(taskProgress, tableName, regionModel, "region distributed successfully");
        } catch (Exception ex) {
            throw new Exception("Error distributing region: '" + tableName + ":" + regionModel.toShortString() + "'", ex);
        }

        LOGGER.info("Done distributing region '{}:{}'", tableName, regionModel.toShortString());
    }

    @Override
    public void functionalizedAction(TableName tableName, RegionModel regionModel, RegionTaskFunctionalized task, String reservedReplicas) throws Exception {
        throw new NotImplementedException("Unable to run progressed task as functionalized");
    }

    private void updateTaskProgress(AtomicReference<String> taskProgress, TableName tableName, RegionModel regionModel, String text) {
        taskProgress.set(text);
        LOGGER.info("Distributing region '{}:{}': {}", tableName, regionModel.toShortString(), text);
    }

    /**
     * Returns the folder for the future replica data in the selected data directory.
     * For a splitted replica named sourceName-destName_id, takes destName_id.
     */
    public String chooseReplicaDir(TableName tableName, RegionModel regionModel) {
        String name = regionModel.getName();
        if (name.contains("-")) {
            name = name.substring(name.indexOf("-") + 1);
        }
        return String.join("/",
                chooseDataDir(tableName, regionModel),
                tableName.getNamespace(),
                tableName.getTableName(),
                name)
                + "_" + regionModel.getReplicaNum();
    }

    /**
     * Picks the most suitable directory for the future replica.
     */
    public String chooseDataDir(TableName tableName, RegionModel regionModel) {
        Optional<String> resultDataDir;
        List<DriveDetails> driveDetails = computeDrivesDetails(tableName, regionModel);
        LOGGER.info("Drive details: " + driveDetails);

        int diskUsageThresholdPercent = regionServerConfiguration.getDataConfiguration().getSimpleRegionDistributionThresholdPercent();

        if (driveDetails.stream().allMatch(dd -> dd.getUsedSpacePercent() < diskUsageThresholdPercent)) {
            LOGGER.info("All disks used by less than {} percent. Using simple distribution logic", diskUsageThresholdPercent);

            // Find the least loaded disk (by number of regions)
            Optional<DriveDetails> chosenDrive = driveDetails.stream()
                    .min(Comparator.comparingLong(DriveDetails::getReplicasOverallCount));

            // If the disk has several data folders, pick the least loaded one (by number of regions)
            resultDataDir = chosenDrive.flatMap(drive ->
                    drive.getDataDirsDetails().entrySet().stream()
                            .min(Comparator.comparingLong(Map.Entry::getValue))
                            .map(Map.Entry::getKey)
            );
        } else {
            LOGGER.info("Found disks that are used more than {} percent. Using probability distribution logic", diskUsageThresholdPercent);

            Map<DriveDetails, Double> drivesProbabilities = computeDrivesProbabilities(tableName, regionModel, driveDetails);
            LOGGER.info("Distributing region '{}:{}': calculated drives probabilities {}",
                    tableName, regionModel.toShortString(), drivesProbabilities.entrySet().stream()
                            .map(dir -> String.format("\t%ndrive: %s, probability: %s", dir.getKey().getDrive(), dir.getValue()))
                            .collect(Collectors.toList()));
            List<DriveDetails> drives = new ArrayList<>(drivesProbabilities.keySet());

            boolean accepted;
            while (true) {
                DriveDetails chosenDrive;
                if (drives.size() > 1) {
                    // Place the replica on a random disk with the computed probability until it hits the target
                    chosenDrive = drives.get(RANDOM.nextInt(drives.size()));
                    int randomNumber = RANDOM.nextInt(100);
                    long probability = Math.round(drivesProbabilities.get(chosenDrive) * 100);
                    accepted = randomNumber < probability;
                    LOGGER.info("Distributing region '{}:{}': drive={}, random={}, probability={}, accepted={}",
                            tableName, regionModel.toShortString(), chosenDrive.getDrive(), randomNumber, probability, accepted);
                } else {
                    // Place the replica on the only available disk
                    accepted = true;
                    chosenDrive = drives.get(0);
                }
                if (accepted) {
                    // If the disk has several data folders, pick the least loaded one
                    resultDataDir = chosenDrive.getDataDirsDetails().entrySet().stream()
                            .sorted(Comparator.comparingLong(Map.Entry::getValue))
                            .map(Map.Entry::getKey)
                            .findFirst();
                    break;
                }
            }
        }

        Preconditions.checkArgument(resultDataDir.isPresent(), "Data directory not found");
        LOGGER.info("Distributing region '{}:{}': data directory to be used is {}", tableName, regionModel.toShortString(), resultDataDir.get());
        return resultDataDir.get();
    }

    /**
     * Computes the probability of the replica being placed on each disk.
     */
    private Map<DriveDetails, Double> computeDrivesProbabilities(TableName tableName, RegionModel regionModel, List<DriveDetails> driveDetails) {
        Map<DriveDetails, Double> drivesProbabilities = new HashMap<>();
        long totalFreeSpace = driveDetails.stream()
                .map(DriveDetails::getUsableSpaceMb)
                .mapToLong(Long::longValue).sum();
        long totalReplicasCount = driveDetails.stream()
                .map(DriveDetails::getReplicasOverallCount)
                .mapToLong(Long::longValue).sum();
        LOGGER.info("Distributing region '{}:{}': cluster free space: {} mb, cluster replicas count: {}",
                tableName, regionModel.toShortString(), totalFreeSpace, totalReplicasCount);
        driveDetails.forEach(details -> {
            double spaceProbability = details.getUsableSpaceMb() / (double) totalFreeSpace;
            if (totalReplicasCount == 0) {
                drivesProbabilities.put(details, spaceProbability);
            } else {
                double countFirstProbability = details.getReplicasOverallCount() / (double) totalReplicasCount;
                double deltaProbability = spaceProbability - countFirstProbability;
                if (deltaProbability < 0.05) {
                    drivesProbabilities.put(details, spaceProbability);
                } else {
                    drivesProbabilities.put(details, deltaProbability);
                }
            }
        });
        OptionalDouble minDeltaProbability = drivesProbabilities.values().stream().mapToDouble(v -> v).min();
        Preconditions.checkArgument(minDeltaProbability.isPresent(), "Min delta probability not found");
        double absMinDeltaProbability = Math.abs(minDeltaProbability.getAsDouble());
        drivesProbabilities.forEach((drive, deltaProbability) ->
                drivesProbabilities.put(drive, deltaProbability + absMinDeltaProbability));
        double newDeltaProbabilitySum = drivesProbabilities.values().stream()
                .mapToDouble(v -> v).sum();
        drivesProbabilities.forEach((drive, newDeltaProbability) ->
                drivesProbabilities.put(drive, newDeltaProbability / newDeltaProbabilitySum));
        return drivesProbabilities;
    }

    /**
     * Returns disk information: storage name, free space, data folders and their region counts.
     */
    private List<DriveDetails> computeDrivesDetails(TableName tableName, RegionModel regionModel) {
        Map<String, DriveDetails> drivesDetails = new HashMap<>();
        for (String dataDir : regionServerConfiguration.getRsDirs()) {
            try {
                FileStore fileStore = Files.getFileStore(Paths.get(dataDir));
                String drive = fileStore.name();
                long freeSpaceMb = fileStore.getUsableSpace() / 1024L / 1024L;
                long usedSpacePercent = 100 - 100 * fileStore.getUsableSpace() / fileStore.getTotalSpace();
                long replicasDistributed = replicaMapperProcessor.getReplicasCount(dataDir);
                DriveDetails driveDetails = drivesDetails.get(drive);
                if (driveDetails == null) {
                    driveDetails = new DriveDetails(drive, freeSpaceMb, usedSpacePercent);
                    driveDetails.addDataDir(dataDir, replicasDistributed);
                    drivesDetails.put(drive, driveDetails);
                } else {
                    driveDetails.addDataDir(dataDir, replicasDistributed);
                }
                LOGGER.info("Distributing region '{}:{}': found data directory: {}, drive: {}, free space: {} mb",
                        tableName, regionModel.toShortString(), dataDir, drive, freeSpaceMb);
            } catch (IOException ex) {
                LOGGER.error("Distributing region '{}:{}': unable to check data directory: {}. Skipping it",
                        tableName, regionModel.toShortString(), dataDir);
            }
        }
        LOGGER.info("Distributing region '{}:{}': found drives: {}", tableName, regionModel.toShortString(), drivesDetails);
        Preconditions.checkArgument(!drivesDetails.isEmpty(), "No available data directories/drives found. Check the logs");
        return new ArrayList<>(drivesDetails.values());
    }

    private static class DriveDetails {
        // Storage name
        private final String drive;

        // Data folders on the disk with their replica counts
        private final Map<String, Long> dataDirsDetails;

        // Free disk space
        private final long usableSpaceMb;

        // Used space, percent
        private final long usedSpacePercent;

        DriveDetails(String drive, long usableSpaceMb, long usedSpacePercent) {
            this.drive = drive;
            this.dataDirsDetails = new HashMap<>();
            this.usableSpaceMb = usableSpaceMb;
            this.usedSpacePercent = usedSpacePercent;
        }

        void addDataDir(String dataDir, long replicasDistributed) {
            dataDirsDetails.put(dataDir, replicasDistributed);
        }

        long getReplicasOverallCount() {
            return dataDirsDetails.values().stream().mapToLong(v -> v).sum();
        }

        String getDrive() {
            return drive;
        }

        Map<String, Long> getDataDirsDetails() {
            return dataDirsDetails;
        }

        long getUsableSpaceMb() {
            return usableSpaceMb;
        }

        public long getUsedSpacePercent() {
            return usedSpacePercent;
        }

        @Override
        public String toString() {
            return String.format("[usableSpaceMb=%s, usedSpacePercent=%s, dataDirs=%s]",
                    usableSpaceMb, usedSpacePercent, dataDirsDetails
            );
        }
    }
}
