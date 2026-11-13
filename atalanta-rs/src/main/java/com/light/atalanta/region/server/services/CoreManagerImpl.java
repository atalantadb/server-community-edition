/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services;

import com.google.common.base.Preconditions;
import com.light.atalanta.RepeatableExecutor;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.StorageParam;
import com.light.atalanta.region.server.utils.UniqueNameThreadFactory;
import com.light.atalanta.region.server.vo.*;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.transport.TransportConfiguration;
import com.light.atalanta.transport.TransportConnectionFactory;
import com.light.atalanta.transport.messages.RsResponse;
import com.light.atalanta.transport.TRSConnectionStub;
import com.light.atalanta.transport.messages.TableRegionActionRequest;
import com.light.atalanta.transport.messages.TransportMessage;
import com.light.atalanta.util.ShutdownManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.*;
import static com.light.atalanta.services.ZooKeeperContextHelper.*;
import static com.light.atalanta.services.ZooKeeperCoreContext.*;

/**
 * Manages the core: start, monitoring, stop.
 */
public class CoreManagerImpl implements CoreManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoreManagerImpl.class);

    private final ReplicaMapperProcessor replicaMapperProcessor;

    private final ZooKeeperCoreContext zooContext;

    private final GrpcConfiguration grpcConf;

    private final CoreConfiguration coreConf;

    private final MasterConfiguration masterConf;

    private final ExecutorService initialAssignExecutorService;

    private final ThreadFactory initialAssignThreadFactory = new UniqueNameThreadFactory("atalanta-initial-assign-pool");

    private final ZookeeperHelper zooHelper;

    private final RegionServerConfiguration conf;


    public CoreManagerImpl(RegionServerConfiguration conf, ZooKeeperCoreContext zooContext, ReplicaMapperProcessor replicaMapperProcessor, ZookeeperHelper zooHelper) {
        this.conf = conf;
        this.coreConf = conf.getCoreConfiguration();
        this.grpcConf = conf.getGrpcConfiguration();
        this.masterConf = conf.getMasterConfiguration();
        this.zooContext = zooContext;
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.initialAssignExecutorService = Executors.newFixedThreadPool(
                conf.getGrpcConfiguration().getActiveProcessingRequests(), initialAssignThreadFactory
        );
        this.zooHelper = zooHelper;
    }

    /**
     * Sends ASSIGN requests to the core for all regions owned by this RS.
     */
    @Override
    public void initialAssignAndProcessOrphan() {
        LOGGER.info("Performing initial assign core regions");
        List<InitialAssignReplica> replicasToAssign = new ArrayList<>();
        // Assigned replicas. The rest will be renamed or deleted.
        List<RegionDistributionModel> regionsToRetain = new ArrayList<>();
        try {
            Map<TableName, List<RegionDistributionModel>> tablesDistributions = new RepeatableExecutor()
                    .withIncrementTimeout(1_000L)
                    .withMaxRetries(10)
                    .execute(() -> {
                        try {
                            return zooContext.getAllDistributions();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
            // Iterate over the tables
            for (Map.Entry<TableName, List<RegionDistributionModel>> entry : tablesDistributions.entrySet()) {
                TableName tableName = entry.getKey();
                // Iterate over the regions
                for (List<RegionDistributionModel> unfilteredReplicas : zooContext.groupReplicasByRegion(entry.getValue())) {
                    // Mark the replica to keep, according to the algorithm
                    regionsToRetain.addAll(filterNotCreatingNotDroppedNotAssigningNotRecoveringNotSyncing(filterOurRegionReplicas(unfilteredReplicas)));
                    // Filter the list, keeping only "active" replicas
                    List<RegionDistributionModel> allFilteredReplicas = filterRegionReplicasDesc(unfilteredReplicas);
                    LOGGER.debug("Filtered replicas for {} is: {}", unfilteredReplicas.get(0).getRegionModel().getName(), allFilteredReplicas);
                    // Iterate over the active replicas on this RS to assign them.
                    // UNASSIGNING replicas are not assigned, but they count when computing the reserves.
                    for (RegionDistributionModel replica : removeUnassigning(filterOurRegionReplicas(allFilteredReplicas))) {
                        // Assign the replica. Use the active replicas to find the reserve addresses.
                        InitialAssignReplica iir = new InitialAssignReplica(tableName, allFilteredReplicas, replica);
                        replicasToAssign.add(iir);
                    }
                }
            }
            makeInitialAssign(replicasToAssign);
        } catch (Exception ex) {
            ShutdownManager.exitWithError("Unable to perform initial core regions assign", ex);
        } finally {
            initialAssignExecutorService.shutdownNow();
        }

        LOGGER.info("REGIONS TO RETAIN = {}", regionsToRetain);

        // Rename or delete all replicas that were not kept
        try {
            replicaMapperProcessor.retain(regionsToRetain);
        } catch (Exception ex) {
            ShutdownManager.exitWithError("Unable to rename/clean orphan replicas", ex);
        }
        LOGGER.info("Initial core regions assign performed");
    }

    // Assign the reserves first, then the leaders
    private void makeInitialAssign(List<InitialAssignReplica> replicasToAssign) throws ExecutionException, InterruptedException {
        List<InitialAssignReplica> leaderReplicasToAssign = new ArrayList<>();
        List<InitialAssignReplica> reservedReplicasToAssign = new ArrayList<>();
        replicasToAssign.forEach(it -> {
            if (it.getReplica().getRegionModel().getReplicaNum() == 0) {
                leaderReplicasToAssign.add(it);
            } else {
                reservedReplicasToAssign.add(it);
            }
        });
        if (!reservedReplicasToAssign.isEmpty()) {
            LOGGER.info("Assigning {} reserved replicas ...", reservedReplicasToAssign.size());
            CompletableFuture<?>[] initialAssigns = new CompletableFuture[reservedReplicasToAssign.size()];
            for (int i = 0; i < initialAssigns.length; i++) {
                InitialAssignReplica iir = reservedReplicasToAssign.get(i);
                initialAssigns[i] = CompletableFuture.runAsync(
                        () -> assignReplica(iir), initialAssignExecutorService
                );
            }
            CompletableFuture.allOf(initialAssigns).get();
        }
        if (!leaderReplicasToAssign.isEmpty()) {
            LOGGER.info("Assigning {} leader replicas ...", leaderReplicasToAssign.size());
            CompletableFuture<?>[] initialAssigns = new CompletableFuture[leaderReplicasToAssign.size()];
            for (int i = 0; i < initialAssigns.length; i++) {
                InitialAssignReplica iir = leaderReplicasToAssign.get(i);
                initialAssigns[i] = CompletableFuture.runAsync(
                        () -> assignReplica(iir), initialAssignExecutorService
                );
            }
            CompletableFuture.allOf(initialAssigns).get();
        }
    }

    private void assignReplica(InitialAssignReplica iir) {
        TableName tableName = iir.getTableName();
        RegionDistributionModel replica = iir.getReplica();
        List<RegionDistributionModel> filteredReplicas = iir.getAllFilteredReplicas();
        RegionModel regionModel = replica.getRegionModel();
        // Fail if we have no data for this region
        Preconditions.checkArgument(replicaMapperProcessor.contains(regionModel), "Unable to find data files for replica: " + replica.toHumanString());
        LOGGER.info("Assigning region: '{}_{}' for table: '{}'", regionModel.getName(), regionModel.getReplicaNum(), tableName);
        String replicaDir = replicaMapperProcessor.getReplicaDir(regionModel);
        if (conf.getDataConfiguration().isCheckRegioninfo()) {
            LOGGER.info("Start check .regioninfo");
            checkRegioninfo(tableName, replicaDir);
            LOGGER.info("Check .regioninfo is complete");
        } else {
            LOGGER.info(".regioninfo checker is disabled");
        }
        String reservedReplicas = StringUtils.EMPTY;
        // If the replica is a leader, collect the reserve addresses
        if (regionModel.getReplicaNum() == 0) {
            reservedReplicas = zooContext.getReservedAddresses(filteredReplicas, replica.getRegionModel().getName(), masterConf.getReplicationFactor());
            LOGGER.debug("Reserved replicas for {} is: {}", replica.getRegionModel().toIdentityString(), reservedReplicas);
        }
        TransportConfiguration transportConfiguration = TransportConfiguration.defaultConfiguration().set(TransportConfiguration.ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY, String.valueOf(coreConf.getSocketTimeoutMs()));
        // The actual request to the core
        for (int i = 0; i < coreConf.getConnectionRetryTimes(); i++) {
            try (TRSConnectionStub connectionStub = TransportConnectionFactory.createConnectionStub(transportConfiguration,
                    coreConf.getHost(),
                    coreConf.getRsPort())) {
                TransportMessage regionRequest = new TableRegionActionRequest(tableName,
                        regionModel,
                        TableRegionActionRequest.Command.ASSIGN,
                        replicaDir,
                        reservedReplicas);
                RsResponse response = connectionStub.sendRequestToRs(regionRequest);
                if (response.isNegativeAnswer()) {
                    throw new CompletionException(
                            new Exception("Core ASSIGN operation error. Response: " + response + ", replica: " + iir)
                    );
                }
                break;
            } catch (SocketTimeoutException ex) {
                if (i == coreConf.getConnectionRetryTimes() - 1) {
                    ShutdownManager.exitWithError("Unable to receive core response", ex);
                } else {
                    long sleep = coreConf.getConnectionRetrySleepMs();
                    LOGGER.error("Unable to receive core response, sleep " + sleep + " ms and retry #" + (i + 1), ex);
                    try {
                        Thread.sleep(sleep);
                    } catch (InterruptedException interruptedException) {
                        throw new CompletionException(interruptedException);
                    }
                }
            } catch (Exception ex) {
                throw new CompletionException(new Exception("Error assigning replica " + iir, ex));
            }
        }
    }

    private void checkRegioninfo(TableName tableName, String replicaDir) {
        try {
            String basePath = getBasePathForTableExtParam(tableName);
            boolean isZkExtParamExist = zooHelper.checkExists(basePath);

            File regionInfo = new File(replicaDir + "/.regioninfo");
            boolean isRegioninfoExist = regionInfo.exists();
            Map<String, String> params = isRegioninfoExist ? parseParams(regionInfo) : new HashMap<>();
            if (isZkExtParamExist) {
                if (isRegioninfoExist) {
                    compareRegioninfoFileAndZk(params, basePath, replicaDir);
                }
            }
            addExtParamToZk(tableName, params, basePath);
            if (!isRegioninfoExist) {
                RegionModel.writeRegionInfoFile(replicaDir, new StorageParam());
            }
        } catch (IOException e) {
            ShutdownManager.exitWithError("Cannot check regioninfo", e);
        }
    }

    private void addExtParamToZk(TableName tableName, Map<String, String> params, String basePath) throws ZKException {
        if (!zooHelper.checkExists(ATALANTA_METADATA)) {
            createZkNodeNoException(ATALANTA_METADATA, StringUtils.EMPTY);
        }
        if (!zooHelper.checkExists(ATALANTA_NS)) {
            createZkNodeNoException(ATALANTA_NS, StringUtils.EMPTY);
        }
        if (!zooHelper.checkExists(ATALANTA_NS + ZK_SEPARATOR + tableName.getNamespace())) {
            createZkNodeNoException(ATALANTA_NS + ZK_SEPARATOR + tableName.getNamespace(), StringUtils.EMPTY);
        }
        if (!zooHelper.checkExists(ATALANTA_NS + ZK_SEPARATOR + tableName.getNamespace() + ZK_SEPARATOR + tableName.getTableName())) {
            createZkNodeNoException(ATALANTA_NS + ZK_SEPARATOR + tableName.getNamespace() + ZK_SEPARATOR + tableName.getTableName(), StringUtils.EMPTY);
        }
        String path = String.join(ZK_SEPARATOR, basePath, RECORD_COMPRESSION_TYPE);
        if (!zooHelper.checkExists(path)) {
            createZkNodeNoException(path, params.getOrDefault("record_compression", "lz"));
        }
        path = String.join(ZK_SEPARATOR, basePath, COMPRESSION_TYPE);
        if (!zooHelper.checkExists(path)) {
            createZkNodeNoException(path, params.getOrDefault("compression", "none"));
        }
        path = String.join(ZK_SEPARATOR, basePath, DICTIONARY);
        if (!zooHelper.checkExists(path)) {
            createZkNodeNoException(path, params.getOrDefault("dictionary", "true"));
        }
        path = String.join(ZK_SEPARATOR, basePath, RECORD_TARGET_COUNT);
        if (!zooHelper.checkExists(path)) {
            createZkNodeNoException(path, params.getOrDefault("record_target_count", "0"));
        }
    }

    private void createZkNodeNoException(String path, String data) {
        try {
            zooHelper.create(path, data );
        } catch (Exception e) {
            //ignore exception
            LOGGER.debug("ignore exception");
        }
    }

    private void compareRegioninfoFileAndZk(Map<String, String> params, String basePath, String replicaDir) throws ZKException {
        String path = String.join(ZK_SEPARATOR, basePath, RECORD_COMPRESSION_TYPE);
        if (zooHelper.checkExists(path)) {
            String zkValue = zooHelper.getNotNullString(path);
            String riValue = params.getOrDefault("record_compression", "lz");
            if (!zkValue.equals(riValue)) {
                ShutdownManager.exitWithError("Property 'record_compression' from .regioninfo is not equals to ZK. "
                        + "zkValue: " + zkValue
                        + ", .regioninfo value: " + params.get("record_compression")
                        + ", replicaDir: " + replicaDir);
            }
        }
        path = String.join(ZK_SEPARATOR, basePath, DICTIONARY);
        if (zooHelper.checkExists(path)) {
            String zkValue = zooHelper.getNotNullString(path);
            String riValue = params.getOrDefault("dictionary", "true");
            if (!zkValue.equals(riValue)) {
                ShutdownManager.exitWithError("Property 'dictionary' from .regioninfo is not equals to ZK. " +
                        "zkValue: " + zkValue
                        + ", .regioninfo value: " + params.get("dictionary")
                        + ", replicaDir: " + replicaDir);
            }
        }
        path = String.join(ZK_SEPARATOR, basePath, RECORD_TARGET_COUNT);
        if (zooHelper.checkExists(path)) {
            String zkValue = zooHelper.getNotNullString(path);
            String riValue = params.getOrDefault("record_target_count", "0");
            if (!zkValue.equals(riValue)) {
                ShutdownManager.exitWithError("Property 'record_target_count' from .regioninfo is not equals to ZK. " +
                        "zkValue: " + zkValue
                        + ", .regioninfo value: "
                        + params.get("record_target_count")
                        + ", replicaDir: " + replicaDir);
            }
        }
        path = String.join(ZK_SEPARATOR, basePath, COMPRESSION_TYPE);
        if (zooHelper.checkExists(path)) {
            String zkValue = zooHelper.getNotNullString(path);
            String riValue = params.getOrDefault("compression", "none");
            if (!zkValue.equals(riValue)) {
                ShutdownManager.exitWithError("Property 'compression' from .regioninfo is not equals to ZK. "
                        + "zkValue: " + zkValue
                        + ", .regioninfo value: " + params.get("compression")
                        + ", replicaDir: " + replicaDir);
            }
        }
    }

    private Map<String, String> parseParams(File regionInfo) throws IOException {
        List<String> lines = FileUtils.readLines(regionInfo, StandardCharsets.UTF_8);
        return lines.stream()
                .map( l -> l.replaceAll("\\s+",""))
                .filter(l -> l.contains("="))
                .map( l -> l.split("="))
                .collect(Collectors.toMap( s -> s[0].toLowerCase(),
                        s -> s[1].toLowerCase()));
    }

    /**
     * If the replica has a CREATED, ASSIGNED or ENABLED status, those entries are used and all others with the same replicaNum are dropped.
     * Otherwise the replica with the UNASSIGNING status is used (with the same replicaNum).
     * If there is no UNASSIGNING either, DISABLING is used.
     * The list is then sorted by replicaNum, descending.
     */
    static List<RegionDistributionModel> filterRegionReplicasDesc(List<RegionDistributionModel> unfiltered) {
        List<Integer> processedReplicaNumbers = new ArrayList<>();
        List<RegionDistributionModel> filtered = new ArrayList<>();
        for (RegionDistributionModel rdm : unfiltered) {
            int replicaNum = rdm.getRegionModel().getReplicaNum();
            if (processedReplicaNumbers.contains(replicaNum)) {
                continue;
            }
            List<RegionDistributionModel> sameReplicas = getReplicasWithNum(unfiltered, replicaNum);
            if (sameReplicas.stream().anyMatch(CoreManagerImpl::statusCreatedOrAssignedOrEnabled)) {
                sameReplicas = sameReplicas.stream()
                        .filter(CoreManagerImpl::statusCreatedOrAssignedOrEnabled)
                        .collect(Collectors.toList());
            } else if (sameReplicas.stream().anyMatch(CoreManagerImpl::statusUnassigning)) {
                sameReplicas = sameReplicas.stream()
                        .filter(CoreManagerImpl::statusUnassigning)
                        .collect(Collectors.toList());
            } else if (sameReplicas.stream().anyMatch(CoreManagerImpl::statusDisabling)) {
                sameReplicas = sameReplicas.stream()
                        .filter(CoreManagerImpl::statusDisabling)
                        .collect(Collectors.toList());
            } else {
                // If no status fits, these replicas of the region are not used
                sameReplicas = new ArrayList<>();
            }
            filtered.addAll(sameReplicas);
            processedReplicaNumbers.add(replicaNum);
        }
        filtered = filtered.stream().
                sorted(Comparator.comparingInt(o -> o.getRegionModel().getReplicaNum()))
                .collect(Collectors.toList());
        Collections.reverse(filtered);
        return filtered;
    }

    private List<RegionDistributionModel> filterOurRegionReplicas(List<RegionDistributionModel> filtered) {
        return filtered.stream().filter(rdm -> makeRegionServerLocation(rdm).equals(grpcConf.myName()))
                .collect(Collectors.toList());
    }

    private List<RegionDistributionModel> filterNotCreatingNotDroppedNotAssigningNotRecoveringNotSyncing(List<RegionDistributionModel> source) {
        return source.stream()
                .filter(rdm -> !statusCreatingOrDroppedOrAssigningOrRecoveringOrSyncing(rdm))
                .collect(Collectors.toList());
    }

    private List<RegionDistributionModel> removeUnassigning(List<RegionDistributionModel> replicas) {
        return replicas.stream()
                .filter(rdm -> !statusUnassigning(rdm))
                .collect(Collectors.toList());
    }

    private String makeRegionServerLocation(RegionDistributionModel rdm) {
        return rdm.getRegionServerModel().getHost() + ":" + rdm.getRegionServerModel().getJavaPort();
    }

    private static boolean statusUnassigning(RegionDistributionModel rdm) {
        return rdm.getRegionModel().getStatusId() == UNASSIGNING_REGION_STATUS;
    }

    private static boolean statusCreatedOrAssignedOrEnabled(RegionDistributionModel rdm) {
        return rdm.getRegionModel().getStatusId() == CREATED_REGION_STATUS
                || rdm.getRegionModel().getStatusId() == ASSIGNED_REGION_STATUS
                || rdm.getRegionModel().getStatusId() == ENABLED_REGION_STATUS
                || (isReferencingRegionStatus(rdm.getRegionModel()) && getReferencedRegionStatus(rdm.getRegionModel()) == CREATED_REGION_STATUS)
                || (isReferencingRegionStatus(rdm.getRegionModel()) && getReferencedRegionStatus(rdm.getRegionModel()) == ASSIGNED_REGION_STATUS)
                || (isReferencingRegionStatus(rdm.getRegionModel()) && getReferencedRegionStatus(rdm.getRegionModel()) == ENABLED_REGION_STATUS);
    }

    private static boolean statusDisabling(RegionDistributionModel rdm) {
        return rdm.getRegionModel().getStatusId() == DISABLING_REGION_STATUS;
    }

    private static boolean statusCreatingOrDroppedOrAssigningOrRecoveringOrSyncing(RegionDistributionModel rdm) {
        return rdm.getRegionModel().getStatusId() == CREATING_REGION_STATUS
                || rdm.getRegionModel().getStatusId() == DROPPED_REGION_STATUS
                || rdm.getRegionModel().getStatusId() == ASSIGNING_REGION_STATUS
                || rdm.getRegionModel().getStatusId() == RECOVERING_REGION_STATUS
                || rdm.getRegionModel().getStatusId() == FORCE_RECOVERING_REGION_STATUS
                || rdm.getRegionModel().getStatusId() == SYNCING_REGION_STATUS;
    }

    private static List<RegionDistributionModel> getReplicasWithNum(List<RegionDistributionModel> replicas, int replicaNum) {
        return replicas.stream()
                .filter(rdm -> rdm.getRegionModel().getReplicaNum() == replicaNum)
                .collect(Collectors.toList());
    }

    private static class InitialAssignReplica {
        private final TableName tableName;
        private final List<RegionDistributionModel> allFilteredReplicas;
        private final RegionDistributionModel replica;

        private InitialAssignReplica(TableName tableName, List<RegionDistributionModel> allFilteredReplicas, RegionDistributionModel replica) {
            this.tableName = tableName;
            this.allFilteredReplicas = allFilteredReplicas;
            this.replica = replica;
        }

        public TableName getTableName() {
            return tableName;
        }

        public List<RegionDistributionModel> getAllFilteredReplicas() {
            return allFilteredReplicas;
        }

        public RegionDistributionModel getReplica() {
            return replica;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            InitialAssignReplica that = (InitialAssignReplica) o;

            if (!Objects.equals(tableName, that.tableName)) return false;
            if (!Objects.equals(allFilteredReplicas, that.allFilteredReplicas))
                return false;
            return Objects.equals(replica, that.replica);
        }

        @Override
        public int hashCode() {
            int result = tableName != null ? tableName.hashCode() : 0;
            result = 31 * result + (allFilteredReplicas != null ? allFilteredReplicas.hashCode() : 0);
            result = 31 * result + (replica != null ? replica.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "InitialAssignReplica{" +
                    "tableName=" + tableName +
                    ", allFilteredReplicas=" + allFilteredReplicas +
                    ", replica=" + replica +
                    '}';
        }
    }
}
