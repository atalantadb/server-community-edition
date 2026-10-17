/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.runners;

import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.FRegionServerJmxConnectionException;
import com.light.atalanta.master.services.Master;
import com.light.atalanta.master.services.UniqueNameThreadFactory;
import com.light.atalanta.master.services.recovery.RecoveryRSContext;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import com.light.atalanta.metrics.RegionMetrics;
import com.light.atalanta.metrics.RsJmxClient;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZookeeperHelper;
import com.light.atalanta.util.ZookeeperStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class RegionsSplitRunner implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionsSplitRunner.class);

    private final AtomicLong splittingTables;
    private final ScheduledExecutorService requestsMonitoring;

    private final AtalantaDBConfiguration conf;
    private final ZooKeeperMasterContext zooContext;
    private final ZookeeperHelper zooHelper;
    private final RsJmxClient rsJmxClient;

    private final RecoveryRSContext recoveryRSContext;

    private final ScheduledExecutorService scheduledExecutorService;

    private final ThreadFactory statsThreadFactory = new UniqueNameThreadFactory("atalanta-split-stats-pool");
    private final ThreadFactory runnerThreadFactory = new UniqueNameThreadFactory("atalanta-split-runner-pool");
    private final ThreadFactory parentThreadFactory = new UniqueNameThreadFactory("atalanta-split-parent-pool");

    public RegionsSplitRunner(AtalantaDBConfiguration conf, ZooKeeperMasterContext zooContext, ZookeeperHelper zooHelper, RsJmxClient rsJmxClient, RecoveryRSContext recoveryRSContext) {
        this.conf = conf;
        this.zooContext = zooContext;
        this.zooHelper = zooHelper;
        this.rsJmxClient = rsJmxClient;
        this.recoveryRSContext = recoveryRSContext;
        this.splittingTables = new AtomicLong(0L);
        this.requestsMonitoring = Executors.newSingleThreadScheduledExecutor(statsThreadFactory);
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(runnerThreadFactory);
    }

    public void action(Master master) {
        startMonitoringAsync(master);
        if (conf.getDataConfiguration().getOperationsMonitoringEnabled()) {
            startOperationsMonitoring();
        }
    }

    private void startOperationsMonitoring() {
        requestsMonitoring.scheduleAtFixedRate(() -> {
            long count = splittingTables.get();
            if (count > 0) {
                LOGGER.info("Auto splitting tables count: [{}]", count);
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    // Monitors the running RS, finds regions larger than the threshold, and runs a split
    // Takes at most regionsAutoSplitTableThreadsCount tables per iteration for splitting
    // The rest go to the next iteration
    private void startMonitoringAsync(Master master) {
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            LOGGER.trace("Auto splitting analyze iteration ...");
            if (recoveryRSContext.isSyncRunning()) {
                LOGGER.info("Skipping auto splitting check since SYNC process is running");
                return;
            }
            int maxSplitTables = conf.getDataConfiguration().getRegionsAutoSplitTableThreadsCount();
            ExecutorService executorService = Executors.newFixedThreadPool(maxSplitTables, parentThreadFactory);
            try {
                // Find the regions that exceeded the size limit and split them
                Map<TableName, List<String>> tablesToSplit = getTablesToSplit(maxSplitTables);
                boolean isNeedToClearMetrics = !tablesToSplit.isEmpty();
                CountDownLatch cdl = new CountDownLatch(tablesToSplit.size());
                for (Map.Entry<TableName, List<String>> tableToSplit : tablesToSplit.entrySet()) {
                    executorService.execute(() -> {
                        try {
                            splittingTables.incrementAndGet();
                            master.splitTable(tableToSplit.getKey(), tableToSplit.getValue(), null);
                        } catch (Throwable ex) {
                            LOGGER.error("Error auto splitting table: '{}' regions: {}", tableToSplit.getKey(), tableToSplit.getValue(), ex);
                        } finally {
                            splittingTables.decrementAndGet();
                            cdl.countDown();
                        }
                    });
                }
                if (tablesToSplit.size() > 0) {
                    LOGGER.info("Awaiting while auto splitting process will be completed. Tables to split: {}", tablesToSplit.keySet());
                    cdl.await();
                    LOGGER.info("Auto splitting process was completed");
                } else {
                    LOGGER.trace("No tables for auto splitting found");
                }
                if (isNeedToClearMetrics) {
                    rsJmxClient.clearMetrics();
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOGGER.error("Unable to auto splitting regions", ex);
            } catch (FRegionServerJmxConnectionException ex) {
                LOGGER.error("Unable to auto splitting regions: unable to connect to RS through JMX. Maybe it is turned off?", ex);
            } catch (Exception ex) {
                LOGGER.error("Unable to auto splitting regions", ex);
            } finally {
                executorService.shutdownNow();
            }
        }, conf.getDataConfiguration().getRegionsAutoSplitInitialCheckDelayMs(), conf.getDataConfiguration().getRegionsAutoSplitCheckDelayMs(), TimeUnit.MILLISECONDS);
    }

    private Map<TableName, List<String>> getTablesToSplit(int maxSplitTables) throws Exception {
        Map<TableName, List<String>> tablesToSplit = new HashMap<>();
        Map<SplitDisk, Long> plannedDiskBytes = new HashMap<>();
        // Collect the metrics of running RS
        List<RegionServerModel> runningRS = zooContext.getRunningRegionServers();
        Map<RegionServerModel, RSMetrics> rsMetricsStorage = new HashMap<>();
        runningRS.forEach(rs -> rsMetricsStorage.put(rs, new RSMetrics(rsJmxClient.getRsJmxMetrics(rs).getRegionsMetrics())));
        // Analyze the regions per table
        Map<TableName, List<RegionDistributionModel>> allDistributions = zooContext.getAllDistributions();
        for (TableName tableName : allDistributions.keySet()) {
            if (tablesToSplit.size() >= maxSplitTables) {
                break;
            }
            if (isAutoSplittingNotNeeded(tableName)) {
                LOGGER.warn("Skip table '{}' auto splitting analyze because of status", tableName);
                continue;
            }
            List<String> regionNamesToSplit = getRegionNamesToSplit(tableName, rsMetricsStorage, runningRS,
                    allDistributions.get(tableName), plannedDiskBytes);
            if (!regionNamesToSplit.isEmpty()) {
                tablesToSplit.put(tableName, regionNamesToSplit);
            }
        }
        return tablesToSplit;
    }

    private List<String> getRegionNamesToSplit(TableName tableName, Map<RegionServerModel, RSMetrics> rsMetricsStorage,
                                              List<RegionServerModel> runningRS, List<RegionDistributionModel> tableRdms,
                                              Map<SplitDisk, Long> plannedDiskBytes) {
        long triggerBytes = Math.multiplyExact(conf.getDataConfiguration().getRegionsAutoSplitTriggerMb(), 1024L * 1024);
        int thresholdPercent = 100 - conf.getDataConfiguration().getRegionsAutoSplitBatchMarginPercent();
        long batchThresholdBytes = triggerBytes / 100 * thresholdPercent + triggerBytes % 100 * thresholdPercent / 100;
        List<List<RegionDistributionModel>> replicasByRegion = zooContext.groupReplicasByRegion(tableRdms);
        List<SplitCandidate> candidates = new ArrayList<>();
        for (List<RegionDistributionModel> regionReplicas : replicasByRegion) {
            Set<RegionDistributionModel> notRunningReplicas = regionReplicas.stream()
                    .filter(rdm -> !runningRS.contains(rdm.getRegionServerModel()))
                    .collect(Collectors.toSet());
            RegionDistributionModel leader = regionReplicas.stream()
                    .filter(rdm -> rdm.getRegionModel().getReplicaNum() == 0)
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("Unable to find leader for region: " + regionReplicas.get(0).getRegionModel().getName()));
            String regionName = leader.getRegionModel().getName();
            // Metric presence checks
            if (!notRunningReplicas.isEmpty()) {
                LOGGER.warn("Skip region '{}:{}' auto splitting analyze, because some replicas are not running: {}", tableName, regionName, notRunningReplicas);
                continue;
            }
            Map<RegionDistributionModel, RegionMetrics> regionMetrics = new HashMap<>();
            for (RegionDistributionModel replica : regionReplicas) {
                RSMetrics rsMetrics = rsMetricsStorage.get(replica.getRegionServerModel());
                RegionMetrics metrics = rsMetrics == null ? null : rsMetrics.get(regionName, replica.getRegionModel().getReplicaNum());
                if (metrics != null) {
                    regionMetrics.put(replica, metrics);
                }
            }
            if (regionReplicas.size() != conf.getDataConfiguration().getReplicationFactor()
                    || regionMetrics.size() != regionReplicas.size()) {
                LOGGER.warn("Skip region '{}:{}' auto splitting analyze, because some replicas or their metrics are missing", tableName, regionName);
                continue;
            }
            long regionSizeBytes = regionMetrics.get(leader).getSize();
            if (regionSizeBytes >= triggerBytes || regionSizeBytes > batchThresholdBytes) {
                candidates.add(new SplitCandidate(regionName, regionSizeBytes, regionMetrics));
            }
        }

        // Regions that reached or exceeded the limit go first
        // Regions close to the limit must not trigger a split by themselves
        candidates.sort(Comparator.comparingLong(SplitCandidate::sizeBytes).reversed());
        List<String> regionNamesToSplit = new ArrayList<>();
        for (SplitCandidate candidate : candidates) {
            if (candidate.sizeBytes() < triggerBytes && regionNamesToSplit.isEmpty()) {
                break;
            }
            if (reserveSplitSpace(tableName, candidate, plannedDiskBytes)) {
                regionNamesToSplit.add(candidate.name());
                LOGGER.info("Initiating auto splitting for region: '{}:{}', size: {} bytes, trigger: {} bytes, batch margin: {}%",
                        tableName, candidate.name(), candidate.sizeBytes(), triggerBytes,
                        conf.getDataConfiguration().getRegionsAutoSplitBatchMarginPercent());
            }
        }
        return regionNamesToSplit;
    }

    private boolean reserveSplitSpace(TableName tableName, SplitCandidate candidate, Map<SplitDisk, Long> plannedDiskBytes) {
        long reservedBytes = Math.multiplyExact(conf.getDataConfiguration().getRegionsAutoSplitReservedMb(), 1024L * 1024);
        Map<SplitDisk, Long> requiredBytes = new HashMap<>();
        Map<SplitDisk, Long> availableBytes = new HashMap<>();
        candidate.metrics().forEach((replica, metrics) -> {
            SplitDisk disk = new SplitDisk(replica.getRegionServerModel().getHost(), metrics.getDiskName());
            requiredBytes.merge(disk, Math.max(candidate.sizeBytes(), metrics.getSize()), Math::addExact);
            // Despite its name, usageDiskSpace is populated from FileStore.getUsableSpace()
            availableBytes.merge(disk, metrics.getUsageDiskSpace() - reservedBytes, Math::min);
        });
        for (Map.Entry<SplitDisk, Long> entry : requiredBytes.entrySet()) {
            long available = availableBytes.get(entry.getKey()) - plannedDiskBytes.getOrDefault(entry.getKey(), 0L);
            if (entry.getValue() > available) {
                LOGGER.warn("Skip region '{}:{}' auto splitting: disk {}, needed {} bytes, available after planned splits {} bytes",
                        tableName, candidate.name(), entry.getKey(), entry.getValue(), available);
                return false;
            }
        }
        requiredBytes.forEach((disk, size) -> plannedDiskBytes.merge(disk, size, Math::addExact));
        return true;
    }

    private record SplitCandidate(String name, long sizeBytes, Map<RegionDistributionModel, RegionMetrics> metrics) {
    }

    private record SplitDisk(String host, String name) {
    }

    private boolean isAutoSplittingNotNeeded(TableName tableName) throws Exception {
        String tablePath = ZooKeeperContextHelper.getTablePath(tableName);
        ZookeeperStatus tableStatus = ZookeeperStatus.fromString(zooHelper.getNotNullString(tablePath));
        return tableStatus == ZookeeperStatus.CREATING
                || tableStatus == ZookeeperStatus.DROPPING
                || tableStatus == ZookeeperStatus.ENABLING
                || tableStatus == ZookeeperStatus.ASSIGNING
                || tableStatus == ZookeeperStatus.DISABLING
                || tableStatus == ZookeeperStatus.DISABLED
                || tableStatus == ZookeeperStatus.SPLITTING
                || tableStatus == ZookeeperStatus.RECOVERING;
    }

    private static class RSMetrics {
        private final Map<String, Map<Integer, RegionMetrics>> rsMetrics;

        RSMetrics(Map<String, Map<Integer, RegionMetrics>> rsMetrics) {
            this.rsMetrics = rsMetrics;
        }

        public RegionMetrics get(String regionName, Integer replicaNum) {
            Map<Integer, RegionMetrics> replicasMetrics = rsMetrics.get(regionName);
            if (replicasMetrics == null) {
                return null;
            }
            return rsMetrics.get(regionName).get(replicaNum);
        }
    }

    @Override
    public void close() throws IOException {
        requestsMonitoring.shutdownNow();
        scheduledExecutorService.shutdownNow();
    }
}
