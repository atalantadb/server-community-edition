/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.runners;

import com.light.atalanta.TableName;
import com.light.atalanta.master.services.Master;
import com.light.atalanta.master.services.UniqueNameThreadFactory;
import com.light.atalanta.master.services.recovery.RecoveryRSContext;
import com.light.atalanta.master.vo.AtalantaDBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.light.atalanta.master.services.helpers.TableCandidateSelector.ALL_DB_REGIONS;

/**
 * Runs automatic region rebalancing with the RebalanceTableRegions algorithm.
 */
public class RegionsRebalanceRunner implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionsRebalanceRunner.class);

    private final AtomicLong rebalancingTables;
    private final ScheduledExecutorService requestsMonitoring;

    private final AtalantaDBConfiguration conf;
    private final ScheduledExecutorService scheduledExecutorService;
    private final RecoveryRSContext recoveryRSContext;

    private final ThreadFactory statsThreadFactory = new UniqueNameThreadFactory("atalanta-rebalance-stats-pool");
    private final ThreadFactory runnerThreadFactory = new UniqueNameThreadFactory("atalanta-rebalance-runner-pool");

    public RegionsRebalanceRunner(AtalantaDBConfiguration conf, RecoveryRSContext recoveryRSContext) {
        this.conf = conf;
        this.rebalancingTables = new AtomicLong(0L);
        this.requestsMonitoring = Executors.newSingleThreadScheduledExecutor(statsThreadFactory);
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(runnerThreadFactory);
        this.recoveryRSContext = recoveryRSContext;
    }

    public void action(Master master) {
        startMonitoringAsync(master);
        if (conf.getDataConfiguration().getOperationsMonitoringEnabled()) {
            startOperationsMonitoring();
        }
    }

    private void startOperationsMonitoring() {
        requestsMonitoring.scheduleAtFixedRate(() -> {
            long count = rebalancingTables.get();
            if (count > 0) {
                LOGGER.info("Auto rebalancing tables count: [{}]", count);
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    // Monitors the running RS and the region distribution across them.
    // Runs rebalancing when the distribution is skewed.
    private void startMonitoringAsync(Master master) {
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            LOGGER.trace("Auto rebalancing analyze iteration ...");
            if (recoveryRSContext.isSyncRunning()) {
                LOGGER.info("Skipping auto rebalancing check since SYNC process is running");
                return;
            }
            try {
                List<TableName> tablesToRebalance = master.getTableNamesToRebalance(ALL_DB_REGIONS, true);
                if (!tablesToRebalance.isEmpty()) {
                    LOGGER.info("Awaiting while auto rebalancing process will be completed");
                } else {
                    LOGGER.trace("No tables for auto rebalancing found");
                }
                master.rebalanceTableRegions(tablesToRebalance, rebalancingTables);
                LOGGER.info("Auto rebalancing process was completed");
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOGGER.error("Unable to perform auto regions rebalancing", ex);
            } catch (Exception ex) {
                LOGGER.error("Unable to perform auto reigons rebalancing", ex);
            }
        }, conf.getDataConfiguration().getRegionsAutoRebalanceInitialCheckDelayMs(), conf.getDataConfiguration().getRegionsAutoRebalanceCheckDelayMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        requestsMonitoring.shutdownNow();
        scheduledExecutorService.shutdownNow();
    }
}
