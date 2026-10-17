/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.scheduled;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.light.atalanta.exceptions.AtalantaTransportRuntimeException;
import com.light.atalanta.region.server.enums.CoreState;
import com.light.atalanta.region.server.services.DefaultDisposableService;
import com.light.atalanta.region.server.services.RegionServerInfoSupplier;
import com.light.atalanta.region.server.services.metrics.JmxRsMetrics;
import com.light.atalanta.region.server.utils.BashUtils;
import com.light.atalanta.region.server.utils.UniqueNameThreadFactory;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import com.light.atalanta.services.ZookeeperHelperExtended;
import com.light.atalanta.transport.TransportConfiguration;
import com.light.atalanta.transport.TransportConnectionFactory;
import com.light.atalanta.transport.messages.CanaryRequest;
import com.light.atalanta.transport.messages.PingResponse;
import com.light.atalanta.transport.TRSConnectionStub;
import com.light.atalanta.transport.messages.TransportMessage;
import com.light.atalanta.util.ShutdownManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import static com.light.atalanta.region.server.enums.CoreState.BAD_HEALTH;
import static com.light.atalanta.region.server.enums.CoreState.WORKING;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CoreHealthChecker extends AbstractScheduledService implements DefaultDisposableService {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoreHealthChecker.class);

    private static final String GET_CORE_PROCESS_PID = "ps ax | grep adb | grep %s.prop | grep -v grep| awk '{print $1}'";

    private final RegionServerConfiguration conf;
    private final ZookeeperHelperExtended zkHelper;
    private final RegionServerInfoSupplier serverInfo;
    private final ExecutorService executor;
    private final JmxRsMetrics jmxRsMetrics;
    private volatile boolean shuttingDown;

    private final ThreadFactory healthCheckerThreadFactory = new UniqueNameThreadFactory("atalanta-health-checker-pool");

    public CoreHealthChecker(RegionServerInfoSupplier serverInfo, ZookeeperHelperExtended zkHelper, RegionServerConfiguration conf, JmxRsMetrics jmxRsMetrics) {
        this.conf = conf;
        this.zkHelper = zkHelper;
        this.serverInfo = serverInfo;
        this.jmxRsMetrics = jmxRsMetrics;
        this.executor = Executors.newSingleThreadExecutor(healthCheckerThreadFactory);
    }

    @Override
    protected void runOneIteration() {
        if (shuttingDown) return;
        LOGGER.info("Start checking Core state.");

        final Future<CoreState> future = executor.submit(this::requestCoreState);

        CoreState state;
        try {
            LOGGER.info("Started waiting response for {}ms", conf.getHealthCheckerConfiguration().getCoreTimeoutMs());
            state = future.get(conf.getHealthCheckerConfiguration().getCoreTimeoutMs(), MILLISECONDS);
        } catch (Exception e) {
            LOGGER.error("Error waiting Core-state.", e);
            state = BAD_HEALTH;
        }
        checkState(state);
    }

    private CoreState requestCoreState() {
        TransportConfiguration transportConfiguration = TransportConfiguration.defaultConfiguration().set(TransportConfiguration.ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY, String.valueOf(conf.getCoreConfiguration().getSocketTimeoutMs()));
        try (TRSConnectionStub connectionStub =
                     TransportConnectionFactory.createConnectionStub(
                             transportConfiguration,
                             conf.getCoreConfiguration().getHost(),
                             conf.getCoreConfiguration().getRsPort())) {
            TransportMessage regionRequest = new CanaryRequest();
            connectionStub.sendMessage(regionRequest);
            try {
                PingResponse pingResponse = new PingResponse(connectionStub.receiveBytes());
                LOGGER.info("Metrics count: {}", pingResponse.getRegionMetrics().size());
                LOGGER.trace("Metrics: {}", pingResponse);
                jmxRsMetrics.addCoreStat(pingResponse);
            } catch (AtalantaTransportRuntimeException ex) {
                LOGGER.error("Wrong ping response from core!", ex);
                return BAD_HEALTH;
            }
            LOGGER.info("Core is working");
            return WORKING;
        } catch (Exception e) {
            LOGGER.error("Error getting Core-state.", e);
            return BAD_HEALTH;
        }
    }

    private synchronized void checkState(CoreState state) {
        if (shuttingDown) return;
        try {
            if (state == WORKING) {
                LOGGER.info("Core state = '{}'. All OK.", state);
                return;
            }

            LOGGER.error("Core didn't answer or State is: {}. Stopping RS and Core.", state);
            killCore();
        } catch (InterruptedException ex) {
            LOGGER.error("Thread interrupted.", ex);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOGGER.error("Error checking state.", e);
        }

        unregisterRs();
        ShutdownManager.exitWithError("Core hadn't answered. RS stopped.");
    }

    private void unregisterRs() {
        try {
            zkHelper.unregisterRs(serverInfo.regionServerId());
        } catch (Exception e) {
            LOGGER.error("Error unregistering RS in ZK.", e);
        }
    }

    public synchronized void beginShutdown() {
        shuttingDown = true;
        if (isRunning()) stopAsync();
        executor.shutdownNow();
    }

    /** Same TERM/KILL sequence as a ping failure, without unregistering or exiting RS. */
    public void shutdownCore() throws IOException, InterruptedException {
        killCore();
        while (true) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
            List<String> pids = BashUtils.getCommandResponse(BashUtils.runShellCommand(
                    String.format(GET_CORE_PROCESS_PID, conf.getHealthCheckerConfiguration().getCoreName())));
            if (pids.isEmpty()) return;
            if (pids.size() > 1) throw new IOException("Ambiguous core process list: " + pids);
            // SIGKILL delivery is asynchronous; the shutdown deadline bounds this wait
            Thread.sleep(50);
        }
    }

    private void killCore() throws IOException, InterruptedException {
        final String coreName = conf.getHealthCheckerConfiguration().getCoreName();
        Preconditions.checkNotNull(coreName, "Core name mustn't be null.");
        Process process = BashUtils.runShellCommand(String.format(GET_CORE_PROCESS_PID, coreName));
        List<String> pids = BashUtils.getCommandResponse(process);
        if (pids.size() != 1) {
            LOGGER.error("Can't kill Core due to wrong number of Core processes: {}", pids.size());
            return;
        }

        LOGGER.info("Start killing Core process with PID = '{}' gracefully.", pids.get(0));
        BashUtils.runShellCommand("kill " + pids.get(0));
        Thread.sleep(3_000);
        LOGGER.info("Check that Core was killed.");
        process = BashUtils.runShellCommand(String.format(GET_CORE_PROCESS_PID, coreName));
        pids = BashUtils.getCommandResponse(process);

        if (pids.size() > 1) {
            LOGGER.error("Can't kill Core due to wrong number of Core processes: {}", pids.size());
        } else if (pids.size() == 1) {
            LOGGER.warn("Core wasn't killed gracefully. Start killing hard.");
            BashUtils.runShellCommand("kill -9 " + pids.get(0));
        } else {
            LOGGER.info("Core was killed gracefully.");
        }
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(conf.getHealthCheckerConfiguration().getCoreCheckIntervalMs(),
                conf.getHealthCheckerConfiguration().getCoreCheckIntervalMs(), MILLISECONDS);
    }
}
