/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.services;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.light.atalanta.TableName;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionServerModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Interaction with region servers.
 * Supports two types of interaction:
 * <ol>
 * <li>Functionalized: see {@link RegionServerConnector#atomic} and {@link RegionServerConnector#result};</li>
 * <li>Progressed: see {@link RegionServerConnector#monitor}.</li>
 * </ol>
 */
public class RegionServerConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionServerConnector.class);

    private final int reconnectTimeoutMs;

    private final int responseTimeoutMs;

    private final int progressCalculatorIntervalMs;

    private final Function<TableName, Boolean> checkIncomeTableRequestsEnabled;

    public RegionServerConnector(
            int reconnectTimeoutMs, int responseTimeoutMs, int progressCalculatorIntervalMs,
            Function<TableName, Boolean> checkIncomeTableRequestsEnabled
    ) {
        this.reconnectTimeoutMs = reconnectTimeoutMs;
        this.responseTimeoutMs = responseTimeoutMs;
        this.progressCalculatorIntervalMs = progressCalculatorIntervalMs;
        this.checkIncomeTableRequestsEnabled = checkIncomeTableRequestsEnabled;
    }

    /**
     * Executes specified operation synchronously on RS, i.e., waits for the completion.
     * Method takes *functionalized* type of gRPC action, meaning it's assumed that the process will be close to atomic.
     * E.g., stopping tasks on RS.
     */
    public void atomic(RegionServerModel rs, String operationName, FunctionalizedGrpcAction operation) {
        operation.accept(RegionServerGrpcStub.sync(rs, reconnectTimeoutMs));
    }

    /**
     * Same as atomic(), but returns the result.
     */
    public RegionServerProto.RegionDirectoryStructure result(RegionServerModel rs, String operationName, FunctionalizedGrpcActionWithResult operation) {
        RegionServerProto.RegionDirectoryStructure response = operation.accept(
                RegionServerGrpcStub.sync(rs, reconnectTimeoutMs)
        );
        Preconditions.checkArgument(response != null, "no response received from rs " + rs);
        return response;
    }

    /**
     * Executes specified operation synchronously on RS and waits for the result.
     * Method takes the *progressed* type of gRPC action,
     * meaning it's assumed that the process will be monitored for sustainability.
     * E.g., distributing, disabling, enabling, recovering or splitting region.
     * RS will be required to send the current operation state on an ongoing basis.
     * When no state is received within getRsProgressCalculatorIntervalMs time, operation will be aborted.
     */
    public void monitor(TableName tableName, String operationName, RegionDistributionModel rdm, String reserved, ProgressedGrpcAction action, boolean interruptible, AtomicBoolean interrupted) throws Throwable {
        RegionServerGrpcStub.Async asyncStub = RegionServerGrpcStub.async(rdm.getRegionServerModel(), reconnectTimeoutMs);
        AtomicReference<Throwable> exception = new AtomicReference<>();
        new RegionServerProgressedInteraction(
                responseTimeoutMs, progressCalculatorIntervalMs, checkIncomeTableRequestsEnabled
        ).sendRegionServerRequest(
                tableName, operationName, rdm, reserved, exception, asyncStub, action, interruptible, interrupted
        ).await();
        // Completed successfully logging is in the RegionServerProgressedInteraction's internals
        if (exception.get() != null) {
            throw exception.get();
        }
    }

    public void monitor(TableName tableName, String operationName, RegionDistributionModel rdm, String reserved, ProgressedGrpcAction action, boolean interruptible) throws Throwable {
        monitor(tableName, operationName, rdm, reserved, action, interruptible, new AtomicBoolean(false));
    }

    public void monitor(TableName tableName, String operationName, RegionDistributionModel rdm, String reserved, ProgressedGrpcAction action) throws Throwable {
        monitor(tableName, operationName, rdm, reserved, action, true, new AtomicBoolean(false));
    }
}
