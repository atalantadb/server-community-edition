/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.operations.progressed;

import com.light.atalanta.TableName;
import com.light.atalanta.model.RegionModel;
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
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Enables work with a region.
 * Sends the START request to the core.
 */
public class StartRegion implements RegionOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartRegion.class.getName());

    private final RegionServerConfiguration regionServerConfiguration;

    private final ReplicaMapperProcessor replicaMapperProcessor;

    public StartRegion(RegionServerConfiguration regionServerConfiguration, ReplicaMapperProcessor replicaMapperProcessor) {
        this.regionServerConfiguration = regionServerConfiguration;
        this.replicaMapperProcessor = replicaMapperProcessor;
    }

    @Override
    public void progressedAction(TableName tableName, RegionModel regionModel, AtomicReference<String> taskProgress, AtomicBoolean interrupted, String reservedReplicas) throws Exception {
        LOGGER.info("Start starting region '{}:{}'", tableName, regionModel.toShortString());
        TransportConfiguration transportConfiguration = TransportConfiguration.defaultConfiguration().set(TransportConfiguration.ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY, String.valueOf(regionServerConfiguration.getCoreConfiguration().getSocketTimeoutMs()));
        updateTaskProgress(taskProgress, tableName, regionModel, "establishing core connection");
        try (TRSConnectionStub connectionStub = TransportConnectionFactory.createConnectionStub(
                transportConfiguration,
                regionServerConfiguration.getCoreConfiguration().getHost(),
                regionServerConfiguration.getCoreConfiguration().getRsPort())) {

            updateTaskProgress(taskProgress, tableName, regionModel, "sending core request");
            TransportMessage regionRequest = new TableRegionActionRequest(
                    tableName, regionModel, TableRegionActionRequest.Command.START,
                    replicaMapperProcessor.getReplicaDir(regionModel), reservedReplicas);
            RsResponse response = connectionStub.sendRequestToRs(regionRequest);

            updateTaskProgress(taskProgress, tableName, regionModel, "waiting & checking core response");
            if (response.isNegativeAnswer()) {
                throw new Exception("Core START operation error. Response: " + response);
            }

            // If we were interrupted, don't continue
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }

            updateTaskProgress(taskProgress, tableName, regionModel, "region started successfully");
        } catch (Exception ex) {
            throw new Exception("Error starting region: '" + tableName + ":" + regionModel.toShortString() + "'", ex);
        }

        LOGGER.info("Done starting region '{}:{}'", tableName, regionModel.toShortString());
    }

    @Override
    public void functionalizedAction(TableName tableName, RegionModel regionModel, RegionTaskFunctionalized task, String reservedReplicas) throws Exception {
        throw new NotImplementedException("Unable to run progressed task as functionalized");
    }

    private void updateTaskProgress(AtomicReference<String> taskProgress, TableName tableName, RegionModel regionModel, String text) {
        taskProgress.set(text);
        LOGGER.info("Starting region '{}:{}': {}", tableName, regionModel.toShortString(), text);
    }
}
