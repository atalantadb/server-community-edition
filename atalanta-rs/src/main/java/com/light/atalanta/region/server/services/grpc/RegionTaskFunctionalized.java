/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.grpc;

import com.light.atalanta.RegionTaskType;
import com.light.atalanta.TableName;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.region.server.services.operations.RegionOperation;
import com.light.atalanta.util.ProtoConverters;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A region operation task. Created when a gRPC request arrives; kept in the run queue and in the task storage until it completes.
 * Returns data, so it can't send progress.
 */
public class RegionTaskFunctionalized implements RegionTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionTaskFunctionalized.class);

    private final RegionTaskContext context;

    private final RegionOperation regionOperation;

    private final RegionTaskDescription description;

    private final AtomicReference<StreamObserver<RegionServerProto.RegionDirectoryStructure>> responseObserver;

    public RegionTaskFunctionalized(RegionTaskContext context) {
        context.getRequestsCount().incrementAndGet();
        this.context = context;
        this.description = context.getDescription();
        this.responseObserver = new AtomicReference<>(context.getDataResponseObserver());
        this.regionOperation = context.getRegionOperation();
    }

    @Override
    public void run() {
        LOGGER.debug("Start executing request: '{}'", description);

        try {
            context.getProcessingRequestsCount().incrementAndGet();
            TableName tableName = ProtoConverters.toTableName(description.getRequest().getTableName());
            RegionModel regionModel = ProtoConverters.toRegionModel(description.getRequest());

            // Run the operation.
            // Unlike RegionTaskProgressed, the result is sent to the caller inside action(),
            // through this::sendResponse.
            // On error, it is sent from this class in the catch block below.
            regionOperation.functionalizedAction(tableName, regionModel, this, description.getRequest().getReservedReplicas());

            LOGGER.debug("Successfully finished executing request: {}", description);
        } catch (GrpcConnectException ex) {
            // If the error is a gRPC error, don't send it over gRPC
            LOGGER.error("Unable to perform request '{}'", description, ex);
        } catch (Exception ex) {
            // In all other cases send it over gRPC
            LOGGER.error("Unable to perform request '{}'", description, ex);
            // Send the error to the master
            sendError(ex);
        } finally {
            context.getRequestsCount().decrementAndGet();
            context.getProcessingRequestsCount().decrementAndGet();
        }
    }

    public void sendResponse(RegionServerProto.RegionDirectoryStructure response) throws Exception {
        try {
            responseObserver.get().onNext(response);
            responseObserver.get().onCompleted();
        } catch (Exception ex) {
            throw new GrpcConnectException(String.format("Unable to send result to caller for '%s'. Reason: '%s'", description, getExceptionText(ex)));
        }
    }

    private void sendError(Exception e) {
        try {
            LOGGER.debug("Sending error to caller for '{}'", description);
            responseObserver.get().onError(ProtoConverters.toProtoException(e));
        } catch (Exception ex) {
            LOGGER.error("Unable to send error to caller for '{}'. Reason: '{}'.", description, getExceptionText(ex));
        }
    }

    private String getExceptionText(Exception ex) {
        String reason;
        // Find out the cause of the error
        if (ex.getClass().equals(StatusRuntimeException.class) && ((StatusRuntimeException) ex).getStatus().getCode() == Status.Code.CANCELLED) {
            // The master is gone
            reason = "Connection reset";
        } else {
            // Another error
            reason = ex.getClass() + " " + ex.getMessage();
        }
        return reason;
    }

    @Override
    public TableName getTableName() {
        return ProtoConverters.toTableName(context.getDescription().getRequest().getTableName());
    }

    @Override
    public RegionTaskType getTaskType() {
        return context.getDescription().getTaskType();
    }

    @Override
    public void sendGracefulStopSignal() {
    }

    // Replaces the gRPC connection used to send the progress, the result or the error
    @Override
    public void setDataResponseObserver(StreamObserver<RegionServerProto.RegionDirectoryStructure> responseObserver) {
        throw new NotImplementedException("You can't change active gRPC connection for functionalized task");
    }

    @Override
    public StreamObserver<RegionServerProto.RegionDirectoryStructure> getDataResponseObserver() {
        return responseObserver.get();
    }

    @Override
    public void setProgressResponseObserver(StreamObserver<RegionServerProto.RegionOperationProgress> responseObserver) {
        throw new NotImplementedException("Unable to set progress response observer to functionalized task");
    }

    public static class GrpcConnectException extends Exception {
        GrpcConnectException(String message) {
            super(message);
        }
    }

    @Override
    public String toString() {
        return description.toString();
    }
}
