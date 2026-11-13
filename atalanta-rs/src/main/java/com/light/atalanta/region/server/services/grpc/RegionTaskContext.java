/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.grpc;

import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.region.server.services.operations.RegionOperation;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dependencies of a region operation task.
 */
public final class RegionTaskContext {

    /* gRPC service level dependencies */

    private final AtomicLong requestsCount;
    private final AtomicLong processingRequestsCount;
    private final Map<RegionTaskDescription, RegionTask> taskStorage;
    private final RegionServerConfiguration regionServerConfiguration;

    /* Task level dependencies */

    private final RegionTaskDescription description;
    private final RegionOperation regionOperation;
    private final StreamObserver<RegionServerProto.RegionOperationProgress> progressResponseObserver;
    private final StreamObserver<RegionServerProto.RegionDirectoryStructure> dataResponseObserver;

    private RegionTaskContext(AtomicLong requestsCount,
                              AtomicLong processingRequestsCount,
                              Map<RegionTaskDescription, RegionTask> taskStorage,
                              RegionServerConfiguration regionServerConfiguration,
                              RegionTaskDescription description,
                              RegionOperation regionOperation,
                              StreamObserver<RegionServerProto.RegionOperationProgress> progressResponseObserver,
                              StreamObserver<RegionServerProto.RegionDirectoryStructure> dataResponseObserver) {
        this.requestsCount = requestsCount;
        this.processingRequestsCount = processingRequestsCount;
        this.taskStorage = taskStorage;
        this.regionServerConfiguration = regionServerConfiguration;
        this.description = description;
        this.regionOperation = regionOperation;
        this.progressResponseObserver = progressResponseObserver;
        this.dataResponseObserver = dataResponseObserver;
    }

    public AtomicLong getRequestsCount() {
        return requestsCount;
    }

    public AtomicLong getProcessingRequestsCount() {
        return processingRequestsCount;
    }

    public Map<RegionTaskDescription, RegionTask> getTaskStorage() {
        return taskStorage;
    }

    public RegionServerConfiguration getRegionServerConfiguration() {
        return regionServerConfiguration;
    }

    public RegionTaskDescription getDescription() {
        return description;
    }

    public RegionOperation getRegionOperation() {
        return regionOperation;
    }

    public StreamObserver<RegionServerProto.RegionOperationProgress> getProgressResponseObserver() {
        return progressResponseObserver;
    }

    public StreamObserver<RegionServerProto.RegionDirectoryStructure> getDataResponseObserver() {
        return dataResponseObserver;
    }

    public static RegionTaskContextBuilder newBuilder() {
        return new RegionTaskContextBuilder();
    }

    public static class RegionTaskContextBuilder {
        private AtomicLong requestsCount;
        private AtomicLong processingRequestsCount;
        private Map<RegionTaskDescription, RegionTask> taskStorage;
        private RegionServerConfiguration regionServerConfiguration;
        private RegionTaskDescription description;
        private RegionOperation regionOperation;
        private StreamObserver<RegionServerProto.RegionOperationProgress> progressResponseObserver;
        private StreamObserver<RegionServerProto.RegionDirectoryStructure> dataResponseObserver;

        public RegionTaskContextBuilder setRequestsCount(AtomicLong requestsCount) {
            this.requestsCount = requestsCount;
            return this;
        }

        public RegionTaskContextBuilder setProcessingRequestsCount(AtomicLong processingRequestsCount) {
            this.processingRequestsCount = processingRequestsCount;
            return this;
        }

        public RegionTaskContextBuilder setTaskStorage(Map<RegionTaskDescription, RegionTask> taskStorage) {
            this.taskStorage = taskStorage;
            return this;
        }

        public RegionTaskContextBuilder setRegionServerConfiguration(RegionServerConfiguration regionServerConfiguration) {
            this.regionServerConfiguration = regionServerConfiguration;
            return this;
        }

        public RegionTaskContextBuilder setDescription(RegionTaskDescription description) {
            this.description = description;
            return this;
        }

        public RegionTaskContextBuilder setRegionOperation(RegionOperation regionOperation) {
            this.regionOperation = regionOperation;
            return this;
        }

        public RegionTaskContextBuilder setProgressResponseObserver(StreamObserver<RegionServerProto.RegionOperationProgress> progressResponseObserver) {
            this.progressResponseObserver = progressResponseObserver;
            return this;
        }

        public RegionTaskContextBuilder setDataResponseObserver(StreamObserver<RegionServerProto.RegionDirectoryStructure> dataResponseObserver) {
            this.dataResponseObserver = dataResponseObserver;
            return this;
        }

        public RegionTaskContext build() {
            return new RegionTaskContext(requestsCount, processingRequestsCount, taskStorage, regionServerConfiguration, description, regionOperation, progressResponseObserver, dataResponseObserver);
        }
    }
}
