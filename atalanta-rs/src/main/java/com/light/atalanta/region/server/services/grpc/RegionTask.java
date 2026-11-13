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
import io.grpc.stub.StreamObserver;

public interface RegionTask extends Runnable {
    void setProgressResponseObserver(StreamObserver<RegionServerProto.RegionOperationProgress> responseObserver);
    void setDataResponseObserver(StreamObserver<RegionServerProto.RegionDirectoryStructure> responseObserver);
    StreamObserver<RegionServerProto.RegionDirectoryStructure> getDataResponseObserver();
    TableName getTableName();
    RegionTaskType getTaskType();
    void sendGracefulStopSignal();
}
