/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.services;

import com.light.atalanta.grpc.RegionServerProto;
import io.grpc.stub.StreamObserver;

public interface ProgressedGrpcAction {
    void accept(
            RegionServerGrpcStub.Async stub,
            RegionServerProto.RegionRequest request,
            StreamObserver<RegionServerProto.RegionOperationProgress> streamObserver
    );
}
