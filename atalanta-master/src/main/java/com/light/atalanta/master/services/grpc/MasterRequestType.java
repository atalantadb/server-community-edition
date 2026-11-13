/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.grpc;

public enum MasterRequestType {
    createNamespace,
    createTable,
    enableTable,
    disableTable,
    dropTable,
    moveRegion,
    splitRegion,
    startReplicasRebalanceAsync,
    syncTableReplicas,
    stopReplicasRebalance,
    startRegionsRebalanceAsync,
    stopRegionsRebalance,
    createSnapshot,
    cloneSnapshot,
    deleteSnapshot,
    startRegionServer,
    stopRegionServer,
    modifyTable,
}
