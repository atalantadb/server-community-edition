/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.operations;

import com.light.atalanta.TableName;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.region.server.services.grpc.RegionTaskFunctionalized;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Region operation.
 */
public interface RegionOperation {

    // An operation that does the work and sends progress regularly
    // "interrupted" is a reference flag can be used not just within a runner of tasks in RS (RegionTaskProgressed),
    // but also within the tasks themselves (such as MoveRegion), because some of them (again, MoveRegion)
    // do send progressed tasks to other RS, so we need to have a mechanism to interrupt source as well
    void progressedAction(TableName tableName, RegionModel regionModel, AtomicReference<String> taskProgress, AtomicBoolean interrupted, String reservedReplicas) throws Exception;

    // An operation that sends data but doesn't send progress
    void functionalizedAction(TableName tableName, RegionModel regionModel, RegionTaskFunctionalized task, String reservedReplicas) throws Exception;
}
