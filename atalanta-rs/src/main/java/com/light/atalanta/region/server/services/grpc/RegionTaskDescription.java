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
import com.light.atalanta.util.ProtoConverters;

import java.util.Locale;
import java.util.Objects;

/**
 * Region operation task information.
 * A separate class so that it can be used as a Map key.
 */
public class RegionTaskDescription {
    private final RegionServerProto.RegionRequest request;
    private final RegionTaskType taskType;
    private final TableName tableName;

    public RegionTaskDescription(RegionTaskType taskType, RegionServerProto.RegionRequest request) {
        this.taskType = taskType;
        this.request = request;
        this.tableName = ProtoConverters.toTableName(request.getTableName());
    }

    public RegionServerProto.RegionRequest getRequest() {
        return request;
    }

    public RegionTaskType getTaskType() {
        return taskType;
    }

    public TableName getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RegionTaskDescription that = (RegionTaskDescription) o;
        return request.equals(that.request) && taskType == that.taskType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(request, taskType);
    }

    @Override
    public String toString() {
        return String.join(":",
                taskType.toString().toLowerCase(Locale.ROOT),
                request.getTableName().getNamespace(),
                request.getTableName().getTable(),
                ProtoConverters.toRegionModel(request).toShortString());
    }
}
