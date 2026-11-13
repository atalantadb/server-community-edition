/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.util;

import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import com.google.rpc.Status;
import com.light.atalanta.TableName;
import com.light.atalanta.grpc.ATALANTAProto;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.model.*;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.DEFAULT_DATA_BLOCK_SIZE;

/**
 * Proto <-> POJO converters.
 */
public final class ProtoConverters {

    private ProtoConverters() { }

    public static List<ByteString> toProtoSplitKeys(byte[][] splitKeys) {
        List<ByteString> protoSplitKeys = new ArrayList<>();
        for (byte[] bound : splitKeys) {
            protoSplitKeys.add(ByteString.copyFrom(bound));
        }
        return protoSplitKeys;
    }

    public static List<byte[]> protoSplitKeysToBytes(List<ByteString> protoSplitKeys) {
        // Number of boundaries: 5 regions have 4 boundaries
        return protoSplitKeys.stream()
                .map(ByteString::toByteArray)
                .sorted( (o1, o2) -> BytesComparator.compareTo(o1, o2))
                .collect(Collectors.toList());
    }

    public static ATALANTAProto.ProtoTableName toProtoTableName(TableName tableName) {
        return ATALANTAProto.ProtoTableName.newBuilder()
                .setNamespace(tableName.getNamespace())
                .setTable(tableName.getTableName())
                .build();
    }

    public static ATALANTAProto.ProtoTableName toProtoTableName(String namespaceOnly) {
        return ATALANTAProto.ProtoTableName.newBuilder()
                .setNamespace(namespaceOnly)
                .build();
    }

    public static TableName toTableName(ATALANTAProto.ProtoTableName tableName) {
        return TableName.valueOf(tableName.getNamespace(), tableName.getTable());
    }

    public static RegionServerModel toRegionServerModel(ATALANTAProto.ProtoRegionServer protoRegionServer) {
        return new RegionServerModel(protoRegionServer.getName(),
                protoRegionServer.getHost(),
                protoRegionServer.getJavaPort(),
                protoRegionServer.getCoreClientPort(), 0);
    }

    public static ATALANTAProto.ProtoRegionServer toProtoRegionServer(RegionServerModel regionServer) {
        return ATALANTAProto.ProtoRegionServer.newBuilder()
                .setName(regionServer.getName())
                .setHost(regionServer.getHost())
                .setJavaPort(regionServer.getJavaPort())
                .setCoreClientPort(regionServer.getCoreClientPort())
                .build();
    }

    public static RegionServerProto.RegionRequest toProtoRegion(TableName tableName, RegionModel region, String reserved) {
        RegionServerProto.RegionRequest.Builder requestBuilder = RegionServerProto.RegionRequest.newBuilder();
        requestBuilder.setTableName(toProtoTableName(tableName))
                .setRegionName(region.getName())
                .setReplicaNum(region.getReplicaNum())
                .setStatusId(region.getStatusId())
                .setBeginKey(ByteString.copyFrom(region.getBeginKey()))
                .setEndKey(ByteString.copyFrom(region.getEndKey()))
                .setReservedReplicas(reserved)
                .setBlockSize(region.getDataBlockSize())
                .setCompressType(region.getCompressType().name())
                .setRecordCompressType(region.getRecordCompressionType().name())
                .setDictionary(region.getDictionary())
                .setRecordTargetCount(region.getRecordTargetCount());
        if (region instanceof RegionFileRequest) {
            RegionFileRequest regionFileRequest = (RegionFileRequest) region;
            requestBuilder.setRegionFileRequest(RegionServerProto.RegionFileRequest.newBuilder()
                            .setRegionFile(RegionServerProto.RegionFile.newBuilder()
                                    .setFileName(regionFileRequest.getFileName())
                                    .setFileSize(regionFileRequest.getFileSize())
                            )
                            .setReceiverIp(regionFileRequest.getReceiverIp())
                            .setTcpPort(regionFileRequest.getTcpPort())
            );
        }
        return requestBuilder.build();
    }

    public static StatusRuntimeException toProtoException(Throwable e) {
        Status status = Status.newBuilder()
                .setCode(Code.ABORTED.getNumber())
                .setMessage(recursivelyGetMessage(new StringBuilder(), e).toString())
                .build();
        return StatusProto.toStatusRuntimeException(status);
    }

    public static RegionModel toRegionModel(RegionServerProto.RegionRequest regionRequest) {
        RegionModel regionModel = new RegionModel(regionRequest.getRegionName(),
                regionRequest.getReplicaNum(),
                regionRequest.getStatusId(),
                regionRequest.getBeginKey().toByteArray(),
                regionRequest.getEndKey().toByteArray(),
                regionRequest.getBlockSize() > 0 ? regionRequest.getBlockSize() : 2,
                StringUtils.isNotEmpty(regionRequest.getCompressType()) ? CompressType.valueOf(regionRequest.getCompressType()) : CompressType.none,
                RecordCompressionType.valueOf(regionRequest.getRecordCompressType()),
                regionRequest.getDictionary(),
                regionRequest.getRecordTargetCount());
        if (!regionRequest.getRegionFileRequest().equals(RegionServerProto.RegionFileRequest.getDefaultInstance())) {
            RegionServerProto.RegionFileRequest regionFileRequest = regionRequest.getRegionFileRequest();
            return new RegionFileRequest(
                    regionModel,
                    regionFileRequest.getRegionFile().getFileName(),
                    regionFileRequest.getRegionFile().getFileSize(),
                    regionFileRequest.getReceiverIp(),
                    regionFileRequest.getTcpPort()
            );
        }
        return regionModel;
    }

    /**
     * Builds a message chain from the exception and its causes.
     */
    public static StringBuilder recursivelyGetMessage(StringBuilder sb, Throwable e) {
        if (!sb.toString().equals(StringUtils.EMPTY)) {
            sb.append(" -> ");
        }
        sb.append(e.getClass());
        sb.append(": ");
        sb.append(e.getMessage());
        Throwable cause = e.getCause();
        if (cause != null) {
            recursivelyGetMessage(sb, cause);
        }
        return sb;
    }
}
