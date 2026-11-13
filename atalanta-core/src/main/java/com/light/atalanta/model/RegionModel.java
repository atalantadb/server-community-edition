/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.model;

import com.light.atalanta.DbRegion;
import com.light.atalanta.Preconditions;
import com.light.atalanta.util.Bytes;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

import static com.light.atalanta.ATALANTAConstants.*;
import static com.light.atalanta.services.ZooKeeperContextHelper.*;

/**
 * Region information.
 * String form: {region_name}:{replica_num}:{status_id}:{key_range_begin}:{key_range_end}.
 */
@SuppressFBWarnings("EI_EXPOSE_REP")
public class RegionModel implements DbRegion {

    private final String name;

    private final int replicaNum;

    private final int statusId;

    private final byte[] beginKey;

    private final byte[] endKey;

    private final int dataBlockSize;

    private String toString = null;

    private final String toHumanString;

    private final String toShortString;

    private final String toIdentityString;

    private CompressType compressType;

    private RecordCompressionType recordCompressionType;

    private boolean dictionary;

    private Long recordTargetCount;

    private String regioninfo = null;

    // Visible for testing
    public RegionModel(String name, int replicaNum, int statusId, byte[] beginKey, byte[] endKey) {
        this(name, replicaNum, statusId, beginKey, endKey, DEFAULT_DATA_BLOCK_SIZE, CompressType.none,
                RecordCompressionType.none, false, 0L);
    }

    public RegionModel(String name, int replicaNum, int statusId, byte[] beginKey, byte[] endKey, int dataBlockSize,
                       CompressType compressType, RecordCompressionType recordCompressionType, Boolean dictionary,
                       Long recordTargetCount) {
        Preconditions.checkNotNullOrEmpty(name, "Region name can't be null or empty");
        Preconditions.checkArgument(replicaNum >= 0, "Region replica number must be greater than zero");
        Preconditions.checkArgument((statusId >= MINIMUM_REGION_STATUS_VALUE && statusId <= MAXIMUM_REGION_STATUS_VALUE) || isReferencingRegionStatus(statusId),
                "Region status id must be between " + MINIMUM_REGION_STATUS_VALUE + " and " + MAXIMUM_REGION_STATUS_VALUE + ". Current: " + statusId);
        Preconditions.checkArgument(beginKey != null, "Region key range begin can't be null");
        Preconditions.checkArgument(endKey != null && endKey.length > 0, "Region key range end can't be null or empty");
        this.name = name;
        this.replicaNum = replicaNum;
        this.statusId = statusId;
        this.beginKey = Bytes.cloneBytesArray(beginKey);
        this.endKey = Bytes.cloneBytesArray(endKey);
        this.dataBlockSize = dataBlockSize;
        this.compressType = compressType;
        this.recordCompressionType = recordCompressionType;
        this.dictionary = dictionary;
        this.recordTargetCount = recordTargetCount;
        this.toHumanString = String.join(CSV_SEPARATOR, name + "_" + replicaNum,
                String.valueOf(statusId),
                Bytes.toHex(beginKey).equals(StringUtil.EMPTY_STRING) ? "0" : Bytes.toHex(beginKey),
                Bytes.toHex(endKey));
        this.toShortString = String.join(CSV_SEPARATOR, name, String.valueOf(replicaNum), String.valueOf(statusId));
        this.toIdentityString = name + "_" + replicaNum;
    }

    private String getRegionInfo() {
        return "COMPRESSION=" + this.compressType + System.lineSeparator() +
                "RECORD_COMPRESSION=" + this.recordCompressionType + System.lineSeparator() +
                "DICTIONARY=" + String.valueOf(this.dictionary).toUpperCase() + System.lineSeparator() +
                "RECORD_TARGET_COUNT=" + this.recordTargetCount + System.lineSeparator();
    }

    private String getToStringStr() {
        return String.join(CSV_SEPARATOR, this.name,
                String.valueOf(this.replicaNum), String.valueOf(this.statusId),
                Bytes.toHex(this.beginKey).equals(StringUtil.EMPTY_STRING) ? "0" : Bytes.toHex(this.beginKey),
                Bytes.toHex(this.endKey),
                String.valueOf(this.dataBlockSize),
                this.compressType.name(),
                this.recordCompressionType.name(),
                String.valueOf(this.dictionary),
                String.valueOf(this.recordTargetCount));
    }

    public static RegionModel fromBase64String(String base64String) {
        if (StringUtils.isEmpty(base64String)) {
            return null;
        }
        String[] regionInfo = base64String.split(CSV_SEPARATOR, -1);
        Preconditions.checkArgument (regionInfo.length == 5 || regionInfo.length == 7 || regionInfo.length == 10,
                "Unable to create region from base64 string: " + base64String + " it is damaged.");
        String regionName = regionInfo[0];
        int replicaNum = Integer.parseInt(regionInfo[1]);
        int statusId = Integer.parseInt(regionInfo[2]);
        byte[] beginKey = Base64.getDecoder().decode(regionInfo[3]);
        byte[] endKey = Base64.getDecoder().decode(regionInfo[4]);
        int dataBlockSize = regionInfo.length > 5 ? Integer.parseInt(regionInfo[5]) : DEFAULT_DATA_BLOCK_SIZE;
        CompressType compressType =  regionInfo.length > 6 ? CompressType.valueOf(regionInfo[6]) : CompressType.none;
        RecordCompressionType recordCompressionType = RecordCompressionType.lz;
        boolean dictionary = true;
        long recordTargetCount = 0L;
        if (regionInfo.length == 10) {
            recordCompressionType = RecordCompressionType.valueOf(regionInfo[7]);
            Preconditions.checkArgument("true".equalsIgnoreCase(regionInfo[8])
                            || "false".equalsIgnoreCase(regionInfo[8]),
                    "Invalid dictionary flag in region: " + base64String);
            dictionary = Boolean.parseBoolean(regionInfo[8]);
            recordTargetCount = Long.parseLong(regionInfo[9]);
        }
        return new RegionModel(regionName, replicaNum, statusId, beginKey, endKey, dataBlockSize, compressType,
                recordCompressionType, dictionary, recordTargetCount);
    }

    public String getName() {
        return name;
    }

    public int getReplicaNum() {
        return replicaNum;
    }

    public int getStatusId() {
        return statusId;
    }

    public byte[] getBeginKey() {
        return beginKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    /**
     * Returns the string form with Base64-encoded keys.
     */
    public String toBase64String() {
        return String.join(CSV_SEPARATOR, name,
                String.valueOf(replicaNum), String.valueOf(statusId),
                Base64.getEncoder().encodeToString(beginKey),
                Base64.getEncoder().encodeToString(endKey),
                String.valueOf(dataBlockSize),
                compressType.name(),
                recordCompressionType.name(),
                String.valueOf(dictionary),
                String.valueOf(recordTargetCount));
    }

    public String toBase64StringForZk() {
        return String.join(CSV_SEPARATOR, name,
                String.valueOf(replicaNum), String.valueOf(statusId),
                Base64.getEncoder().encodeToString(beginKey),
                Base64.getEncoder().encodeToString(endKey),
                String.valueOf(dataBlockSize),
                compressType.name());
    }

    @Override
    public String toString() {
        if (toString == null) {
            this.toString = getToStringStr();
        }
        return toString;
    }

    public String toHumanString() {
        return toHumanString;
    }

    public String toShortString() {
        return toShortString;
    }

    public String toIdentityString() {
        return toIdentityString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RegionModel that = (RegionModel) o;

        if (replicaNum != that.replicaNum) {
            return false;
        }
        if (statusId != that.statusId) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (!Arrays.equals(beginKey, that.beginKey)) {
            return false;
        }
        return Arrays.equals(endKey, that.endKey);
    }

    // 16-character UUID without separators
    public static String makeRandomRegionName() {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        uuid = uuid.substring(uuid.length() / 2);
        return uuid;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + replicaNum;
        result = 31 * result + statusId;
        result = 31 * result + Arrays.hashCode(beginKey);
        result = 31 * result + Arrays.hashCode(endKey);
        return result;
    }

    public int getDataBlockSize() {
        return dataBlockSize;
    }

    public CompressType getCompressType() {
        return compressType;
    }

    @Override
    public byte[] getStartKey() {
        return beginKey;
    }

    public RecordCompressionType getRecordCompressionType() {
        return recordCompressionType;
    }

    public boolean getDictionary() {
        return dictionary;
    }

    public Long getRecordTargetCount() {
        return recordTargetCount;
    }

    public void writeRegionInfoFile(String dir) throws IOException {
        if (regioninfo == null) {
            this.regioninfo = getRegionInfo();
        }
        writeRegionInfoFile(dir, regioninfo);
    }

    public static void writeRegionInfoFile(String dir, StorageParam storageParam) throws IOException {
        String regioninfo = "COMPRESSION=" + storageParam.getCompressType() + System.lineSeparator() +
                "RECORD_COMPRESSION=" + storageParam.getRecordCompressionType() + System.lineSeparator() +
                "DICTIONARY=" + String.valueOf(storageParam.getDictionary()).toLowerCase() + System.lineSeparator() +
                "RECORD_TARGET_COUNT=" + storageParam.getRecordTargetCount() + System.lineSeparator();
        writeRegionInfoFile(dir, regioninfo);
    }

    private static void writeRegionInfoFile(String dir, String regioninfo) throws IOException {
        FileUtils.writeStringToFile(new File(dir + "/" + ".regioninfo"),
                regioninfo.toLowerCase(), StandardCharsets.UTF_8);
    }

    public void setDictionary(boolean dictionary) {
        this.dictionary = dictionary;
        this.toString = null;
        this.regioninfo = null;
    }

    public void setRecordTargetCount(Long recordTargetCount) {
        this.recordTargetCount = recordTargetCount;
        this.toString = null;
        this.regioninfo = null;
    }

    public void setRecordCompressionType(RecordCompressionType recordCompressionType) {
        this.recordCompressionType = recordCompressionType;
        this.toString = null;
        this.regioninfo = null;
    }

    public void setCompressType(CompressType compressType) {
        this.compressType = compressType;
        this.toString = null;
        this.regioninfo = null;
    }
}
