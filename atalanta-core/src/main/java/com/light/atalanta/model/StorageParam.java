/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.light.atalanta.exceptions.NoRetryException;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

public class StorageParam {

    private CompressType compressType = CompressType.none;
    private RecordCompressionType recordCompressionType = RecordCompressionType.lz;
    private boolean dictionary = true;
    private Long recordTargetCount = 0L;

    private static ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    public StorageParam() {

    }

    public StorageParam(CompressType compressType, RecordCompressionType recordCompressionType, Boolean dictionary, Long recordTargetCount, List<RegionDistributionModel> rdmList) {
        if (compressType != null && recordCompressionType != null
                && compressType != CompressType.none && recordCompressionType != RecordCompressionType.none) {
            throw new NoRetryException("You cannot use record compression and block compression at the same time!. block compression type: "
                    + compressType.name() + " record compression type: " + recordCompressionType.name());

        }
        RegionModel rm = rdmList.get(0).getRegionModel();
        if (dictionary != null && dictionary != rm.getDictionary()) {
            throw new NoRetryException("Dictionary modification is non implemented");
        }
        if (compressType == null) {
            if (recordCompressionType != null
                    && recordCompressionType != RecordCompressionType.none
                    && rm.getCompressType() != CompressType.none) {
                throw new NoRetryException("Cannot enable record compression for a table with block compression!. table block compression type: "
                        + rm.getCompressType().name() + ", request record compression type: " + recordCompressionType.name());
            }
        }
        if (recordCompressionType == null) {
            if (compressType != null
                    && compressType != CompressType.none
                    && rm.getRecordCompressionType() != RecordCompressionType.none) {
                throw new NoRetryException("Cannot enable block compression for a table with record compression!. request block compression type: "
                        + compressType.name() + ", table record compression type: " + rm.getRecordCompressionType().name());
            }
        }

        this.compressType = compressType != null ? compressType : rm.getCompressType();
        this.recordCompressionType = recordCompressionType != null ? recordCompressionType
                : rm.getRecordCompressionType();
        this.dictionary = dictionary != null ? dictionary : rm.getDictionary();
        this.recordTargetCount = recordTargetCount == null ? rm.getRecordTargetCount() : recordTargetCount;
        checkStorageCfg(this, rdmList);
    }

    public static StorageParam fromYaml(String yaml) throws JsonProcessingException {
        StorageParam prm = yamlMapper.readValue(yaml, StorageParam.class);
        return prm;
    }

    public CompressType getCompressType() {
        return compressType;
    }

    public void setCompressType(CompressType compressType) {
        this.compressType = compressType;
    }

    public RecordCompressionType getRecordCompressionType() {
        return recordCompressionType;
    }

    public void setRecordCompressionType(RecordCompressionType recordCompressionType) {
        this.recordCompressionType = recordCompressionType;
    }

    public boolean getDictionary() {
        return dictionary;
    }

    public void setDictionary(boolean dictionary) {
        this.dictionary = dictionary;
    }

    public Long getRecordTargetCount() {
        return recordTargetCount;
    }

    public void setRecordTargetCount(Long recordTargetCount) {
        this.recordTargetCount = recordTargetCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StorageParam that = (StorageParam) o;
        return dictionary == that.dictionary
                && compressType == that.compressType
                && recordCompressionType == that.recordCompressionType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(compressType, recordCompressionType, dictionary);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", StorageParam.class.getSimpleName() + "[", "]")
                .add("compressType=" + compressType)
                .add("recordCompressionType=" + recordCompressionType)
                .add("dictionary=" + dictionary)
                .add("recordTargetCount=" + recordTargetCount)
                .toString();
    }

    public boolean compareWithRegion(RegionModel regionModel) {
        return compareFormatWithRegion(regionModel)
                && recordTargetCount == regionModel.getRecordTargetCount();
    }

    public boolean compareFormatWithRegion(RegionModel regionModel) {
        return dictionary == regionModel.getDictionary()
                && compressType == regionModel.getCompressType()
                && recordCompressionType == regionModel.getRecordCompressionType();
    }

    public String toYaml() throws JsonProcessingException {
        return yamlMapper.writeValueAsString(this);
    }

    private void checkStorageCfg(StorageParam storageCfg, List<RegionDistributionModel> rdmList) {

        RegionModel regionModel = rdmList.stream().findAny().get().getRegionModel();

    }

}
