/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.tool;

/** Immutable configuration collected by the interactive questionnaire. */
public final class DataToolConfig {

    public static final int MAX_DICTIONARY_QUALIFIER_BYTES = 60 * 1024;

    private final ConnectionType connectionType;
    private final int regionCount;
    private final boolean recordCompressionEnabled;
    private final boolean qualifierDictionaryRequested;
    private final long recordCount;
    private final int writerThreadCount;
    private final boolean batchWritesEnabled;
    private final boolean countRecordsEnabled;
    private final boolean readRecordsEnabled;
    private final boolean scanRecordsEnabled;

    public DataToolConfig(ConnectionType connectionType,
                          int regionCount,
                          boolean recordCompressionEnabled,
                          boolean qualifierDictionaryRequested) {
        this(connectionType, regionCount, recordCompressionEnabled,
                qualifierDictionaryRequested, DataWriter.DEFAULT_RECORD_COUNT,
                100, true, true, true, true);
    }

    public DataToolConfig(ConnectionType connectionType,
                          int regionCount,
                          boolean recordCompressionEnabled,
                          boolean qualifierDictionaryRequested,
                          int writerThreadCount,
                          boolean batchWritesEnabled) {
        this(connectionType, regionCount, recordCompressionEnabled,
                qualifierDictionaryRequested, DataWriter.DEFAULT_RECORD_COUNT, writerThreadCount,
                batchWritesEnabled, true, true, true);
    }

    public DataToolConfig(ConnectionType connectionType,
                          int regionCount,
                          boolean recordCompressionEnabled,
                          boolean qualifierDictionaryRequested,
                          int writerThreadCount,
                          boolean batchWritesEnabled,
                          boolean countRecordsEnabled,
                          boolean readRecordsEnabled) {
        this(connectionType, regionCount, recordCompressionEnabled,
                qualifierDictionaryRequested, DataWriter.DEFAULT_RECORD_COUNT,
                writerThreadCount, batchWritesEnabled, countRecordsEnabled,
                readRecordsEnabled, true);
    }

    public DataToolConfig(ConnectionType connectionType,
                          int regionCount,
                          boolean recordCompressionEnabled,
                          boolean qualifierDictionaryRequested,
                          long recordCount,
                          int writerThreadCount,
                          boolean batchWritesEnabled,
                          boolean countRecordsEnabled,
                          boolean readRecordsEnabled) {
        this(connectionType, regionCount, recordCompressionEnabled,
                qualifierDictionaryRequested, recordCount, writerThreadCount,
                batchWritesEnabled, countRecordsEnabled, readRecordsEnabled, true);
    }

    public DataToolConfig(ConnectionType connectionType,
                          int regionCount,
                          boolean recordCompressionEnabled,
                          boolean qualifierDictionaryRequested,
                          long recordCount,
                          int writerThreadCount,
                          boolean batchWritesEnabled,
                          boolean countRecordsEnabled,
                          boolean readRecordsEnabled,
                          boolean scanRecordsEnabled) {
        if (connectionType == null) {
            throw new IllegalArgumentException("Connection type is required.");
        }
        if (regionCount <= 0) {
            throw new IllegalArgumentException("Region count must be greater than zero.");
        }
        if (recordCount <= 0) {
            throw new IllegalArgumentException("Record count must be greater than zero.");
        }
        if (writerThreadCount <= 0) {
            throw new IllegalArgumentException("Writer thread count must be greater than zero.");
        }
        if ((long) regionCount * DataWriter.PUTS_PER_REGION > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Region count is too large for batch writes.");
        }
        this.connectionType = connectionType;
        this.regionCount = regionCount;
        this.recordCompressionEnabled = recordCompressionEnabled;
        this.qualifierDictionaryRequested = qualifierDictionaryRequested;
        this.recordCount = recordCount;
        this.writerThreadCount = writerThreadCount;
        this.batchWritesEnabled = batchWritesEnabled;
        this.countRecordsEnabled = countRecordsEnabled;
        this.readRecordsEnabled = readRecordsEnabled;
        this.scanRecordsEnabled = scanRecordsEnabled;
    }

    public ConnectionType getConnectionType() {
        return connectionType;
    }

    public int getRegionCount() {
        return regionCount;
    }

    public boolean isRecordCompressionEnabled() {
        return recordCompressionEnabled;
    }

    public boolean isQualifierDictionaryRequested() {
        return qualifierDictionaryRequested;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public int getWriterThreadCount() {
        return writerThreadCount;
    }

    public boolean isBatchWritesEnabled() {
        return batchWritesEnabled;
    }

    public boolean isCountRecordsEnabled() {
        return countRecordsEnabled;
    }

    public boolean isReadRecordsEnabled() {
        return readRecordsEnabled;
    }

    public boolean isScanRecordsEnabled() {
        return scanRecordsEnabled;
    }

    public int getBatchSize() {
        return batchWritesEnabled ? regionCount * DataWriter.PUTS_PER_REGION : 1;
    }

    /**
     * The dictionary can only be enabled when requested and the combined
     * qualifier size is strictly less than 60 KiB.
     */
    public boolean isQualifierDictionaryEnabled(long totalQualifierBytes) {
        if (totalQualifierBytes < 0) {
            throw new IllegalArgumentException("Total qualifier size cannot be negative.");
        }
        return qualifierDictionaryRequested
                && totalQualifierBytes < MAX_DICTIONARY_QUALIFIER_BYTES;
    }
}
