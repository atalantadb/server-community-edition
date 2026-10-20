/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.transport.RegionPosition;
import com.light.atalanta.util.Bytes;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.light.atalanta.util.Bytes.alignToSize;

public class Scan implements ScanRows {
    private static final Logger LOGGER = LoggerFactory.getLogger(Scan.class);

    private byte[] startKey;
    private byte[] stopKey;
    private int blockNumber;
    private int bs = 0xFFFF;
    private int regionPosition;
    private int cacheSize = 100;
    private boolean limited = false;

    // Column family -> requested qualifiers
    private Map<String, Set<String>> familyMap = new HashMap<>();
    private int batch; // Not implemented
    private int caching; // Not implemented
    private int maxResultSize; // Not implemented
    private DbSingleColumnValueFilter singleColumnValueFilter; // Not implemented

    // TODO: Temporary solution
    private static boolean isNewAPI = false;

    public static void setupNewAPI() {
        isNewAPI = true;
    }

    public Scan() {
        if (isNewAPI) {
            this.startKey = Bytes.START_BYTES_UNIFORM;
            this.stopKey = Bytes.START_BYTES_UNIFORM;
        } else {
            this.startKey = alignToSize(Bytes.cloneBytesArray(Bytes.START_BYTES_UNIFORM), 8);
            this.stopKey = alignToSize(Bytes.cloneBytesArray(Bytes.END_BYTES_UNIFORM), 8);
        }

        this.blockNumber = 0;
        this.regionPosition = RegionPosition.UNSET.getPosition();
    }

    public Scan(byte[] startKey) {
        if (isNewAPI) {
            this.startKey = startKey;
            this.stopKey = new byte[0];
        } else {
            this.startKey = alignToSize(Bytes.cloneBytesArray(startKey), 8);
            this.stopKey = alignToSize(Bytes.cloneBytesArray(Bytes.END_BYTES_UNIFORM), 8);
        }

        this.blockNumber = 0;
        this.regionPosition = RegionPosition.UNSET.getPosition();
    }

    public Scan(byte[] startKey, byte[] stopKey) {
        if (startKey.length > 4096) {
            throw new IllegalArgumentException("Start key too long: " + startKey.length);
        }
        if (stopKey.length > 4096) {
            throw new IllegalArgumentException("End key too long: " + stopKey.length);
        }

        if (isNewAPI) {
            this.startKey = startKey;
            this.stopKey = stopKey;
        } else {
            if (startKey.length > 8) {
                LOGGER.warn("Start key is greater than 8 bytes. It can lead unexpected behavior while scanning");
            }
            if (stopKey.length > 8) {
                LOGGER.warn("Stop key is greater than 8 bytes. It can lead unexpected behavior while scanning");
            }
            this.startKey = alignToSize(Bytes.cloneBytesArray(startKey), 8);
            this.stopKey = alignToSize(Bytes.cloneBytesArray(stopKey), 8);
        }

        this.blockNumber = 0;
        this.regionPosition = RegionPosition.UNSET.getPosition();
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getStopKey() {
        return stopKey;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
        this.limited = true;
    }

    public boolean isLimited() {
        return limited;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    public void setBlockNumber(int blockNumber) {
        this.blockNumber = blockNumber;
    }

    public int getRegionPosition() {
        return regionPosition;
    }

    public void setRegionPosition(int regionPosition) {
        this.regionPosition = regionPosition;
    }

    @Override
    public ScanRows addColumn(byte[] family, byte[] qualifier) {
        String familyStr = new String(family, StandardCharsets.UTF_8);
        Set<String> set = this.familyMap.get(familyStr);
        if (set == null) {
            set = new TreeSet<>();
        }
        set.add(qualifier == null ? StringUtils.EMPTY : new String(qualifier, StandardCharsets.UTF_8));
        this.familyMap.put(familyStr, set);
        return this;
    }

    @Override
    public void setFilter(DbSingleColumnValueFilter singleColumnValueFilter) {
        this.singleColumnValueFilter = singleColumnValueFilter;
    }

    @Override
    public void setBatch(int batch) {
        this.batch = batch;
    }

    @Override
    public void setCaching(int caching) {
        this.caching = caching;
    }

    @Override
    public void setMaxResultSize(int maxResultSize) {
        this.maxResultSize = maxResultSize;
    }

    @Override
    public void setLimit(int limit) {
        this.cacheSize = limit;
        this.limited = true;
    }

    @Override
    public Scan withStartRow(byte[] startKey) {
        if (isNewAPI) {
            this.startKey = startKey;
        } else {
            if (startKey.length > 8) {
                LOGGER.warn("Start key is greater than 8 bytes. It can lead unexpected behavior while scanning");
            }
            this.startKey = alignToSize(Bytes.cloneBytesArray(startKey), 8);
        }
        return this;
    }

    @Override
    public Scan withStopRow(byte[] stopKey) {
        if (isNewAPI) {
            this.stopKey = stopKey;
        } else {
            if (stopKey.length > 8) {
                LOGGER.warn("Stop key is greater than 8 bytes. It can lead unexpected behavior while scanning");
            }
            this.stopKey = alignToSize(Bytes.cloneBytesArray(stopKey), 8);
        }
        return this;
    }

    public Map<String, Set<String>> getFamilyMap() {
        return familyMap;
    }

    public int getBs() {
        return this.bs;
    }

    public void setBs(int bs) {
        this.bs = bs;
    }

    @Override
    public String toString() {
        return "Scan{" +
                "startKey=" + Arrays.toString(startKey) +
                ", stopKey=" + Arrays.toString(stopKey) +
                ", blockNumber=" + blockNumber +
                ", bs=" + bs +
                ", regionPosition=" + regionPosition +
                ", cacheSize=" + cacheSize +
                ", limited=" + limited +
                ", familyMap=" + familyMap +
                ", batch=" + batch +
                ", caching=" + caching +
                ", maxResultSize=" + maxResultSize +
                ", singleColumnValueFilter=" + singleColumnValueFilter +
                '}';
    }
}
