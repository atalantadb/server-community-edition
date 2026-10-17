/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.api.DbTable;
import com.light.atalanta.transport.values.RegionInfo;
import com.light.atalanta.util.BytesComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class ClientScanner implements ResultScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientScanner.class);

    private Scan scan;
    private final Table table;

    private int currentRegionNumber = 0;
    private final LinkedList<Result> currentResults;

    private RegionScanner currentRegionScanner;

    private byte[] currentRegionStartKey;
    private byte[] currentRegionEndKey;

    public ClientScanner(ScanRows scan, DbTable table) {
        this.scan = (Scan) scan;
        this.table = (Table) table;
        this.currentResults = new LinkedList<>();
        this.scan.setRegionPosition(this.table.getSortedRegions().get(currentRegionNumber).getPosition());
        this.currentRegionScanner = new RegionScanner(this.scan, this.table);
        this.currentRegionStartKey = this.table.getSortedRegions().get(currentRegionNumber).getStartKey();
        this.currentRegionEndKey = this.table.getSortedRegions().get(currentRegionNumber).getEndKey();

        byte[] scanStartKey = ((Scan) scan).getStartKey();
        if (scanStartKey != null && scanStartKey.length != 0) {
            currentRegionNumber = this.table.getSortedRegions().size() - 1;
            for (int i = 0; i < this.table.getSortedRegions().size(); ++i) {
                if (BytesComparator.BYTES_COMPARATOR.compare(this.table.getSortedRegions().get(i).getStartKey(), scanStartKey) <= 0 &&
                        BytesComparator.BYTES_COMPARATOR.compare(this.table.getSortedRegions().get(i).getEndKey(), scanStartKey) > 0) {
                    currentRegionNumber = i;
                    break;
                }
            }
            this.currentRegionStartKey = this.table.getSortedRegions().get(currentRegionNumber).getStartKey();
            this.currentRegionEndKey = this.table.getSortedRegions().get(currentRegionNumber).getEndKey();
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("ClientScanner initialized: scanStartKey={}, scanStopKey={}, currentRegionNumber={}, currentRegionRange=[{}, {}]",
                    Arrays.toString(this.scan.getStartKey()), Arrays.toString(this.scan.getStopKey()), currentRegionNumber,
                    Arrays.toString(currentRegionStartKey), Arrays.toString(currentRegionEndKey));
        }
    }

    @Override
    public Result next() throws IOException {
        if (currentRegionScanner == null) {
            return null;
        }
        if (currentResults.isEmpty()) {
            Result r = null;
            if (currentRegionScanner.fullScanned()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Current region fully scanned: regionNumber={}, range=[{}, {})", currentRegionNumber, Arrays.toString(currentRegionStartKey), Arrays.toString(currentRegionEndKey));
                }
                currentRegionScanner = nextRegionScanner();
            }
            while (r == null) {
                if (currentRegionScanner == null || currentRegionScanner.fullScanned()) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Switching to next region scanner: currentRegionNumber={}, currentRange=[{}, {})", currentRegionNumber, Arrays.toString(currentRegionStartKey), Arrays.toString(currentRegionEndKey));
                    }
                    currentRegionScanner = nextRegionScanner();
                    if (currentRegionScanner == null) {
                        LOGGER.debug("No next region scanner found, scan is finished");
                        return null;
                    }
                }
                Result[] rs = scanRegion();
                if (rs != null) {
                    currentResults.addAll(Arrays.asList(rs));
                }
                r = currentResults.poll();
            }
            return r;
        }

        return currentResults.poll();
    }

    private Result[] scanRegion() throws IOException {
        try {
            return currentRegionScanner.next(scan.getCacheSize());
        } catch (RuntimeException e) {
            LOGGER.warn("Exception during scan, will restart from currentRegionStartKey={}. Error: {}", Arrays.toString(currentRegionStartKey), e.getMessage());
            handleTopologyChange();
            return new Result[0];
        }
    }

    private void handleTopologyChange() {
        List<RegionInfo> freshRegions = this.table.getSortedRegions();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Handle topology change: currentRegionStartKey={}, regionsInSnapshot={}", Arrays.toString(currentRegionStartKey), freshRegions.size());
        }

        int targetIdx = -1;
        for (int i = 0; i < freshRegions.size(); i++) {
            RegionInfo ri = freshRegions.get(i);
            int cmpStart = BytesComparator.BYTES_COMPARATOR.compare(ri.getStartKey(), currentRegionStartKey);
            int cmpEnd = BytesComparator.BYTES_COMPARATOR.compare(ri.getEndKey(), currentRegionStartKey);
            if (cmpStart <= 0 && cmpEnd > 0) {
                targetIdx = i;
                break;
            }
        }
        if (targetIdx < 0) {
            targetIdx = freshRegions.size() - 1;
        }
        currentRegionNumber = targetIdx;
        RegionInfo target = freshRegions.get(targetIdx);

        scan = new Scan(currentRegionStartKey, scan.getStopKey());
        scan.setRegionPosition(target.getPosition());
        scan.setBlockNumber(0);
        currentRegionEndKey = target.getEndKey();
        currentRegionScanner = new RegionScanner(scan, table);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Topology change resolved: newRegionIndex={}, newRange=[{}, {}), regionPosition={}", currentRegionNumber, Arrays.toString(currentRegionStartKey), Arrays.toString(currentRegionEndKey), target.getPosition());
        }
    }

    private RegionScanner nextRegionScanner() {
        currentRegionNumber++;
        if (this.table.getSortedRegions().size() > currentRegionNumber) {
            LOGGER.debug("Next region selected from current snapshot: regionIndex={}", currentRegionNumber);
            return buildRegionScanner(this.table.getSortedRegions().get(currentRegionNumber));
        }
        List<RegionInfo> freshRegions = table.getSortedRegions();
        if (freshRegions.isEmpty()) {
            LOGGER.warn("No regions in snapshot while resolving next region");
            return null;
        }
        int oldRegionCount = this.table.getSortedRegions().size();

        byte[] continueFromKey = currentRegionEndKey;
        boolean splitDetected = freshRegions.size() > oldRegionCount;

        if (splitDetected) {
            continueFromKey = currentRegionStartKey;
        } else {
            byte[] lastScanStartKey = scan.getStartKey();
            for (RegionInfo ri : freshRegions) {
                int cmpStart = BytesComparator.BYTES_COMPARATOR.compare(ri.getStartKey(), lastScanStartKey);
                int cmpEnd = BytesComparator.BYTES_COMPARATOR.compare(ri.getEndKey(), lastScanStartKey);
                if (cmpStart <= 0 && cmpEnd > 0) {
                    continueFromKey = ri.getEndKey();
                    break;
                }
            }
        }
        for (int i = 0; i < freshRegions.size(); i++) {
            RegionInfo ri = freshRegions.get(i);
            if (BytesComparator.BYTES_COMPARATOR.compare(ri.getStartKey(), continueFromKey) >= 0) {
                currentRegionNumber = i;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Next region resolved by continueFromKey={}, regionIndex={}, regionRange=[{}, {})",
                            Arrays.toString(continueFromKey), currentRegionNumber,
                            Arrays.toString(ri.getStartKey()), Arrays.toString(ri.getEndKey()));
                }
                return buildRegionScanner(ri);
            }
        }
        LOGGER.warn("Failed to resolve next region by continueFromKey={}", Arrays.toString(continueFromKey));
        return null;
    }

    private RegionScanner buildRegionScanner(RegionInfo ri) {
        byte[] startKey = ri.getStartKey();
        if (scan.getStopKey().length != 0 && BytesComparator.BYTES_COMPARATOR.compare(startKey, scan.getStopKey()) >= 0) {
            return null;
        }
        scan = new Scan(startKey, scan.getStopKey());
        scan.setRegionPosition(ri.getPosition());
        scan.setBlockNumber(0);
        currentRegionStartKey = startKey;
        currentRegionEndKey = ri.getEndKey();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Build region scanner: regionIndex={}, regionPosition={}, range=[{}, {}), stopKey={}",
                    currentRegionNumber, ri.getPosition(), Arrays.toString(currentRegionStartKey),
                    Arrays.toString(currentRegionEndKey), Arrays.toString(scan.getStopKey()));
        }
        return new RegionScanner(scan, table);
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
        ArrayList<Result> resultSets = new ArrayList<>(nbRows);
        for (int i = 0; i < nbRows; i++) {
            Result next = next();
            if (next != null) {
                resultSets.add(next);
            } else {
                break;
            }
        }
        return resultSets.toArray(new Result[0]);
    }

    @Override
    public void close() {
    }

    @Override
    public Iterator<Result> iterator() {
        return new Iterator<Result>() {
            Result next = null;

            @Override
            public boolean hasNext() {
                if (next == null) {
                    try {
                        next = ClientScanner.this.next();
                        return next != null;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return true;
            }

            @Override
            public Result next() {
                if (!hasNext()) {
                    return null;
                }
                Result temp = next;
                next = null;
                return temp;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
