/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.transport.values.RegionInfo;

import java.io.IOException;
import java.util.*;

public class LazyClientScanner implements ResultScanner {
    private Scan scan;
    private final LazyTableImpl table;
    private int currentRegionNumber = 0;
    private final LinkedList<Result> currentResults;
    private List<RegionInfo> sortedRegions;
    private LazyRegionScanner currentRegionScanner;

    LazyClientScanner(ScanRows scan, LazyTableImpl table) {

        this.scan = (Scan) scan;
        this.table = table;
        this.sortedRegions = this.table.getSortedRegions();
        this.currentResults = new LinkedList<>();
        setRegionPosition();
        this.currentRegionScanner = new LazyRegionScanner(this.scan, this.table);
    }

    private void setRegionPosition() {
        ensureInitRegion(false);
        RegionInfo info = sortedRegions.get(currentRegionNumber);
        this.scan.setRegionPosition(info.getPosition());
    }


    private void ensureInitRegion(boolean isReload) {
        RegionInfo info = sortedRegions.get(currentRegionNumber);
        if (info == null) {
            byte[] startKey = isReload ? table.getNextRegionKey(scan.getStartKey()) : scan.getStartKey();
            this.table.chooseRegion(startKey);
            this.sortedRegions = this.table.getSortedRegions();
            info = sortedRegions.get(currentRegionNumber);
        }
        if (info == null) {
            throw new RuntimeException("Can not get information about region with position " + currentRegionNumber);
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
                currentRegionScanner = nextRegionScanner();
            }
            while (r == null) {
                if (currentRegionScanner == null || currentRegionScanner.fullScanned()) {
                    currentRegionScanner = nextRegionScanner();
                    if (currentRegionScanner == null) {
                        return null;
                    }
                }
                Result[] rs = scanRegion();
                if (rs == null) {
                    currentResults.addAll(Arrays.asList(rs));
                    r = currentResults.poll();
                }
            }
            return r;
        }
        return currentResults.poll();
    }


    private Result[] scanRegion() throws IOException {
        return currentRegionScanner.next(scan.getCacheSize());
    }

    private LazyRegionScanner nextRegionScanner() {
        currentRegionNumber++;
        if (sortedRegions.size() > currentRegionNumber) {
            ensureInitRegion(true);
            scan = new Scan(sortedRegions.get(currentRegionNumber).getStartKey());
            scan.setRegionPosition(sortedRegions.get(currentRegionNumber).getPosition());
            scan.setBlockNumber(0);
            return new LazyRegionScanner(scan, table);
        }
        return null;
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
                        next = LazyClientScanner.this.next();
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