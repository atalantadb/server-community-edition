/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.transport.messages.ResultOperation;
import com.light.atalanta.transport.messages.ScanResultContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class LazyRegionScanner implements ResultScanner {
    private final Scan currentScan;
    private final LazyTableImpl table;
    private ScanResultContainer container = null;

    LazyRegionScanner(Scan scan, LazyTableImpl table) {
        this.currentScan = scan;
        this.table = table;
    }

    @Override
    public Result next() throws IOException {
        if (container == null || (container.getResultOperations().isEmpty() && !container.isFinished())) {
            makeScan();
        }
        if (container.isFinished()) {
            return null;
        } else {
            ResultOperation ro = container.next();
            if (ro == null) {
                return null;
            } else {
                return OperationConverter.fromOperation(ro);
            }
        }
    }

    private void makeScan() throws IOException {
        currentScan.setBlockNumber(currentScan.getBlockNumber() + 1);
        container = table.scanRegion(currentScan);
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

    public boolean fullScanned() {
        return container != null && container.isFinished();
    }


    @Override
    public Iterator<Result> iterator() {
        return new Iterator<Result>() {
            Result next = null;

            @Override
            public boolean hasNext() {
                if (next == null) {
                    try {
                        next = LazyRegionScanner.this.next();
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