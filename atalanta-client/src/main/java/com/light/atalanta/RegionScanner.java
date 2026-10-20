/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.transport.messages.ResultOperation;
import com.light.atalanta.transport.messages.ScanResultContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class RegionScanner implements ResultScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionScanner.class);
    private final Scan currentScan;
    private final Table table;
    private byte[] scannedRowkey;

    private ScanResultContainer container = null;

    RegionScanner(Scan scan, Table table) {
        this.currentScan = scan;
        this.table = table;
    }

    @Override
    public Result next() throws IOException {
        if (container == null || (container.getResultOperations().isEmpty() && !container.isFinished())) {
            makeScan();
            checkScanContainer();
        }
        if (container.isFinished()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Region scanner finished: startKey={}, stopKey={}, regionPosition={}, lastDeliveredRowkey={}",
                        Arrays.toString(currentScan.getStartKey()), Arrays.toString(currentScan.getStopKey()),
                        currentScan.getRegionPosition(), Arrays.toString(scannedRowkey));
            }
            return null;
        } else {
            ResultOperation ro = container.next();
            if (ro == null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Region scanner returned null operation for unfinished container: startKey={}, regionPosition={}, block={}",
                            Arrays.toString(currentScan.getStartKey()), currentScan.getRegionPosition(), currentScan.getBlockNumber());
                }
                return null;
            } else {
                scannedRowkey = container.getFirstRowkey();
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Region scanner delivered row: regionPosition={}, block={}, deliveredRowkey={}, containerFirstRowkey={}",
                            currentScan.getRegionPosition(), currentScan.getBlockNumber(),
                            Arrays.toString(ro.getRowkey()), Arrays.toString(container.getFirstRowkey()));
                }
                return OperationConverter.fromOperation(ro);
            }
        }
    }

    private void checkScanContainer() {
        if (container.getFirstRowkey() != null && Arrays.equals(container.getFirstRowkey(), this.scannedRowkey)) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Detected repeated scan block head rowkey={}, clean current container", Arrays.toString(container.getFirstRowkey()));
            }
            container.cleanContainer();
        }
    }

    private void makeScan() throws IOException {
        int previousBlock = currentScan.getBlockNumber();
        int requestedBlock = previousBlock + 1;
        currentScan.setBlockNumber(requestedBlock);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Region scan request: startKey={}, stopKey={}, regionPosition={}, requestedBlock={}, prevBlock={}, bs={}",
                    Arrays.toString(currentScan.getStartKey()), Arrays.toString(currentScan.getStopKey()),
                    currentScan.getRegionPosition(), currentScan.getBlockNumber(), previousBlock, currentScan.getBs());
        }
        container = table.scanRegion(currentScan);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Region scan response: clientBlock={}, realBlock={}, bs={}, tryNum={}, finished={}, rows={}, firstRowkey={}",
                    container.getClientBlockNumber(), container.getRealBlockNumber(), container.getBs(), container.getTryNum(),
                    container.isFinished(), container.getResultOperations().size(), Arrays.toString(container.getFirstRowkey()));
        }
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
                        next = RegionScanner.this.next();
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
