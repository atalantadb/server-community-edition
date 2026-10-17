/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.util.Bytes;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class ScanResultContainerV1 implements ScanResultContainer {

    private final int blockNumber;
    private final short tryNumber;
    private final LinkedList<ResultOperation> resultOperations;
    private byte[] firstRowkey;
    private boolean isFinished;

    ScanResultContainerV1(int blockNumber, short tryNumber) {
        this.blockNumber = blockNumber;
        this.tryNumber = tryNumber;
        this.resultOperations = new LinkedList<>();
        this.isFinished = true;
    }

    public void addResultOperation(ResultOperation resultOperation) {
        this.resultOperations.add(resultOperation);
        this.isFinished = false;
    }

    public void cleanContainer() {
        this.resultOperations.clear();
        this.firstRowkey = null;
    }

    public void setFinished() {
        this.isFinished = true;
    }

    public List<ResultOperation> getResultOperations() {
        return resultOperations;
    }

    public boolean isFinished() {
        return this.isFinished;
    }

    public ResultOperation next() {
        return resultOperations.poll();
    }

    public ResultOperation first() {
        return resultOperations.getFirst();
    }

    public ResultOperation removeFirst() {
        return resultOperations.poll();
    }

    @Override
    public int getClientBlockNumber() {
        return 0;
    }

    @Override
    public int getRealBlockNumber() {
        return 0;
    }

    @Override
    public int getBs() {
        return 0;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    public short getTryNum() {
        return tryNumber;
    }

    public byte[] getFirstRowkey() {
        return firstRowkey;
    }

    public void setFirstRowkey(byte[] firstRowkey) {
        this.firstRowkey = Bytes.cloneBytesArray(firstRowkey);
    }

    @Override
    public String toString() {
        return "ScanResultContainerV1{" +
                "blockNumber=" + blockNumber +
                ", tryNumber=" + tryNumber +
                ", firstRowkey=" + Arrays.toString(firstRowkey) +
                ", isFinished=" + isFinished +
                '}';
    }
}

