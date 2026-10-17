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
import java.util.stream.Collectors;

public class ScanResultContainerV2 implements ScanResultContainer {
    private final int clientBlockNumber;
    private final short tryNumber;
    private final int bs;
    private final int realBlockNumber;
    private final LinkedList<ResultOperation> resultOperations;
    private byte[] firstRowkey;
    private boolean isFinished;

    ScanResultContainerV2(int clientBlockNumber, short tryNumber) {
        this(clientBlockNumber, tryNumber, 0, 1);
    }

    ScanResultContainerV2(int clientBlockNumber, short tryNumber, int bs, int realBlockNumber) {
        this.clientBlockNumber = clientBlockNumber;
        this.tryNumber = tryNumber;
        this.bs = bs;
        this.realBlockNumber = realBlockNumber;
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

    public int getClientBlockNumber() {
        return clientBlockNumber;
    }

    @Override
    public int getBlockNumber() {
        return getClientBlockNumber();
    }

    public int getBs() {
        return bs;
    }

    public short getTryNum() {
        return (short) tryNumber;
    }

    public byte[] getFirstRowkey() {
        return firstRowkey;
    }

    public void setFirstRowkey(byte[] firstRowkey) {
        this.firstRowkey = Bytes.cloneBytesArray(firstRowkey);
    }

    @Override
    public String toString() {
        return "ScanResultContainerV2{" +
                "clientBlockNumber=" + clientBlockNumber +
                ", tryNumber=" + tryNumber +
                ", bs=" + bs +
                ", realBlockNumber=" + realBlockNumber +
                ", resultOperations=" + resultOperations.stream().map(Operation::getRowkey).map(Arrays::toString).collect(Collectors.joining(",")) +
                ", firstRowkey=" + Arrays.toString(firstRowkey) +
                ", isFinished=" + isFinished +
                '}';
    }

    public int getRealBlockNumber() {
        return realBlockNumber;
    }
}
