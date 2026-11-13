/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;


import java.util.List;

public interface ScanResultContainer {
    public List<ResultOperation> getResultOperations();

    public boolean isFinished();

    public ResultOperation next();

    public ResultOperation first();

    public ResultOperation removeFirst();

    public int getClientBlockNumber();
    public int getRealBlockNumber();

    public int getBs();

    public short getTryNum();
    public int getBlockNumber();

    public byte[] getFirstRowkey();

    public void setFirstRowkey(byte[] firstRowkey);

    public void cleanContainer();

    public void setFinished();

    void addResultOperation(ResultOperation ro);

}
