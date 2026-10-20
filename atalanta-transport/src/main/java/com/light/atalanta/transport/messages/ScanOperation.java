/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;


public interface ScanOperation extends PositionedTransportMessage, TablePositionedTransportMessage {

    public int getBlockNumber();

    public long getTryNumber();

    public void setTryNumber(long tryNumber);

    public byte[] getStartKey();

    public byte[] getEndKey();

    public int getDataBlockSize();

}
