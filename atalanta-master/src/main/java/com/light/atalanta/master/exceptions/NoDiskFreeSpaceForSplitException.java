/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.exceptions;

public class NoDiskFreeSpaceForSplitException extends RuntimeException{
    public NoDiskFreeSpaceForSplitException(String region) {
        super("No free dick space for split region " + region);
    }
}
