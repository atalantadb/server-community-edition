/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.serializers;

public class PutDataHolder {
    private static ThreadLocal<PutData> threadLocalPuts = ThreadLocal.withInitial(PutData::new);

    public static PutData getPutData() {
        return threadLocalPuts.get();
    }
}
