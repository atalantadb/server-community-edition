/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.serializers;


public class PutSerializer {

    private static final ThreadLocal<PutToByteSerializer> serializers = ThreadLocal.withInitial(PutToByteSerializer::new);

    public static PutToByteSerializer getSerializer() {
        return serializers.get();
    }
}
