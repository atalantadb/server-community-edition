/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.data.files;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class AtalantaMeta {

    private final ByteBuffer meta;

    public AtalantaMeta(ByteBuffer meta) {
        this.meta = meta;
    }

    public ByteBuffer getMeta() {
        return meta;
    }

    public void writeMeta(FileChannel outCh) throws IOException {
        meta.position(0);
        outCh.write(meta);
    }

}
