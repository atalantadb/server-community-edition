/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.values;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface SerializedObject {

    // Writes the message at the current buffer position.
    // Implementations must return the same instance and must not clear or rewind the buffer.
    ByteBuffer serializeObject(ByteBuffer bb) throws IOException;

    int getSizeInBytes();

}
