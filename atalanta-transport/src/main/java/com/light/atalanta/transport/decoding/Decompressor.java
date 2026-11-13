/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.decoding;

import com.light.atalanta.transport.messages.TransportMessage;

import java.io.IOException;

public interface Decompressor {

    byte[] decompressBytes(byte[] bytes) throws IOException;

    TransportMessage decompressMessage(byte[] bytes) throws IOException;

}
