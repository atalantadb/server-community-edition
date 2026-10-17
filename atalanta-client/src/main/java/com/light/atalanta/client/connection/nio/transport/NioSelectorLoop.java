/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.transport;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;

public interface NioSelectorLoop {
    void shutdown() throws IOException, InterruptedException;
    CompletableFuture<ChannelContext> register(SocketChannel socketChannel, ChannelContext channelContext);
}
