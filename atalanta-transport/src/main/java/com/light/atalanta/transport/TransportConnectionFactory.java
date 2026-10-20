/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport;

import java.io.IOException;
import java.net.ServerSocket;

public class TransportConnectionFactory {
    public static TRSConnectionStub createConnectionStub(TransportConfiguration configuration, String host, int port) {
        return new TRSConnectionStub(configuration, host, port);
    }

    public static ServerSocket createServerSocket(TransportConfiguration configuration, int port) throws IOException {
        return new ServerSocket(port);
    }
}
