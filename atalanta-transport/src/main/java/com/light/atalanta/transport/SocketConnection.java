/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport;

import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;
import com.light.atalanta.transport.exceptions.UnavailableCoreException;
import com.light.atalanta.transport.messages.TransportMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SocketConnection {

    public static final int CHUNK_MESSAGE_SIZE = 64 * 1_024;
    static final long MAX_MESSAGE_SIZE = Math.max(CHUNK_MESSAGE_SIZE, Math.min(Runtime.getRuntime().maxMemory() / 5 * 2, Integer.MAX_VALUE));


    private final InputStream in;
    private final OutputStream out;

    private final ConnectionReader reader;
    private final ConnectionWriter writer;

    public SocketConnection(TransportConfiguration configuration, InputStream in, OutputStream out) {
        this.in = in;
        this.out = out;

        this.reader = new ConnectionReader(configuration, in);
        this.writer = new ConnectionWriter(out);
    }

    // Writes
    public void writeMessage(TransportMessage message) throws IOException {
        writer.writeMessage(message);
    }

    public void writeMessage(byte[] message, int offset, int length) throws IOException {
        writer.writeMessage(message, offset, length);
    }

    // Reads
    public TransportMessage readMessage(String location) throws IOException, NotActualRegionMappingException, UnavailableCoreException {
        return reader.readMessage(location);
    }

    public void readAnswer(String location) throws IOException, NotActualRegionMappingException, UnavailableCoreException {
        reader.read(location);
    }

    public byte[] readMessageBytes() throws IOException, UnavailableCoreException {
        return reader.readMessageBytes();
    }

    // Visible for testing
    InputStream getIn() {
        return in;
    }

    OutputStream getOut() {
        return out;
    }
}
