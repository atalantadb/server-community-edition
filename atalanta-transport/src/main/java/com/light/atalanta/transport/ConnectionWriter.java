/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport;

import com.light.atalanta.transport.buffers.ByteArrayBuffer;
import com.light.atalanta.transport.exceptions.AtalantaSerializationException;
import com.light.atalanta.transport.messages.BigResultOperation;
import com.light.atalanta.transport.messages.Operation;
import com.light.atalanta.transport.messages.TransportMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Sends messages over the connection.
 */
public class ConnectionWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionWriter.class);

    private final OutputStream out;

    public ConnectionWriter(final OutputStream out) {
        this.out = out;
    }

    public void writeMessage(byte[] message, int offset, int length) throws IOException {
        out.write(message, offset, length);
    }

    public void writeMessage(TransportMessage message) throws IOException {
        final TransportMessage send;
        final ByteBuffer buffer;
        try {
            boolean isBigMessage = message.getMessageSize() > SocketConnection.CHUNK_MESSAGE_SIZE
                    && message instanceof Operation;
            send = isBigMessage ? new BigResultOperation((Operation) message) : message;
            ByteArrayBuffer<TransportMessage> tb = send.getBuffer();
            buffer = tb.serializeMessage(send);
        } catch (AtalantaSerializationException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new AtalantaSerializationException("Cannot serialize transport message "
                    + message.getClass().getName(), e);
        }
        out.write(buffer.array(), buffer.arrayOffset(), buffer.position());
    }
}
