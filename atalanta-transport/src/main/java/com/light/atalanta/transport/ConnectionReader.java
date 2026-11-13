/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport;

import com.light.atalanta.NumbersByteHelper;
import com.light.atalanta.exceptions.AtalantaTransportRuntimeException;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;
import com.light.atalanta.transport.exceptions.UnavailableCoreException;
import com.light.atalanta.transport.messages.*;
import com.light.atalanta.tracers.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class ConnectionReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionReader.class);

    private final ThreadLocal<byte[]> localHeaderBuffer = ThreadLocal.withInitial(() -> new byte[5]);
    private final ThreadLocal<byte[]> localBytesForResponse = ThreadLocal.withInitial(() -> new byte[SocketConnection.CHUNK_MESSAGE_SIZE]);

    private final TransportConfiguration configuration;
    private final InputStream in;

    public ConnectionReader(final TransportConfiguration configuration, final InputStream in) {
        this.configuration = configuration;
        this.in = in;
    }

    public void read(String location) throws IOException, NotActualRegionMappingException, UnavailableCoreException {
        byte[] header = readHeader();
        // The first int (4 bytes) is the total message size
        int size = NumbersByteHelper.fromBytes_int(header, 0);
        // The fifth byte is the message type
        byte msgType = header[4];
        if (size < 5) {
            // The smallest valid message is 5 bytes: the size and the type
            throw new AtalantaTransportRuntimeException("Invalid message size: " + size);
        }
        // This condition tests corner case with EMPTY_OPERATION_BYTES
        // for this message readMes will be -1 because of message size
        if (size == 5) {
            // Empty result message: skip it
            return;
        }
        if (size <= SocketConnection.CHUNK_MESSAGE_SIZE) {
            readMessage(size);
            OperationFactory.checkReturnedExceptions(configuration, MessageType.fromByte(msgType), location);
        } else if (size < SocketConnection.MAX_MESSAGE_SIZE || BulkHelper.isBulkMessage(msgType)) {
            readBigMessage(size);
            OperationFactory.checkReturnedExceptions(configuration, MessageType.fromByte(msgType), location);
        } else {
            discardMessage(size, msgType);
            throw new IOException("Message size too big: " + size + ", type: " + msgType);
        }
    }

    public TransportMessage readMessage(String location) throws IOException, NotActualRegionMappingException, UnavailableCoreException {
        byte[] bytes = readMessageBytes();
        LOGGER.trace("Receive {} bytes: {}", bytes.length, bytes);

        MessageType type = OperationHelper.getTransportMessageType(bytes);
        if (type == MessageType.CURRENT_REGION_MAP || type == MessageType.INVALID_REGION_MAP_ERROR) {
            TableRegionMapResponse response = new TableRegionMapResponse(bytes);
            LOGGER.trace("Receive TableRegionMapResponse: {}", response);
            return response;
        }

        TransportMessage tm = OperationFactory.fromBytes(configuration, type, bytes, location);
        LOGGER.trace("Receive transport message: {}", tm);
        return tm;
    }

    public byte[] readMessageBytes() throws IOException, UnavailableCoreException {
        byte[] bytes = readMessageBytes0();
        if (OperationHelper.isBigMessage(bytes)) {
            BigResultOperation bo = new BigResultOperation(bytes);
            return bo.getOriginalUnzippedMessage();
        } else {
            return bytes;
        }
    }

    private byte[] readMessageBytes0() throws IOException, UnavailableCoreException {
        byte[] header = readHeader();

        // The first int (4 bytes) is the total message size
        int size = NumbersByteHelper.fromBytes_int(header, 0);
        // The fifth byte is the message type
        byte msgType = header[4];

        if (size < 5) {
            // The smallest valid message is 5 bytes: the size and the type
            throw new AtalantaTransportRuntimeException("Invalid message size: " + size);
        }
        // This condition tests corner case with EMPTY_OPERATION_BYTES
        // for this message readMes will be -1 because of message size
        if (size == 5) {
            // Empty result message: skip it
            return EmptyResultOperation.EMPTY_OPERATION_BYTES;
        }

        if (size < SocketConnection.MAX_MESSAGE_SIZE || BulkHelper.isBulkMessage(msgType)) {
            byte[] msgBuf = new byte[size];
            System.arraycopy(header, 0, msgBuf, 0, 5);
            int toRead = size - 5;
            int readMes = 0;
            while (readMes < toRead) {
                int r = in.read(msgBuf, 5 + readMes, toRead - readMes);
                if (r < 0) {
                    throw new AtalantaTransportRuntimeException("Unexpected end of stream (read < size)");
                }
                readMes += r;
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Receive {} bytes: {}", msgBuf.length, msgBuf);
                Writer.addLog(msgBuf, msgBuf.length);
            }
            return msgBuf;
        } else {
            discardMessage(size, msgType);
            throw new IOException("Message size too big: " + size + ", type: " + msgType);
        }
    }

    byte[] readHeader() throws IOException, UnavailableCoreException {
        // Read exactly 5 bytes: 4 for the size and 1 for the type
        byte[] header = localHeaderBuffer.get();
        int readHeader = in.read(header, 0, 5);
        if (readHeader < 0) {
            throw new UnavailableCoreException();
        }
        return header;
    }

    private void readMessage(int size) throws IOException {
        int readMes = in.read(localBytesForResponse.get(), 0, size - 5);
        // This condition tests corner case with EMPTY_OPERATION_BYTES
        // for this message readMes will be -1 because of message size
        if (size == 5) {
            // Empty result message: skip it
            return;
        }
        if (readMes != size - 5) {
            throw new AtalantaTransportRuntimeException();
        }
        if (LOGGER.isTraceEnabled()) {
            byte[] bb = Arrays.copyOfRange(localBytesForResponse.get(), 0, size);
            LOGGER.trace("Receive {} bytes: {}", bb.length, bb);
            Writer.addLog(bb, readMes);
        }
    }

    private void readBigMessage(int size) throws IOException {
        int toRead = size - 5;
        byte[] respBuf = new byte[toRead];
        int readCount = 0;
        while (readCount < toRead) {
            int res = in.read(respBuf, readCount, toRead - readCount);
            if (res < 0) {
                throw new AtalantaTransportRuntimeException("Unexpected end of stream while reading message body");
            }
            readCount += res;
        }
    }

    private void discardMessage(int size, byte msgType) throws IOException {
        LOGGER.warn("Msg size too big: {}, msgType: {}", size, msgType);
        // 5 bytes are already read. Discard up to MAX_RESPONSE_SIZE - 5 bytes
        // so that internal buffers do not overflow.
        byte[] discardBuf = new byte[(int) (SocketConnection.MAX_MESSAGE_SIZE - 5)];
        int readMes = in.read(discardBuf, 0, discardBuf.length);
        if (readMes > 50) {
            discardBuf = Arrays.copyOf(discardBuf, 50);
        }
        LOGGER.warn("Discarded bytes (up to {}): {}", discardBuf.length, size);
    }
}
