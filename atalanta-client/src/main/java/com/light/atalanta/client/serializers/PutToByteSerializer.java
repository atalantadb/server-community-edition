/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.serializers;

import com.light.atalanta.NumbersByteHelper;
import com.light.atalanta.Put;
import com.light.atalanta.transport.messages.MessageType;

import java.nio.ByteBuffer;

public class PutToByteSerializer {

    private static final int MAX_BUFFER_SIZE = 10 * 1024;
    private final ByteBuffer CURRENT_BUFFER = ByteBuffer.allocate(MAX_BUFFER_SIZE);
    private final ByteCollector byteCollector;

    PutToByteSerializer() {
        this.byteCollector = new ByteCollector();
        byteCollector.setBuffer(CURRENT_BUFFER);
    }

    public ByteCollector serializePut(Put put, int regionPosition) {
        if (!put.isReusable()) {
            return null;
        }
        PutData data = put.getPutData();
        byteCollector.setSize(data.getDataSize());
        makeSerialization(data, regionPosition);
        CURRENT_BUFFER.position(0);
        data.cleanup();
        return byteCollector;
    }

    private void makeSerialization(PutData data, int regionPosition) {
        if (data.isEmpty()) {
            throw new RuntimeException("Reusable put is empty");
        }
        CURRENT_BUFFER.put(NumbersByteHelper.toBytes_int(data.getDataSize()));
        CURRENT_BUFFER.put(MessageType.PUT.getTypeByte());
        CURRENT_BUFFER.put(NumbersByteHelper.toBytes_int(regionPosition));
        CURRENT_BUFFER.put(NumbersByteHelper.toBytes_short((short) 0));
        CURRENT_BUFFER.put(NumbersByteHelper.toBytes_short((short) 1));

        CURRENT_BUFFER.put((byte) 0);
        CURRENT_BUFFER.put(NumbersByteHelper.toBytes_short((short) 0));

        for (int i = 0; i < data.getElementsCount(); i++) {
            data.getColumnFamilies()[i].serializeColumnFamily(CURRENT_BUFFER, data.getRowkey());
        }
    }

    public static class ByteCollector {
        private ByteBuffer buffer;
        private Integer size;

        ByteCollector() {
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(ByteBuffer bb) {
            this.buffer = bb;
        }

        public Integer getSize() {
            return size;
        }

        public void setSize(Integer size) {
            this.size = size;
        }

        public byte[] getBytes() {
            return buffer.array();
        }
    }
}
