/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.buffers;

import com.light.atalanta.transport.messages.TransportMessage;
import com.light.atalanta.transport.values.KeyValues;
import com.light.atalanta.transport.values.QualifierValue;
import com.light.atalanta.transport.values.SerializedObject;

public class TransportBufferFactory {

    private static final ThreadLocal<ByteArrayBuffer<TransportMessage>> transportBuffers = ThreadLocal.withInitial(ByteArrayTransportMessageBuffer::new);
    private static final ThreadLocal<ByteArrayBuffer<KeyValues>> keyValuesBuffers = ThreadLocal.withInitial(ByteArrayKeyValuesBuffer::new);
    private static final ThreadLocal<ByteArrayBuffer<QualifierValue>> qualifiersBuffers = ThreadLocal.withInitial(ByteArrayQualifierValueBuffer::new);
    private static final ThreadLocal<ByteArrayBuffer<TransportMessage>> defaultTMBuffers = ThreadLocal.withInitial(DefaultByteArrayTransportMessageBuffer::new);
    private static final ThreadLocal<ByteArrayBuffer<SerializedObject>> defaultSOBuffers = ThreadLocal.withInitial(DefaultByteArraySerializedObjectBuffer::new);

    public static ByteArrayBuffer<TransportMessage> getDefaultTransportBuffer() {
        return defaultTMBuffers.get();
    }

    public static ByteArrayBuffer<SerializedObject> getDefaultSerializedObjectBuffer() {
        return defaultSOBuffers.get();
    }

    public static ByteArrayBuffer<TransportMessage> getTransportBuffer() {
        return transportBuffers.get();
    }

    public static ByteArrayBuffer<KeyValues> getKeyValuesBuffer() {
        return keyValuesBuffers.get();
    }

    public static ByteArrayBuffer<QualifierValue> getQualifierBuffer() {
        return qualifiersBuffers.get();
    }

}
