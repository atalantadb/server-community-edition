/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.NumbersByteHelper;
import com.light.atalanta.transport.RegionPosition;
import com.light.atalanta.transport.decoding.DecodingFactory;
import com.light.atalanta.transport.exceptions.AtalantaSerializationException;
import com.light.atalanta.transport.values.KeyValues;
import com.light.atalanta.transport.values.QualifierValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class BigResultOperation extends ResultOperation {
    private volatile CompressedKeyValues[] compressedValues;
    private volatile int compressedMessageSize = -1;

    public BigResultOperation(Operation originalOperation) {
        this.msgSize = 0;
        this.msgType = originalOperation.getMessageType();
        this.regionPos = originalOperation.getRegionPosition();
        this.chunkNo = originalOperation.getChunkNo();
        this.chunkTotal = originalOperation.getChunkTotal();
        this.flagInfo = 1;
        this.flagReserve = originalOperation.getFlagReserve();
        if (originalOperation instanceof TablePositionedTransportMessage) {
            this.setTablePosition(((TablePositionedTransportMessage) originalOperation).getTablePosition());
        }

        this.values = originalOperation.getValues();
    }

    public BigResultOperation(byte[] fromBytes)  {
        this(fromBytes, 0, fromBytes.length);
    }

    public BigResultOperation(byte[] fromBytes, int offset, int length)  {
        if (offset < 0 || length < TRANSPORT_MESSAGE_HEADER_LENGTH
                || offset > fromBytes.length - length) {
            throw new IllegalArgumentException("Invalid big operation range");
        }
        int declaredSize = OperationHelper.getMessageSize(fromBytes, offset);
        if (declaredSize != length) {
            throw new IllegalArgumentException("Big operation size " + declaredSize
                    + " does not match range length " + length);
        }
        this.msgType = MessageType.fromByte(fromBytes[offset + LENGTH_MESSAGE_SIZE]);
        this.regionPos = new RegionPosition(NumbersByteHelper.fromBytes_int(fromBytes,
                offset + LENGTH_MESSAGE_SIZE + LENGTH_MESSAGE_TYPE));
        this.chunkNo = NumbersByteHelper.fromBytes_short(fromBytes, offset + 9);
        this.chunkTotal = NumbersByteHelper.fromBytes_short(fromBytes, offset + 11);

        // Do not propagate the flag for a big operation: its bytes are decompressed later
        this.flagInfo = 0;
        this.flagReserve = NumbersByteHelper.fromBytes_short(fromBytes, offset + 14);

        List<KeyValues> values = new ArrayList<>();

        byte[] compressedBytes = new byte[length - TRANSPORT_MESSAGE_HEADER_LENGTH];
        System.arraycopy(fromBytes, offset + TRANSPORT_MESSAGE_HEADER_LENGTH,
                compressedBytes, 0, compressedBytes.length);
        int kvPos = 0;
        while (kvPos < compressedBytes.length) {
            int sizeKey = NumbersByteHelper.fromBytes_int(compressedBytes, kvPos);
            kvPos += 4;
            int sizeCf = NumbersByteHelper.fromBytes_int(compressedBytes, kvPos);
            kvPos += 4;
            kvPos += 4;

            byte[] key = new byte[sizeKey];
            System.arraycopy(compressedBytes, kvPos, key, 0, sizeKey);
            kvPos += sizeKey;

            byte[] cf = new byte[sizeCf];
            System.arraycopy(compressedBytes, kvPos, cf, 0, sizeCf);
            kvPos += sizeCf;

            byte[] payload = new byte[compressedBytes.length - kvPos];
            System.arraycopy(compressedBytes, kvPos, payload, 0, compressedBytes.length - kvPos);
            kvPos += payload.length;

            byte[] orig = decodePayload(payload);

            byte[] kvBytes = new byte[12 + sizeKey + sizeCf + orig.length];
            System.arraycopy(NumbersByteHelper.toBytes_int(sizeKey), 0, kvBytes, 0, 4);
            System.arraycopy(NumbersByteHelper.toBytes_int(sizeCf), 0, kvBytes, 4, 4);
            System.arraycopy(NumbersByteHelper.toBytes_int(orig.length), 0, kvBytes, 8, 4);
            System.arraycopy(key, 0, kvBytes, 12, key.length);
            System.arraycopy(cf, 0, kvBytes, 12 + key.length, cf.length);

            System.arraycopy(orig, 0, kvBytes, 12 + key.length + cf.length, orig.length);

            KeyValues kv = new KeyValues(kvBytes);
            values.add(kv);
        }

        this.values = values.toArray(new KeyValues[0]);
        // The received large object is already decompressed
        this.msgSize = getOriginalSize();
    }

    private byte[] decodePayload(byte[] payload) {
        try {
            return DecodingFactory.decompress(payload);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] getOriginalUnzippedMessage() throws IOException {
        this.msgSize = getOriginalSize();
        return deserializeOriginal(ByteBuffer.allocate(msgSize).order(ByteOrder.LITTLE_ENDIAN)).array();
    }

    private ByteBuffer deserializeOriginal(ByteBuffer bb) throws IOException {
        bb.order(ByteOrder.LITTLE_ENDIAN).putInt(msgSize);
        bb.put(msgType.getTypeByte());
        bb.putInt(regionPos.getPosition());
        bb.putShort(chunkNo);
        bb.putShort(chunkTotal);

        bb.put(flagInfo);
        bb.putShort(flagReserve);

        for (KeyValues kv : values) {
            deserializeOriginalKV(kv, bb);
        }
        return bb;
    }

    private void deserializeOriginalKV(KeyValues kv, ByteBuffer result) throws IOException {
        int kvSize = 0;
        for (QualifierValue qualifierValue : kv.getValues()) {
            kvSize += qualifierValue.getSizeInBytes();
        }
        result.order(ByteOrder.LITTLE_ENDIAN);
        result.putInt(kv.getRowKey().length);
        result.putInt(kv.getCfName().length);
        result.putInt(kvSize);
        result.put(kv.getRowKey());
        result.put(kv.getCfName());
        for (QualifierValue qv : kv.getValues()) {
            qv.serializeObject(result);
        }
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer bb) throws IOException {
        CompressedKeyValues[] prepared = prepareCompressedValues();
        ByteBuffer messageBuffer = bb.order(ByteOrder.LITTLE_ENDIAN);
        messageBuffer.putInt(compressedMessageSize);
        messageBuffer.put(msgType.getTypeByte());
        messageBuffer.putInt(regionPos.getPosition());
        if (getTablePosition() >= 0) {
            messageBuffer.putInt(getTablePosition());
        } else {
            messageBuffer.putShort(chunkNo);
            messageBuffer.putShort(chunkTotal);
        }
        messageBuffer.put(flagInfo);
        messageBuffer.putShort(flagReserve);
        for (CompressedKeyValues kv : prepared) {
            messageBuffer.putInt(kv.rowKey.length);
            messageBuffer.putInt(kv.cfName.length);
            messageBuffer.putInt(kv.payload.length);
            messageBuffer.put(kv.rowKey);
            messageBuffer.put(kv.cfName);
            messageBuffer.put(kv.payload);
        }
        return messageBuffer;
    }

    private CompressedKeyValues[] prepareCompressedValues() throws IOException {
        CompressedKeyValues[] prepared = compressedValues;
        if (prepared != null) {
            return prepared;
        }
        synchronized (this) {
            if (compressedValues != null) {
                return compressedValues;
            }
            CompressedKeyValues[] result = new CompressedKeyValues[values.length];
            int messageSize = TRANSPORT_MESSAGE_HEADER_LENGTH;
            for (int i = 0; i < values.length; i++) {
                KeyValues kv = values[i];
                int payloadSize = 0;
                for (QualifierValue qv : kv.getValues()) {
                    payloadSize = Math.addExact(payloadSize, qv.getSizeInBytes());
                }
                ByteBuffer payload = ByteBuffer.allocate(payloadSize).order(ByteOrder.LITTLE_ENDIAN);
                for (QualifierValue qv : kv.getValues()) {
                    qv.serializeObject(payload);
                }
                byte[] compressed = DecodingFactory.compress(payload.array());
                result[i] = new CompressedKeyValues(kv.getRowKey(), kv.getCfName(), compressed);
                messageSize = Math.addExact(messageSize, result[i].sizeInBytes());
            }
            compressedMessageSize = messageSize;
            compressedValues = result;
            return result;
        }
    }

    private int getOriginalSize(){
        int kvSize = Arrays.stream(values).mapToInt(KeyValues::getSizeInBytes).sum();
        return TRANSPORT_MESSAGE_HEADER_LENGTH + kvSize;
    }

    @Override
    public int getMessageSize() {
        try {
            prepareCompressedValues();
            return compressedMessageSize;
        } catch (IOException | RuntimeException e) {
            throw new AtalantaSerializationException("Cannot compress big transport message", e);
        }
    }

    private static final class CompressedKeyValues {
        private final byte[] rowKey;
        private final byte[] cfName;
        private final byte[] payload;

        private CompressedKeyValues(byte[] rowKey, byte[] cfName, byte[] payload) {
            this.rowKey = rowKey;
            this.cfName = cfName;
            this.payload = payload;
        }

        private int sizeInBytes() {
            return Math.addExact(KeyValues.KEY_VALUES_OBJECT_HEADER_LENGTH,
                    Math.addExact(payload.length, Math.addExact(rowKey.length, cfName.length)));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Operation operation = (Operation) o;
        return chunkNo == operation.chunkNo && chunkTotal == operation.chunkTotal && flagInfo == operation.flagInfo && flagReserve == operation.flagReserve && msgType == operation.msgType && regionPos.equals(operation.regionPos) && Arrays.equals(values, operation.values);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(msgType, chunkNo, chunkTotal, flagInfo, flagReserve, regionPos);
        result = 31 * result + Arrays.hashCode(values);
        return result;
    }
}
