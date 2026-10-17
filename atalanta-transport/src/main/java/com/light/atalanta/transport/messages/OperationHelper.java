/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.NumbersByteHelper;
import com.light.atalanta.transport.RegionPosition;

import static com.light.atalanta.transport.messages.Operation.*;

public class OperationHelper {

    public static int getMessageSize(byte[] fromBytes) {
        return getMessageSize(fromBytes, 0);
    }

    public static int getMessageSize(byte[] fromBytes, int from) {
        return NumbersByteHelper.fromBytes_int(fromBytes, from);
    }

    public static MessageType getTransportMessageType(byte[] fromBytes) {
        return MessageType.fromByte(fromBytes[4]);
    }

    public static RegionPosition getRegionPosition(byte[] fromBytes) {
        return new RegionPosition(NumbersByteHelper.fromBytes_int(fromBytes, LENGTH_MESSAGE_SIZE + LENGTH_MESSAGE_TYPE));
    }

    public static boolean isBigOperation(byte[] bytes) {
        // An operation is big if it is longer than 13 bytes (guards against EMPTY_RESULT)
        // and byte 13 is 1 (the compression flag)
        return bytes.length > 13 && bytes[13] == 1;
    }

    public static boolean isBigMessage(byte[] bytes) {
        // A message is big if it is longer than 13 bytes (guards against EMPTY_RESULT),
        if (bytes.length > 13) {
            // byte 13 is 1 (the compression flag)
            if (bytes[13] == 1) {
                // and the message type is not CURRENT_REGION_MAP
                // (compared byte-wise to avoid message type conversion errors)
                return bytes[4] != MessageType.CURRENT_REGION_MAP.getTypeByte();
            }
        }
        return false;
    }
}
