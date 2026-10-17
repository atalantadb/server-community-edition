/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.transport.exceptions.BadDataException;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;

public class BulkHelper {

    public static final int NOT_USE_TABLE_POSITION = -1;

    public static void checkBulkGet(byte[] fromBytes) throws NotActualRegionMappingException {
        checkBulkOperation(fromBytes, MessageType.BULK_GET_OPERATION);
    }

    public static void checkBulkPut(byte[] fromBytes) throws NotActualRegionMappingException {
        checkBulkOperation(fromBytes, MessageType.BULK_PUT_OPERATION);
    }

    public static void checkBulkResult(byte[] fromBytes) throws NotActualRegionMappingException {
        checkBulkOperation(fromBytes, MessageType.BULK_RESULT_OPERATION);
    }

    private static void checkBulkOperation(byte[] fromBytes, MessageType expectedType) throws NotActualRegionMappingException {
        MessageType type = OperationHelper.getTransportMessageType(fromBytes);
        if (type == MessageType.INVALID_REGION_MAP_ERROR || type == MessageType.KEY_OUT_OF_RANGE_ERROR) {
            throw new NotActualRegionMappingException();
        }
        if (type != expectedType) {
            throw new BadDataException("Incorrect type=" + type + " for bulk operation=" + expectedType + " size=" + fromBytes.length);
        }
    }

    public static boolean isBulkMessage(byte msgType) {
        return msgType == MessageType.BULK_RESULT_OPERATION.getTypeByte();
    }
}
