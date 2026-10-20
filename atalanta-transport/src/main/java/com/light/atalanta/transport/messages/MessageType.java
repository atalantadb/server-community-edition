/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.transport.exceptions.UnknownTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum MessageType {

    EMPTY_MESSAGE((byte) 0),
    GET((byte) 1),
    PUT((byte) 2),
    DIRECT_WRITE((byte) 4),
    DIRECT_TRUNC((byte) 5),
    DELETE((byte) 6),
    RESULT((byte) 7),
    AUTH((byte) 8),
    AUTH_NONE((byte) 9),
    GET_REGION_MAP((byte) 10),
    INVALID_REGION_MAP_ERROR((byte) 11),
    CURRENT_REGION_MAP((byte) 12),
    PING((byte) 14),
    SCAN((byte) 15),
    RS_ANSWER((byte) 16),
    ROW_COUNT((byte) 21),
    BULK_GET_OPERATION((byte) 30),
    BULK_PUT_OPERATION((byte) 31),
    PUT_AND_GET((byte) 32),
    GET_REGION_MAP_WITH_TIMEOUT((byte) 33),
    BULK_GET_OPERATION_NIO((byte) 40),
    BULK_PUT_OPERATION_NIO((byte) 41),
    // Scan with an explicit table position on a shared NIO connection
    SCAN_NIO((byte) 43),
    BULK_RESULT_OPERATION((byte) 70),
    RECORD_TOO_LARGE_ERROR((byte) 100),
    CORE_RS_TOO_BUSY_ERROR((byte) 101),
    KEY_OUT_OF_RANGE_ERROR((byte) 102),
    BAD_DATA_ERROR((byte) 103);

    private final byte type;

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageType.class);

    MessageType(byte b) {
        this.type = b;
    }

    public byte getTypeByte() {
        return type;
    }

    public static MessageType fromByte(byte b) {
        switch (b) {
            case 0: {
                return EMPTY_MESSAGE;
            }
            case 1: {
                return GET;
            }
            case 2: {
                return PUT;
            }
            case 4: {
                return DIRECT_WRITE;
            }
            case 5: {
                return DIRECT_TRUNC;
            }
            case 6: {
                return DELETE;
            }
            case 7: {
                return RESULT;
            }
            case 8: {
                return AUTH;
            }
            case 9: {
                return AUTH_NONE;
            }
            case 10: {
                return GET_REGION_MAP;
            }
            case 11: {
                return INVALID_REGION_MAP_ERROR;
            }
            case 12: {
                return CURRENT_REGION_MAP;
            }
            case 14: {
                return PING;
            }
            case 15: {
                return SCAN;
            }
            case 16: {
                return RS_ANSWER;
            }
            case 21: {
                return ROW_COUNT;
            }
            case 30: {
                return BULK_GET_OPERATION;
            }
            case 31: {
                return BULK_PUT_OPERATION;
            }
            case 32: {
                return PUT_AND_GET;
            }
            case 33: {
                return GET_REGION_MAP_WITH_TIMEOUT;
            }
            case 40: {
                return BULK_GET_OPERATION_NIO;
            }
            case 41: {
                return BULK_PUT_OPERATION_NIO;
            }
            case 43: {
                return SCAN_NIO;
            }
            case 70: {
                return BULK_RESULT_OPERATION;
            }
            case 100: {
                return RECORD_TOO_LARGE_ERROR;
            }
            case 101: {
                return CORE_RS_TOO_BUSY_ERROR;
            }
            case 102: {
                return KEY_OUT_OF_RANGE_ERROR;
            }
            case 103: {
                return BAD_DATA_ERROR;
            }
            default: {
                throw new UnknownTypeException(b);
            }
        }
    }
}
