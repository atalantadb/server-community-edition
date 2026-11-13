/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.RepeatableExecutor;
import com.light.atalanta.transport.TransportConfiguration;
import com.light.atalanta.transport.exceptions.BadDataException;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;
import com.light.atalanta.transport.exceptions.RecordTooLargeException;
import com.light.atalanta.transport.exceptions.UnknownTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.light.atalanta.transport.TransportConfiguration.ATALANTA_TRANSPORT_CORE_UNAVAILABLE_TIMEOUT_PROPERTY;
import static com.light.atalanta.transport.messages.RsResponse.LENGTH_MESSAGE_SIZE;
import static com.light.atalanta.transport.messages.RsResponse.LENGTH_MESSAGE_TYPE;

public class OperationFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(OperationFactory.class);

    @SuppressWarnings("unchecked")
    public static <T extends Operation> T fromBytes(MessageType type, byte[] fromBytes, String location, long timeout) throws NotActualRegionMappingException {
        checkReturnedExceptions(type, location, timeout);
        switch (type) {
            case EMPTY_MESSAGE: {
                return (T) EmptyResultOperation.EMPTY_OPERATION;
            }
            case GET: {
                return (T) ResultOperation.EMPTY;
            }
            case RESULT:
            case DELETE:
            case PUT: {
                return generateOperation(fromBytes);
            }
            case DIRECT_WRITE:
        }

        throw new UnknownTypeException(fromBytes[4]);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Operation> T generateOperation(byte[] fromBytes) {
        if (fromBytes.length > LENGTH_MESSAGE_SIZE + LENGTH_MESSAGE_TYPE) {
            if (OperationHelper.isBigOperation(fromBytes)) {
                return (T) new BigResultOperation(fromBytes);
            } else {
                return (T) new ResultOperation(fromBytes);
            }
        } else {
            return (T) EmptyResultOperation.EMPTY_OPERATION;
        }
    }

    public static <T extends Operation> T fromBytes(TransportConfiguration configuration, MessageType type, byte[] fromBytes, String location) throws NotActualRegionMappingException {
        return fromBytes(type, fromBytes, location, Long.parseLong(configuration.get(ATALANTA_TRANSPORT_CORE_UNAVAILABLE_TIMEOUT_PROPERTY)));
    }

    public static void checkReturnedExceptions(TransportConfiguration configuration, MessageType type, String location) throws NotActualRegionMappingException {
        checkReturnedExceptions(type, location, Long.parseLong(configuration.get(ATALANTA_TRANSPORT_CORE_UNAVAILABLE_TIMEOUT_PROPERTY)));
    }

    public static void checkReturnedExceptions(MessageType type, String location, long timeout) throws NotActualRegionMappingException {
        if (type == MessageType.INVALID_REGION_MAP_ERROR || type == MessageType.KEY_OUT_OF_RANGE_ERROR) {
            throw new NotActualRegionMappingException(location);
        } else if (type == MessageType.RECORD_TOO_LARGE_ERROR) {
            LOGGER.error(RecordTooLargeException.MESSAGE);
            throw new RecordTooLargeException();
        } else if (type == MessageType.CORE_RS_TOO_BUSY_ERROR) {
            throw new RepeatableExecutor.UnavailableActionException(timeout);
        } else if (type == MessageType.BAD_DATA_ERROR) {
            throw new BadDataException();
        }
    }

    public static <T extends Operation> T fromBytes(TransportConfiguration configuration, byte[] fromBytes, String location) throws NotActualRegionMappingException {
        return fromBytes(configuration, OperationHelper.getTransportMessageType(fromBytes), fromBytes, location);
    }

    public static <T extends Operation> T fromBytes(byte[] fromBytes, String location, long timeout) throws NotActualRegionMappingException {
        return fromBytes(OperationHelper.getTransportMessageType(fromBytes), fromBytes, location, timeout);
    }
}
