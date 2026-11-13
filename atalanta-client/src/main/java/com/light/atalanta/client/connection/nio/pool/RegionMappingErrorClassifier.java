/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.pool;

import com.light.atalanta.exceptions.AtalantaTransportRuntimeException;
import com.light.atalanta.exceptions.NoRetryException;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;
import com.light.atalanta.transport.exceptions.UnknownRegionException;
import com.light.atalanta.transport.messages.MessageType;
import com.light.atalanta.transport.messages.OperationFactory;
import com.light.atalanta.transport.messages.OperationHelper;

import java.io.IOException;

final class RegionMappingErrorClassifier {
    private RegionMappingErrorClassifier() {
    }

    static MessageType validateResponse(byte[] response) throws NotActualRegionMappingException {
        return validateResponse(response, "unknown", 0L);
    }

    static MessageType validateResponse(byte[] response, String location, long unavailableTimeout)
            throws NotActualRegionMappingException {
        MessageType type = OperationHelper.getTransportMessageType(response);
        OperationFactory.checkReturnedExceptions(type, location, unavailableTimeout);
        return type;
    }

    static boolean isRegionMappingError(Throwable error) {
        Throwable cause = error;
        while (cause != null) {
            if (cause instanceof NotActualRegionMappingException)
                return true;

            if (cause == cause.getCause())
                break;

            cause = cause.getCause();
        }
        return false;
    }

    static boolean isRecoverableRoutingError(Throwable error) {
        if (NoRetryException.findCause(error) != null) {
            return false;
        }
        Throwable cause = error;
        while (cause != null) {
            if (cause instanceof NotActualRegionMappingException ||
                    cause instanceof UnknownRegionException ||
                    cause instanceof AtalantaTransportRuntimeException || cause instanceof IOException) {
                return true;
            }


            if (cause == cause.getCause())
                break;

            cause = cause.getCause();
        }
        return false;
    }

    static boolean isUnknownRegionError(Throwable error) {
        Throwable cause = error;
        while (cause != null) {
            if (cause instanceof UnknownRegionException)
                return true;

            if (cause == cause.getCause())
                break;
            cause = cause.getCause();
        }
        return false;
    }
}

