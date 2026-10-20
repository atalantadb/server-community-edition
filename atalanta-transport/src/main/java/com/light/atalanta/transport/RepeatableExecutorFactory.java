/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport;

import com.light.atalanta.RepeatableExecutor;
import com.light.atalanta.transport.exceptions.AtalantaSerializationException;

import static com.light.atalanta.transport.TransportConfiguration.ATALANTA_TRANSPORT_SOCKET_RETRIES_NUMBER_PROPERTY;
import static com.light.atalanta.transport.TransportConfiguration.ATALANTA_TRANSPORT_SOCKET_RETRIES_TIMEOUT_PROPERTY;

public class RepeatableExecutorFactory {
    public static final String ACTION_SEND_MESSAGE = "sendMessage";
    static final String ACTION_REQUEST_COUNT = "requestCount";
    public static final String ACTION_RECEIVE_MESSAGE = "receiveMessage";
    static final String ACTION_SEND_BYTES = "sendBytes";
    public static final String ACTION_SEND_EMPTY_AUTH_REQUEST = "sendEmptyAuthRequest";
    static final String ACTION_GET_SINGLE_RESPONSE = "getSingleResponse";
    static final String ACTION_SEND_SINGLE_REQUEST = "sendSingleRequest";
    static final String ACTION_SEND_REQUEST_TO_RS = "sendRequestToRs";

    private final TransportConfiguration configuration;
    private final ThreadLocal<RepeatableExecutor> sendMessageExecutors;
    private final ThreadLocal<RepeatableExecutor> requestCountExecutors;
    private final ThreadLocal<RepeatableExecutor> receiveMessageExecutors;
    private final ThreadLocal<RepeatableExecutor> sendBytesExecutors;
    private final ThreadLocal<RepeatableExecutor> sendEmptyAuthRequestExecutors;
    private final ThreadLocal<RepeatableExecutor> getSingleResponseExecutors;
    private final ThreadLocal<RepeatableExecutor> sendSingleRequestExecutors;
    private final ThreadLocal<RepeatableExecutor> sendRequestToRsExecutors;


    public RepeatableExecutorFactory(TransportConfiguration configuration) {
        this.configuration = configuration;
        this.sendMessageExecutors = ThreadLocal.withInitial(() -> createDefaultExecutor(ACTION_SEND_MESSAGE));
        this.requestCountExecutors = ThreadLocal.withInitial(() -> createDefaultExecutor(ACTION_REQUEST_COUNT));
        this.receiveMessageExecutors = ThreadLocal.withInitial(() -> createDefaultExecutor(ACTION_RECEIVE_MESSAGE));
        this.sendBytesExecutors = ThreadLocal.withInitial(() -> createDefaultExecutor(ACTION_SEND_BYTES));
        this.sendEmptyAuthRequestExecutors = ThreadLocal.withInitial(() -> createDefaultExecutor(ACTION_SEND_EMPTY_AUTH_REQUEST));
        this.getSingleResponseExecutors = ThreadLocal.withInitial(() -> createDefaultExecutor(ACTION_GET_SINGLE_RESPONSE));
        this.sendSingleRequestExecutors = ThreadLocal.withInitial(() -> createDefaultExecutor(ACTION_SEND_SINGLE_REQUEST));
        this.sendRequestToRsExecutors = ThreadLocal.withInitial(() -> createDefaultExecutor(ACTION_SEND_REQUEST_TO_RS));
    }

    private RepeatableExecutor createDefaultExecutor(String actionName) {
        return new RepeatableExecutor()
                .withActionName(actionName)
                .withMaxRetries(Long.parseLong(configuration.get(ATALANTA_TRANSPORT_SOCKET_RETRIES_NUMBER_PROPERTY)))
                .withIncrementTimeout(Long.parseLong(configuration.get(ATALANTA_TRANSPORT_SOCKET_RETRIES_TIMEOUT_PROPERTY)))
                .skipException(AtalantaSerializationException.class);
    }

    public RepeatableExecutor getExecutor(String actionName) {
        switch (actionName) {
            case ACTION_SEND_MESSAGE: {
                return sendMessageExecutors.get();
            }
            case ACTION_REQUEST_COUNT: {
                return requestCountExecutors.get();
            }
            case ACTION_RECEIVE_MESSAGE: {
                return receiveMessageExecutors.get();
            }
            case ACTION_SEND_BYTES: {
                return sendBytesExecutors.get();
            }
            case ACTION_SEND_EMPTY_AUTH_REQUEST: {
                return sendEmptyAuthRequestExecutors.get();
            }
            case ACTION_GET_SINGLE_RESPONSE: {
                return getSingleResponseExecutors.get();
            }
            case ACTION_SEND_SINGLE_REQUEST: {
                return sendSingleRequestExecutors.get();
            }
            case ACTION_SEND_REQUEST_TO_RS: {
                return sendRequestToRsExecutors.get();
            }
        }
        throw new RuntimeException("Incorrect configuration");
    }
}
