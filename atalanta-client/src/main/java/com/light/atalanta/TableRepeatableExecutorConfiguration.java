/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;


import com.light.atalanta.config.AtalantaProperties;
import com.light.atalanta.transport.exceptions.BadDataException;
import com.light.atalanta.transport.exceptions.AtalantaSerializationException;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;
import com.light.atalanta.transport.exceptions.UnavailableServerException;
import com.light.atalanta.transport.exceptions.UnknownRegionException;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;

public class TableRepeatableExecutorConfiguration {
    static final String ACTION_SCAN_REGION = "scanRegion";
    static final String ACTION_PROCESS_BYTES_WITH_RETRY = "processBytesWithRetry";
    static final String ACTION_MAKE_OPERATIONS_WITH_RETRY = "makeOperationsWithRetry";
    public static final String ACTION_PROCESS_OPERATIONS_WITH_RETRY = "processOperationsWithRetry";
    static final String ACTION_COUNT_WITH_RETRY = "count";

    private final DbConfiguration configuration;
    private final ThreadLocal<RepeatableExecutor> scanRegionExecutors;
    private final ThreadLocal<RepeatableExecutor> processBytesWithRetryExecutors;
    private final ThreadLocal<RepeatableExecutor> makeOperationsWithRetryExecutors;
    private final ThreadLocal<RepeatableExecutor> processOperationsWithRetryExecutors;
    private final ThreadLocal<RepeatableExecutor> processCountRetryExecutors;

    public TableRepeatableExecutorConfiguration(DbConfiguration configuration) {
        this.configuration = configuration;
        this.scanRegionExecutors = ThreadLocal.withInitial(() -> createDefaultExecutor(ACTION_SCAN_REGION));
        this.processBytesWithRetryExecutors = ThreadLocal.withInitial(() -> createDefaultExecutor(ACTION_PROCESS_BYTES_WITH_RETRY));
        this.makeOperationsWithRetryExecutors = ThreadLocal.withInitial(() -> createDefaultExecutor(ACTION_MAKE_OPERATIONS_WITH_RETRY));
        this.processOperationsWithRetryExecutors = ThreadLocal.withInitial(() -> createDefaultExecutor(ACTION_PROCESS_OPERATIONS_WITH_RETRY));
        this.processCountRetryExecutors = ThreadLocal.withInitial(() -> createDefaultExecutor(ACTION_COUNT_WITH_RETRY));
    }

    public RepeatableExecutor addExceptionActions(RepeatableExecutor re, Runnable runnable) {
        return re.withExceptionalAction(IOException.class, runnable, false)
                .withExceptionalAction(SocketTimeoutException.class, runnable, false)
                .withExceptionalAction(SocketException.class, runnable, false)
                .withExceptionalAction(UnknownRegionException.class, runnable, false)
                .withExceptionalAction(UnavailableServerException.class, runnable, false)
                .withExceptionalAction(BadDataException.class, runnable, false)
                .withExceptionalAction(NotActualRegionMappingException.class, runnable, false);
    }

    private RepeatableExecutor createDefaultExecutor(String actionName) {
        String maxRetriesVal = configuration.getOrDefault(AtalantaProperties.ATALANTA_CLIENT_TABLE_RETRIES_NUMBER, "1000");
        String timeoutVal = configuration.getOrDefault(AtalantaProperties.ATALANTA_CLIENT_TABLE_RETRIES_TIMEOUT_MS, "5000");
        long maxRetries = Long.parseLong(maxRetriesVal);
        long timeout = Long.parseLong(timeoutVal);
        return new RepeatableExecutor()
                .withActionName(actionName)
                .withMaxRetries(maxRetries)
                .withStaticTimeout(timeout)
                .skipException(AtalantaSerializationException.class);
    }

    public RepeatableExecutor getExecutor(String actionName) {
        switch (actionName) {
            case ACTION_SCAN_REGION: {
                return scanRegionExecutors.get();
            }
            case ACTION_PROCESS_BYTES_WITH_RETRY: {
                return processBytesWithRetryExecutors.get();
            }
            case ACTION_MAKE_OPERATIONS_WITH_RETRY: {
                return makeOperationsWithRetryExecutors.get();
            }
            case ACTION_PROCESS_OPERATIONS_WITH_RETRY: {
                return processOperationsWithRetryExecutors.get();
            }
            case ACTION_COUNT_WITH_RETRY: {
                return processCountRetryExecutors.get();
            }
        }
        throw new RuntimeException("Incorrect configuration");
    }
}
