/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.exceptions.NoRetryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class RepeatableExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(RepeatableExecutor.class);

    private static final long DEFAULT_RETRIES = 5;

    private long initialTimeout;

    private String actionName = null;
    private long maxRetries = -1;
    private long timeout = 0;
    private boolean incrementTime = false;

    private int currentRetries = 0;

    private Map<Class<? extends Exception>, ActionOnException> exceptionAction = null;
    private Set<Class<? extends Exception>> skippedException = null;

    public RepeatableExecutor() {
    }

    public RepeatableExecutor withMaxRetries(long maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public RepeatableExecutor withActionName(String name) {
        this.actionName = name;
        return this;
    }

    public RepeatableExecutor withStaticTimeout(long timeout) {
        this.timeout = timeout;
        this.initialTimeout = timeout;
        return this;
    }

    public RepeatableExecutor withIncrementTimeout(long timeout) {
        this.timeout = timeout;
        this.initialTimeout = timeout;
        this.incrementTime = true;
        return this;
    }

    public RepeatableExecutor withExceptionalAction(Class<? extends Exception> exceptionClass, Runnable action, boolean isCritical) {
        if (this.exceptionAction == null) {
            this.exceptionAction = new HashMap<>();
        }
        this.exceptionAction.put(exceptionClass, new ActionOnException(exceptionClass, action, isCritical));
        return this;
    }

    public RepeatableExecutor skipException(Class<? extends Exception> exceptionClass) {
        if (this.skippedException == null) {
            this.skippedException = new HashSet<>();
        }
        this.skippedException.add(exceptionClass);
        return this;
    }

    public <T> T execute(Supplier<T> supplier) {
        checkParameters();
        Exception exc = null;
        cleanUpExecutor();
        for (; currentRetries < maxRetries; currentRetries++) {
            try {
                T result = supplier.get();
                cleanUpExecutor();
                return result;
            } catch (Exception e) {
                NoRetryException terminalError = NoRetryException.findCause(e);
                if (terminalError != null) {
                    cleanUpExecutor();
                    throw terminalError;
                }
                String name = actionName != null ? "[" + actionName + "]" : "<unknown>";
                if (currentRetries == 0) {
                    LOGGER.error("An error occurred! Will try {} more times...", maxRetries, e);
                }
                exc = e;
                if (isExceptionSkipped(e)) {
                    LOGGER.warn("Exception {} should be skipped by current RepeatableExecutor({})", e.getMessage(), name);
                    cleanUpExecutor();
                    throw e;
                }
                String errorMsg = e.getMessage() + (e.getCause() != null && !"null".equals(e.getCause().getMessage()) ? " " + e.getCause().getMessage() : "");
                LOGGER.info("Action " + name + "(retry {} / {}) failed while executing: {}", currentRetries + 1, maxRetries, errorMsg);
                LOGGER.debug("Error", e);
                if (maxRetries == 1 || currentRetries >= maxRetries) {
                    cleanUpExecutor();
                    throw e;
                }
                if (exc instanceof UnavailableActionException) {
                    long specificTimeout = ((UnavailableActionException) exc).getTimeout();
                    LOGGER.info("Action couldn't be executed due to downstream availability. Additional timeout: {}", specificTimeout);
                    sleep(specificTimeout);
                } else {
                    sleep(timeout);
                }
                if (timeout != 0 && incrementTime) {
                    timeout += timeout;
                }
                if (this.exceptionAction != null) {
                    this.exceptionAction.values().forEach(action -> action.execute(e));
                }
            }
        }
        if (exc != null) {
            throw (RuntimeException) exc;
        } else {
            throw new RuntimeException(exc);
        }
    }

    private void cleanUpExecutor() {
        this.currentRetries = 0;
        this.timeout = this.initialTimeout;
    }

    private boolean isExceptionSkipped(Exception e) {
        if (this.skippedException == null) {
            return false;
        }

        if (this.skippedException.contains(e.getClass())) {
            return true;
        }

        Throwable cause = e.getCause();
        if (cause == null) {
            return false;
        }
        return this.skippedException.contains(cause.getClass());
    }

    private static void sleep(long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (Exception sleep) {
            LOGGER.error("Error while sleeping...", sleep);
        }
    }

    private void checkParameters() {
        if (maxRetries < 0) {
            withMaxRetries(DEFAULT_RETRIES);
        }
        if (maxRetries == 0 && timeout == 0) {
            throw new IllegalArgumentException("Incorrect setup");
        }
    }

    private static class ActionOnException {
        private final Class<? extends Exception> exceptionClass;
        private final Runnable action;
        private final boolean isCriticalAction;

        ActionOnException(Class<? extends Exception> exceptionClass, Runnable action, boolean isCriticalAction) {
            this.exceptionClass = exceptionClass;
            this.action = action;
            this.isCriticalAction = isCriticalAction;
        }

        void execute(Exception e) {
            if (exceptionClass == null || action == null) {
                return;
            }
            if (exceptionClass.equals(e.getClass()) || (e.getCause() != null && exceptionClass.equals(e.getCause().getClass()))) {
                try {
                    action.run();
                } catch (Exception exc) {
                    LOGGER.error(exc.getMessage(), exc);
                    if (isCriticalAction) {
                        throw exc;
                    }
                }
            }
        }
    }

    public static class UnavailableActionException extends RuntimeException {
        private final long timeout;

        public UnavailableActionException(long millis) {
            this.timeout = millis;
        }

        public long getTimeout() {
            return timeout;
        }
    }
}
