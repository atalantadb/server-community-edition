/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.transport;

import com.light.atalanta.DbConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.light.atalanta.config.AtalantaProperties.ATALANTA_CLIENT_NIO_SELECTOR_LOOP_SIZE;
import static com.light.atalanta.config.AtalantaProperties.ATALANTA_CONNECTION_POOL_SIZE;

public final class NioSelectorLoopGroup implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(NioSelectorLoopGroup.class);
    private static final int WRITE_THREADS_PER_SELECTOR = 5;

    private final List<NioSelectorLoop> selectorLoops;
    private final AtomicInteger nextIndex = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final NioShutdownLogging shutdownLogging = new NioShutdownLogging();

    public NioSelectorLoopGroup(DbConfiguration configuration) {
        int configuredLoopSize = configuration.getInt(ATALANTA_CLIENT_NIO_SELECTOR_LOOP_SIZE);
        if (configuredLoopSize < 0)
            throw new IllegalArgumentException(ATALANTA_CLIENT_NIO_SELECTOR_LOOP_SIZE + " must not be negative");
        int loopSize = configuredLoopSize > 0 ? configuredLoopSize : configuration.getInt(ATALANTA_CONNECTION_POOL_SIZE) / WRITE_THREADS_PER_SELECTOR + 1;
        LOGGER.info("Initializing {} NIO selector loops (configured: {})", loopSize, configuredLoopSize);

        List<NioSelectorLoop> created = new ArrayList<>(loopSize);
        try {
            for (int i = 0; i < loopSize; i++)
                created.add(new NioSelectorLoopImpl(shutdownLogging));
        } catch (Throwable creationFailure) {
            closeAfterConstructionFailure(created, creationFailure);
            throw creationFailure;
        }
        selectorLoops = Collections.unmodifiableList(created);
    }

    public NioSelectorLoop next() throws IOException {
        if (closed.get())
            throw new IOException("NIO selector loop group is closed");
        int index = Math.floorMod(nextIndex.getAndIncrement(), selectorLoops.size());
        NioSelectorLoop result = selectorLoops.get(index);
        if (closed.get()) {
            throw new IOException("NIO selector loop group is closed");
        }
        return result;
    }

    public int size() {
        return selectorLoops.size();
    }

    public NioShutdownLogging.Scope beginShutdownLogging() {
        return shutdownLogging.begin();
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true))
            return;

        IOException failure = null;
        boolean interrupted = false;
        for (NioSelectorLoop selectorLoop : selectorLoops) {
            try {
                selectorLoop.shutdown();
            } catch (InterruptedException error) {
                interrupted = true;
                failure = addFailure(failure,
                        new IOException("Interrupted while stopping NIO selector loop", error));
            } catch (IOException error) {
                failure = addFailure(failure, error);
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void closeAfterConstructionFailure(List<NioSelectorLoop> loops, Throwable creationFailure) {
        boolean interrupted = false;
        for (NioSelectorLoop loop : loops) {
            try {
                loop.shutdown();
            } catch (InterruptedException cleanupFailure) {
                interrupted = true;
                creationFailure.addSuppressed(cleanupFailure);
            } catch (Throwable cleanupFailure) {
                creationFailure.addSuppressed(cleanupFailure);
            }
        }
        if (interrupted)
            Thread.currentThread().interrupt();
    }

    private static IOException addFailure(IOException current, IOException next) {
        if (current == null)
            return next;
        if (current != next)
            current.addSuppressed(next);
        return current;
    }
}
