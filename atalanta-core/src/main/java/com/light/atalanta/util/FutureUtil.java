/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.util;

import com.light.atalanta.model.RegionDistributionModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public final class FutureUtil {

    private FutureUtil() {
    }

    public static <T> void waitCompletion(CompletableFuture<T>[] futures) throws ExecutionException, InterruptedException {
        CompletableFuture.allOf(futures).get();
    }

    public static <R,T> List<R> waitAndGet(List<CompletableFuture<T>> futures) throws IOException {
        List<R> results = new ArrayList<>(futures.size());
        try {
            for (CompletableFuture<T> future : futures) {
                R r = (R) future.get();
                results.add(r);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("The operation was interrupted", e);
        } catch (ExecutionException e) {
            throw new IOException("Asynchronous task execution failed", e.getCause());
        }
        return results;

    }
}
