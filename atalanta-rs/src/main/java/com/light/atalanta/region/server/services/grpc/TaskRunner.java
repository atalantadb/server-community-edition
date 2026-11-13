/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.grpc;

import com.light.atalanta.TableName;
import com.light.atalanta.region.server.utils.UniqueNameThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Runs the given task with the given parameter.
 * Can stop it on request.
 */
public class TaskRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskRunner.class);

    private final ConcurrentHashMap<RegionTask, ExecutorService> tasks = new ConcurrentHashMap<>();

    private final ThreadFactory runnerThreadFactory = new UniqueNameThreadFactory("atalanta-task-runner-pool");

    public void run(RegionTask task) {
        ExecutorService operationExecutor = Executors.newSingleThreadExecutor(runnerThreadFactory);
        try {
            CountDownLatch running = new CountDownLatch(1);

            Runnable operationThread = () -> {
                try {
                    task.run();
                } finally {
                    tasks.remove(task);
                    running.countDown();
                }
            };

            tasks.put(task, operationExecutor);
            operationExecutor.submit(operationThread);
            try {
                running.await();
            } catch (InterruptedException ignore) {
                // ignore
            }
        } catch (Exception ex) {
            LOGGER.error("Task execution error: {}", task, ex);
            throw ex;
        } finally {
            LOGGER.debug("Freeing up resources after task: {}, start", task);
            operationExecutor.shutdownNow();
            try {
                operationExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
                LOGGER.debug("Freeing up resources after task: {}, finish", task);
            } catch (InterruptedException ex) {
                LOGGER.error("Error waiting operationExecutor to finish: interrupted");
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * This method gracefully waits for the task to complete.
     * Works within the specified table.
     * Waits for all tables if tableName is null.
     */
    public void wait(TableName tableName, String tableNameString) throws InterruptedException {
        Iterator<Map.Entry<RegionTask, ExecutorService>> taskItr = tasks.entrySet().iterator();
        while (taskItr.hasNext()) {
            Map.Entry<RegionTask, ExecutorService> cur = taskItr.next();
            if (tableName == null || cur.getKey().getTableName().equals(tableName)) {
                LOGGER.warn("Wait tasks for {}: start waiting for the running task, task: '{}'", tableNameString, cur.getKey());
                cur.getKey().sendGracefulStopSignal();
                LOGGER.warn("Wait tasks for {}: graceful shutdown signal has been sent, task: '{}'", tableNameString, cur.getKey());
                try {
                    cur.getValue().awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
                    LOGGER.warn("Wait tasks for {}: finish waiting for the running task, task: '{}'", tableNameString, cur.getKey());
                } catch (InterruptedException interrupted) {
                    LOGGER.warn("Error waiting tasks for {}: interrupted, task: '{}'", tableNameString, cur.getKey());
                    Thread.currentThread().interrupt();
                    throw interrupted;
                }
                taskItr.remove();
                LOGGER.warn("Wait tasks for {}: task has been stopped, task: '{}'", tableNameString, cur.getKey());
            }
        }
    }

    public void stopAll() {
        tasks.forEach((key, value) -> {
            key.sendGracefulStopSignal();
            value.shutdownNow();
        });
    }
}
