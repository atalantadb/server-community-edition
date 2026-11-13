/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.pool;

import com.light.atalanta.ConnectionThreadLocal;
import com.light.atalanta.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AtalantaGetWorkerThreadNio extends Thread{
    private static final Logger LOGGER = LoggerFactory.getLogger(AtalantaGetWorkerThreadNio.class);
    private final ConnectionThreadLocal connection;
    private final Runnable runnable;

    private List<TableName> tableToClose = new ArrayList<>();

    private static final List<AtalantaGetWorkerThreadNio> readThreads = new CopyOnWriteArrayList<>();

    private Lock lock = new ReentrantLock();

    public AtalantaGetWorkerThreadNio(Runnable runnable, ConnectionThreadLocal connection) {
        this.runnable = runnable;
        this.connection = connection;
        readThreads.add(this);
    }

    public static void closeTable(TableName tableName) {
        readThreads.forEach(t -> {
            t.lock.lock();
            try {
                 t.tableToClose.add(tableName);
            } finally {
               t.lock.unlock();
            }
        });
    }

    public static void reopenTableIfNeeded(TableName tableName) {
        readThreads.forEach(t -> {
            t.lock.lock();
            try {
                t.tableToClose.remove(tableName);
            } finally {
                t.lock.unlock();
            }
        });
    }

    @Override
    public void run() {
        try {
            closeIfNeeded();
            runnable.run();
        } catch (Throwable er) {
            LOGGER.error("error in get pool !", er);
        } finally {
            connection.closeAllTableThreadLocal();
            readThreads.remove(this);
            LOGGER.info("all tables has been closed. Thread: {}, threadId: {}", Thread.currentThread(), Thread.currentThread().getId());
        }
    }

    private void closeIfNeeded() {
        if (!tableToClose.isEmpty()) {
            lock.lock();
            try {
                for (TableName t : tableToClose) {
                    try {
                        connection.getTable(t).close();
                    } catch (IOException e) {
                        LOGGER.error("Cannot close table " + t, e);
                    }
                }
                tableToClose = new ArrayList<>();
            } finally {
                lock.unlock();
            }
        }
    }

}
