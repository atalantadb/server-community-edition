/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.pool;

import com.light.atalanta.*;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;
import com.light.atalanta.transport.messages.MessageType;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.light.atalanta.Result.EMPTY_RESULT;
import static com.light.atalanta.config.AtalantaProperties.ATALANTA_CLIENT_ENABLE_BULK_API;
import static com.light.atalanta.util.FutureUtil.waitAndGet;

@Deprecated
public class TableAsyncMutation extends Table implements DbBufferedMutator {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableAsyncMutation.class);

    private static final int HASH_MUL = 109;
    private final Map<TableName, TableAsyncMutation> tableCache;
    private final List<AsyncMutationProc> asyncMutationProcList;
    private static int scanRoundRobinCurIndex;
    private final TableName tableName;
    private final Connection connection;
    private final Lock closeLock = new ReentrantLock();
    private boolean isClosed = false;

    public TableAsyncMutation(TableName tableName, Connection connection,
                              List<AsyncMutationProc> asyncMutationProcList, Map<TableName,
            TableAsyncMutation> tablesCache) throws NotActualRegionMappingException, IOException {
        super((ConnectionThreadLocal) connection, tableName, getActualRegionsMapping((ConnectionThreadLocal) connection, tableName));
        this.tableName = tableName;
        this.connection = connection;
        this.asyncMutationProcList = asyncMutationProcList;
        scanRoundRobinCurIndex = ThreadLocalRandom.current().nextInt(0, this.asyncMutationProcList.size());
        this.tableCache = tablesCache;
        AtalantaGetWorkerThread.reopenTableIfNeeded(tableName);
    }

    private static Map<RegionServerModel, List<RegionModel>> getActualRegionsMapping(ConnectionThreadLocal connection, TableName tableName) {
        try {
            return connection.getActualRegionsMapping(tableName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void mutate(DbMutation mutation) throws IOException {
        mutateAsync(mutation);
    }

    @Override
    public void mutateNoFlush(DbMutation mutation) throws IOException {
        mutateAsync(mutation);
    }

    @Override
    public void mutate(List<DbMutation> mutations) throws IOException {
        mutateAsync(mutations);
    }

    @Override
    public void mutateNoFlush(List<DbMutation> mutations) throws IOException {
        mutateAsync(mutations);
    }

    @Override
    public Result get(Get get) throws IOException {
        AsyncMutationProc asyncMutationProc = getAsyncMutationProcByKey(get.getRow());
        try {
            return (Result) asyncMutationProc.get(get, tableName).get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Cannot get data from ATALANTA", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Result[] get(List<Get> listGet) throws IOException {
        if (isBulkApi()) {
            return getBulk(listGet);
        } else {
            return getList(listGet).toArray(new Result[0]);
        }
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        AsyncMutationProc asyncMutationProc = getRoundRobinAsyncProc();
        return asyncMutationProc.getScanner(scan, tableName);
    }

    @Override
    public void mutateRow(RowMutations mutation) throws IOException {
        mutateAsync(mutation);
    }

    @Override
    public void flush() throws IOException {
        for (AsyncMutationProc amp : asyncMutationProcList) {
            amp.waitAllOperation();
        }
    }

    @Override
    public TableName getName() {
        return tableName;
    }

    @Override
    public void put(List<Put> putList) throws IOException {
        if (isBulkApi()) {
            putBulk(putList);
        } else {
            waitAndGet(putAsync(putList));
        }
    }

    @Override
    public void put(Put put) throws IOException {
        try {
            mutateAsync(put).get();
        } catch (InterruptedException e) {
            LOGGER.info("interrupted!");
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
        waitAndGet(deleteAsync(deletes));
    }

    @Override
    public void delete(Delete delete) throws IOException {
        try {
            mutateAsync(delete).get();
        } catch (InterruptedException e) {
            LOGGER.info("interrupted!");
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long count() throws IOException {
        // TODO: Temporary workaround
        return connection.getTable(tableName).count();
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    @Override
    public Result mutateRowAndGet(RowMutations mutation) throws IOException {
        throw new NotImplementedException();
    }

    public List<Result> getList(List<Get> gets) throws IOException {
        AtomicReference<IOException> error = new AtomicReference<>(null);
        List<CompletableFuture<Result>> futures = gets.stream()
                .map(g -> {
                    try {
                        return getInternal(g);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
        List<Result> res = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
        if (error.get() != null) {
            throw new IOException(error.get());
        }
        return res;
    }

    public Result deleteAndGet(Delete delete) throws IOException {
        try {
            return mutateAsync(delete).get();
        } catch (InterruptedException e) {
            LOGGER.warn("interrupted!");
            return EMPTY_RESULT;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Result> deleteAndGetList(List<Delete> deletes) throws IOException {
        return waitAndGet(deleteAsync(deletes));
    }

    @Override
    public void putReusablePut(Put put) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void putReusablePuts(List<Put> puts) throws IOException {
        throw new NotImplementedException();
    }

    public void destroyTableConnection() throws IOException {
        closeLock.lock();
        try {
            if (isClosed) {
                return;
            }
            flush();
            AtalantaGetWorkerThread.closeTable(tableName);
            tableCache.remove(this);
            isClosed = true;
        } finally {
            closeLock.unlock();
        }
    }

    private CompletableFuture<Result> mutateAsync(DbMutation mutation) {
        AsyncMutationProc asyncMutationProc = getAsyncMutationProcByKey(mutation.getRow());
        return asyncMutationProc.mutate(mutation, tableName);
    }

    private List<CompletableFuture<Result>> mutateAsync(List<DbMutation> mutations) {
        return mutations.stream()
                .map(this::mutateAsync)
                .collect(Collectors.toList());
    }

    private List<CompletableFuture<Result>> putAsync(List<Put> mutations) {
        return mutations.stream()
                .map(this::mutateAsync)
                .collect(Collectors.toList());
    }

    private List<CompletableFuture<Result>> deleteAsync(List<Delete> mutations) {
        return mutations.stream()
                .map(this::mutateAsync)
                .collect(Collectors.toList());
    }

    private CompletableFuture<Result> getInternal(Get get) throws IOException {
        AsyncMutationProc asyncMutationProc = getAsyncMutationProcByKey(get.getRow());
        return asyncMutationProc.get(get, tableName);
    }

    private AsyncMutationProc getRoundRobinAsyncProc() {
        int index = (++scanRoundRobinCurIndex) % asyncMutationProcList.size();
        return asyncMutationProcList.get(index);
    }

    private AsyncMutationProc getAsyncMutationProcByKey(byte[] row) {
        int index = Math.abs(hashCode109(row) % asyncMutationProcList.size());
        return asyncMutationProcList.get(index);
    }

    private int hashCode109(byte[] value) {
        if (value.length == 0) {
            return 0;
        }
        int h = value[0];
        for (int i = 1; i < value.length; ++i) {
            h = h * HASH_MUL + value[i];
        }
        return h * HASH_MUL;
    }

    protected boolean isBulkApi() {
        return connection == null ? false : connection.getConfiguration().getBoolean(ATALANTA_CLIENT_ENABLE_BULK_API);
    }

}
