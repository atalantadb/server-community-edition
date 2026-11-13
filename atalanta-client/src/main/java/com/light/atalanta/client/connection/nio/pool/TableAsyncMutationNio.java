/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.pool;

import com.google.common.hash.Hashing;
import com.light.atalanta.*;
import com.light.atalanta.config.AtalantaProperties;
import com.light.atalanta.exceptions.NoRetryException;
import com.light.atalanta.transport.exceptions.AtalantaSerializationException;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;
import com.light.atalanta.transport.messages.BulkResponseOperation;
import com.light.atalanta.transport.messages.MessageType;
import com.light.atalanta.util.Bytes;
import com.light.atalanta.util.BytesComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TableAsyncMutationNio extends Table {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableAsyncMutationNio.class);
    public static final Result[] EMPTY_RESULT_ARRAY = new Result[0];
    private final Map<TableName, TableAsyncMutationNio> tableCache;
    private final List<AsyncMutationProcNio> asyncMutationProcNioList;
    private final TableName tableName;
    private final ConnectionThreadLocal connection;
    private final Lock closeLock = new ReentrantLock();
    private final long maxRetries;
    private final long timeout;
    private final long minBacthSize;
    private final long autoFlushSize;
    private boolean isClosed = false;
    private final DbConfiguration conf;
    private final int scanIdx;
    private final Queue<Pair<List<CompletableFuture<Object>>, List<DbMutation>>> queueForFlush = new ConcurrentLinkedQueue<>();
    private final AtomicInteger queueForFlushSize = new AtomicInteger(0);
    private final Function<byte[], Integer> asyncMutationProcNioIndexSupplier;

    public TableAsyncMutationNio(TableName tableName, ConnectionThreadLocal connection,
                                 List<AsyncMutationProcNio> asyncMutationProcNioList, Map<TableName,
            TableAsyncMutationNio> tablesCache, DbConfiguration conf) throws NotActualRegionMappingException, IOException {
        this.tableName = tableName;
        this.connection = connection;
        this.asyncMutationProcNioList = asyncMutationProcNioList;
        this.conf = conf;
        this.tableCache = tablesCache;
        AtalantaGetWorkerThreadNio.reopenTableIfNeeded(tableName);
        scanIdx = (tableName.hashCode() & 0x7FFFFFFF) % asyncMutationProcNioList.size();
        maxRetries = Long.parseLong(conf.getOrDefault(AtalantaProperties.ATALANTA_CLIENT_TABLE_RETRIES_NUMBER, "1000"));
        timeout = Long.parseLong(conf.getOrDefault(AtalantaProperties.ATALANTA_CLIENT_TABLE_RETRIES_TIMEOUT_MS, "5000"));
        String hashAlg = conf.getOrDefault(AtalantaProperties.ATALANTA_CONNECTION_POOL_HASH_BY_THREAD_ID, "true");
        minBacthSize = Long.parseLong(conf.getOrDefault(AtalantaProperties.ATALANTA_CLIENT_MIN_BATCH_SIZE, "2"));
        autoFlushSize = Long.parseLong(conf.getOrDefault(AtalantaProperties.ATALANTA_CLIENT_AUTO_FLUSH_SIZE, "10"));

        boolean hashByThreadId = Boolean.valueOf(hashAlg);
        asyncMutationProcNioIndexSupplier = hashByThreadId ? this::getAsyncMutationProcThreadId : this::getAsyncMutationProcMurmur32;
    }

    @Override
    public Result get(Get get) throws IOException {
        Result[] results = get(Collections.singletonList(get));
        return results.length > 0 ? results[0] : Result.EMPTY_RESULT;
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
        if (gets.size() > 1 && gets.size() <= minBacthSize) {
            List<Result> results = new ArrayList<>(gets.size());
            for (Get get : gets) {
                results.add(get(get));
            }
            return results.toArray(new Result[0]);
        }
        return getList(gets).toArray(EMPTY_RESULT_ARRAY);
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        AsyncMutationProcNio asyncMutationProcNio = asyncMutationProcNioList.get(scanIdx);
        return asyncMutationProcNio.getScanner(scan, tableName);
    }

    @Override
    public TableName getName() {
        return tableName;
    }

    @Override
    public void put(Put put) throws IOException {
        put(Collections.singletonList(put));
    }

    @Override
    public void put(List<Put> putList) throws IOException {
        if (putList.size() > 1 && putList.size() <= minBacthSize) {
            for (Put put : putList) {
                put(put);
            }
            return;
        }
        Throwable error = null;
        for (int retryCounter = 0; retryCounter < maxRetries; ++retryCounter) {
            try {
                List<CompletableFuture<Object>> completableFutureList = opListWithoutRes(putList);
                for (CompletableFuture<Object> future : completableFutureList) {
                    List<CompletableFuture<Object>> cf = (List<CompletableFuture<Object>>) future.get();
                    CompletableFuture.allOf(cf.toArray(new CompletableFuture[0])).get();
                }
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                error = e.getCause();
                canRepeatException(error);
                LOGGER.warn("Error executing Put: {}, retry {}/{}, sleep {} ms", e.getMessage(), retryCounter, maxRetries, timeout);
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
        throw new RuntimeException(error);
    }

    private List<Result> decodeMutationResults(Object response) {
        if (response instanceof Result) {
            return Collections.singletonList((Result) response);
        }
        if (response instanceof BulkResponseOperation) {
            return ((BulkResponseOperation) response).getResultOperations().stream()
                    .map(Result::new)
                    .collect(Collectors.toList());
        }
        throw new IllegalStateException("Unexpected mutation response type: " +
                (response == null ? "null" : response.getClass().getName()));
    }

    private <T extends RowKey> List<Result> awaitOrderedMutationResults(List<T> operations,
            List<CompletableFuture<Object>> futures) throws InterruptedException, ExecutionException {
        Map<byte[], Deque<Integer>> positions = new TreeMap<>(BytesComparator.BYTES_COMPARATOR);
        for (int i = 0; i < operations.size(); i++) {
            positions.computeIfAbsent(operations.get(i).getRow(), key -> new ArrayDeque<>()).add(i);
        }

        List<Result> ordered = new ArrayList<>(Collections.nCopies(operations.size(), null));
        for (CompletableFuture<Object> future : futures) {
            for (Result result : decodeMutationResults(future.get())) {
                Deque<Integer> rowPositions = positions.get(result.getRow());
                if (rowPositions == null || rowPositions.isEmpty()) {
                    throw new IllegalStateException("Unexpected or duplicate mutation result for row: " +
                            Bytes.toStringBinary(result.getRow()));
                }
                ordered.set(rowPositions.removeFirst(), result);
            }
        }
        for (int i = 0; i < ordered.size(); i++) {
            if (ordered.get(i) == null) {
                throw new IllegalStateException("Missing mutation result at index " + i + ", row: " +
                        Bytes.toStringBinary(operations.get(i).getRow()));
            }
        }
        return ordered;
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
        Throwable error = null;
        for (int retryCounter = 0; retryCounter < maxRetries; ++retryCounter) {
            try {
                List<CompletableFuture<Object>> completableFutureList = opListWithoutRes(deletes);
                for (CompletableFuture<Object> future : completableFutureList) {
                    List<CompletableFuture<Object>> cf = (List<CompletableFuture<Object>>) future.get();
                    CompletableFuture.allOf(cf.toArray(new CompletableFuture[0])).get();
                }
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                error = e.getCause();
                canRepeatException(error);
                LOGGER.warn("Error executing Delete: {}, retry {}/{}, sleep {} ms", e.getMessage(), retryCounter, maxRetries, timeout);
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
        throw new RuntimeException(error);
    }

    @Override
    public void delete(Delete delete) throws IOException {
        delete(Collections.singletonList(delete));
    }

    @Override
    public List<Result> deleteAndGetList(List<Delete> deletes) throws IOException {
        Throwable error = null;
        for (int retryCounter = 0; retryCounter < maxRetries; ++retryCounter) {
            try {
                return awaitOrderedMutationResults(deletes, opListAsync(deletes));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                error = e.getCause();
                canRepeatException(error);
                LOGGER.warn("Error executing DeleteAndGetList: {}, retry {}/{}, sleep {} ms", e.getMessage(), retryCounter, maxRetries, timeout);
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
        throw new RuntimeException(error);
    }

    @Override
    public void mutateRow(RowMutations mutation) throws IOException {
        List<DbMutation> mutations = Collections.singletonList(mutation);
        Throwable error = null;
        for (int retryCounter = 0; retryCounter < maxRetries; ++retryCounter) {
            try {
                List<CompletableFuture<Object>> completableFutureList = opListWithoutRes(mutations);
                for (CompletableFuture<Object> future : completableFutureList) {
                    List<CompletableFuture<Object>> cf = (List<CompletableFuture<Object>>) future.get();
                    CompletableFuture.allOf(cf.toArray(new CompletableFuture[0])).get();
                }
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                error = e.getCause();
                canRepeatException(error);
                LOGGER.warn("Error executing mutation: {}, retry {}/{}, sleep {} ms", e.getMessage(), retryCounter, maxRetries, timeout);
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
        throw new RuntimeException(error);
    }

    @Override
    public void mutateNoFlush(DbMutation mutation) throws IOException {
        mutateNoFlush(Collections.singletonList(mutation));
    }

    @Override
    public void mutateNoFlush(List<DbMutation> mutations) throws IOException {
        mutateInternal(mutations, true);
    }

    private void mutateInternal(List<DbMutation> mutations, boolean isNoBlocked) throws IOException {
        Throwable error = null;
        List<DbMutation> curBatch = mutations;
        for (int retryCounter = 0; retryCounter < maxRetries; ++retryCounter) {
            try {
                List<CompletableFuture<Object>> completableFutureList = opListWithoutRes(mutations);
                for (CompletableFuture<Object> future : completableFutureList) {
                    List<CompletableFuture<Object>> cf = (List<CompletableFuture<Object>>) future.get();
                    if (isNoBlocked) {
                        queueForFlush.add(new Pair<>(cf, curBatch));
                        if (queueForFlushSize.incrementAndGet() >= autoFlushSize) {
                            flush();
                        }
                        curBatch = null;
                    } else {
                        CompletableFuture.allOf(cf.toArray(new CompletableFuture[0])).get();
                    }
                }
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                error = e.getCause();
                canRepeatException(error);
                LOGGER.warn("Error executing mutation: {}, retry {}/{}, sleep {} ms", e.getMessage(), retryCounter, maxRetries, timeout);
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
        throw new RuntimeException(error);
    }

    @Override
    public void mutate(DbMutation mutation) throws IOException {
        List<DbMutation> mutations = Collections.singletonList(mutation);
        mutateNoFlush(mutations);
    }

    @Override
    public void mutate(List<DbMutation> mutations) throws IOException {
        mutateNoFlush(mutations);
    }


    @Override
    public void flush() throws IOException {
        List<Pair<List<CompletableFuture<Object>>, List<DbMutation>>> futuresToWait = new ArrayList<>();
        Pair<List<CompletableFuture<Object>>, List<DbMutation>> future;
        while ((future = queueForFlush.poll()) != null) {
            futuresToWait.add(future);
            // TODO: Bound outstanding buffered requests across concurrent flushes, release capacity only on completion, not dequeue
            queueForFlushSize.decrementAndGet();
        }
        for (Pair<List<CompletableFuture<Object>>, List<DbMutation>> futureTo : futuresToWait) {
            for (int retryCounter = 0; retryCounter < maxRetries; ++retryCounter) {
                try {
                    CompletableFuture.allOf(futureTo.getFirst().toArray(new CompletableFuture[0])).get();
                    break;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    canRepeatException(e.getCause());
                    LOGGER.warn("Error executing flush: {}, retry {}/{}, sleep {} ms", e.getMessage(), retryCounter, maxRetries, timeout);
                    try {
                        Thread.sleep(timeout);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    mutateInternal(futureTo.getSecond(), false);
                    break; // Prevent an endless retry loop
                }
            }
        }
    }


    @Override
    public long count() throws IOException {
        return connection.getTable(tableName).count();
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    public List<Result> getList(List<Get> gets) throws IOException {
        long maxRetries = Long.parseLong(conf.getOrDefault(AtalantaProperties.ATALANTA_CLIENT_TABLE_RETRIES_NUMBER, "1000"));
        long timeout = Long.parseLong(conf.getOrDefault(AtalantaProperties.ATALANTA_CLIENT_TABLE_RETRIES_TIMEOUT_MS, "5000"));
        for (int i = 0; i < maxRetries; ++i) {
            try {
                return getListResult(gets);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof IllegalArgumentException) {
                    throw new RuntimeException(e);
                }
                canRepeatException(e.getCause());
                LOGGER.error("Retry {}/{}. Cannot get data.", i, maxRetries, e);
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        throw new IOException("Cannot get data for keys: size: " + gets.size() + ", first key: " +
                (gets.size() > 0 ? Bytes.toStringBinary(gets.iterator().next().getRow()) : "none"));
    }

    private void canRepeatException(Throwable t) throws IOException {
        NoRetryException terminalError = NoRetryException.findCause(t);
        if (terminalError != null) {
            throw terminalError;
        }
        for (Throwable cause = t; cause != null; cause = cause.getCause()) {
            if (cause instanceof AtalantaSerializationException) {
                throw (AtalantaSerializationException) cause;
            }
            if (cause.getCause() == cause) {
                break;
            }
        }
        if (t instanceof IllegalArgumentException) {
            throw new IOException(t);
        }
    }

    private <T extends RowKey> List<CompletableFuture<Object>> opListAsync(List<T> ops) throws IOException {
        List<CompletableFuture<Object>> res = opListWithoutRes(ops);

        List<CompletableFuture<Object>> results = new ArrayList<>(ops.size());
        try {
            for (CompletableFuture<Object> future : res) {
                Object o = future.get();
                if (o instanceof List) {
                    List<CompletableFuture<Object>> r = (List<CompletableFuture<Object>>) o;
                    results.addAll(r);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("The operation was interrupted", e);
        } catch (ExecutionException e) {
            throw new IOException("Asynchronous task execution failed", e.getCause());
        }
        return results;
    }

    private <T extends RowKey> List<CompletableFuture<Object>> opListWithoutRes(List<T> ops) throws IOException {
        if (ops.size() == 1) {
            T op = ops.get(0);
            int index = getAsyncMutationProcByKey(op.getRow());
            return Collections.singletonList(asyncMutationProcNioList.get(index).operation(ops, tableName));
        }

        Map<Integer, List<T>> operationsByProcessor = new TreeMap<>();
        for (T op : ops) {
            int index = getAsyncMutationProcByKey(op.getRow());
            operationsByProcessor.computeIfAbsent(index, ignored -> new ArrayList<>()).add(op);
        }
        List<CompletableFuture<Object>> res = new ArrayList<>(operationsByProcessor.size());
        for (Map.Entry<Integer, List<T>> entry : operationsByProcessor.entrySet()) {
            res.add(asyncMutationProcNioList.get(entry.getKey()).operation(entry.getValue(), tableName));
        }
        return res;
    }

    private List<Result> getListResult(List<Get> ops) throws IOException, ExecutionException {
        Map<Integer, ProcessorGetBatch> operationsByProcessor = new TreeMap<>();

        for (int i = 0; i < ops.size(); ++i) {
            Get op = ops.get(i);
            int index = getAsyncMutationProcByKey(op.getRow());
            operationsByProcessor.computeIfAbsent(index, ignored -> new ProcessorGetBatch()).add(op, i);
        }
        List<ProcessorGetResponse> responses = new ArrayList<>(operationsByProcessor.size());
        for (Map.Entry<Integer, ProcessorGetBatch> entry : operationsByProcessor.entrySet()) {
            AsyncMutationProcNio asyncMutationProcNio = asyncMutationProcNioList.get(entry.getKey());
            ProcessorGetBatch batch = entry.getValue();
            responses.add(new ProcessorGetResponse(batch,
                    asyncMutationProcNio.operation(batch.operations, tableName)));
        }

        List<Result> results = new ArrayList<>(Collections.nCopies(ops.size(), null));
        try {
            for (ProcessorGetResponse response : responses) {
                Result[] responseResults = (Result[]) response.future.get();
                if (responseResults.length != response.batch.size) {
                    throw new IllegalStateException("Processor bulk get response count " + responseResults.length
                            + " does not match request count " + response.batch.size);
                }
                for (int i = 0; i < responseResults.length; i++) {
                    results.set(response.batch.sourceIndexes[i], responseResults[i]);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("The operation was interrupted", e);
        }
        return results;
    }

    private static final class ProcessorGetBatch {
        private final List<Get> operations = new ArrayList<>();
        private int[] sourceIndexes = new int[8];
        private int size;

        private void add(Get operation, int sourceIndex) {
            if (size == sourceIndexes.length) {
                sourceIndexes = Arrays.copyOf(sourceIndexes, size << 1);
            }
            operations.add(operation);
            sourceIndexes[size++] = sourceIndex;
        }
    }

    private static final class ProcessorGetResponse {
        private final ProcessorGetBatch batch;
        private final CompletableFuture<Object> future;

        private ProcessorGetResponse(ProcessorGetBatch batch, CompletableFuture<Object> future) {
            this.batch = batch;
            this.future = future;
        }
    }

    public Result deleteAndGet(Delete delete) throws IOException {
        List<Result> results = deleteAndGetList(Collections.singletonList(delete));
        return results.isEmpty() ? Result.EMPTY_RESULT : results.get(0);
    }

    public void destroyTableConnection() throws IOException {
        closeLock.lock();
        try {
            if (isClosed) {
                return;
            }
            flush();
            AtalantaGetWorkerThreadNio.closeTable(tableName);
            tableCache.remove(this);
            isClosed = true;
        } finally {
            closeLock.unlock();
        }
    }

    private int getAsyncMutationProcByKey(byte[] row) throws IOException {
        return asyncMutationProcNioIndexSupplier.apply(row);
    }

    private Integer getAsyncMutationProcMurmur32(byte[] row) {
        int hash = Hashing.murmur3_32_fixed().hashBytes(row).asInt();
        return (hash & Integer.MAX_VALUE) % asyncMutationProcNioList.size();
    }

    private static AtomicLong threadCnt = new AtomicLong(0);
    private Map<Long, Integer> threadIdToAsyncMutProcIdx = new ConcurrentHashMap<>();

    private Integer getAsyncMutationProcThreadId(byte[] row) {
        return threadIdToAsyncMutProcIdx.computeIfAbsent(Thread.currentThread().getId(),
                k -> (int) (threadCnt.incrementAndGet() % asyncMutationProcNioList.size()));
    }

    @Override
    public List<RegionLocation> getAllRegionLocations() throws IOException {
        return asyncMutationProcNioList.get(scanIdx).getAllRegionLocations(tableName);
    }

    @Override
    public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
        return super.getStartEndKeys();
    }

    @Override
    public byte[][] getStartKeys() throws IOException {
        return super.getStartKeys();
    }

    @Override
    public byte[][] getEndKeys() throws IOException {
        return super.getEndKeys();
    }
}
