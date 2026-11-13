/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.client.connection.nio.pool;

import com.light.atalanta.*;
import com.light.atalanta.client.MutationValidator;
import com.light.atalanta.client.connection.nio.transport.TRSClientAsyncConnection;
import com.light.atalanta.client.connection.nio.transport.TRSClientConnectionImpl;
import com.light.atalanta.config.AtalantaProperties;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.transport.TRSClientConnection;
import com.light.atalanta.transport.RegionPosition;
import com.light.atalanta.transport.exceptions.AtalantaSerializationException;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;
import com.light.atalanta.transport.exceptions.UnknownRegionException;
import com.light.atalanta.transport.messages.*;
import com.light.atalanta.transport.values.RegionInfo;
import com.light.atalanta.transport.values.TableRegionServerInfo;
import com.light.atalanta.util.Bytes;
import com.light.atalanta.util.BytesComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.light.atalanta.client.connection.nio.pool.TableAsyncMutationNio.EMPTY_RESULT_ARRAY;
import static com.light.atalanta.transport.messages.BulkHelper.NOT_USE_TABLE_POSITION;
import static com.light.atalanta.util.FutureUtil.waitAndGet;

// Routing state of this table over the physical transports referenced by regionServerConnections.
// Several NioTable instances may share one transport; it is not owned by the table.
public class NioTable extends Table {
    private static final Logger LOGGER = LoggerFactory.getLogger(NioTable.class);
    private static final ScheduledExecutorService RETRY_SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "atalanta-nio-retry");
                thread.setDaemon(true);
                return thread;
            });
    private final long timeout;
    private final int maxRetries;
    private final DbConfiguration conf;
    private final Map<RowKey, Integer> retryCounters = new HashMap<>();
    private final AtomicReference<CompletableFuture<Void>> topologyRefresh = new AtomicReference<>();
    private final boolean isTrace;

    public NioTable(ConnectionNio connection, TableName tableName, Map<RegionServerModel,
            List<RegionModel>> regionsMapping, DbConfiguration atalantaConfiguration) throws IOException, NotActualRegionMappingException {
        super(connection, tableName, regionsMapping);
        this.conf = atalantaConfiguration;
        isTrace = LOGGER.isTraceEnabled();
        timeout = Long.parseLong(conf.getOrDefault(AtalantaProperties.ATALANTA_CLIENT_TABLE_RETRIES_TIMEOUT_MS, "5000"));
        maxRetries = Math.max(1, Integer.parseInt(conf.getOrDefault(
                AtalantaProperties.ATALANTA_CLIENT_TABLE_RETRIES_NUMBER, "1000")));
    }


    // tableStructuresCleanup() removes only the bindings of this table


    @Override
    protected void releaseRegionServerConnections() {
        if (!LOGGER.isTraceEnabled()) {
            return;
        }
        regionServerConnections.forEach((core, transport) ->
                LOGGER.trace("Detach table from shared core transport: table={}, core={}, transportId={}, " +
                                "location={}, connectionId={}",
                        tableName, core, Integer.toHexString(System.identityHashCode(transport)),
                        transport.getLocation(), Integer.toHexString(System.identityHashCode(connection))));
    }

    public void put(Put put) throws IOException {
        put(Collections.singletonList(put));
    }

    public void put(List<Put> puts) throws IOException {
        putBulk(puts);
    }

    public Result get(Get get) throws IOException {
        MutationValidator.validateRow(get);
        Throwable lastError = null;
        for (int retryCounter = 0; retryCounter < maxRetries; ++retryCounter) {
            GetOperation getOperation = OperationConverter.toOperation(get);

            try {
                TRSClientConnectionImpl stub = (TRSClientConnectionImpl) selectConnection(getOperation);
                getOperation.setTablePosition(0);
                ResultOperation resultOperation = stub.sendGet(getOperation, tableName);
                return OperationConverter.fromOperation(resultOperation);
            } catch (Exception e) {
                lastError = e;
                if (!RegionMappingErrorClassifier.isRecoverableRoutingError(e) || retryCounter + 1 >= maxRetries) {
                    throwReadFailure(e);
                }
                LOGGER.warn("Retry get for table {} after routing recovery, attempt {}/{}, key: {}",
                        tableName, retryCounter + 1, maxRetries, Bytes.toStringBinary(get.getRow()));
                waitForReadRetry(Collections.singletonList(get), e);
            }
        }
        throw new IOException("Cannot get key: " + Bytes.toStringBinary(get.getRow()), lastError);
    }

    public Result[] get(List<Get> gets) throws IOException {
        if (isBulkApi()) {
            for (Get get : gets) {
                MutationValidator.validateRow(get);
            }
            return getBulk(gets);
        } else {
            ArrayList<Object> res = new ArrayList<>(gets.size());
            for (Get g : gets) {
                res.add(get(g));
            }
            return res.toArray(EMPTY_RESULT_ARRAY);
        }
    }

    public void mutate(List<DbMutation> mutations) throws IOException {
        mutateNoFlush(mutations);
    }

    public void mutateNoFlush(DbMutation mutation) throws IOException {
        mutateNoFlush(Collections.singletonList(mutation));
    }

    // Internal completion tracking for ordinary put/mutate/delete operations.
    // Does not enable PUT_AND_GET and is deliberately not part of the public API.
    List<CompletableFuture<Object>> submitMutations(List<DbMutation> mutations) throws IOException {
        List<Pair<RowKey, CompletableFuture<byte[]>>> res = mutateNoFlushInternal(mutations);
        return res.stream()
                .map(future -> future.getSecond().thenApply(this::decodeMutationResponse))
                .collect(Collectors.toList());
    }

    private Pair<RowKey, CompletableFuture<byte[]>> executeMutationWithRetry(DbMutation mutation, int attempt)
            throws IOException {
        List<Pair<RowKey, CompletableFuture<byte[]>>> responses;
        try {
            responses = executeMutation(mutation);
        } catch (IOException | RuntimeException error) {
            if (!RegionMappingErrorClassifier.isRecoverableRoutingError(error)) {
                throw error;
            }
            // Routing or sending can fail before a response future exists.
            // Apply the same recovery and retry limit as for a failed asynchronous reply.
            responses = Collections.singletonList(new Pair<>(mutation, failedFuture(error)));
        }
        if (responses.size() != 1) {
            throw new IllegalStateException("Expected one response for a single mutation, got " + responses.size());
        }
        Pair<RowKey, CompletableFuture<byte[]>> response = responses.get(0);
        CompletableFuture<byte[]> responseWithRetry = response.getSecond()
                .thenApply(bytes -> {
                    validateMutationResponse(bytes);
                    return bytes;
                }).<CompletableFuture<byte[]>>handle((bytes, error) -> {
                    if (error == null) {
                        return CompletableFuture.completedFuture(bytes);
                    }
                    if (!RegionMappingErrorClassifier.isRecoverableRoutingError(error) || attempt + 1 >= maxRetries) {
                        return NioTable.<byte[]>failedFuture(error);
                    }
                    LOGGER.warn("Retry mutation for table {} after routing recovery, attempt {}/{}",
                            tableName, attempt + 1, maxRetries);
                    return recoverRouting(mutation, error).thenCompose(unused -> {
                        return delayRetry();
                    }).thenCompose(unused -> {
                                try {
                                    return executeMutationWithRetry(mutation, attempt + 1).getSecond();
                                } catch (IOException e) {
                                    return NioTable.<byte[]>failedFuture(e);
                                }
                            });
                })
                .thenCompose(future -> future);
        return new Pair<>(response.getFirst(), responseWithRetry);
    }

    private CompletableFuture<Void> delayRetry() {
        if (timeout <= 0) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> delay = new CompletableFuture<>();
        RETRY_SCHEDULER.schedule(() -> delay.complete(null), timeout, TimeUnit.MILLISECONDS);
        return delay;
    }

    private Object decodeMutationResponse(byte[] bytes) {
        try {
            MessageType type = validateMutationResponse(bytes);
            return decodeSuccessfulMutationResponse(bytes, type, tableName.toString(), timeout);
        } catch (NotActualRegionMappingException e) {
            throw new CompletionException(e);
        }
    }

    static Object decodeSuccessfulMutationResponse(byte[] bytes, MessageType type,
                                                   String location, long timeout)
            throws NotActualRegionMappingException {
        if (type == MessageType.BULK_RESULT_OPERATION) {
            return new BulkResponseOperation(bytes);
        }
        ResultOperation resultOperation = OperationFactory.fromBytes(type, bytes, location, timeout);
        return resultOperation == EmptyResultOperation.EMPTY_OPERATION
                ? Result.EMPTY_RESULT
                : new Result(resultOperation);
    }

    private MessageType validateMutationResponse(byte[] bytes) {
        try {
            return RegionMappingErrorClassifier.validateResponse(bytes, tableName.toString(), timeout);
        } catch (NotActualRegionMappingException e) {
            throw new CompletionException(e);
        }
    }

    private CompletableFuture<Void> refreshRegionMap(DbMutation mutation) {
        Operation operation;
        if (mutation instanceof Put) {
            operation = OperationConverter.toOperation((Put) mutation);
        } else if (mutation instanceof Delete) {
            operation = OperationConverter.toOperation((Delete) mutation);
        } else if (mutation instanceof RowMutations) {
            operation = OperationConverter.createMutation((RowMutations) mutation);
        } else {
            return failedFuture(new IllegalArgumentException("Unknown mutation type: " +
                    mutation.getClass().getName()));
        }
        try {
            TRSClientAsyncConnection stub = (TRSClientAsyncConnection) selectConnection(operation);
            return refreshRegionMapWithFallback(stub);
        } catch (UnknownRegionException e) {
            return refreshTopologyFromZooKeeper();
        }
    }

    private CompletableFuture<Void> recoverRouting(DbMutation mutation, Throwable error) {
        CompletableFuture<Void> running = topologyRefresh.get();
        if (running != null) {
            return running;
        }
        if (RegionMappingErrorClassifier.isUnknownRegionError(error) ||
                RegionMappingErrorClassifier.isRegionMappingError(error)) {
            LOGGER.warn("Incomplete or invalid region map for table {}. Refresh leader topology from ZooKeeper", tableName);
            return refreshTopologyFromZooKeeper();
        }
        return refreshRegionMap(mutation);
    }

    private CompletableFuture<Void> refreshRegionMapWithFallback(TRSClientAsyncConnection stub) {
        return stub.refreshRegionMap(tableName)
                .handle((unused, error) -> {
                    if (error == null)
                        return CompletableFuture.<Void>completedFuture(null);
                    if (!RegionMappingErrorClassifier.isRecoverableRoutingError(error))
                        return NioTable.<Void>failedFuture(error);

                    LOGGER.warn("Connection or region map refresh failed for table {}. Refresh topology from ZooKeeper",
                            tableName);
                    return refreshTopologyFromZooKeeper();
                })
                .thenCompose(future -> future);
    }

    private CompletableFuture<Void> refreshTopologyFromZooKeeper() {
        CompletableFuture<Void> marker = new CompletableFuture<>();
        while (true) {
            CompletableFuture<Void> running = topologyRefresh.get();
            if (running != null)
                return running;
            if (topologyRefresh.compareAndSet(null, marker))
                break;
        }

        CompletableFuture.runAsync(() -> {
            try {
                calculateTableInfo(getStableRegionsMapping());
                LOGGER.info("Table topology refreshed from ZooKeeper for table {}", tableName);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }).whenComplete((unused, error) -> {
            // The rebuild is finished. Retire its marker before notifying waiters:
            // their inline continuations may immediately need another refresh.
            topologyRefresh.compareAndSet(marker, null);
            if (error == null) {
                marker.complete(null);
            } else {
                marker.completeExceptionally(error);
                LOGGER.warn("Cannot refresh table topology from ZooKeeper for table {}", tableName, error);
            }
        });
        return marker;
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable error) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(error);
        return future;
    }


    public void delete(Delete delete) throws IOException {
        mutate(Collections.singletonList(delete));
    }

    public void mutateRow(RowMutations mutation) throws IOException {
        mutate(Collections.singletonList(mutation));
    }

    public void mutateNoFlush(List<DbMutation> mutations) throws IOException {
        mutateNoFlushInternal(mutations);
    }

    private List<Pair<RowKey, CompletableFuture<byte[]>>> mutateNoFlushInternal(List<DbMutation> mutations) throws IOException {
        List<Pair<RowKey, CompletableFuture<byte[]>>> responses = new ArrayList<>(mutations.size());
        List<Put> puts = new ArrayList<>();
        for (DbMutation mutation : mutations) {
            if (mutation instanceof Put) {
                puts.add((Put) mutation);
                continue;
            }
            if (!puts.isEmpty()) {
                responses.addAll(putBulkInternal(puts, 0));
                puts.clear();
            }
            responses.add(executeMutationWithRetry(mutation, 0));
        }
        if (!puts.isEmpty()) {
            responses.addAll(putBulkInternal(puts, 0));
        }
        return responses;
    }

    private List<Pair<RowKey, CompletableFuture<byte[]>>> executeMutation(DbMutation mutation) throws IOException {
        if (mutation instanceof Put) {
            return putBulkNoFlush(Collections.singletonList((Put) mutation));
        }
        if (mutation instanceof Delete) {
            return deleteAsync((Delete) mutation);
        }
        if (mutation instanceof RowMutations) {
            return mutateRowAsync((RowMutations) mutation);
        }
        throw new IllegalArgumentException("Unknown mutation type: " + mutation.getClass().getName());
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
        for (Delete delete : deletes) {
            deleteAsync(delete);
        }
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    protected void initSingleConnection(RegionServerModel rsModel, List<RegionModel> regionModels) throws IOException, NotActualRegionMappingException {
        long lockWaitStartedNanos = System.nanoTime();
        LOGGER.trace("Waiting for NIO table bootstrap lock: table={}, core={}, connectionId={}",
                tableName, rsModel.getName(), Integer.toHexString(System.identityHashCode(connection)));
        synchronized (connection) {
            LOGGER.debug("Initiate connection to {}", rsModel.getName());
            TRSClientConnection stub = ((ConnectionNio) connection).getConnectionToCore(rsModel.getName());
            long lockWaitMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - lockWaitStartedNanos);
            LOGGER.trace("Acquired NIO table bootstrap lock: table={}, core={}, transportId={}, " +
                            "waitMicros={}, connectionId={}",
                    tableName, rsModel.getName(), Integer.toHexString(System.identityHashCode(stub)),
                    lockWaitMicros, Integer.toHexString(System.identityHashCode(connection)));

            // Connection locking serializes table bootstrap workers. The transport additionally
            // defers ordinary sends, which do not acquire this connection's monitor.
            LOGGER.trace("Binding transport to table: table={}, core={}, transportId={}, location={}, connectionId={}",
                    tableName, rsModel.getName(), Integer.toHexString(System.identityHashCode(stub)), stub.getLocation(),
                    Integer.toHexString(System.identityHashCode(connection)));
            LOGGER.trace("Send auth request to: {}", stub.getLocation());
            List<String> regions = regionModels.stream().map(r -> r.getName() + "_0").collect(Collectors.toList());
            LOGGER.trace("There are {} primary replicas in {}", regions.size(), rsModel.getName());
            TableRegionServerInfo trsInfo = stub.authenticateAndGetTableRegionServerInfo(
                    regions, tableName);
            LOGGER.debug("Nested information about table regions in {} for table {}:\n{}", rsModel.getName(), tableName, trsInfo.toString());
            info.put(rsModel, trsInfo);
            regionServerConnections.put(rsModel.getName(), stub);
            LOGGER.trace("Completed NIO table bootstrap: table={}, core={}, transportId={}, location={},  connectionId={}",
                    tableName, rsModel.getName(), Integer.toHexString(System.identityHashCode(stub)),
                    stub.getLocation(), Integer.toHexString(System.identityHashCode(connection)));
        }
    }

    @Override
    protected void validateRegionRangeMap() {
        super.validateRegionRangeMap();
        TablesKeyMappingStorage.put(tableName, keyMapping);
    }

    @Override
    public TRSClientConnection selectConnection(Operation operation) {
        // A concurrent topology refresh clears these related maps separately.
        // An incomplete route is recoverable, not a null dereference. Keep the
        // resolved values local; recovery/retries belong to the caller.
        RegionInfo regionInfo = getRegionInfoPosition(operation.getRowkey());
        if (regionInfo == null) {
            throw new UnknownRegionException();
        }
        RegionServerModel server = allRegions.get(regionInfo);
        if (server == null) {
            LOGGER.debug("No server binding for region {} in NIO table {}", regionInfo, tableName);
            throw new UnknownRegionException();
        }
        TRSClientConnection connection = regionServerConnections.get(server.getName());
        if (connection == null) {
            LOGGER.debug("No transport binding for server {} in NIO table {}", server.getName(), tableName);
            throw new UnknownRegionException();
        }
        operation.setRegionPosition(regionInfo.getPosition());
        int currentPosition = ((TRSClientAsyncConnection) connection).getRegionPosition(tableName,
                operation.getRowkey());
        if (currentPosition != RegionPosition.UNSET.getPosition()) {
            operation.setRegionPosition(currentPosition);
        }
        return connection;
    }

    @Override
    protected void putBulk(List<Put> puts) throws IOException {
        putBulkInternal(puts);
        flush();
    }

    protected List<Pair<RowKey, CompletableFuture<byte[]>>> putBulkNoFlush(List<Put> puts) throws IOException {
        List<Pair<RowKey, CompletableFuture<byte[]>>> res = putBulkInternal(puts, 0);
        return res;
    }

    private List<Pair<RowKey, CompletableFuture<byte[]>>> putBulkInternal(List<Put> puts) {
        return putBulkInternal(puts, 0);
    }

    private List<Pair<RowKey, CompletableFuture<byte[]>>> putBulkInternal(List<Put> puts, int attempt) {
        for (Put put : puts) {
            MutationValidator.validatePut(put);
        }
        try {
            if (puts.size() == 1) {
                Put put = puts.get(0);
                PutOperation operation = OperationConverter.toOperation(put);
                TRSClientAsyncConnection connection = (TRSClientAsyncConnection) selectConnection(operation);
                CompletableFuture<byte[]> response = connection.sendMessage0(operation, tableName);
                return Collections.singletonList(new Pair<>(put, withBulkRetry(puts, response, attempt)));
            }

            List<Pair<RowKey, CompletableFuture<byte[]>>> res = new ArrayList<>(puts.size());
            List<PutOperation> putOperations = OperationConverter.toOperations(puts);
            Map<Pair<TRSClientAsyncConnection, Integer>, BulkPutOperation> mapped = new HashMap<>();
            for (PutOperation put : putOperations) {
                TRSClientConnection stub = selectConnection(put);
                int position = put.getRegionPosition().getPosition();
                Pair<TRSClientAsyncConnection, Integer> p = new Pair<>((TRSClientAsyncConnection) stub, position);
                mapped.computeIfAbsent(p,
                        ignored -> new BulkPutOperation(position, 0))
                        .addPutOperation(put);
            }
            mapped.forEach((k, v) -> {
                try {
                    for (Pair<List<PutOperation>, CompletableFuture<byte[]>> response :
                            k.getFirst().sendBulkPutAsync(v, tableName)) {
                        List<Put> chunk = new ArrayList<>(response.getFirst().size());
                        for (PutOperation operation : response.getFirst()) {
                            RowKey source = operation.getRowKey();
                            if (!(source instanceof Put)) {
                                throw new IllegalStateException("Bulk PutOperation has no source Put");
                            }
                            chunk.add((Put) source);
                        }
                        CompletableFuture<byte[]> responseWithRetry = withBulkRetry(chunk,
                                response.getSecond(), attempt);
                        res.add(new Pair<>(chunk.get(0), responseWithRetry));
                    }
                } catch (Exception e) {
                    LOGGER.error("putBulk error.", e);
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    } else {
                        throw new RuntimeException(e);
                    }
                }
            });
            return res;
        } catch (AtalantaSerializationException e) {
            LOGGER.error("Cannot serialize bulk put request.", e);
            throw e;
        } catch (Exception e) {
            if (RegionMappingErrorClassifier.isRecoverableRoutingError(e)) {
                // Routing/sending can fail before any response future is available.
                // Use the same asynchronous recovery as failed replies, never the
                // blocking legacy actualizeRegionsMap() from a completion callback.
                List<Put> retryPuts = new ArrayList<>(puts);
                return Collections.singletonList(new Pair<>(retryPuts.get(0),
                        withBulkRetry(retryPuts, failedFuture(e), attempt)));
            }
            LOGGER.error("putBulk common error.", e);
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        }

    }

    private CompletableFuture<byte[]> withBulkRetry(List<Put> sourcePuts, CompletableFuture<byte[]> response,
                                                     int attempt) {
        // mutateNoFlushInternal reuses/clears its temporary list between mixed operations
        List<Put> puts = new ArrayList<>(sourcePuts);
        return response.thenApply(bytes -> {
            validateMutationResponse(bytes);
            return bytes;
        }).<CompletableFuture<byte[]>>handle((bytes, error) -> {
            if (error == null) {
                return CompletableFuture.completedFuture(bytes);
            }
            if (!RegionMappingErrorClassifier.isRecoverableRoutingError(error) || attempt + 1 >= maxRetries) {
                return NioTable.<byte[]>failedFuture(error);
            }
            LOGGER.warn("Retry bulk mutation chunk for table {} after routing recovery, attempt {}/{}",
                    tableName, attempt + 1, maxRetries);
            return recoverRouting(puts.get(0), error)
                    .thenCompose(unused -> delayRetry())
                    .thenCompose(unused -> {
                        List<Pair<RowKey, CompletableFuture<byte[]>>> retried = putBulkInternal(puts, attempt + 1);
                        if (retried.isEmpty()) {
                            return NioTable.<byte[]>failedFuture(new IllegalStateException("Bulk retry returned no response futures"));
                        }
                        // A batch rejected before routing may become several wire chunks.
                        // These writes return acknowledgements, not previous row values;
                        // succeed only after EVERY replacement chunk has succeeded.
                        CompletableFuture<?>[] replies = retried.stream().map(Pair::getSecond)
                                .toArray(CompletableFuture[]::new);
                        return CompletableFuture.allOf(replies)
                                .thenApply(unusedReply -> retried.get(0).getSecond().join());
                    });
        }).thenCompose(future -> future);
    }

    @Override
    protected Result[] getBulk(List<Get> gets) {
        Throwable lastError = null;
        for (int retryCounter = 0; retryCounter < maxRetries; ++retryCounter) {
            List<GetOperation> getOperations = OperationConverter.toOperations(gets);
            for (int i = 0; i < getOperations.size(); i++) {
                getOperations.get(i).setSourceIndex(i);
            }
            Result[] results = new Result[gets.size()];

            try {
                Map<TRSClientConnectionImpl, Map<Integer, BulkGetOperation>> mapped = new IdentityHashMap<>();
                for (GetOperation g : getOperations) {
                    TRSClientConnectionImpl stub = (TRSClientConnectionImpl) selectConnection(g);
                    int position = g.getRegionPosition().getPosition();
                    mapped.computeIfAbsent(stub, ignored -> new HashMap<>())
                            .computeIfAbsent(position,
                                    ignored -> new BulkGetOperation(position, 0))
                            .addGetOperation(g);
                }
                int regionGroupCount = 0;
                for (Map<Integer, BulkGetOperation> regionGroups : mapped.values()) {
                    regionGroupCount += regionGroups.size();
                }
                List<Pair<List<GetOperation>, CompletableFuture<byte[]>>> responseChunks =
                        new ArrayList<>(regionGroupCount);
                mapped.forEach((stub, regionGroups) -> {
                    for (BulkGetOperation bulkGet : regionGroups.values()) {
                        try {
                            responseChunks.addAll(stub.sendBulkGetAsync(bulkGet, tableName));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
                List<CompletableFuture<byte[]>> responseFutures = new ArrayList<>(responseChunks.size());
                for (Pair<List<GetOperation>, CompletableFuture<byte[]>> chunk : responseChunks) {
                    responseFutures.add(chunk.getSecond());
                }
                List<byte[]> responses = waitAndGet(responseFutures);
                for (int chunkIndex = 0; chunkIndex < responses.size(); chunkIndex++) {
                    byte[] bytes = responses.get(chunkIndex);
                    validateMutationResponse(bytes);
                    List<ResultOperation> chunkResults = new BulkResponseOperation(bytes).getResultOperations();
                    List<GetOperation> chunkRequests = responseChunks.get(chunkIndex).getFirst();
                    if (chunkResults.size() != chunkRequests.size()) {
                        throw new IllegalStateException("Bulk get response count " + chunkResults.size()
                                + " does not match request count " + chunkRequests.size());
                    }
                    for (int i = 0; i < chunkResults.size(); i++) {
                        int sourceIndex = chunkRequests.get(i).getSourceIndex();
                        if (sourceIndex < 0 || sourceIndex >= results.length || results[sourceIndex] != null) {
                            throw new IllegalStateException("Invalid bulk get source index: " + sourceIndex);
                        }
                        results[sourceIndex] = OperationConverter.fromOperation(chunkResults.get(i));
                    }
                }
                for (int i = 0; i < results.length; i++) {
                    if (results[i] == null) {
                        throw new IllegalStateException("Missing bulk get result at index " + i);
                    }
                }
                return results;
            } catch (Exception e) {
                lastError = e;
                if (!RegionMappingErrorClassifier.isRecoverableRoutingError(e) || retryCounter + 1 >= maxRetries) {
                    throwReadFailure(e);
                }
                LOGGER.warn("Retry bulk get for table {} after routing recovery, attempt {}/{}, keyListSize: {}",
                        tableName, retryCounter + 1, maxRetries, gets.size());
                waitForReadRetry(gets, e);
            }
        }
        throw new RuntimeException("Cannot get keys: " + gets.size(), lastError);
    }

    private void waitForReadRetry(List<Get> gets, Throwable error) {
        CompletableFuture<?> refresh;
        if (RegionMappingErrorClassifier.isUnknownRegionError(error) ||
                RegionMappingErrorClassifier.isRegionMappingError(error)) {
            if (RegionMappingErrorClassifier.isRegionMappingError(error)) {
                LOGGER.warn("Invalid region map for table {} during read. Refresh leader topology from ZooKeeper", tableName);
            }
            refresh = refreshTopologyFromZooKeeper();
        } else {
            Set<TRSClientAsyncConnection> connections = Collections.newSetFromMap(new IdentityHashMap<>());
            for (Get get : gets) {
                GetOperation operation = OperationConverter.toOperation(get);
                connections.add((TRSClientAsyncConnection) selectConnection(operation));
            }
            CompletableFuture<?>[] refreshes = connections.stream()
                    .map(this::refreshRegionMapWithFallback)
                    .toArray(CompletableFuture[]::new);
            refresh = CompletableFuture.allOf(refreshes);
        }
        try {
            refresh.thenCompose(unused -> delayRetry()).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
        } catch (ExecutionException e) {
            throw new CompletionException(e.getCause());
        }
    }

    private static void throwReadFailure(Throwable error) {
        if (error instanceof RuntimeException) {
            throw (RuntimeException) error;
        }
        throw new CompletionException(error);
    }

    private List<Pair<RowKey, CompletableFuture<byte[]>>> deleteAsync(Delete mutation) {
        Operation operation = OperationConverter.toOperation(mutation);
        return operationAsync(operation, mutation);
    }

    private List<Pair<RowKey, CompletableFuture<byte[]>>> mutateRowAsync(RowMutations mutation) {
        Operation operation = OperationConverter.createMutation(mutation);
        return operationAsync(operation, mutation);
    }

    private List<Pair<RowKey, CompletableFuture<byte[]>>> operationAsync(Operation operation, RowKey rowKey) {
        try {
            RegionInfo regionInfo = getRegionInfoPosition(operation.getRowkey());
            if (regionInfo == null)
                throw new UnknownRegionException();
            TRSClientConnection stub = selectConnection(operation);
            TRSClientAsyncConnection stubAsync = (TRSClientAsyncConnection) stub;

            List<Pair<RowKey, CompletableFuture<byte[]>>> res = new ArrayList<>();
            res.add(new Pair<>(rowKey, stubAsync.sendMessage0(operation, tableName)));
            return res;
        } catch (UnknownRegionException
                 | IOException e) {
            throw new RuntimeException(e);
        }
    }

}
