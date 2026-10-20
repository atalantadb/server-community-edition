/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.light.atalanta.api.DbTable;
import com.light.atalanta.api.DelRows;
import com.light.atalanta.api.PutRows;
import com.light.atalanta.client.MutationValidator;
import com.light.atalanta.client.serializers.PutSerializer;
import com.light.atalanta.client.serializers.PutToByteSerializer;
import com.light.atalanta.collections.CopyOnWriteArrayMap;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.tracers.Writer;
import com.light.atalanta.transport.TRSClientConnection;
import com.light.atalanta.transport.TRSConnectionStub;
import com.light.atalanta.transport.exceptions.*;
import com.light.atalanta.transport.messages.*;
import com.light.atalanta.transport.values.RegionInfo;
import com.light.atalanta.transport.values.TableRegionServerInfo;
import com.light.atalanta.util.Bytes;
import com.light.atalanta.util.BytesComparator;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.light.atalanta.config.AtalantaProperties.*;
import static com.light.atalanta.transport.messages.BulkHelper.NOT_USE_TABLE_POSITION;

public class  Table implements DbTable, RegionLocator, BufferedMutator, AutoCloseable, LazyTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Table.class);

    protected final TableName tableName;
    protected final ConcurrentHashMap<RegionInfo, RegionServerModel> allRegions;
    protected final ConcurrentHashMap<RegionServerModel, TableRegionServerInfo> info;
    protected final ConcurrentHashMap<String, TRSClientConnection> regionServerConnections;
    protected final ConcurrentNavigableMap<byte[], RegionInfo> keyMapping;
    protected final ConnectionThreadLocal connection;

    private final int dataBlockSize;
    private final Semaphore semaphore = new Semaphore(1);
    private final AtomicBoolean mFirst = new AtomicBoolean(true);
    protected final TableRepeatableExecutorConfiguration retryConfig;

    protected Table() {
        tableName = null;
        allRegions = null;
        info = null;
        regionServerConnections = null;
        keyMapping = null;
        connection = null;
        dataBlockSize = 0;
        retryConfig = null;
    }

    public Table(ConnectionThreadLocal connection, TableName tableName, Map<RegionServerModel, List<RegionModel>> regionsMapping) throws IOException, NotActualRegionMappingException {
        LOGGER.debug("Create new table instance: {}", tableName);
        dataBlockSize = initDataBlockSize(tableName, regionsMapping);
        this.connection = connection;
        this.tableName = tableName;

        this.info = new ConcurrentHashMap<>();
        this.allRegions = new ConcurrentHashMap<>();
        this.regionServerConnections = new ConcurrentHashMap<>();
        this.keyMapping = new CopyOnWriteArrayMap<>(BytesComparator.BYTES_COMPARATOR);
        this.retryConfig = new TableRepeatableExecutorConfiguration(connection.getConfiguration());

        calculateTableInfo(regionsMapping);

        LOGGER.debug("Instance of {} already created", tableName);
    }

    private static RuntimeException prepareRuntimeException(Exception e) {
        return e.getCause() != null ? new RuntimeException(e.getCause()) : new RuntimeException(e);
    }

    private int initDataBlockSize(TableName tableName, Map<RegionServerModel, List<RegionModel>> regionsMapping) {
        return regionsMapping.entrySet().stream()
                .findAny()
                .orElseThrow(() -> new RuntimeException("Region Server not found for table: " + tableName))
                .getValue().stream()
                .findAny()
                .orElseThrow(() -> new RuntimeException("Region not found for table: " + tableName))
                .getDataBlockSize();
    }

    // Scan operations

    protected void calculateTableInfo(Map<RegionServerModel, List<RegionModel>> regionsMapping) throws IOException, NotActualRegionMappingException {
        LOGGER.debug("Calculate table info: {}", tableName);
        ExecutorService executorService = null;
        try {
            tableStructuresCleanup();
            executorService = Executors.newFixedThreadPool(8, new ThreadFactoryBuilder().setNameFormat("init-connect-%d").build());
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            ExecutorService finalExecutorService = executorService;
            regionsMapping.forEach((key, value) -> futures.add(CompletableFuture.runAsync(() -> {
                try {
                    initSingleConnection(key, value);
                } catch (Exception e) {
                    LOGGER.error("Cannot connect to RS: {}. Table: {}", key, tableName);
                    throw new RuntimeException(e);
                }
            }, finalExecutorService)));
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            executorService.shutdown();
            if (executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                LOGGER.info("New connections successfully created");
            } else {
                executorService.shutdownNow();
                LOGGER.info("Forcibly close all renewal connection's threads");
            }
            executorService = null;
        } catch (Exception e) {
            LOGGER.error("Cannot calculate table info. Table: {}", tableName, e);
            Throwable t = e.getCause();
            if (t instanceof IOException) {
                throw (IOException) t;
            } else if (t instanceof NotActualRegionMappingException) {
                throw (NotActualRegionMappingException) t;
            } else {
                throw new RuntimeException(e.getCause());
            }
        } finally {
            if (executorService != null) {
                LOGGER.warn("Cant close pool that initiate connections");
                try {
                    executorService.shutdown();
                    if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                        executorService.shutdownNow();
                        LOGGER.info("Forcibly close all renewal connection's threads");
                    }
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }
        for (Map.Entry<RegionServerModel, TableRegionServerInfo> entry : info.entrySet()) {
            for (RegionInfo ri : entry.getValue().getRegions()) {
                RegionServerModel rsModel = entry.getKey();
                allRegions.put(ri, rsModel);
                keyMapping.put(ri.getStartKey(), ri);
            }
        }
        validateRegionRangeMap();
        LOGGER.debug("Nested table info ready");
    }

    protected Map<RegionServerModel, List<RegionModel>> getStableRegionsMapping() throws Exception {
        connection.waitTableStability(tableName);
        return connection.getActualRegionsMapping(tableName);
    }

    protected void initSingleConnection(RegionServerModel rsModel, List<RegionModel> regionModels) throws IOException, NotActualRegionMappingException {
        LOGGER.debug("Initiate connection to {}", rsModel.getName());
        TRSConnectionStub stub = connection.getTopology().getNewConnectionToCore(rsModel.getName());
        LOGGER.trace("Send auth request to: {}", stub.getLocation());
        
            stub.sendEmptyAuthRequest();
        
        List<String> regions = regionModels.stream().map(r -> r.getName() + "_0").collect(Collectors.toList());
        LOGGER.trace("There are {} primary replicas in {}", regions.size(), rsModel.getName());
        TableRegionServerInfo trsInfo = stub.getTableRegionServerInfo(regions, tableName);
        LOGGER.debug("Nested information about table regions in {} for table {}:\n{}", rsModel.getName(), tableName, trsInfo.toString());
        info.put(rsModel, trsInfo);
        regionServerConnections.put(rsModel.getName(), stub);
    }

    protected void tableStructuresCleanup() {
        LOGGER.debug("Cleanup {} table structures", tableName.toString());
        clearMaps();
        releaseRegionServerConnections();
        this.regionServerConnections.clear();
        LOGGER.debug("All inner table's structures cleaned");
    }


    protected void releaseRegionServerConnections() {
        this.regionServerConnections.forEach((key, val) -> {
            try {
                LOGGER.debug("Closing table transport during cleanup: table={}, core={}, transportId={}, location={}",
                        tableName, key, Integer.toHexString(System.identityHashCode(val)), val.getLocation());
                val.close();
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
        });
    }

    private void clearMaps() {
        this.info.clear();
        this.allRegions.clear();
        this.keyMapping.clear();
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        // The region map may still be updating while connections initialize; retry to ride out that window
        return new RepeatableExecutor().withStaticTimeout(500).withMaxRetries(3).execute(() -> {
            if (scan.isLimited()) {
                LOGGER.debug("Scanner has limit: {}", scan.getCacheSize());
                return new LimitedClientScanner(scan, this);
            }
            return new ClientScanner(scan, this);
        });
    }

    List<RegionInfo> getSortedRegions() {
        return new ArrayList<>(keyMapping.values());
    }

    private TRSClientConnection chooseRegion(Scan scan) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Choose {}'s region of for scan started with: {}", tableName, Arrays.toString(scan.getStartKey()));
        }
        if (keyMapping.isEmpty()) {
            actualizeRegionsMap();
            scan.setBlockNumber(1);
            throw new RuntimeException();
        }
        Map.Entry<byte[], RegionInfo> entry = keyMapping.floorEntry(scan.getStartKey());
        if (entry == null) {
            actualizeRegionsMap();
            scan.setBlockNumber(1);
            throw new RuntimeException();
        } else if (BytesComparator.compareTo(entry.getValue().getEndKey(), scan.getStartKey()) <= 0) {
            actualizeRegionsMap();
            scan.setBlockNumber(1);
            throw new RuntimeException();
        }
        RegionInfo ri = entry.getValue();
        String rsName = allRegions.get(ri).getName();
        LOGGER.trace("Selected region of table {}: {}", tableName, rsName);
        return regionServerConnections.get(rsName);
    }

    private boolean isStopKeyOpen(byte[] stopKey) {
        return Arrays.equals(Bytes.START_BYTES_UNIFORM, stopKey) || Arrays.equals(stopKey, Bytes.END_BYTES_UNIFORM);
    }

    // Put operations
    ScanResultContainer scanRegion(Scan scan) {
        try {

            AtomicReference<ScanOperation> scanOperation = new AtomicReference<>();
            RepeatableExecutor repeatableExecutor = retryConfig.addExceptionActions(
                    retryConfig.getExecutor(TableRepeatableExecutorConfiguration.ACTION_SCAN_REGION),
                    this::actualizeRegionsMap);
            return repeatableExecutor.execute(() -> {
                try {
                    // TODO: Merge these two calls
                    TRSClientConnection stub = chooseRegion(scan);
                    RegionInfo ri = keyMapping.floorEntry(scan.getStartKey()).getValue();

                    byte[] tmpEndKey = scan.getStopKey();
                    if (isStopKeyOpen(tmpEndKey)) {
                        tmpEndKey = ri.getEndKey();
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Scan range coverage check: scanStartKey={}, scanStopKey={}, effectiveStopKey={}, regionPosition={}, mapSize={}",
                                Arrays.toString(scan.getStartKey()), Arrays.toString(scan.getStopKey()), Arrays.toString(tmpEndKey),
                                ri.getPosition(), keyMapping.size());
                    }

                    if (!isScanRangeFullyCovered(scan.getStartKey(), tmpEndKey)) {
                        LOGGER.warn("Scan range is not fully covered before scan request. Trigger regions map actualization: startKey={}, effectiveStopKey={}, regionPosition={}",
                                Arrays.toString(scan.getStartKey()), Arrays.toString(tmpEndKey), ri.getPosition());
                        actualizeRegionsMap();
                        if (!isScanRangeFullyCovered(scan.getStartKey(), tmpEndKey)) {
                            LOGGER.error("Scan range is still not fully covered after regions map refresh: startKey={}, effectiveStopKey={}, regionPosition={}, mapSize={}",
                                    Arrays.toString(scan.getStartKey()), Arrays.toString(tmpEndKey), ri.getPosition(), keyMapping.size());
                            throw new RuntimeException("Scan range is not fully covered after regions map refresh");
                        }
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Scan range is fully covered after regions map refresh: startKey={}, effectiveStopKey={}, regionPosition={}",
                                    Arrays.toString(scan.getStartKey()), Arrays.toString(tmpEndKey), ri.getPosition());
                        }
                    }

                    LOGGER.trace("Selected region: {}", ri);

                    if (scanOperation.get() == null) {
                        scanOperation.set(isHBaseFilter()
                                ? OperationConverter.fromScanV2(scan, ri.getPosition(), dataBlockSize)
                                : OperationConverter.fromScanV1(scan, ri.getPosition(), dataBlockSize));
                    } else {
                        scanOperation.set(isHBaseFilter()
                                ? OperationConverter.createScanV2(scan, ri.getPosition(), scan.getBlockNumber(), (short) scanOperation.get().getTryNumber(), dataBlockSize)
                                : OperationConverter.createScanV1(scan, ri.getPosition(), scan.getBlockNumber(), (short) scanOperation.get().getTryNumber(), dataBlockSize));
                    }
                    LOGGER.debug("Scan operation for scan {} created: {}", scan, scanOperation.get());
                    ScanResultContainer container = stub.makeScan(scanOperation.get(), tableName);
                    LOGGER.debug("Scan results: {}", container);
                    if (isHBaseFilter()) {
                        scan.setBlockNumber(container.getRealBlockNumber());
                        scan.setBs(container.getBs());
                    }
                    return waitingForCorrectContainer(stub, container, scanOperation.get());
                } catch (NotActualRegionMappingException e) {
                    LOGGER.warn("Scan got not-actual region mapping: table={}, startKey={}, stopKey={}, block={}, bs={}, regionPos={}, mapSize={}, ranges={}.",
                            tableName,
                            Arrays.toString(scan.getStartKey()),
                            Arrays.toString(scan.getStopKey()),
                            scan.getBlockNumber(),
                            scan.getBs(),
                            scan.getRegionPosition(),
                            keyMapping.size(),
                            buildRegionsRangesSnapshot(keyMapping.values()),
                            e);
                    scan.setBlockNumber(1);
                    actualizeRegionsMap();
                    throw new RuntimeException(e);
                } catch (NullPointerException |
                         UnknownRegionException |
                         UnavailableServerException e) {
                    actualizeRegionsMap();
                    scan.setBlockNumber(1);
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    actualizeRegionsMap();
                    if (isHBaseFilter()) {
                        scanOperation.get().setTryNumber(scanOperation.get().getTryNumber() + 1);
                    }
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            if (LOGGER.isTraceEnabled()) {
                Writer.save();
            }
            LOGGER.warn("Scan failed and will be restarted from block 1: table={}, startKey={}, stopKey={}, block={}, bs={}, regionPos={}, mapSize={}, ranges={}",
                    tableName,
                    Arrays.toString(scan.getStartKey()),
                    Arrays.toString(scan.getStopKey()),
                    scan.getBlockNumber(),
                    scan.getBs(),
                    scan.getRegionPosition(),
                    keyMapping.size(),
                    buildRegionsRangesSnapshot(keyMapping.values()),
                    e);
            actualizeRegionsMap();
            scan.setBlockNumber(1);
            throw prepareRuntimeException(e);
        }
    }

    private ScanResultContainer waitingForCorrectContainer(TRSClientConnection stub, ScanResultContainer
            container, ScanOperation scanOperation) throws IOException {
        if (container.isFinished()) {
            return container;
        }
        boolean isSameResults = isSameScanResults(container, scanOperation);
        if (isSameResults) {
            LOGGER.trace("Container contains actual results");
            return container;
        }
        final long timer = System.currentTimeMillis();
        final long scanIterationTimeout = connection.getConfiguration().getLong(ATALANTA_CLIENT_SCAN_ITERATION_TIMEOUT);
        while (!isSameResults && (System.currentTimeMillis() - timer) < scanIterationTimeout) {
            try {
                LOGGER.debug("Read socket for actual scan results");
                byte[] bytes = stub.receiveBytes();
                container = isHBaseFilter()
                        ? ScanOperationV2.getResultsFromScan(bytes, stub.getLocation())
                        : ScanOperationV1.getResultsFromScan(bytes, stub.getLocation());
                isSameResults = isSameScanResults(container, scanOperation);
            } catch (UnavailableCoreException | NotActualRegionMappingException e) {
                actualizeRegionsMap();
                throw new RuntimeException(e);
            }
        }
        if (isSameResults) {
            return container;
        }
        throw new RuntimeException("Scanner execution timeout");
    }

    private boolean isSameScanResults(ScanResultContainer container, ScanOperation scanOperation) {
        if (isHBaseFilter()) {
            return container.getClientBlockNumber() == scanOperation.getBlockNumber();
        } else {
            return container.getBlockNumber() == scanOperation.getBlockNumber() && container.getTryNum() == scanOperation.getTryNumber();
        }
    }

    public void put(Put put) throws IOException {
        MutationValidator.validatePut(put);
        RepeatableExecutor repeatableExecutor = retryConfig.addExceptionActions(
                retryConfig.getExecutor(TableRepeatableExecutorConfiguration.ACTION_MAKE_OPERATIONS_WITH_RETRY),
                this::actualizeRegionsMap);
        OperationConverter.fromOperation(repeatableExecutor.execute(() -> {
            try {
                return processSingleOperation(OperationConverter.toOperation((Put) put));
            } catch (UnknownRegionException | IOException | NotActualRegionMappingException |
                     UnavailableServerException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    private CompletableFuture<Result> deleteAndGet(DelRows del) {
        try {
            CompletableFuture<Result> res = new CompletableFuture<>();
            res.complete(deleteAndGet((Delete) del));
            return res;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private CompletableFuture<Result> rowMutatesAndGet(RowMutations rowMutations) {
        try {
            CompletableFuture<Result> res = new CompletableFuture<>();
            res.complete(mutateRowAndGet(rowMutations));
            return res;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
        for (Delete delete : deletes) {
            delete(delete);
        }
    }

    @Override
    public void mutateRow(RowMutations mutation) throws IOException {
        MutationValidator.validateRowMutations(mutation);
        
        Operation mutationOperation = OperationConverter.createMutation(mutation);
        processOperationsWithRetry(Collections.singletonList(mutationOperation));
    }

    public Result mutateRowAndGet(RowMutations mutation) throws IOException {
        MutationValidator.validateRowMutations(mutation);
        
        Operation mutationOperation = OperationConverter.createMutation(mutation);
        List<? extends Operation> results = processOperationsWithRetry(Collections.singletonList(mutationOperation));
        return results.size() > 0 ? OperationConverter.fromOperation(results.get(0)) : null;
    }

    public void put(List<Put> puts) throws IOException {
        if (isBulkApi()) {
            putBulk(puts);
        } else {
            for (Put put : puts) {
                MutationValidator.validatePut(put);
            }
            for (Put put : puts) {
                put(put);
            }
        }
    }

    protected void putBulk(List<Put> puts) throws IOException {
        for (Put put : puts) {
            MutationValidator.validatePut(put);
        }
        List<PutOperation> putOperations = OperationConverter.toOperations(puts);

        RepeatableExecutor repeatableExecutor = retryConfig.addExceptionActions(
                retryConfig.getExecutor(TableRepeatableExecutorConfiguration.ACTION_MAKE_OPERATIONS_WITH_RETRY),
                this::actualizeRegionsMap);
        repeatableExecutor.execute(() -> {
            try {
                if (keyMapping.isEmpty()) {
                    calculateTableInfo(connection.getActualRegionsMapping(tableName));
                }
                Map<Pair<TRSClientConnection, Integer>, BulkPutOperation> mapped = new HashMap<>();
                for (PutOperation put : putOperations) {
                    TRSClientConnection stub = selectConnection(put);
                    int position = put.getRegionPosition().getPosition();
                    Pair<TRSClientConnection, Integer> p = new Pair<>(stub, position);
                    mapped.putIfAbsent(p, new BulkPutOperation(position, NOT_USE_TABLE_POSITION));
                    mapped.get(p).addPutOperation(put);
                }
                mapped.forEach((k, v) -> {
                    try {
                        TRSClientConnection stub = k.getFirst();
                        putBulk(v, stub);
                    } catch (AtalantaSerializationException e) {
                        throw e;
                    } catch (Exception e) {
                        LOGGER.error("putBulk error.", e);
                        clearMaps();
                        if (e instanceof RuntimeException) {
                            throw (RuntimeException) e;
                        } else {
                            throw new RuntimeException(e);
                        }
                    }
                });
            } catch (AtalantaSerializationException e) {
                throw e;
            } catch (Exception e) {
                LOGGER.error("putBulk common error.", e);
                clearMaps();
                if (e.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) e.getCause();
                } else if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            }
            return null;
        });
    }

    private void putBulk(BulkPutOperation v, TRSClientConnection stub) throws IOException {
        stub.sendBulkPut(v, tableName);
    }

    public void putReusablePut(Put put) throws IOException {
        MutationValidator.validatePut(put);
        
        PutToByteSerializer.ByteCollector collector = PutSerializer.getSerializer().serializePut((Put) put, getRegionInfoPosition(put.getRow()).getPosition());
        ByteBuffer bb = collector.getBuffer();
        if (bb != null) {
            processBytesWithRetry(put.getRow(), bb, collector.getSize());
        } else {
            List<PutOperation> putOperations = new ArrayList<>(1);
            putOperations.add(OperationConverter.toOperation((Put) put));
            processOperationsWithRetry(putOperations);
            putOperations.clear();
        }
    }

    public void putReusablePuts(List<Put> puts) throws IOException {
        for (Put put : puts) {
            MutationValidator.validatePut(put);
        }
        List<PutOperation> putOperations = null;
        for (PutRows p : puts) {
            PutToByteSerializer.ByteCollector collector = PutSerializer.getSerializer().serializePut((Put) p, getRegionInfoPosition(p.getRow()).getPosition());
            if (collector.getBuffer() != null) {
                processBytesWithRetry(p.getRow(), collector.getBuffer(), collector.getSize());
            } else {
                if (putOperations == null) {
                    putOperations = new ArrayList<>(1);
                }
                putOperations.add(OperationConverter.toOperation((Put) p));
                processOperationsWithRetry(putOperations);
                putOperations.clear();
            }
        }
    }

    // Get operations
    public Result get(Get get) throws IOException {
        MutationValidator.validateRow(get);
        RepeatableExecutor repeatableExecutor = retryConfig.addExceptionActions(
                retryConfig.getExecutor(TableRepeatableExecutorConfiguration.ACTION_MAKE_OPERATIONS_WITH_RETRY),
                this::actualizeRegionsMap);
        return OperationConverter.fromOperation(repeatableExecutor.execute(() -> {
            try {
                return processSingleOperation(OperationConverter.toOperation(get));
            } catch (UnknownRegionException | IOException | NotActualRegionMappingException |
                     UnavailableServerException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    public Result[] get(List<Get> gets) throws IOException {
        if (isBulkApi()) {
            return getBulk(gets);
        } else {
            return getList(gets).toArray(new Result[0]);
        }
    }

    protected Result[] getBulk(List<Get> gets) {
        Map<byte[], Integer> positions = new CopyOnWriteArrayMap<>(BytesComparator.BYTES_COMPARATOR);
        AtomicInteger counter = new AtomicInteger(-1);
        List<Get> Gets = gets.stream().map(g -> {
            positions.put(g.getRow(), counter.incrementAndGet());
            return (Get) g;
        }).collect(Collectors.toList());
        List<GetOperation> getOperations = OperationConverter.toOperations(Gets);
        Result[] results = new Result[gets.size()];
        RepeatableExecutor repeatableExecutor = retryConfig.addExceptionActions(
                retryConfig.getExecutor(TableRepeatableExecutorConfiguration.ACTION_MAKE_OPERATIONS_WITH_RETRY),
                this::actualizeRegionsMap);
        repeatableExecutor.execute(() -> {
            try {
                Map<Pair<TRSClientConnection, Integer>, BulkGetOperation> mapped = new HashMap<>();
                for (GetOperation g : getOperations) {
                    TRSClientConnection stub = selectConnection(g);
                    int position = g.getRegionPosition().getPosition();
                    Pair<TRSClientConnection, Integer> p = new Pair<>(stub, position);
                    mapped.putIfAbsent(p, new BulkGetOperation(position, NOT_USE_TABLE_POSITION));
                    mapped.get(p).addGetOperation(g);
                }
                mapped.forEach((k, v) -> {
                    try {
                        TRSClientConnection stub = k.getFirst();
                        for (ResultOperation resultOperation : stub.sendBulkGet(v, tableName)) {
                            Result fromOperation = OperationConverter.fromOperation(resultOperation);
                            results[positions.get(fromOperation.getRow())] = fromOperation;
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                return null;
            } catch (Exception e) {
                if (e.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) e.getCause();
                } else {
                    throw e;
                }
            }
        });
        return results;
    }

    public List<Result> getList(List<Get> gets) throws IOException {
        List<Operation> getOperations = OperationConverter.toOperations(gets);
        List<? extends Operation> results = processOperationsWithRetry(getOperations);
        return OperationConverter.fromOperations(results);
    }

    // Delete operations
    @Override
    public void delete(Delete delete) throws IOException {
        deleteAndGetList(Collections.singletonList(delete));
    }

    public Result deleteAndGet(Delete delete) throws IOException {
        List<Result> deletes = deleteAndGetList(Collections.singletonList(delete));
        return deletes != null && deletes.size() > 0 ? deletes.get(0) : null;
    }

    public List<Result> deleteAndGetList(List<Delete> deletes) throws IOException {
        List<Operation> deleteOperations = OperationConverter.toOperations(deletes);
        List<? extends Operation> results = processOperationsWithRetry(deleteOperations);
        return OperationConverter.fromOperations(results);
    }

    protected RegionInfo getRegionInfoPosition(byte[] rowkey) {
        Map.Entry<byte[], RegionInfo> entry = keyMapping.floorEntry(rowkey);
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    private void processBytesWithRetry(byte[] rowkey, ByteBuffer bb, int size) {
        try {
            RepeatableExecutor repeatableExecutor = retryConfig.addExceptionActions(
                    retryConfig.getExecutor(TableRepeatableExecutorConfiguration.ACTION_PROCESS_BYTES_WITH_RETRY),
                    this::actualizeRegionsMap);
            repeatableExecutor.execute(() -> {
                try {
                    processByteOperation(rowkey, bb, size);
                    return null;
                } catch (UnknownRegionException | UnavailableServerException | NotActualRegionMappingException |
                         IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            if (LOGGER.isTraceEnabled()) {
                Writer.save();
            }
            actualizeRegionsMap();
            throw prepareRuntimeException(e);
        }
    }

    private void processByteOperation(byte[] rowkey, ByteBuffer bb, int size) throws
            IOException, NotActualRegionMappingException, UnavailableServerException {
        RegionInfo regionInfo = getRegionInfoPosition(rowkey);
        if (regionInfo == null) {
            throw new UnknownRegionException();
        }
        RegionServerModel rsm = allRegions.get(regionInfo);
        TRSClientConnection stub = regionServerConnections.get(rsm.getName());

        stub.sendBytes(bb, 0, size);
    }

    private void makeOperationsWithRetry(List<? extends Operation> operations) {
        try {
            RepeatableExecutor repeatableExecutor = retryConfig.addExceptionActions(
                    retryConfig.getExecutor(TableRepeatableExecutorConfiguration.ACTION_MAKE_OPERATIONS_WITH_RETRY),
                    this::actualizeRegionsMap);
            repeatableExecutor.execute(() -> {
                try {
                    makeOperations(operations);
                    return null;
                } catch (UnknownRegionException | UnavailableServerException | NotActualRegionMappingException |
                         IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            if (LOGGER.isTraceEnabled()) {
                Writer.save();
            }
            actualizeRegionsMap();
            throw prepareRuntimeException(e);
        }
    }

    private void makeOperations(List<? extends Operation> operations) throws
            IOException, NotActualRegionMappingException, UnavailableServerException {
        for (Operation op : operations) {
            makeSingleOperation(op);
        }
    }

    private void makeSingleOperation(Operation op) throws
            NotActualRegionMappingException, IOException, UnavailableServerException {
        RegionInfo regionInfo = getRegionInfoPosition(op.getRowkey());
        if (regionInfo == null) {
            throw new UnknownRegionException();
        }
        RegionServerModel rsm = allRegions.get(regionInfo);
        TRSClientConnection stub = regionServerConnections.get(rsm.getName());

        stub.sendRequest(op, info.get(rsm));
    }

    private List<? extends Operation> processOperationsWithRetry(List<? extends Operation> operations) throws
            IOException {
        try {
            return processOperations(operations);
        } catch (Exception e) {
            if (LOGGER.isTraceEnabled()) {
                Writer.save();
            }
            actualizeRegionsMap();
            throw prepareRuntimeException(e);
        }
    }

    private List<? extends Operation> processOperations(List<? extends Operation> operations) throws
            IOException, NotActualRegionMappingException, UnavailableServerException {
        List<Operation> results = new ArrayList<>(operations.size());
        for (Operation op : operations) {
            RepeatableExecutor repeatableExecutor = retryConfig.addExceptionActions(
                    retryConfig.getExecutor(TableRepeatableExecutorConfiguration.ACTION_PROCESS_OPERATIONS_WITH_RETRY),
                    this::actualizeRegionsMap);
            results.add(repeatableExecutor.execute(() -> {
                try {
                    return processSingleOperation(op);
                } catch (UnknownRegionException
                         | UnavailableServerException
                         | NotActualRegionMappingException
                         | IOException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        return results;
    }

    private Operation processSingleOperation(Operation op) throws
            NotActualRegionMappingException, IOException, UnavailableServerException {
        RegionInfo regionInfo = getRegionInfoPosition(op.getRowkey());
        if (regionInfo == null) {
            throw new UnknownRegionException();
        }
        RegionServerModel rsm = allRegions.get(regionInfo);
        TRSClientConnection stub = regionServerConnections.get(rsm.getName());

        return stub.getResponse(op, info.get(rsm));
    }

    public TRSClientConnection selectConnection(Operation op) {
        RegionInfo regionInfo = getRegionInfoPosition(op.getRowkey());
        if (regionInfo == null) {
            throw new UnknownRegionException();
        }
        RegionServerModel rsm = allRegions.get(regionInfo);
        op.setRegionPosition(regionInfo.getPosition());
        return regionServerConnections.get(rsm.getName());
    }

    @Override
    public long count() throws IOException {
        AtomicLong counter = new AtomicLong(0L);
        return retryConfig.addExceptionActions(
                retryConfig.getExecutor(TableRepeatableExecutorConfiguration.ACTION_COUNT_WITH_RETRY),
                this::actualizeRegionsMap).execute(() -> {
            try {
                for (Map.Entry<RegionInfo, RegionServerModel> entry : allRegions.entrySet()) {
                    TRSClientConnection stub = regionServerConnections.get(entry.getValue().getName());
                    counter.addAndGet(stub.requestCount(new RowCountRequest(entry.getKey().getPosition()), tableName).getCounts());
                }
                return counter.get();
            } catch (UnknownRegionException
                     | NotActualRegionMappingException
                     | IOException e) {
                actualizeRegionsMap();
                counter.set(0L);
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        flush();
        tableStructuresCleanup();
        connection.close(this);
    }

    @Override
    public void mutate(DbMutation mutation) throws IOException {
        if (mutation instanceof DelRows) {
            this.delete((Delete) mutation);
        } else if (mutation instanceof PutRows || mutation instanceof Append) {
            this.put((Put) mutation);
        } else if (mutation instanceof RowMutations) {
            this.mutateRow((RowMutations) mutation);
        } else {
            throw new RuntimeException("unknown mutation type: " + mutation.getClass().getName());
        }
    }

    @Override
    public void mutate(List<DbMutation> mutations) throws IOException {
        for (DbMutation mutation : mutations) {
            mutate(mutation);
        }
    }

    @Override
    public void mutateNoFlush(List<DbMutation> mutations) throws IOException {
        mutate(mutations);
    }

    public void mutateNoFlush(DbMutation mutation) throws IOException {
        mutate(mutation);
    }

    @Override
    public void flush() throws IOException {

    }

    public String getTableName() {
        return tableName.toString();
    }

    @Override
    public TableName getName() {
        return tableName;
    }

    @Override
    public List<RegionLocation> getAllRegionLocations() throws IOException {
        return allRegions.entrySet().stream()
                .sorted((o1, o2) -> BytesComparator.compareTo(o1.getKey().getStartKey(), o2.getKey().getStartKey()))
                .map(e -> new RegionLocation(fromRegionInfo(e.getKey()), e.getValue()))
                .collect(Collectors.toList());
    }

    private Region fromRegionInfo(RegionInfo info) {
        return new Region(info.getRegionName().getName(), info.getStartKey(), info.getEndKey());
    }

    protected void actualizeRegionsMap() {
        try {
            // TODO: Replace this ad hoc coordination with a dedicated refresh primitive.
            // Many threads may call this method at once. Without coordination each of them would create
            // its own connections, which would then be lost: they would not be tracked in regionServerConnections.
            // The first thread refreshes the region map and related state; the others wait for its result.
            LOGGER.debug("Thread id={}, current object={}", Thread.currentThread().getId(), this);
            // Only the first arriving thread performs the refresh
            if (mFirst.getAndSet(false)) {
                LOGGER.debug("Thread comes and will try to take semaphore");
                semaphore.acquire();
                LOGGER.debug("Semaphore acquired");
                try {
                    new RepeatableExecutor()
                            .withActionName("actualizeRegionsMap")
                            .withMaxRetries(10)
                            .withStaticTimeout(300)
                            .execute(() -> {
                                FutureTask<Void> task = new FutureTask<>(() -> {
                                    LOGGER.debug("Start regions map actualization");
                                    connection.waitTableStability(tableName);
                                    Map<RegionServerModel, List<RegionModel>> regionsMapping = connection.getActualRegionsMapping(tableName);
                                    calculateTableInfo(regionsMapping);
                                    if (!isRegionRangeMapFull()) {
                                        LOGGER.warn("Actualized regions map is not full for table {}. Retry map request", tableName);
                                        throw new RuntimeException("Actualized regions map is not full");
                                    }
                                    return null;
                                });
                                LOGGER.debug("Run recalculating task");
                                task.run();
                                try {
                                    task.get(15, TimeUnit.SECONDS);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                                LOGGER.debug("Recalculation is done");
                                return null;
                            });
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                } finally {
                    semaphore.release();
                    // Reset the marker for the next refresh
                    mFirst.set(true);
                    LOGGER.debug("Semaphore released and flag returned");
                }
            } else {
                LOGGER.debug("Try to acquire semaphore");
                // Wait for the refreshing thread, with a timeout to avoid blocking forever
                if (semaphore.tryAcquire(3, TimeUnit.SECONDS)) {
                    LOGGER.debug("Actualization was done by another thread");
                    semaphore.release();
                } else {
                    LOGGER.debug("Can't acquire semaphore");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void validateRegionRangeMap() {
        if (!isRegionRangeMapFull()) {
            LOGGER.error("Regions map contains gap or overlap: table={}, mapSize={}, ranges={}",
                    tableName,
                    keyMapping.size(),
                    buildRegionsRangesSnapshot(keyMapping.values()));
            throw new IllegalStateException("Regions map contains gap or overlap for table " + tableName);
        }
    }

    private String buildRegionsRangesSnapshot(Collection<RegionInfo> inputRegions) {
        return inputRegions.stream()
                .sorted((o1, o2) -> BytesComparator.compareTo(o1.getStartKey(), o2.getStartKey()))
                .map(ri -> String.format("%s:[%s..%s)",
                        ri.getRegionName(),
                        Arrays.toString(ri.getStartKey()),
                        Arrays.toString(ri.getEndKey())))
                .collect(Collectors.joining(", "));
    }

    private boolean isRegionRangeMapFull(Collection<RegionInfo> inputRegions) {
        List<RegionInfo> regions = new ArrayList<>(inputRegions);
        regions.sort((o1, o2) -> BytesComparator.compareTo(o1.getStartKey(), o2.getStartKey()));
        if (regions.isEmpty()) {
            LOGGER.warn("Regions map validation failed: no regions found for table {}", tableName);
            return false;
        }

        for (int i = 1; i < regions.size(); i++) {
            RegionInfo previous = regions.get(i - 1);
            RegionInfo current = regions.get(i);
            int checkAlignment = BytesComparator.compareTo(previous.getEndKey(), current.getStartKey());
            if (checkAlignment != 0) {
                LOGGER.warn("Regions map validation failed for table {} between regions {} and {}. previousEndKey={}, currentStartKey={}",
                        tableName, previous.getRegionName(), current.getRegionName(), Arrays.toString(previous.getEndKey()), Arrays.toString(current.getStartKey()));
                return false;
            }
        }

        return true;
    }

    boolean isRegionRangeMapFull() {
        return isRegionRangeMapFull(keyMapping.values());
    }

    private boolean isScanRangeFullyCovered(byte[] scanStartKey, byte[] scanStopKey) {
        byte[] effectiveStopKey = isStopKeyOpen(scanStopKey) ? Bytes.END_BYTES_UNIFORM : scanStopKey;

        List<RegionInfo> regions = new ArrayList<>(keyMapping.values());
        regions.sort((a, b) -> BytesComparator.compareTo(a.getStartKey(), b.getStartKey()));
        if (regions.isEmpty()) {
            LOGGER.warn("Scan range coverage check failed: no regions. scan=[{}, {})", Arrays.toString(scanStartKey), Arrays.toString(effectiveStopKey));
            return false;
        }
        byte[] expectedFrom = scanStartKey;
        boolean touched = false;

        for (RegionInfo ri : regions) {
            byte[] regionStart = ri.getStartKey();
            byte[] regionEnd = ri.getEndKey();
            if (BytesComparator.compareTo(regionEnd, scanStartKey) <= 0) {
                continue;
            }
            if (BytesComparator.compareTo(regionStart, effectiveStopKey) >= 0) {
                break;
            }
            touched = true;
            byte[] segmentStart = maxKey(regionStart, scanStartKey);
            byte[] segmentEnd = minKey(regionEnd, effectiveStopKey);
            if (BytesComparator.compareTo(segmentStart, expectedFrom) > 0) {
                LOGGER.warn("Scan range gap detected: expectedFrom={}, actualStart={}, region={}, scan=[{}, {})",
                        Arrays.toString(expectedFrom), Arrays.toString(segmentStart), ri.getRegionName(),
                        Arrays.toString(scanStartKey), Arrays.toString(effectiveStopKey));
                return false;
            }
            if (BytesComparator.compareTo(segmentEnd, expectedFrom) > 0) {
                expectedFrom = segmentEnd;
            }
            if (BytesComparator.compareTo(expectedFrom, effectiveStopKey) >= 0) {
                return true;
            }
        }

        boolean covered = touched && BytesComparator.compareTo(expectedFrom, effectiveStopKey) >= 0;
        if (!covered) {
            LOGGER.warn("Scan range is not fully covered: coveredUntil={}, requiredUntil={}, scan=[{}, {})",
                    Arrays.toString(expectedFrom), Arrays.toString(effectiveStopKey), Arrays.toString(scanStartKey), Arrays.toString(effectiveStopKey));
        }
        return covered;
    }

    private byte[] maxKey(byte[] a, byte[] b) {
        return BytesComparator.compareTo(a, b) >= 0 ? a : b;
    }

    private byte[] minKey(byte[] a, byte[] b) {
        return BytesComparator.compareTo(a, b) <= 0 ? a : b;
    }

    @Override
    public boolean isLazyTable() {
        return false;
    }

    protected boolean isBulkApi() {
        return connection == null ? false : connection.getConfiguration().getBoolean(ATALANTA_CLIENT_ENABLE_BULK_API);
    }

    private boolean isHBaseFilter() {
        return connection.getConfiguration().getBoolean(ATALANTA_HBASE_SCAN_FILTER);
    }

}
