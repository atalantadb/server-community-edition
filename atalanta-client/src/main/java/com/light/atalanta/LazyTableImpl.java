/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.api.DbRegionLocator;
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
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.light.atalanta.config.AtalantaProperties.ATALANTA_CLIENT_SCAN_ITERATION_TIMEOUT;
import static com.light.atalanta.config.AtalantaProperties.ATALANTA_HBASE_SCAN_FILTER;

public class LazyTableImpl implements DbTable, DbRegionLocator, DbBufferedMutator, LazyTable, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LazyTable.class);
    final TableName tableName;

    final Map<String, TRSConnectionStub> regionServerConnections; // region server -> connection
    final ConcurrentNavigableMap<byte[], Pair<RegionModel, RegionServerModel>> keyMetaInformationMapping; // region name -> region server
    final ConcurrentNavigableMap<byte[], RegionInfo> keyInfoMapping; // start key -> region info
    final ConnectionThreadLocal connection;

    private final TableRepeatableExecutorConfiguration retryConfig;
    private final int dataBlockSize;

    LazyTableImpl(ConnectionThreadLocal connection, TableName tableName, Map<RegionServerModel, List<RegionModel>> regionsMapping)
            throws IOException, NotActualRegionMappingException {
        LOGGER.debug("Create new lazy table instance: {}", tableName);
        this.dataBlockSize = initDataBlockSize(tableName, regionsMapping);
        this.connection = connection;
        this.retryConfig = new TableRepeatableExecutorConfiguration(connection.getConfiguration());

        this.tableName = tableName;
        this.regionServerConnections = new HashMap<>();
        this.keyMetaInformationMapping = new CopyOnWriteArrayMap<>(BytesComparator.BYTES_COMPARATOR);
        fillRegionServerModel(regionsMapping);
        this.keyInfoMapping = new CopyOnWriteArrayMap<>(BytesComparator.BYTES_COMPARATOR);
        fillKeyMapping(regionsMapping);
        
        LOGGER.debug("Instance of {}(lazy) already created", tableName);
    }


    private void actualizeRegionsMap() {
        Map<RegionServerModel, List<RegionModel>> regionsMapping;
        try {
            regionsMapping = connection.getActualRegionsMapping(tableName);
        } catch (Exception e) {

            throw new RuntimeException(e);
        }
        tableStructuresCleanup();
        fillRegionServerModel(regionsMapping);
        fillKeyMapping(regionsMapping);
    }

    private void fillRegionServerModel(Map<RegionServerModel, List<RegionModel>> regionsMapping) {
        keyMetaInformationMapping.clear();
        regionsMapping.forEach((key, value) -> value.forEach(rm -> keyMetaInformationMapping.put(rm.getBeginKey(), new Pair<>(rm, key))));
    }

    private void fillKeyMapping(Map<RegionServerModel, List<RegionModel>> regionsMapping) {
        keyInfoMapping.clear();
        regionsMapping.values().stream().flatMap(Collection::stream).forEach(rm -> {
            final byte[] beginKey = rm.getBeginKey();
            keyInfoMapping.put(beginKey, null);
        });
    }

    private int initDataBlockSize(TableName tableName, Map<RegionServerModel, List<RegionModel>> regionsMapping) {
        return regionsMapping.entrySet().stream()
                .findAny()
                .orElseThrow(() -> new RuntimeException("Region Server not found for table:" + tableName)).getValue().stream()
                .findAny()
                .orElseThrow(() -> new RuntimeException("Region not found for table:" + tableName)).getDataBlockSize();
    }

    void tableStructuresCleanup() {
        LOGGER.debug("Cleanup {} table structures", tableName.toString());
        this.keyMetaInformationMapping.clear();
        this.keyInfoMapping.clear();
        this.regionServerConnections.forEach((key, val) -> {
            try {
                val.close();
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
        });
        this.regionServerConnections.clear();
        LOGGER.debug("All inner table's structures cleaned");
    }


    // Scan operations
    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        Scan Scan = (Scan) scan;
        if (Scan.isLimited()) {
            LOGGER.debug("Scanner has limit: {}", Scan.getCacheSize());
            return new LazyLimitedClientScanner(Scan, this);

        }
        return new LazyClientScanner(scan, this);
    }

    public List<RegionInfo> getSortedRegions() {
        List<RegionInfo> infos = new ArrayList<>(keyMetaInformationMapping.size());
        keyMetaInformationMapping.forEach((k, v) -> infos.add(keyInfoMapping.get(k)));
        return infos;
    }

    public byte[] getNextRegionKey(byte[] currentKey) {
        Map.Entry<byte[], Pair<RegionModel, RegionServerModel>> entry = keyMetaInformationMapping.higherEntry(currentKey);
        return entry == null ? Bytes.toBytes("FFFFFFFF") : entry.getValue().getFirst().getBeginKey();
    }

    TRSConnectionStub chooseRegion(byte[] startKey) {
        LOGGER.debug("Choose {}'s region of for scan started with: {}", tableName, Arrays.toString(startKey));
        Map.Entry<byte[], Pair<RegionModel, RegionServerModel>> meta = keyMetaInformationMapping.floorEntry(startKey);
        if (meta == null) {
            actualizeRegionsMap();
            meta = keyMetaInformationMapping.floorEntry(startKey);
        }
        final Pair<RegionModel, RegionServerModel> pair = meta.getValue();
        final RegionServerModel rsModel = pair.getSecond();
        TRSConnectionStub stub = regionServerConnections.get(rsModel.getName());
        if (stub == null) {
            try {
                stub = connection.getTopology().getNewConnectionToCore(rsModel.getName());
                
                    stub.sendEmptyAuthRequest();
                
                List<String> regionNames = getAllRegionsForRS(rsModel);
                TableRegionServerInfo trsInfo = stub.getTableRegionServerInfo(regionNames, tableName);
                List<RegionInfo> regionInfos = trsInfo.getRegions();
                final TRSConnectionStub finalStub = stub;
                regionInfos.forEach(ri -> {
                    keyInfoMapping.put(ri.getStartKey(), ri);
                    regionServerConnections.put(rsModel.getName(), finalStub);
                });
            } catch (IOException | NotActualRegionMappingException e) {
                throw new RuntimeException(e);
            }
        }
        return stub;
    }

    private List<String> getAllRegionsForRS(RegionServerModel rsModel) {
        List<String> names = new ArrayList<>();
        for (Pair<RegionModel, RegionServerModel> pair : keyMetaInformationMapping.values()) {
            if (pair.getSecond().equals(rsModel)) {
                names.add(getRegionReplicaName(pair.getFirst().getName()));
            }
        }

        return names;
    }

    private String getRegionReplicaName(String regionName) {
        return regionName + "_0";
    }


    public ScanResultContainer scanRegion(Scan scan) {
        try {
            RepeatableExecutor repeatableExecutor = retryConfig
                    .addExceptionActions(retryConfig.getExecutor(TableRepeatableExecutorConfiguration.ACTION_SCAN_REGION), this::actualizeRegionsMap);
            return repeatableExecutor.execute(() -> {
                try {
                    // TODO: Merge these two calls
                    TRSConnectionStub stub = chooseRegion(scan.getStartKey());
                    RegionInfo ri = keyInfoMapping.floorEntry(scan.getStartKey()).getValue();
                    LOGGER.debug("Selected region: {}", ri);
                    ScanOperation scanOperation = OperationConverter.fromScanV1(scan, ri.getPosition(), dataBlockSize);
                    ScanResultContainer container = stub.makeScan(scanOperation, tableName);
                    return waitingForCorrectContainer(stub, container, scanOperation);
                } catch (UnknownRegionException | UnavailableServerException | NotActualRegionMappingException |
                         IOException e) {
                    actualizeRegionsMap();
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


    private ScanResultContainer waitingForCorrectContainer(TRSConnectionStub stub, ScanResultContainer container, ScanOperation scanOperation) throws IOException {
        boolean isSameResults = isSameScanResults(container, scanOperation);
        if (isSameResults) {
            LOGGER.debug("Container contains actual results");
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
        return container.getClientBlockNumber() == scanOperation.getBlockNumber()
                && container.getTryNum() == scanOperation.getTryNumber();
    }

    // Put operations
    public void put(Put put) throws IOException {
        MutationValidator.validatePut(put);
        putList(Collections.singletonList((Put) put));
    }

    @Override
    public void mutateRow(RowMutations mutation) throws IOException {
        MutationValidator.validateRowMutations(mutation);
        Operation mutationOperation = OperationConverter.createMutation((RowMutations) mutation);
        processOperationsWithRetry(Collections.singletonList(mutationOperation));
    }

    @Override
    public void mutateNoFlush(DbMutation mutation) throws IOException {
        mutate(mutation);
    }

    @Override
    public void mutateNoFlush(List<DbMutation> mutations) throws IOException {
        mutate(mutations);
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
        for (Delete delete : deletes) {
            delete(delete);
        }
    }

    public void put(List<Put> puts) throws IOException {
        for (Put put : puts) {
            MutationValidator.validatePut(put);
        }
        putList(puts);
    }

    public List<Put> putList(List<Put> puts) {
        for (Put put : puts) {
            MutationValidator.validatePut(put);
        }
        List<PutOperation> putOperations = OperationConverter.toOperations(puts);
        List<? extends Operation> results = processOperationsWithRetry(putOperations);
        return OperationConverter.fromOperations(results);
    }

    public void putReusablePut(Put put) {
        MutationValidator.validatePut(put);
        PutToByteSerializer.ByteCollector collector = PutSerializer.getSerializer().serializePut(put, getRegionInfoPosition(put.getRow()).getPosition());
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

    public void putReusablePuts(List<Put> puts) {
        for (Put put : puts){
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
        List<Result> resultList = getList(Collections.singletonList((Get) get));
        return resultList != null && resultList.size() > 0 ? resultList.get(0) : null;
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
        return getList(gets.stream().map(g -> (Get) g).collect(Collectors.toList()))
                .toArray(new Result[0]);
    }

    public List<Result> getList(List<Get> gets) throws IOException {
        List<Operation> getOperations = OperationConverter.toOperations(gets);
        List<? extends Operation> results = processOperationsWithRetry(getOperations);
        return OperationConverter.fromOperations(results);
    }

    // Delete operations
    @Override
    public void delete(Delete delete) throws IOException {
        deleteAndGetList(Collections.singletonList((Delete) delete));
    }

    public Delete deleteAndGet(Delete delete) throws IOException {
        List<Delete> deletes = deleteAndGetList(Collections.singletonList(delete));
        return deletes != null && deletes.size() > 0 ? deletes.get(0) : null;
    }

    public List<Delete> deleteAndGetList(List<Delete> deletes) {
        List<Operation> deleteOperations = OperationConverter.toOperations(deletes);
        List<? extends Operation> results = processOperationsWithRetry(deleteOperations);
        return OperationConverter.fromOperations(results);
    }


    public RegionInfo getRegionInfoPosition(byte[] rowkey) {
        Map.Entry<byte[], RegionInfo> entry = keyInfoMapping.floorEntry(rowkey);
        return entry != null ? entry.getValue() : null;
    }

    private void processBytesWithRetry(byte[] rowkey, ByteBuffer bb, int size) {
        try {
            RepeatableExecutor repeatableExecutor = retryConfig
                    .addExceptionActions(retryConfig.getExecutor(TableRepeatableExecutorConfiguration.ACTION_PROCESS_BYTES_WITH_RETRY), this::actualizeRegionsMap);
            repeatableExecutor.execute(() -> {
                try {
                    processByteOperation(rowkey, bb, size);
                    return null;
                } catch (UnknownRegionException | UnavailableServerException | NotActualRegionMappingException |
                         IOException e) {
                    actualizeRegionsMap();
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

    private void processByteOperation(byte[] rowkey, ByteBuffer bb, int size) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        RegionInfo regionInfo = getRegionInfoPosition(rowkey);
        TRSConnectionStub stub;
        if (regionInfo == null) {
            stub = chooseRegion(rowkey);
        } else {
            RegionServerModel rsm = keyMetaInformationMapping.get(rowkey).getSecond();
            stub = regionServerConnections.get(rsm.getName());
        }
        stub.sendBytes(bb, 0, size);
    }

    private void makeOperationsWithRetry(List<? extends Operation> operations) {
        try {
            RepeatableExecutor repeatableExecutor = retryConfig
                    .addExceptionActions(retryConfig.getExecutor(TableRepeatableExecutorConfiguration.ACTION_MAKE_OPERATIONS_WITH_RETRY), this::actualizeRegionsMap);
            repeatableExecutor.execute(() -> {
                try {
                    makeOperations(operations);
                    return null;
                } catch (UnknownRegionException | UnavailableServerException | NotActualRegionMappingException |
                         IOException e) {
                    actualizeRegionsMap();
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

    private void makeOperations(List<? extends Operation> operations) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        for (Operation op : operations) {
            makeSingleOperation(op);
        }
    }

    private void makeSingleOperation(Operation op) throws IOException {
        RegionInfo regionInfo = getRegionInfoPosition(op.getRowkey());
        TRSConnectionStub stub;
        if (regionInfo == null) {
            stub = chooseRegion(op.getRowkey());
            regionInfo = getRegionInfoPosition(op.getRowkey());
        } else {

            RegionServerModel rsm = keyMetaInformationMapping.get(op.getRowkey()).getSecond();
            stub = regionServerConnections.get(rsm.getName());
        }
        if (regionInfo == null) {
            throw new UnknownRegionException();
        }
        op.setRegionPosition(regionInfo.getPosition());
        stub.sendRequest(op);
    }

    private List<? extends Operation> processOperationsWithRetry(List<? extends Operation> operations) {
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

    private List<? extends Operation> processOperations(List<? extends Operation> operations) {
        List<Operation> results = new ArrayList<>(operations.size());
        for (Operation op : operations) {
            RepeatableExecutor repeatableExecutor = retryConfig
                    .addExceptionActions(retryConfig.getExecutor(TableRepeatableExecutorConfiguration.ACTION_PROCESS_OPERATIONS_WITH_RETRY), this::actualizeRegionsMap);
            results.add(repeatableExecutor.execute(() -> {
                try {
                    return processSingleOperation(op);
                } catch (UnknownRegionException | UnavailableServerException | NotActualRegionMappingException |
                         IOException e) {
                    actualizeRegionsMap();
                    throw new RuntimeException(e);
                }
            }));
        }
        return results;
    }


    private Operation processSingleOperation(Operation op) throws NotActualRegionMappingException, IOException, UnavailableServerException {
        RegionInfo regionInfo = getRegionInfoPosition(op.getRowkey());
        TRSConnectionStub stub;
        if (regionInfo == null) {
            stub = chooseRegion(op.getRowkey());
            regionInfo = getRegionInfoPosition(op.getRowkey());
        } else {

            RegionServerModel rsm = keyMetaInformationMapping.floorEntry(op.getRowkey()).getValue().getSecond();
            stub = regionServerConnections.get(rsm.getName());
        }
        op.setRegionPosition(regionInfo.getPosition());
        return stub.getResponse(op);
    }

    @Override
    public void close() {
        tableStructuresCleanup();
        connection.close(this);
    }

    @Override
    public void mutate(DbMutation mutation) throws IOException {
        if (mutation instanceof DelRows) {
            this.delete((Delete) mutation);
        } else if (mutation instanceof PutRows || mutation instanceof Append) {
            this.put((Put) mutation);
        } else {
            throw new RemoteException("unknown mutation type: " + mutation.getClass().getName());
        }
    }

    @Override
    public void mutate(List<DbMutation> mutations) throws IOException {
        for (DbMutation mutation : mutations) {
            mutate(mutation);
        }
    }

    @Override
    public void flush() throws IOException {
    }

    public String getTableName() {
        return tableName.toString();
    }

    public TableName getName() {
        return tableName;
    }

    private static RuntimeException prepareRuntimeException(Exception e) {
        return e.getCause() != null ? new RuntimeException(e.getCause()) : new RuntimeException(e);
    }

    @Override
    public List<RegionLocation> getAllRegionLocations() throws IOException {
        // Not implemented
        return new ArrayList<>();
    }

    @Override
    public boolean isLazyTable() {
        return true;
    }

    private boolean isHBaseFilter() {
        return connection.getConfiguration().getBoolean(ATALANTA_HBASE_SCAN_FILTER);
    }
}
