/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport;

import com.light.atalanta.RepeatableExecutor;

import com.light.atalanta.NameOfTable;
import com.light.atalanta.exceptions.AtalantaTransportRuntimeException;
import com.light.atalanta.transport.exceptions.*;
import com.light.atalanta.transport.messages.*;
import com.light.atalanta.transport.values.*;
import com.light.atalanta.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.light.atalanta.transport.TransportConfiguration.*;
import static com.light.atalanta.transport.messages.BulkHelper.NOT_USE_TABLE_POSITION;
import static com.light.atalanta.transport.messages.OperationFactory.checkReturnedExceptions;

public class TRSConnectionStub implements TRSClientConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(TRSConnectionStub.class);

    final String host;
    final int port;
    final int socketTimeout;
    final String location;
    final TransportConfiguration transportConfiguration;
    final RepeatableExecutorFactory repeatableExecutorFactory;
    final SocketFactoryProvider socketFactoryProvider;

    Socket socket;
    InputStream in;
    OutputStream out;
    SocketConnection socketConnection;

    private final Map<NameOfTable, TableRegionServerInfo> info = new HashMap<>();

    static final AtomicLong openSocketCount = new AtomicLong();
    static final AtomicLong closeSocketCount = new AtomicLong();

    TRSConnectionStub(TransportConfiguration configuration, String host, int port) {
        this(configuration, host, port, Socket::new);
    }

    TRSConnectionStub(TransportConfiguration configuration, String host, int port, SocketFactoryProvider socketFactoryProvider) {
        LOGGER.debug("Create connection to core: {}:{}", host, port);
        this.transportConfiguration = configuration == null ? TransportConfiguration.defaultConfiguration() : configuration;
        this.host = host;
        this.port = port;
        this.location = host + ":" + port;
        this.socketTimeout = Integer.parseInt(this.transportConfiguration.get(ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY));
        this.socketFactoryProvider = socketFactoryProvider;

        initConnection();
        this.repeatableExecutorFactory = new RepeatableExecutorFactory(transportConfiguration);
    }

    public TRSConnectionStub(TransportConfiguration configuration, Socket socket) throws IOException {
        LOGGER.debug("Wrap accepted connection: {}", socket);
        this.transportConfiguration = configuration == null ? TransportConfiguration.defaultConfiguration() : configuration;
        this.host = socket.getInetAddress() == null ? "unknown" : socket.getInetAddress().getHostAddress();
        this.port = socket.getPort();
        this.location = host + ":" + port;
        this.socketTimeout = Integer.parseInt(this.transportConfiguration.get(ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY));
        this.socketFactoryProvider = Socket::new;
        this.repeatableExecutorFactory = new RepeatableExecutorFactory(transportConfiguration);

        this.socket = socket;
        this.socket.setSoTimeout(socketTimeout);
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
        this.socketConnection = new SocketConnection(transportConfiguration, in, out);
    }

    public void writeRawBytes(byte[] bytes) throws IOException {
        socketConnection.writeMessage(bytes, 0, bytes.length);
    }

    // Visible for testing
    TRSConnectionStub(TransportConfiguration configuration, SocketConnection socketConnection, String host, int port) {
        LOGGER.debug("Create connection to core: {}:{}", host, port);
        this.transportConfiguration = configuration == null ? TransportConfiguration.defaultConfiguration() : configuration;
        this.host = host;
        this.port = port;
        this.location = host + ":" + port;
        this.socketTimeout = Integer.parseInt(this.transportConfiguration.get(ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY));
        this.in = socketConnection.getIn();
        this.out = socketConnection.getOut();
        this.socketConnection = socketConnection;
        this.socket = null;
        this.socketFactoryProvider = Socket::new;
        this.repeatableExecutorFactory = new RepeatableExecutorFactory(transportConfiguration);
    }

    TRSConnectionStub(int port) {
        this(null, "localhost", port);
    }

    private void initConnection() {
        try {
            close();
            openSocketCount.incrementAndGet();
            this.socket = socketFactoryProvider.create(host, port);
            this.socket.setSoTimeout(socketTimeout);

            LOGGER.debug("New socket: {}", socket);

            this.in = socket.getInputStream();
            this.out = socket.getOutputStream();
            this.socketConnection = new SocketConnection(transportConfiguration, in, out);
        } catch (IOException e) {
            LOGGER.error("Cant create connection to {}: {}. total open socket: {}, host: {}, port: {}, total closed socket: {}",
                    host, port, openSocketCount.get(), host, port, closeSocketCount.get());
            throw new AtalantaTransportRuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Closing all connections to {}", location);
        }
        if (in != null) {
            in.close();
        }
        if (out != null) {
            out.close();
        }
        if (socket != null) {
            closeSocketCount.incrementAndGet();
            socket.close();
            LOGGER.debug("Socket closed: {}", socket);
        }
    }

    TableRegionMapResponse getRegionMapResponse(TableMapObject mapObject) throws NotActualRegionMappingException, IOException {
        TransportMessage requestRegMap = isTableMapWithTimeout()
                ? new TableRegionMapRequestWithTimeout(mapObject, Integer.parseInt(transportConfiguration.get(ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY)))
                : new TableRegionMapRequest(mapObject);
        LOGGER.debug("Send regions map request to core {}", location);
        sendMessage(requestRegMap);
        LOGGER.debug("Send regions map request to core complete {}", location);
        TableRegionMapResponse regionMap = (TableRegionMapResponse) receiveTableMap(requestRegMap);
        LOGGER.debug("Receive regions map from core: {}, core {}", regionMap, location);
        return regionMap;
    }

    private boolean isTableMapWithTimeout() {
        return Boolean.parseBoolean(transportConfiguration.get(ATALANTA_TRANSPORT_TABLE_MAP_TIMEOUT_ENABLE));
    }

    @Override
    public TableRegionServerInfo getTableRegionServerInfo(List<String> tableRegions, NameOfTable tableName) throws IOException, NotActualRegionMappingException {
        TableMapObject mapObject = new TableMapObject(tableName, tableRegions);
        TableRegionMapResponse regionMap = getRegionMapResponse(mapObject);
        if (regionMap.getMessageType() == MessageType.INVALID_REGION_MAP_ERROR) {
            throw new NotActualRegionMappingException(location);
        }
        return constructTableRegionServerInfo(mapObject, regionMap, tableName);
    }

    TableRegionServerInfo constructTableRegionServerInfo(TableMapObject mapObject, TableRegionMapResponse regionMap, NameOfTable tableName) {
        List<RegionInfo> rInfoList = new ArrayList<>();
        for (int i = 0; i < regionMap.getRegionInfos().length; i++) {
            RegionName rName = mapObject.getRegionNames().get(i);
            RegionInfoResponseObject regionInfoResponseObject = regionMap.getRegionInfos()[i];
            RegionInfo rInfo = new RegionInfo(tableName, rName, new RegionPosition(regionInfoResponseObject.getRegionPosition()),
                    regionInfoResponseObject.getStartKey(), regionInfoResponseObject.getEndKey());
            rInfoList.add(rInfo);
        }

        TableRegionServerInfo tableRSInfo = new TableRegionServerInfo(tableName, rInfoList);
        info.put(tableName, tableRSInfo);
        LOGGER.debug("Table region server info created");
        return tableRSInfo;
    }

    public RsResponse sendRequestToRs(TransportMessage m) throws IOException {
        try {
            RepeatableExecutor repeatableExecutor = repeatableExecutorFactory.getExecutor(RepeatableExecutorFactory.ACTION_SEND_REQUEST_TO_RS);
            return repeatableExecutor.skipException(NotActualRegionMappingException.class)
                    .withExceptionalAction(UnavailableCoreException.class, this::initConnection, false)
                    .execute(() -> {
                        try {
                            sendMessage0(m);
                            RsResponse rsResponse = new RsResponse(receiveBytes());
                            LOGGER.debug("core response: ", rsResponse);
                            return rsResponse;
                        } catch (UnavailableCoreException e) {
                            LOGGER.warn("error sendRequestToRs 0, core: {}", location, e);
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            LOGGER.warn("error sendRequestToRs 1, core: {}", location, e);
                            initConnection();
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            LOGGER.warn("error sendMessage 2, core: {}", location, e);
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }

    public void sendMessage(TransportMessage m) throws IOException {
        try {
            RepeatableExecutor repeatableExecutor = repeatableExecutorFactory.getExecutor(RepeatableExecutorFactory.ACTION_SEND_MESSAGE);
            repeatableExecutor
                    .skipException(SocketException.class)
                    .skipException(SocketTimeoutException.class)
                    .execute(() -> {
                        try {
                            sendMessage0(m);
                            return null;
                        } catch (IOException e) {
                            LOGGER.warn("error sendMessage, core: {}", location, e);
                            initConnection();
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            LOGGER.error("Cant execute 'sendMessage' with retries: {}, core: {}", location, e.getMessage());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }

    @Override
    public void sendBulkPut(BulkPutOperation bulkPut, NameOfTable tableName) throws IOException {
        try {
            List<BulkPutOperation> puts = splitBulkByChunks(bulkPut);
            for (BulkPutOperation p : puts) {
                RepeatableExecutor repeatableExecutor = repeatableExecutorFactory.getExecutor(RepeatableExecutorFactory.ACTION_SEND_MESSAGE);
                repeatableExecutor
                        .skipException(NotActualRegionMappingException.class)
                        .skipException(SocketException.class)
                        .skipException(SocketTimeoutException.class)
                        .execute(() -> {
                            try {
                                    if (p.getOperations().size() == 1) {
                                        // Core limitation: an operation that does not fit into a bulk message is sent separately
                                        sendMessage(p.getOperations().get(0));
                                    } else {
                                        sendMessage(p);
                                    }
                                    byte[] received = receiveBytes();
                                    if (received.length < 16) {
                                        checkReturnedExceptions(transportConfiguration, OperationHelper.getTransportMessageType(received), location);
                                    }
                                
                                return null;
                            } catch (IOException | UnavailableCoreException | NotActualRegionMappingException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        } catch (Exception e) {
            LOGGER.error("Cant execute 'sendMessage' with retries: {}, core: {}", location, e.getMessage());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }

    @Override
    public ResultOperation[] sendBulkGet(BulkGetOperation bulkGet, NameOfTable tableName) throws IOException, NotActualRegionMappingException {
        try {
            List<BulkGetOperation> gets = splitBulkByChunks(bulkGet);
            List<BulkResponseOperation> responseOperations = new ArrayList<>();
            for (BulkGetOperation g : gets) {
                RepeatableExecutor repeatableExecutor = repeatableExecutorFactory.getExecutor(RepeatableExecutorFactory.ACTION_SEND_MESSAGE);
                BulkResponseOperation op = new BulkResponseOperation(repeatableExecutor
                        .skipException(SocketException.class)
                        .skipException(SocketTimeoutException.class)
                        .execute(() -> {
                            try {
                                    sendMessage(g);
                                    return receiveBytes();
                            } catch (IOException | UnavailableCoreException e) {
                                throw new RuntimeException(e);
                            }
                        }));
                responseOperations.add(op);
            }
            return responseOperations.stream().map(BulkResponseOperation::getResultOperations).flatMap(List::stream).toArray(ResultOperation[]::new);
        } catch (Exception e) {
            LOGGER.error("Cant execute 'sendMessage' with retries: {}, core: {}", location, e.getMessage());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends BulkOperation> List<T> splitBulkByChunks(T bulkOperation) {
        int chunkSize = Integer.parseInt(transportConfiguration.get(ATALANTA_TRANSPORT_BULK_CHUNK_SIZE));
        chunkSize = chunkSize < 32768 ? chunkSize : 32768;
        if (bulkOperation.getMessageSize() < chunkSize) {
            return Collections.singletonList(bulkOperation);
        }

        List<T> splitMessages = new ArrayList<>();
        int size = BulkGetOperation.TRANSPORT_MESSAGE_HEADER_LENGTH;
        List<Operation> operations = new ArrayList<>();
        for (Operation op : bulkOperation.getOperations()) {
            int messageSize = op.getMessageSize();
            if (size + messageSize > chunkSize && !operations.isEmpty()) {
                splitMessages.add((T) createBulkOperation(bulkOperation, operations, true));
                operations.clear();
                size = BulkGetOperation.TRANSPORT_MESSAGE_HEADER_LENGTH;
            }
            operations.add(op);
            size += messageSize;
        }
        if (!operations.isEmpty()) {
            splitMessages.add((T) createBulkOperation(bulkOperation, operations, false));
        }
        return splitMessages;
    }

    @SuppressWarnings("unchecked")
    private BulkOperation createBulkOperation(BulkOperation bulkOperation, List<Operation> operations, boolean isNewList) {
        List<? extends Operation> list = isNewList ? new ArrayList<>(operations) : operations;
        return bulkOperation instanceof BulkGetOperation
                ? new BulkGetOperation(bulkOperation.getRegionPosition().getPosition(), NOT_USE_TABLE_POSITION, (List<GetOperation>) list)
                : new BulkPutOperation(bulkOperation.getRegionPosition().getPosition(), NOT_USE_TABLE_POSITION, (List<PutOperation>) list);
    }

    private void sendMessage0(TransportMessage m) throws IOException {
        socketConnection.writeMessage(m);
    }

    @Override
    public RowCountResponse requestCount(RowCountRequest rowCountRequest, NameOfTable tableName) throws IOException, NotActualRegionMappingException {
        try {
            return new RepeatableExecutor()
                    .withStaticTimeout(100)
                    .withMaxRetries(2)
                    .skipException(NotActualRegionMappingException.class)
                    .execute(() -> {
                        try {
                            return requestCount0(rowCountRequest);
                        } catch (NotActualRegionMappingException | UnavailableCoreException | IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            LOGGER.error("Cant execute 'requestCount' with retries: {}, core: {}", location, e.getMessage());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else if (e.getCause() instanceof NotActualRegionMappingException) {
                throw (NotActualRegionMappingException) e.getCause();
            }
            throw e;
        }
    }

    private RowCountResponse requestCount0(RowCountRequest rowCountRequest) throws IOException, UnavailableCoreException, NotActualRegionMappingException {
        if (!rowCountRequest.getRegionPosition().isRegionPositionSet()) {
            throw new UnknownRegionException();
        }
        socketConnection.writeMessage(rowCountRequest);
        byte[] received = receiveBytes();
        if (received.length < 16) {
            checkReturnedExceptions(transportConfiguration, OperationHelper.getTransportMessageType(received), location);
        }
        return new RowCountResponse(received);
    }

    TransportMessage receiveTableMap(TransportMessage requestRegMap) throws IOException, NotActualRegionMappingException {
        try {
            RepeatableExecutor repeatableExecutor = repeatableExecutorFactory.getExecutor(RepeatableExecutorFactory.ACTION_RECEIVE_MESSAGE);
            return repeatableExecutor
                    .skipException(NotActualRegionMappingException.class)
                    .skipException(SocketTimeoutException.class)
                    .skipException(SocketException.class)
                    .withExceptionalAction(UnavailableCoreException.class, this::initConnection, false)
                    .execute(() -> {
                        try {
                            return receiveMessage0();
                        } catch (NotActualRegionMappingException | UnavailableCoreException | IOException e) {
                            LOGGER.warn("error receiveMessage 0, core: {}", location, e);
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            LOGGER.error("Cant execute 'receiveMessage' with retries: {}, core: {}", location, e.getMessage());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else if (e.getCause() instanceof NotActualRegionMappingException) {
                throw (NotActualRegionMappingException) e.getCause();
            }
            throw e;
        }
    }

    private TransportMessage receiveMessage0() throws IOException, NotActualRegionMappingException, UnavailableCoreException {
        return socketConnection.readMessage(location);
    }

    @Override
    public byte[] receiveBytes() throws IOException, UnavailableCoreException {
        try {
            return socketConnection.readMessageBytes();
        } catch (Throwable ex) {
            LOGGER.error("Cannot read message from {}.", location, ex);
            throw ex;
        }
    }

    @Override
    public synchronized ScanResultContainer makeScan(ScanOperation scan, NameOfTable tableName) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        try {
            return sendScan(scan, tableName);
        } catch (UnavailableCoreException e) {
            throw new UnavailableServerException(e);
        }
    }

     private void sendBytes(byte[] bytes, int offset, int length) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        try {
            RepeatableExecutor repeatableExecutor = repeatableExecutorFactory.getExecutor(RepeatableExecutorFactory.ACTION_SEND_BYTES);
            repeatableExecutor.skipException(NotActualRegionMappingException.class)
                    .withExceptionalAction(UnavailableCoreException.class, this::initConnection, false)
                    .execute(() -> {
                        try {
                            sendBytes0(bytes, offset, length);
                            return null;
                        } catch (NotActualRegionMappingException | UnavailableCoreException e) {
                            LOGGER.warn("error sendBytes 0, core: {}", location, e);
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            LOGGER.warn("error sendBytes 1, core: {}", location, e);
                            initConnection();
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            LOGGER.error("Cant execute 'sendBytes' with retries: {}, core: {}", location, e.getMessage());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else if (e.getCause() instanceof NotActualRegionMappingException) {
                throw (NotActualRegionMappingException) e.getCause();
            } else if (e.getCause() instanceof UnavailableCoreException) {
                throw new UnavailableServerException((UnavailableCoreException) e.getCause());
            }
            throw e;
        }
    }

    private void sendBytes0(byte[] bytes, int offset, int length) throws IOException, NotActualRegionMappingException, UnavailableCoreException {
        socketConnection.writeMessage(bytes, offset, length);
        socketConnection.readAnswer(location);
    }

    @Override
    public void sendBytes(ByteBuffer bb, int offset, int length) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        sendBytes(bb.array(), offset, length);
    }

    List<? extends Operation> sendOperations(List<Operation> requests, NameOfTable tableName) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        TableRegionServerInfo tableRegionServerInfo = info.get(tableName);
        if (tableRegionServerInfo == null) {
            throw new UnknownRegionException();
        }
        return getResponses(requests, tableRegionServerInfo);
    }

    @Override
    public void sendRequest(Operation op, TableRegionServerInfo tableInfo) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        sendSingleRequest(op, tableInfo, false);
    }

    public void sendRequest(Operation op) throws UnknownRegionException, IOException {
        if (op.getRegionPosition() == RegionPosition.UNSET) {
            throw new UnknownRegionException();
        }
        try {
            RepeatableExecutor repeatableExecutor = repeatableExecutorFactory.getExecutor(RepeatableExecutorFactory.ACTION_SEND_SINGLE_REQUEST);
            repeatableExecutor.skipException(NotActualRegionMappingException.class)
                    .withExceptionalAction(UnavailableCoreException.class, this::initConnection, false)
                    .execute(() -> {
                        try {
                            sendOperation(op);
                            return null;
                        } catch (UnknownRegionException e) {
                            LOGGER.warn("error sendSingleRequest 0, core: {}", location, e);
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            LOGGER.warn("error sendSingleRequest 1, core: {}", location, e);
                            initConnection();
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            LOGGER.error("Cant execute 'sendSingleRequest' with retries: {}, core: {}", location, e.getMessage());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else if (e.getCause() instanceof UnknownRegionException) {
                throw (UnknownRegionException) e.getCause();
            }
            throw e;
        }
    }

    private List<? extends Operation> getResponses(List<? extends Operation> requests, TableRegionServerInfo tableInfo) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        return getResponses(requests, tableInfo, false);
    }

    @Override
    public Operation getResponse(Operation requests, TableRegionServerInfo tableInfo) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        return getSingleResponse(requests, tableInfo, false);
    }

    public Operation getResponse(Operation requests) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        return getSingleResponse(requests);
    }

    @Override
    public void sendEmptyAuthRequest() throws IOException {
        try {
            RepeatableExecutor repeatableExecutor = repeatableExecutorFactory.getExecutor(RepeatableExecutorFactory.ACTION_SEND_EMPTY_AUTH_REQUEST);
            repeatableExecutor.skipException(NotActualRegionMappingException.class)
                    .withExceptionalAction(UnavailableCoreException.class, this::initConnection, false)
                    .execute(() -> {
                        try {
                            sendEmptyAuthRequest0();
                            return null;
                        } catch (UnavailableCoreException e) {
                            LOGGER.warn("error sendEmptyAuthRequest 0, core: {}", location, e);
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            LOGGER.warn("error sendEmptyAuthRequest 1, core: {}", location, e);
                            initConnection();
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            LOGGER.error("Cant execute 'sendEmptyAuthRequest' with retries: {}, core: {}", location, e.getMessage());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }

    private void sendEmptyAuthRequest0() throws IOException, UnavailableCoreException {
        TransportMessage requestRegMap = EmptyAuthRequest.getInstance();
        sendMessage0(requestRegMap);
        Objects.requireNonNull(receiveBytes(), "Core AUTH response is null");
    }

    private List<? extends Operation> getResponses(List<? extends Operation> requests, TableRegionServerInfo tableInfo, boolean isRegionPositionSet) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        // TODO: Optimize edge cases, such as very few operations (binary search for the first region) or very many
        List<Operation> responses = new ArrayList<>(requests.size());
        for (Operation op : requests) {
            responses.add(getSingleResponse(op, tableInfo, isRegionPositionSet));
        }
        return responses;
    }

    private Operation getSingleResponse(Operation op) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        try {
            RepeatableExecutor repeatableExecutor = repeatableExecutorFactory.getExecutor(RepeatableExecutorFactory.ACTION_GET_SINGLE_RESPONSE);
            return repeatableExecutor
                    .skipException(NotActualRegionMappingException.class)
                    .withExceptionalAction(UnavailableCoreException.class, this::initConnection, false)
                    .execute(() -> {
                        try {
                            return getSingleResponse0(op, location);
                        } catch (UnknownRegionException | NotActualRegionMappingException |
                                 UnavailableCoreException e) {
                            LOGGER.warn("Exception 0. key: {}", Bytes.toStringBinary(op.getRowkey()));
                            LOGGER.warn("error getSingleResponse 0, core: {}", location, e);
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            LOGGER.warn("Exception 1. key: {}", Bytes.toStringBinary(op.getRowkey()));
                            LOGGER.warn("error getSingleResponse 1, core: {}", location, e);
                            initConnection();
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            LOGGER.warn("Exception. key: {}", Bytes.toStringBinary(op.getRowkey()));
            LOGGER.error("Cant execute 'getSingleResponse' with retries: {}, core: {}", location, e.getMessage());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else if (e.getCause() instanceof NotActualRegionMappingException) {
                throw (NotActualRegionMappingException) e.getCause();
            } else if (e.getCause() instanceof UnknownRegionException) {
                throw (UnknownRegionException) e.getCause();
            } else if (e.getCause() instanceof UnavailableCoreException) {
                throw new UnavailableServerException((UnavailableCoreException) e.getCause());
            }
            throw e;
        }
    }

    private Operation getSingleResponse(Operation op, TableRegionServerInfo tableInfo, boolean isRegionPositionSet) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        try {
            RepeatableExecutor repeatableExecutor = repeatableExecutorFactory.getExecutor(RepeatableExecutorFactory.ACTION_GET_SINGLE_RESPONSE);

            return repeatableExecutor
                    .skipException(NotActualRegionMappingException.class)
                    .skipException(SocketTimeoutException.class)
                    .skipException(SocketException.class)
                    .skipException(UnavailableCoreException.class)
                    .execute(() -> {
                        try {
                            return getSingleResponse0(op, tableInfo, isRegionPositionSet, location);
                        } catch (UnknownRegionException | NotActualRegionMappingException | UnavailableCoreException |
                                 IOException e) {
                            LOGGER.warn("Error getSingleResponse, key: {}, core: {}", Bytes.toStringBinary(op.getRowkey()), location, e);
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            LOGGER.error("Cant execute 'getSingleResponse' with retries: {}, key: {}, core: {}", e.getMessage(), Bytes.toStringBinary(op.getRowkey()), location);
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else if (e.getCause() instanceof NotActualRegionMappingException) {
                throw (NotActualRegionMappingException) e.getCause();
            } else if (e.getCause() instanceof UnknownRegionException) {
                throw (UnknownRegionException) e.getCause();
            } else if (e.getCause() instanceof UnavailableCoreException) {
                throw new UnavailableServerException((UnavailableCoreException) e.getCause());
            }
            throw e;
        }
    }

    private Operation getSingleResponse0(Operation op, TableRegionServerInfo tableInfo, boolean isRegionPositionSet, String location) throws IOException, NotActualRegionMappingException, UnavailableCoreException {
        if (!isRegionPositionSet) {
            op.setRegionPosition(tableInfo.getRegionPosition(op.getRowkey()));
        }
        sendOperation(op);
        byte[] received = receiveBytes();
        return OperationFactory.fromBytes(transportConfiguration, received, location);
    }


    private Operation getSingleResponse0(Operation op, String location) throws IOException, NotActualRegionMappingException, UnavailableCoreException {
        if (op.getRegionPosition() == RegionPosition.UNSET) {
            throw new UnknownRegionException();
        }
        sendOperation(op);
        byte[] received = receiveBytes();
        return OperationFactory.fromBytes(transportConfiguration, received, location);
    }

    private void sendSingleRequest(Operation op, TableRegionServerInfo tableInfo, boolean isRegionPositionSet) throws IOException, NotActualRegionMappingException, UnavailableServerException {
        try {
            RepeatableExecutor repeatableExecutor = repeatableExecutorFactory.getExecutor(RepeatableExecutorFactory.ACTION_SEND_SINGLE_REQUEST);
            repeatableExecutor.skipException(NotActualRegionMappingException.class)
                    .withExceptionalAction(UnavailableCoreException.class, this::initConnection, false)
                    .execute(() -> {
                        try {
                            sendSingleRequest0(op, tableInfo, isRegionPositionSet);
                            return null;
                        } catch (UnknownRegionException | NotActualRegionMappingException |
                                 UnavailableCoreException e) {
                            LOGGER.warn("error sendSingleRequest 0, core: {}", location, e);
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            LOGGER.warn("error sendSingleRequest 1, core: {}", location, e);
                            initConnection();
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            LOGGER.error("Cant execute 'sendSingleRequest' with retries: {}, core: {}", location, e.getMessage());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else if (e.getCause() instanceof NotActualRegionMappingException) {
                throw (NotActualRegionMappingException) e.getCause();
            } else if (e.getCause() instanceof UnknownRegionException) {
                throw (UnknownRegionException) e.getCause();
            } else if (e.getCause() instanceof UnavailableCoreException) {
                throw new UnavailableServerException((UnavailableCoreException) e.getCause());
            }
            throw e;
        }
    }

    private void sendSingleRequest0(Operation op, TableRegionServerInfo tableInfo, boolean isRegionPositionSet) throws IOException, NotActualRegionMappingException, UnavailableCoreException {
        if (!isRegionPositionSet) {
            op.setRegionPosition(tableInfo.getRegionPosition(op.getRowkey()));
        }
        sendOperation(op);
        socketConnection.readAnswer(location);
    }

    private void sendOperation(PositionedTransportMessage m) throws IOException {
        if (!m.getRegionPosition().isRegionPositionSet()) {
            throw new UnknownRegionException();
        }
        socketConnection.writeMessage(m);
    }

    private ScanResultContainer sendScan(ScanOperation scan, NameOfTable tableName) throws IOException, NotActualRegionMappingException, UnavailableCoreException {
        TableRegionServerInfo tableRegionServerInfo = info.get(tableName);
        if (tableRegionServerInfo == null) {
            throw new UnknownRegionException();
        }
        return getResponseForScan(scan);
    }

    private ScanResultContainer getResponseForScan(ScanOperation scan) throws IOException, NotActualRegionMappingException, UnavailableCoreException {
        sendOperation(scan);
        byte[] received = receiveBytes();
        if (received.length < 16) {
            checkReturnedExceptions(transportConfiguration, OperationHelper.getTransportMessageType(received), location);
        }
        return Boolean.parseBoolean(transportConfiguration.get(ATALANTA_HBASE_SCAN_FILTER))
                ? ScanOperationV2.getResultsFromScan(received, location)
                : ScanOperationV1.getResultsFromScan(received, location);
    }

    @Override
    public String getLocation() {
        return location;
    }

    public static long getOpenSocketTotal() {
        return openSocketCount.get();
    }

    public static long getCloseSocketTotal() {
        return closeSocketCount.get();
    }

    public static long getCloseSocketCur() {
        return openSocketCount.get() - closeSocketCount.get();
    }
}
