/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.api.DbTable;
import com.light.atalanta.config.AtalantaProperties;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.grpc.MasterServiceGrpc;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.services.*;
import com.light.atalanta.transport.exceptions.NotActualRegionMappingException;
import com.light.atalanta.util.BytesComparator;
import io.grpc.LoadBalancerRegistry;
import io.grpc.ManagedChannel;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.internal.PickFirstLoadBalancerProvider;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.F_MASTER_NODE;
import static com.light.atalanta.services.ZooKeeperContextHelper.*;
import static com.light.atalanta.util.BannerPrinter.makeVersionLine;

public class ConnectionThreadLocal implements Connection, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionThreadLocal.class);

    protected DbConfiguration atalantaConfiguration;
    protected ZookeeperHelper zooHelper;
    protected ZooKeeperCoreContext zooKeeperCoreContext;
    protected DbConfiguration conf;
    protected ManagedChannel masterGrpcChannel;
    protected MasterServiceGrpc.MasterServiceBlockingStub masterGrpcStub;

    protected final ThreadLocal<ConcurrentHashMap<TableName, Table>> tables = ThreadLocal.withInitial(ConcurrentHashMap::new);
    private final ThreadLocal<ConcurrentHashMap<TableName, LazyTableImpl>> lazyTables = ThreadLocal.withInitial(ConcurrentHashMap::new);

    protected ClusterTopology topology;

    private Admin admin = null;
    private boolean isClosed = false;

    static {
        LOGGER.info("AtalantaDb client version: " + makeVersionLine());
        LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());
    }

    private String masterHost;
    private String masterPort;

    public ConnectionThreadLocal() {
    }

    public boolean isClosed() {
        return isClosed;
    }

    public ConnectionThreadLocal(DbConfiguration conf) throws IOException {
        conf.checkConfiguration();
        this.atalantaConfiguration = conf;
        ZkConfig zkConfig = new ZkConfig(
                conf.get(AtalantaProperties.ATALANTA_ZOOKEEPER_CONNECTION_STRING));
        this.zooHelper = new ZookeeperHelperImpl(
                atalantaConfiguration,
                zkConfig,
                conf.get(AtalantaProperties.ATALANTA_ZOOKEEPER_PATH),
                conf.getInt(AtalantaProperties.ATALANTA_ZOOKEEPER_RETRY_COUNT),
                conf.getInt(AtalantaProperties.ATALANTA_ZOOKEEPER_RETRY_DELAY_MS),
                conf.getInt(AtalantaProperties.ATALANTA_ZOOKEEPER_SESSION_TIMEOUT_MS)
        );
        this.conf = conf;
        this.masterGrpcChannel = buildGrpcChannel();
        this.masterGrpcStub = buildMasterGrpcStub(conf);
        this.zooKeeperCoreContext = new ZooKeeperCoreContext(zooHelper, new ZookeeperHelperExtended(zooHelper));
        this.topology = new ClusterTopology(zooKeeperCoreContext, conf);
    }

    protected MasterServiceGrpc.MasterServiceBlockingStub buildMasterGrpcStub(DbConfiguration conf) {
        return MasterServiceGrpc.newBlockingStub(masterGrpcChannel)
                .withDeadlineAfter(conf.getLong(AtalantaProperties.ATALANTA_MASTER_CONNECTION_TIMEOUT_MS), TimeUnit.MILLISECONDS);
    }

    public ClusterTopology getTopology() {
        return topology;
    }

    @Override
    public Table getTable(TableName tableName) throws IOException {
        Map<TableName, Table> tableNameTableMap = tables.get();
        Table table = tableNameTableMap.get(tableName);
        if (table == null) {
            try {
                table = constructNewTable(tableName);
                final Thread curThread = Thread.currentThread();
                LOGGER.info("New table: {}, thread: {}, threadId: {}", tableName, curThread, curThread.getId());
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            tableNameTableMap.put(tableName, table);
        }
        return (Table) table;
    }

    public DbTable getLazyTable(TableName tableName) throws IOException {
        Map<TableName, LazyTableImpl> tableNameTableMap = lazyTables.get();
        LazyTableImpl table = tableNameTableMap.get(tableName);
        if (table == null) {
            try {
                table = constructNewLazyTable(tableName);
                final Thread curThread = Thread.currentThread();
                LOGGER.info("New lazy table: {}, thread: {}, threadId: {}", tableName, curThread, curThread.getId());
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            tableNameTableMap.put(tableName, table);
        }
        return (DbTable) table;
    }


    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        return getTable(tableName);
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
        return getTable(tableName);
    }

    @Override
    public Configuration getConfiguration() {
        return (Configuration) atalantaConfiguration;
    }

    public Admin getAdmin() throws IOException {
        if (admin == null) {
            admin = new Admin(this);
        }
        return admin;
    }

    public MasterServiceGrpc.MasterServiceBlockingStub getMasterStubRuntime() {
        try {
            String url[] = getHostPort();
            if (!masterHost.equals(url[0]) || !masterPort.equals(url[1])) {
                masterGrpcChannel.shutdownNow();
                masterGrpcChannel = buildGrpcChannel();
                masterGrpcStub = buildMasterGrpcStub(conf);
            }
            return masterGrpcStub;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public MasterServiceGrpc.MasterServiceBlockingStub getMasterStub() throws IOException {
        String url[] = getHostPort();
        if (!masterHost.equals(url[0]) || !masterPort.equals(url[1])) {
            masterGrpcChannel.shutdownNow();
            masterGrpcChannel = buildGrpcChannel();
            masterGrpcStub = buildMasterGrpcStub(conf);
        }
        return masterGrpcStub;
    }

    protected ManagedChannel buildGrpcChannel() throws IOException {
        try {
            String[] url = getHostPort();
            LOGGER.debug("FMaster found in {}:{}", url[0], Integer.parseInt(url[1]));
            masterHost = url[0];
            masterPort = url[1];
            return NettyChannelBuilder.forAddress(masterHost, Integer.parseInt(masterPort))
                    .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, atalantaConfiguration.getInt(AtalantaProperties.ATALANTA_MASTER_CONNECTION_TIMEOUT_MS))
                    .usePlaintext()
                    .nameResolverFactory(new DnsNameResolverProvider())
                    .build();
        } catch (IOException ex) {
            LOGGER.error("Unable to connect to master", ex);
            throw ex;
        }
    }

    private String[] getHostPort() throws IOException {
        return zooHelper.getNotNullString(F_MASTER_NODE).split(":");
    }

    protected String getMasterAddress() {
        try {
            return zooHelper.getNotNullString(F_MASTER_NODE).split(":")[0];
        } catch (ZKException e) {
            LOGGER.warn("Cannot read master url from ZK");
            return StringUtils.EMPTY;
        }
    }

    protected Table constructNewTable(TableName tableName) throws Exception {
        int retries = atalantaConfiguration.getInt(AtalantaProperties.ATALANTA_CLIENT_GET_TABLE_RETRY);
        long timeout = atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_CLIENT_GET_TABLE_TIMEOUT_MS);
        return new RepeatableExecutor().withMaxRetries(retries).withStaticTimeout(timeout).execute(() -> {
            try {
                Map<RegionServerModel, List<RegionModel>> regionsMapping = getActualRegionsMapping(tableName);
                LOGGER.debug("Actual regions mapping for table {}:\n{}", tableName, regionsMapping);
                return buildTable(tableName, regionsMapping);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected Table buildTable(TableName tableName, Map<RegionServerModel, List<RegionModel>> regionsMapping)
            throws IOException, NotActualRegionMappingException {
        return new Table(this, tableName, regionsMapping);
    }

    private LazyTableImpl constructNewLazyTable(TableName tableName) throws Exception {
        int retries = atalantaConfiguration.getInt(AtalantaProperties.ATALANTA_CLIENT_GET_TABLE_RETRY);
        long timeout = atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_CLIENT_GET_TABLE_TIMEOUT_MS);
        return new RepeatableExecutor().withMaxRetries(retries).withStaticTimeout(timeout).execute(() -> {
            try {
                Map<RegionServerModel, List<RegionModel>> regionsMapping = getActualRegionsMapping(tableName);
                LOGGER.debug("Actual regions mapping for table {}:\n{}", tableName, regionsMapping);
                return new LazyTableImpl(this, tableName, regionsMapping);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public Map<RegionServerModel, List<RegionModel>> getActualRegionsMapping(TableName tableName) throws Exception {
        zooKeeperCoreContext.checkTableNotDisable(tableName);
        List<RegionDistributionModel> rdmList = zooKeeperCoreContext.getRegionDistributionWithStatuses(tableName, CREATED_REGION_STATUS, ASSIGNED_REGION_STATUS, ENABLED_REGION_STATUS);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Regions distribution for {}:\n{}", tableName, rdmList);
        }
        List<RegionModel> leaderRegions = rdmList
                .stream()
                .map(RegionDistributionModel::getRegionModel)
                .filter(regionModel -> regionModel.getReplicaNum() == 0)
                .collect(Collectors.toList());
        validateRegionRangeMap(tableName, leaderRegions);
        Map<RegionServerModel, List<RegionModel>> map =
                rdmList
                        .stream()
                        .filter(rdm -> rdm.getRegionModel().getReplicaNum() == 0)
                        .collect(Collectors.groupingBy(
                                RegionDistributionModel::getRegionServerModel,
                                Collectors.mapping(RegionDistributionModel::getRegionModel, Collectors.toList()))
                        );
        if (LOGGER.isDebugEnabled()) {
            LOGGER.info("Leader regions mapping for table {}:\n{}", tableName, map);
        }
        if (!isTableRegionsStable(tableName)) {
            throw new IllegalStateException("Table is not ready " + tableName);
        }
        return map;
    }

    private void validateRegionRangeMap(TableName tableName, List<RegionModel> regions) {
        List<RegionModel> sortedRegions = new ArrayList<>(regions);
        sortedRegions.sort((o1, o2) -> BytesComparator.compareTo(o1.getBeginKey(), o2.getBeginKey()));
        if (sortedRegions.isEmpty()) {
            throw new IllegalStateException("No leader regions found for table " + tableName);
        }

        for (int i = 1; i < sortedRegions.size(); i++) {
            RegionModel previous = sortedRegions.get(i - 1);
            RegionModel current = sortedRegions.get(i);
            if (BytesComparator.compareTo(previous.getEndKey(), current.getBeginKey()) != 0) {
                LOGGER.warn("Regions map validation failed for table {} between regions {} and {}. previousEndKey={}, currentStartKey={}",
                        tableName,
                        previous.toIdentityString(),
                        current.toIdentityString(),
                        Arrays.toString(previous.getEndKey()),
                        Arrays.toString(current.getBeginKey()));
                LOGGER.error("Regions map contains gap or overlap: table={}, mapSize={}, ranges={}",
                        tableName,
                        sortedRegions.size(),
                        sortedRegions.stream()
                                .map(r -> String.format("%s:[%s..%s)",
                                        r.toIdentityString(),
                                        Arrays.toString(r.getBeginKey()),
                                        Arrays.toString(r.getEndKey())))
                                .collect(Collectors.joining(", ")));
                throw new IllegalStateException("Regions map contains gap or overlap for table " + tableName);
            }
        }
    }

    private boolean isTableRegionsStable(TableName tableName) {
        try {
            List<RegionDistributionModel> rdmList = zooKeeperCoreContext.getRegionDistributionWithStatuses(tableName,
                    UNASSIGNING_REGION_STATUS,
                    ASSIGNING_REGION_STATUS,
                    ENABLING_REGION_STATUS,
                    SPLITTING_REGION_STATUS,
                    SPLITTED_REGION_STATUS);
            return rdmList.isEmpty();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
    }

    void waitTableStability(TableName tableName) {
        while (!isTableRegionsStable(tableName)) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    void waitWhileRegionsMoving(TableName tableName) {
        try {
            List<RegionDistributionModel> rdmList = zooKeeperCoreContext.getRegionDistributionWithStatuses(tableName, UNASSIGNING_REGION_STATUS, ASSIGNING_REGION_STATUS, SPLITTING_REGION_STATUS, SPLITTED_REGION_STATUS);
            while (!rdmList.isEmpty()) {
                Thread.sleep(50);
                rdmList = zooKeeperCoreContext.getRegionDistributionWithStatuses(tableName, UNASSIGNING_REGION_STATUS, ASSIGNING_REGION_STATUS);
                LOGGER.info("Waiting while regions are moving for table {}. Moving regions: {}", tableName, rdmList);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error(e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public ZookeeperHelper getZooHelper() {
        return zooHelper;
    }

    public ZooKeeperCoreContext getZooKeeperCoreContext() {
        return zooKeeperCoreContext;
    }

    @Override
    public void close() {
        closeAllTableThreadLocal();
        if (admin != null) {
            try {
                admin.close();
            } catch (Exception ex) {
                LOGGER.error("Unable to close Admin properly", ex);
            }
        }
        if (masterGrpcChannel != null) {
            try {
                masterGrpcChannel.shutdown();
                masterGrpcChannel.awaitTermination(atalantaConfiguration.getLong(AtalantaProperties.ATALANTA_MASTER_DISCONNECTION_TIMEOUT_MS), TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                LOGGER.error("Unable to close gRPC channel to master", ex);
            }
        }
        if (zooHelper != null) {
            try {
                zooHelper.close();
            } catch (Exception ex) {
                LOGGER.error("Unable to close ZooKeeper helper properly", ex);
            }
        }

        isClosed = true;
    }


    public void closeAllTableThreadLocal() {
        for (Table table : tables.get().values()) {
            try {
                table.close();
            } catch (Exception ex) {
                LOGGER.error("unable to close table properly", ex);
            }
        }
        for (LazyTableImpl table : lazyTables.get().values()) {
            try {
                table.close();
            } catch (Exception ex) {
                LOGGER.error("unable to close table properly", ex);
            }
        }
    }

    public void close(TableName tableName) {
        final Thread curThread = Thread.currentThread();
        LOGGER.debug("close table: {}, {}, threadId {}", tableName, curThread, curThread.getId());
        if (!tables.get().isEmpty() && tables.get().get(tableName) != null) {
            tables.get().remove(tableName);
        }
        if (!lazyTables.get().isEmpty() && lazyTables.get().get(tableName) != null) {
            lazyTables.get().remove(tableName);
        }
    }

    public void close(DbTable table) {
        close(table.getName());
    }

    // Visible for testing
    public Map<TableName, Table> getAllCachedTables() {
        return tables.get();
    }
}
