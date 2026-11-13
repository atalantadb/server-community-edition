/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.services;

import com.light.atalanta.DbConfiguration;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.util.Bytes;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.*;

public class ZookeeperHelperImpl implements ZookeeperHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperHelperImpl.class);

    private static final String SUBSYSTEM_NAME = "zookeeper";
    private final CuratorFramework curator;
    private final DbConfiguration configuration;
    private final Map<String, List<ZookeeperCacheInvalidationListener>> listeners = new HashMap<>();

    public ZookeeperHelperImpl(ZkConfig zkConfig, String basePath, int connRetryTimes, int connRetrySleepMs, int sessionTimeoutMs) throws ZKException {
        this(null, zkConfig, basePath, connRetryTimes, connRetrySleepMs, sessionTimeoutMs);
    }

    public ZookeeperHelperImpl(DbConfiguration configuration, ZkConfig zkConfig, String basePath, int connRetryTimes, int connRetrySleepMs, int sessionTimeoutMs) throws ZKException {
        this.configuration = configuration;
        CuratorFramework initializedCurator = null;
        try {
            initializedCurator = buildCuratorFramework(zkConfig, basePath, connRetryTimes, connRetrySleepMs, sessionTimeoutMs);
            if (initializedCurator.checkExists().forPath(ZK_SEPARATOR) == null) {
                LOGGER.info("Creating ZooKeeper path: {}", basePath);
                // The main curator is rooted at basePath, so the initial creation needs a curator rooted at the ZooKeeper root
                try (CuratorFramework pureCurator = buildCuratorFramework(zkConfig, ZK_SEPARATOR, connRetryTimes, connRetrySleepMs, sessionTimeoutMs)) {
                    createPathIfAbsent(pureCurator, basePath);
                }
            }
            if (initializedCurator.checkExists().forPath(ZK_SEPARATOR + ATALANTA_METADATA) == null) {
                createPathIfAbsent(initializedCurator, ZK_SEPARATOR + ATALANTA_METADATA);
            }
            this.curator = initializedCurator;
        } catch (InterruptedException ex) {
            closeAfterConstructionFailure(initializedCurator, ex);
            Thread.currentThread().interrupt();
            throw new ZKException(ex);
        } catch (Exception ex) {
            closeAfterConstructionFailure(initializedCurator, ex);
            throw new ZKException(ex);
        } catch (Error error) {
            closeAfterConstructionFailure(initializedCurator, error);
            throw error;
        }
    }

    private static void createPathIfAbsent(CuratorFramework curator, String path) throws Exception {
        try {
            curator.create().creatingParentsIfNeeded().forPath(path);
        } catch (KeeperException.NodeExistsException alreadyExists) {
            // Another process can initialize the shared path after our existence check.
            LOGGER.debug("ZooKeeper path already initialized by another process: {}", path);
        }
    }

    private static void closeAfterConstructionFailure(CuratorFramework curator, Throwable failure) {
        if (curator == null) {
            return;
        }
        // Allow cleanup to wait even if construction was interrupted
        boolean interrupted = Thread.interrupted();
        try {
            curator.close();
        } catch (Throwable cleanupFailure) {
            if (cleanupFailure != failure) {
                failure.addSuppressed(cleanupFailure);
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public String getNotNullString(String path) throws ZKException {
        try {
            byte[] data = curator.getData().forPath(ZK_SEPARATOR + path);
            return (data == null || data.length == 0) ? StringUtil.EMPTY_STRING : Bytes.toString(data);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ZKException(ex);
        } catch (Exception ex) {
            throw new ZKException(ex);
        }
    }

    public boolean setData(String path, String str) throws ZKException {
        setData(path, Bytes.toBytes(str));
        return true;
    }

    // Transactional counterpart of setData()
    public CuratorOp setDataTrans(String path, String str) throws ZKException {
        return setDataTrans(path, Bytes.toBytes(str));
    }

    public Optional<String> get(String path) throws ZKException {
        try {
            return Optional.ofNullable(curator.getData().forPath(ZK_SEPARATOR + path))
                    .map(Bytes::toString);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ZKException(ex);
        } catch (Exception ex) {
            throw new ZKException(ex);
        }
    }

    public void setData(String path, byte[] payload) throws ZKException {
        try {
            curator.setData().forPath(ZK_SEPARATOR + path, payload);
            cacheUpdate(Collections.singletonList(path));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ZKException(ex);
        } catch (Exception ex) {
            throw new ZKException(ex);
        }
    }

    @Override
    public void setDataNoUpdateCache(String path, byte[] payload) throws ZKException {
        try {
            curator.setData().forPath(ZK_SEPARATOR + path, payload);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ZKException(ex);
        } catch (Exception ex) {
            throw new ZKException(ex);
        }
    }

    @Override
    public void updateCache(String path) {
        cacheUpdate(Collections.singletonList(path));
    }

    public CuratorOp setDataTrans(String path, byte[] payload) throws ZKException {
        try {
            return curator.transactionOp().setData().forPath(ZK_SEPARATOR + path, payload);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ZKException(ex);
        } catch (Exception ex) {
            throw new ZKException(ex);
        }
    }

    public List<String> watchedGetChildren() throws ZKException {
        return watchedGetChildren(StringUtils.EMPTY);
    }

    public List<String> watchedGetChildren(String path) throws ZKException {
        try {
            return curator.getChildren().watched().forPath(ZK_SEPARATOR + path);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ZKException(ex);
        } catch (Exception ex) {
            throw new ZKException(ex);
        }
    }

    public List<String> getChildren(String path) throws ZKException {
        try {
            return curator.getChildren().forPath(ZK_SEPARATOR + path);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ZKException(ex);
        } catch (Exception ex) {
            throw new ZKException(ex);
        }
    }

    public void create(String path, String str) throws ZKException {
        create(path, Bytes.toBytes(str));
    }

    @Override
    public void createIfAbsent(String path, String str) throws ZKException {
        try {
            create(path, str);
        } catch (ZKException ex) {
            if (!(ex.getCause() instanceof KeeperException.NodeExistsException)) {
                throw ex;
            }
            // Keep strict create() semantics for tables, snapshots and other unique objects.
            LOGGER.debug("ZooKeeper path already exists: {}", path);
        }
    }

    // Transactional counterpart of create()
    public CuratorOp createTrans(String path, String str) throws ZKException {
        return createTrans(path, Bytes.toBytes(str));
    }

    // Transactional counterpart of create()
    public CuratorOp createTrans(String path) throws ZKException {
        try {
            return curator.transactionOp().create().forPath(ZK_SEPARATOR + path);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ZKException(ex);
        } catch (Exception ex) {
            throw new ZKException(ex);
        }
    }

    public void create(String path, byte[] payload) throws ZKException {
        try {
            curator.create().creatingParentsIfNeeded().forPath(ZK_SEPARATOR + path, payload);
            cacheUpdate(Collections.singletonList(path));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ZKException(ex);
        } catch (Exception ex) {
            throw new ZKException(ex);
        }
    }

    public void createEphemeral(String path, String str) throws ZKException {
        try {
            curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(ZK_SEPARATOR + path, Bytes.toBytes(str));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ZKException(ex);
        } catch (Exception ex) {
            throw new ZKException(ex);
        }
    }

    // Transactional counterpart of create()
    public CuratorOp createTrans(String path, byte[] payload) throws ZKException {
        try {
            return curator.transactionOp().create().forPath(ZK_SEPARATOR + path, payload);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ZKException(ex);
        } catch (Exception ex) {
            throw new ZKException(ex);
        }
    }

    public boolean delete(String path) throws ZKException {
        try {
            curator.delete().deletingChildrenIfNeeded().forPath(ZK_SEPARATOR + path);
            cacheUpdate(Collections.singletonList(path));
            return true;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ZKException(ex);
        } catch (Exception ex) {
            throw new ZKException(ex);
        }
    }

    public CuratorOp deleteTrans(String path) throws ZKException {
        try {
            return curator.transactionOp().delete().forPath(ZK_SEPARATOR + path);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ZKException(ex);
        } catch (Exception ex) {
            throw new ZKException(ex);
        }
    }

    public boolean checkExists(String path) throws ZKException {
        try {
            return curator.checkExists().forPath(ZK_SEPARATOR + path) != null;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ZKException(ex);
        } catch (Exception ex) {
            throw new ZKException(ex);
        }
    }

    @Override
    public void addZookeeperCacheInvalidationListener(String pathStartWith, ZookeeperCacheInvalidationListener listener) {
        List<ZookeeperCacheInvalidationListener> listenerListByPath =
                listeners.computeIfAbsent(pathStartWith, s -> new ArrayList<>());
        listenerListByPath.add(listener);
    }

    @Override
    public CuratorFramework getCurator() {
        return curator;
    }

    @Override
    public List<CuratorTransactionResult> forOperations(List<CuratorOp> ops) throws Exception {
        List<CuratorTransactionResult> res = curator.transaction().forOperations(ops);
        cacheUpdate(ops.stream().map(op -> op.get().getPath()).collect(Collectors.toList()));
        return res;
    }


    public List<CuratorTransactionResult> forOperationsNoCache(List<CuratorOp> ops) throws Exception {
        List<CuratorTransactionResult> resList = curator.transaction().forOperations(ops);
        for(CuratorTransactionResult res : resList) {
            if (res.getError() > 0) {
                throw new RuntimeException("Error for zk operation: error code: " + res.getError() + " path: " + res.getForPath());
            }
        }
        return resList;
    }

    private CuratorFramework buildCuratorFramework(ZkConfig zkConfig,
                                                   String basePath,
                                                   int connRetryTimes,
                                                   int connRetrySleepMs,
                                                   int sessionTimeoutMs) {
        LOGGER.debug("Start creating ZooKeeper for connStr={}", zkConfig.getConnString() + basePath);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(zkConfig.getConnString() + basePath)
                .retryPolicy(new RetryNTimes(connRetryTimes, connRetrySleepMs))
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(sessionTimeoutMs)
                .zookeeperFactory((s, i, watcher, b) -> {
                    ZKClientConfig zkClientConfig = new ZKClientConfig();
                    if (configuration != null && !configuration.getSubsystemOptions(SUBSYSTEM_NAME).isEmpty()) {
                        Map<String, Object> options = configuration.getSubsystemOptions(SUBSYSTEM_NAME);
                        options.forEach((k, v) -> zkClientConfig.setProperty(k, v.toString()));
                    }
                    zkClientConfig.setProperty("zookeeper.sasl.client", "false");
                    zkClientConfig.setProperty("zookeeper.client.secure", "false");
                    return new ZooKeeper(s, i, watcher, zkClientConfig);
                })
                .build();
        try {
            curatorFramework.start();
            LOGGER.debug("Done creating ZooKeeper for connStr={}", zkConfig.getConnString() + basePath);
            return curatorFramework;
        } catch (RuntimeException | Error failure) {
            closeAfterConstructionFailure(curatorFramework, failure);
            throw failure;
        }
    }

    @Override
    public void close() {
        try {
            LOGGER.debug("Start closing ZooKeeper for connStr={}", curator.getZookeeperClient().getCurrentConnectionString());
            curator.close();
            LOGGER.debug("Done closing ZooKeeper for connStr={}", curator.getZookeeperClient().getCurrentConnectionString());
        } catch (Exception ex) {
            LOGGER.error("Unable to close ZooKeeper properly", ex);
        }
    }

    private void cacheUpdate(List<String> pathList) {
        List<String> normPathList = pathList.stream()
                .map(ZookeeperHelperImpl::normPath)
                .filter(path -> !(path.startsWith(ZK_SEPARATOR + ATALANTA_METADATA)
                        || path.startsWith(ZK_SEPARATOR + SNAPSHOT_DIR)
                        || path.equals(ZK_SEPARATOR + F_MASTER_NODE)
                        || path.equals(ZK_SEPARATOR + REGION_SERVER_INFO_SUBPATH)
                        || path.equals(ZK_SEPARATOR + REGION_SERVER_EPHEMERAL_SUBPATH))).collect(Collectors.toList());

        listeners.forEach((k, v) -> {
            List<String> listPath = normPathList.stream()
                    .filter(normPath -> normPath.startsWith(k))
                    .collect(Collectors.toList());
            v.forEach(l -> l.updateCache(listPath));
        });
    }

    private static String normPath(String path) {
        if (path.startsWith(ZK_SEPARATOR)) {
            return normPath(path.substring(1));
        }
        return ZK_SEPARATOR + path;
    }

}
