/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.metrics;

import com.light.atalanta.exceptions.FRegionServerJmxConnectionException;
import com.light.atalanta.model.RegionServerModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.light.atalanta.Preconditions;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RsJmxClientImpl implements RsJmxClient {

    public static final String REGION_COUNT = "regionCount";
    public static final String MEM_USAGE_BYTES = "memUsageBytes";
    public static final String COUNT_GET = "countGet";
    public static final String COUNT_PUT = "countPut";
    public static final String COUNT_DEL = "countDel";
    public static final String KEYS = "keys";
    public static final String DISK_NAME = "diskName";
    public static final String DISK_TOTAL_BYTES = "diskTotalBytes";
    public static final String DISK_USABLE_BYTES = "diskUsageBytes";
    public static final String SIZE = "size";
    public static final String DISKS = "disks";
    public static final String USABLE_DISK_PREFIX = DISKS + "_" + "usable" + "_";
    public static final String TOTAL_DISK_PREFIX = DISKS + "_" + "total" + "_";

    private Map<RegionServerModel, RegionServerMetrics> metricsByRegionServer = new ConcurrentHashMap<>();

    private Map<RegionServerModel, Long> metricsByRegionServerUpdateTime = new ConcurrentHashMap<>();
    private Map<RegionServerModel, AtomicBoolean> metricsByRegionServerUpdateStartFlag = new ConcurrentHashMap<>();
    private final ExecutorService jmxExecutor;
    private Map<RegionServerModel, CompletableFuture> jmxRequests = new ConcurrentHashMap<>();
    private final Map<String, Object> jmxEnv = new HashMap<>();

    public RsJmxClientImpl() {
        jmxExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("jmx-updater-%d").build());
    }

    @Override
    public RegionMetrics getRsJmxMetrics(RegionServerModel rs, String regionName, Integer replicaId) {
        refresh(rs);
        return metricsByRegionServer.getOrDefault(rs, new RegionServerMetrics())
                .getRegionsMetrics()
                .getOrDefault(regionName, new ConcurrentHashMap<>())
                .getOrDefault(replicaId, new RegionMetrics());
    }

    @Override
    public RegionServerMetrics getRsJmxMetrics(RegionServerModel rs) {
        refresh(rs);
        if (metricsByRegionServer.get(rs) == null) {
            waitJmxRequest(rs);
        }
        return metricsByRegionServer.getOrDefault(rs, new RegionServerMetrics());
    }

    @Override
    public RegionMetrics getRsJmxMetricsSync(RegionServerModel rs, String regionName, Integer replicaId) {
        try {
            refresh(rs).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return metricsByRegionServer.getOrDefault(rs, new RegionServerMetrics())
                .getRegionsMetrics()
                .getOrDefault(regionName, new ConcurrentHashMap<>())
                .getOrDefault(replicaId, new RegionMetrics());
    }

    @Override
    public void clearMetrics() {
        metricsByRegionServer.clear();
    }

    @Override
    public CompletableFuture<Object> refresh(RegionServerModel rs) {
        Long updateTime = metricsByRegionServerUpdateTime.getOrDefault(rs, 0L);
        if (System.currentTimeMillis() - updateTime < METRICS_TTL_MS) {
            return  CompletableFuture.completedFuture(null);
        }
        AtomicBoolean isStarted = metricsByRegionServerUpdateStartFlag.getOrDefault(rs, new AtomicBoolean());
        if (!isStarted.get()) {
            isStarted.set(true);
            metricsByRegionServerUpdateStartFlag.put(rs, isStarted);
            jmxRequests.put(rs, CompletableFuture.supplyAsync(() -> {
                refreshFromJmx(rs);
                return null;
            }, jmxExecutor));
            return CompletableFuture.supplyAsync(() -> {
                waitJmxRequest(rs);
                return null;
            }, jmxExecutor);
        }
        return CompletableFuture.completedFuture(null);
    }

    public synchronized void waitJmxRequest(RegionServerModel rs) {
        if (jmxRequests.get(rs) != null) {
            try {
                jmxRequests.get(rs).get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void refreshFromJmx(RegionServerModel rs) {
        Long updateTime = metricsByRegionServerUpdateTime.getOrDefault(rs, 0L);
        if (System.currentTimeMillis() - updateTime < METRICS_TTL_MS) {
            return;
        }
        try {
            String url = getJxmConnectionUri(rs);
            final JMXServiceURL serviceUrl;
            final JMXConnector jmxConnector;
            try {
                serviceUrl = new JMXServiceURL(url);
                jmxConnector = JMXConnectorFactory.connect(serviceUrl, jmxEnv);
            } catch (MalformedURLException e) {
                throw new FRegionServerJmxConnectionException(e);
            }
            try {
                MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();
                ObjectName objectName = new ObjectName("com.light.atalanta:metrics=RegionServer");
                MBeanInfo mbeanInfo = mbeanConn.getMBeanInfo(objectName);
                metricsByRegionServerUpdateTime.put(rs, System.currentTimeMillis());
                metricsByRegionServer.put(rs, buildRegionServerMetrics(mbeanConn, objectName, mbeanInfo.getAttributes()));
            } catch (MalformedObjectNameException e) {
                throw new FRegionServerJmxConnectionException(e);
            } catch (ReflectionException e) {
                throw new FRegionServerJmxConnectionException(e);
            } catch (InstanceNotFoundException e) {
                throw new FRegionServerJmxConnectionException(e);
            } catch (IntrospectionException e) {
                throw new FRegionServerJmxConnectionException(e);
            } finally {
                jmxConnector.close();
            }
        } catch (IOException e) {
            throw new FRegionServerJmxConnectionException(e);
        } finally {
            metricsByRegionServerUpdateStartFlag.get(rs).set(false);
        }
    }

    private RegionServerMetrics buildRegionServerMetrics(MBeanServerConnection mbeanConn, ObjectName objectName, MBeanAttributeInfo[] attributes) {
        try {
            RegionServerMetrics res = new RegionServerMetrics();
            for (MBeanAttributeInfo attr : attributes) {
                if (attr.getName().startsWith("Namespace_")) {
                    buildRegionsMetrics(mbeanConn, objectName, res, attr);
                } else if (attr.getName().startsWith(USABLE_DISK_PREFIX)) {
                    String diskName = attr.getName().substring(USABLE_DISK_PREFIX.length());
                    RegionServerDiskInfo diskInfo = res.getRsDisks().getOrDefault(diskName, new RegionServerDiskInfo());
                    diskInfo.setUsable((Long) mbeanConn.getAttribute(objectName, attr.getName()));
                    res.getRsDisks().put(diskName, diskInfo);
                } else if (attr.getName().startsWith(TOTAL_DISK_PREFIX)) {
                    String diskName = attr.getName().substring(TOTAL_DISK_PREFIX.length());
                    RegionServerDiskInfo diskInfo = res.getRsDisks().getOrDefault(diskName, new RegionServerDiskInfo());
                    diskInfo.setTotalSize((Long) mbeanConn.getAttribute(objectName, attr.getName()));
                    res.getRsDisks().put(diskName, diskInfo);
                }
            }
            Map<String, Map<Integer, RegionMetrics>> rsJmxMetrics = res.getRegionsMetrics();
            res.setRegionCount(rsJmxMetrics.size());
            res.setRegionCountFollower1( rsJmxMetrics.values().stream()
                    .flatMap(m -> m.keySet().stream())
                    .filter(i -> i == 1)
                    .count());
            res.setRegionCountFollower2( rsJmxMetrics.values().stream()
                    .flatMap(m -> m.keySet().stream())
                    .filter(i -> i == 2)
                    .count());
            rsJmxMetrics.values().forEach(mrm -> mrm.values().forEach(rm -> {
                res.setKeysTotal(res.getKeysTotal() + rm.getKeys());
                res.setSizeTotal(res.getSizeTotal() + rm.getSize());
                res.setMemUsageBytesTotal(res.getMemUsageBytesTotal() + rm.getMemUsageBytes());
                res.setCountGetTotal(res.getCountGetTotal() + rm.getCountGet());
                res.setCountPutTotal(res.getCountPutTotal() + rm.getCountPut());
                res.setCountDelTotal(res.getCountDelTotal() + rm.getCountDel());
            }));
            return res;
        } catch (Exception e) {
            throw new FRegionServerJmxConnectionException(e);
        }
    }

    private static void buildRegionsMetrics(MBeanServerConnection mbeanConn, ObjectName objectName, RegionServerMetrics res, MBeanAttributeInfo attr) throws ReflectionException, AttributeNotFoundException, InstanceNotFoundException, MBeanException, IOException {
        final int nsIdx = 1;
        final int tableIdx = 3;
        final int regIdx = 5;
        final int replicaIdx = 6;
        final int paramIdx = 7;
        Map<String, Map<Integer, RegionMetrics>> regionsMetrics = res.getRegionsMetrics();
        String[] attrSplit = attr.getName().split("_");
        Preconditions.checkArgument(attrSplit.length == 8,
                "Wrong jmx metrics format: " + attr.getName());
        Long valueLong = 0L;
        String valueString = "";
        Object attrValue = mbeanConn.getAttribute(objectName, attr.getName());
        if (attrValue instanceof Long) {
            valueLong = (Long) attrValue;
        } else {
            valueString = (String) attrValue;
        }
        Map<Integer, RegionMetrics> regMetricsByReplics = regionsMetrics.getOrDefault(attrSplit[regIdx], new ConcurrentHashMap<>());
        regionsMetrics.put(attrSplit[regIdx], regMetricsByReplics);
        int replica = Integer.parseInt(attrSplit[replicaIdx]);
        RegionMetrics rm = regMetricsByReplics.getOrDefault(replica, new RegionMetrics());
        regMetricsByReplics.put(replica, rm);
        rm.setRegionName(attrSplit[regIdx] + "_" + attrSplit[replicaIdx]);
        rm.setNs(attrSplit[nsIdx].replace("@", "_"));
        rm.setTable(attrSplit[tableIdx].replace("@", "_"));
        if (attrSplit[paramIdx].equals(COUNT_PUT)) {
            rm.setCountPut(Math.max(valueLong, 0));
        } else if (attrSplit[paramIdx].equals(COUNT_GET)) {
            rm.setCountGet(Math.max(valueLong, 0));
        } else if (attrSplit[paramIdx].equals(COUNT_DEL)) {
            rm.setCountDel(Math.max(valueLong, 0));
        } else if (attrSplit[paramIdx].equals(SIZE)) {
            rm.setSize(Math.max(valueLong, 0));
        } else if (attrSplit[paramIdx].equals(KEYS)) {
            rm.setKeys(Math.max(valueLong, 0));
        } else if (attrSplit[paramIdx].equals(MEM_USAGE_BYTES)) {
            rm.setMemUsageBytes(Math.max(valueLong, 0));
        } else if (attrSplit[paramIdx].equals(DISK_USABLE_BYTES)) {
            rm.setUsageDiskSpace(Math.max(valueLong, 0));
        } else if (attrSplit[paramIdx].equals(DISK_TOTAL_BYTES)) {
            rm.setTotalDiskSpace(Math.max(valueLong, 0));
        } else if (attrSplit[paramIdx].equals(DISK_NAME)) {
            rm.setDiskName(valueString);
        }
    }

    private String getJxmConnectionUri(RegionServerModel rs) {
        String url = "service:jmx:rmi:///jndi/rmi://" + rs.getHost() + ":" + rs.getJmxPort() + "/jmxrmi";
        return url;
    }
}
