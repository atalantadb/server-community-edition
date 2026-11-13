/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.metrics;

import com.light.atalanta.metrics.RegionMetrics;
import com.light.atalanta.region.server.services.ReplicaMapperProcessor;
import com.light.atalanta.region.server.utils.RsUtils;
import com.light.atalanta.transport.messages.PingResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.light.atalanta.metrics.RsJmxClientImpl.*;

public class JmxRsMetricsImpl implements JmxRsMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmxRsMetricsImpl.class);

    public static final String JMX_ATTR_REGION = "_region_";

    public static final String JMX_ATTR_NAMESPACE = "Namespace_";

    public static final String JMX_ATTR_TABLE = "_table_";
    public static final String DISK_PREFIX = DISKS + "_";


    private final Map<String, RegionMetrics> coreStatByRegion = new ConcurrentHashMap<>();

    private final ReplicaMapperProcessor replicaMapperProcessor;

    private final PrometheusService prometheusService;

    private final Map<String, Function<String, Object>> jmxAttrsFuncByName;

    private final Set<FileStore> storageList;

    private final String storageListStr;

    public JmxRsMetricsImpl(ReplicaMapperProcessor replicaMapperProcessor, PrometheusService prometheusService, List<String> disks) {
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.prometheusService = prometheusService;
        jmxAttrsFuncByName = initAttrsMap();
        storageList = RsUtils.getStorages(disks);
        storageListStr = storageList.stream()
                .map(FileStore::name)
                .collect(Collectors.joining(","));
    }

    private Map<String, Function<String, Object>> initAttrsMap() {
        HashMap<String, Function<String, Object>> res = new HashMap<>();
        res.put(SIZE, replicaMapperProcessor::getSize);
        res.put(DISK_USABLE_BYTES, this::getDiskUsableByte);
        res.put(DISK_TOTAL_BYTES, this::getDiskTotalByte);
        res.put(DISK_NAME, this::getDiskName);
        res.put(KEYS, this::getKeys);
        res.put(MEM_USAGE_BYTES, this::getMemUsageBytes);
        res.put(COUNT_GET, this::getCountGet);
        res.put(COUNT_PUT, this::getCountPut);
        res.put(COUNT_DEL, this::getCountDel);
        return res;
    }

    private Long getDiskUsableByte(String s) {
        File file = replicaMapperProcessor.getRegDir(s);
        try {
            return file == null ? 0L : Files.getFileStore(file.toPath()).getUsableSpace();
        } catch (IOException e) {
            LOGGER.error("cannot get file store for " + s, e);
            return 0L;
        }
    }

    private long getDiskTotalByte(String s) {
        File file = replicaMapperProcessor.getRegDir(s);
        try {
            return file == null ? 0 : Files.getFileStore(file.toPath()).getTotalSpace();
        } catch (IOException e) {
            LOGGER.error("cannot get file store for " + s, e);
            return 0;
        }
    }

    private String getDiskName(String s) {
        File file = replicaMapperProcessor.getRegDir(s);
        try {
            return file == null ? "unknown" : Files.getFileStore(file.toPath()).name();
        } catch (IOException e) {
            LOGGER.error("cannot get file store for " + s, e);
            return "unknown";
        }
    }

    @Override
    public Object getAttribute(String s) {
        if (REGION_COUNT.equals(s)) {
            return replicaMapperProcessor.getReplicasMapping().size();
        }
        if (s.startsWith(DISK_PREFIX) ) {
            return getDiskSize(s);
        }

        if (s == null) {
            return 0;
        }
        int idxBeg = s.indexOf(JMX_ATTR_REGION) + JMX_ATTR_REGION.length();
        int idxEnd = s.lastIndexOf("_");
        String regionDirName = s.substring(idxBeg, idxEnd);
        String rsAttr = s.substring(idxEnd + 1);
        Function<String, Object> f = jmxAttrsFuncByName.get(rsAttr);
        if (f != null) {
            return f.apply(regionDirName);
        } else {
            LOGGER.error("Unknown RS attr: {}, jmx attr: {}", rsAttr, s);
        }
        return 0;
    }

    @Override
    public void setAttribute(Attribute attribute) {

    }

    @Override
    public AttributeList getAttributes(String[] strings){
        AttributeList resultList = new AttributeList();
        for (String attrName : strings) {
            if (REGION_COUNT.equals(attrName)) {
                resultList.add(new Attribute(attrName, replicaMapperProcessor.getReplicasMapping().size()));
            } else if (DISKS.equals(attrName)) {
                resultList.add(new Attribute(attrName, storageListStr));
            } else if (attrName.startsWith(DISK_PREFIX)) {
                resultList.add(new Attribute(attrName, getDiskSize(attrName)));
            }
            else {
                resultList.add(new Attribute(attrName, replicaMapperProcessor.getSize(attrName)));
            }
        }
        return resultList;
    }

    private Long getDiskSize(String attrName) {
        try {
            if (attrName.startsWith(USABLE_DISK_PREFIX)) {
                String disk = attrName.substring(USABLE_DISK_PREFIX.length());
                for (FileStore fileStore : storageList) {
                    if (fileStore.name().equals(disk)) {
                        return fileStore.getUsableSpace();
                    }
                }
            } else if (attrName.startsWith(TOTAL_DISK_PREFIX)) {
                String disk = attrName.substring(TOTAL_DISK_PREFIX.length());
                for (FileStore fileStore : storageList) {
                    if (fileStore.name().equals(disk)) {
                        return fileStore.getTotalSpace();
                    }
                }
            }
        } catch (IOException ex) {
            LOGGER.error("cannot get size!", ex);
        }
        return 0L;
    }

    @Override
    public AttributeList setAttributes(AttributeList attributeList) {
        return null;
    }

    @Override
    public Object invoke(String s, Object[] objects, String[] strings) {
        return null;
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        return buildDynamicMBeanInfo();
    }

    private MBeanInfo buildDynamicMBeanInfo() {
        List<MBeanAttributeInfo> mBeanAttrs = replicaMapperProcessor.getReplicasMapping().entrySet().stream()
                .flatMap(r -> jmxAttrsFuncByName.keySet().stream().map(a -> toAttrName(r, a)))
                .collect(Collectors.toList());
        mBeanAttrs.add(new MBeanAttributeInfo(REGION_COUNT, Long.class.getSimpleName(),
                "Number of region", true, false, false));
        mBeanAttrs.add(new MBeanAttributeInfo(DISKS, String.class.getSimpleName(),
                "List of disk", true, false, false));
        storageList.stream().forEach( s-> {
            final String name = s.name();
            addSizeAttr(mBeanAttrs, name, "usable");
            addSizeAttr(mBeanAttrs, name, "total");
        });
        return new MBeanInfo(this.getClass().getName(), null,
                mBeanAttrs.toArray(new MBeanAttributeInfo[0]),
                null,
                null,
                new MBeanNotificationInfo[0]);
    }

    private void addSizeAttr(List<MBeanAttributeInfo> mBeanAttrs, String name, String sizeType) {
        String attrName = DISKS + "_" + sizeType + "_"
                + name;
        mBeanAttrs.add(new MBeanAttributeInfo(attrName, Long.class.getSimpleName(),
                "List of disk", true, false, false));
    }

    private MBeanAttributeInfo toAttrName(Map.Entry<ReplicaMapperProcessor.ReplicaDescription, String> entity, String attrName) {
        String[] names = entity.getValue().split("/");
        String name = JMX_ATTR_NAMESPACE + names[names.length - 3].replace("_", "@")
                + JMX_ATTR_TABLE + names[names.length - 2].replace("_", "@")
                + JMX_ATTR_REGION + entity.getKey()
                + "_" + attrName;
        return new MBeanAttributeInfo(name, Long.class.getSimpleName(),
                attrName, true, false, false);
    }

    @Override
    public void addCoreStat(Object pingResponse) {
        List<RegionMetrics> regionMetrics = ((PingResponse) pingResponse).getRegionMetrics();
        regionMetrics.forEach(rm -> coreStatByRegion.put(rm.getRegionName(), rm));
        prometheusService.addRegionMetrics(regionMetrics);
    }

    private long getKeys(String s) {
        RegionMetrics regionMetrics = coreStatByRegion.get(s);
        return regionMetrics == null ? 0 : regionMetrics.getKeys();
    }

    private long getMemUsageBytes(String s) {
        RegionMetrics regionMetrics = coreStatByRegion.get(s);
        return regionMetrics == null ? 0 : regionMetrics.getMemUsageBytes();
    }

    private long getCountGet(String s) {
        RegionMetrics regionMetrics = coreStatByRegion.get(s);
        return regionMetrics == null ? 0 : regionMetrics.getCountGet();
    }

    private long getCountPut(String s) {
        RegionMetrics regionMetrics = coreStatByRegion.get(s);
        return regionMetrics == null ? 0 : regionMetrics.getCountPut();
    }

    private long getCountDel(String s) {
        RegionMetrics regionMetrics = coreStatByRegion.get(s);
        return regionMetrics == null ? 0 : regionMetrics.getCountDel();
    }
}
