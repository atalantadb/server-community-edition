/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.config.DefaultSubsystemConfiguration;
import com.light.atalanta.config.AtalantaProperties;
import com.light.atalanta.config.SubsystemConfiguration;
import com.light.atalanta.config.SubsystemKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Configuration implements DbConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

    private final Map<String, String> properties = new HashMap<>();
    private final Set<String> exposedKeys = new HashSet<>();
    private final Map<String, SubsystemConfiguration> subsystemMap = new HashMap<>();

    public Configuration() {
        initDefaults();
    }

    private void initDefaults() {
        properties.put(AtalantaProperties.ATALANTA_DISABLE_TABLE_TIMEOUT_MS, "600000");
        properties.put(AtalantaProperties.ATALANTA_DROP_TABLE_TIMEOUT_MS, "600000");
        properties.put(AtalantaProperties.ATALANTA_MOVE_REGION_TIMEOUT_MS, "600000");
        properties.put(AtalantaProperties.ATALANTA_SPLIT_REGION_TIMEOUT_MS, "60000");
        properties.put(AtalantaProperties.ATALANTA_REBALANCE_REGION_START_TIMEOUT_MS, "60000");
        properties.put(AtalantaProperties.ATALANTA_STOP_REBALANCE_TASKS_TIMEOUT_MS, "60000");
        properties.put(AtalantaProperties.ATALANTA_ZOOKEEPER_RETRY_COUNT, "1000");
        properties.put(AtalantaProperties.ATALANTA_ZOOKEEPER_RETRY_DELAY_MS, "3000");
        properties.put(AtalantaProperties.ATALANTA_ZOOKEEPER_SESSION_TIMEOUT_MS, "45000");
        properties.put(AtalantaProperties.ATALANTA_CONNECTION_POOL_SIZE, "0");
        properties.put(AtalantaProperties.ATALANTA_CLIENT_ENABLE_BULK_API, "true");
        properties.put(AtalantaProperties.ATALANTA_CONNECTION_POOL_HASH_BY_THREAD_ID, "true");
        properties.put(AtalantaProperties.ATALANTA_CONNECTION_POOL_QUEUE_SIZE, "1000");
        properties.put(AtalantaProperties.ATALANTA_CLIENT_NIO_DIRECT_DISPATCH, "true");
        properties.put(AtalantaProperties.ATALANTA_CLIENT_NIO_SELECTOR_LOOP_SIZE, "0");
        properties.put(AtalantaProperties.ATALANTA_CONNECTION_READ_POOL_SIZE, "8");
        properties.put(AtalantaProperties.ATALANTA_CLIENT_SCAN_ITERATION_TIMEOUT, "6000");
        properties.put(AtalantaProperties.ATALANTA_CONNECTION_RETRY_COUNT, "19");
        properties.put(AtalantaProperties.ATALANTA_CONNECTION_RETRY_DELAY_MS, "0");
        properties.put(AtalantaProperties.ATALANTA_CONNECTION_POOL_REUSABLE, "true");
        properties.put(AtalantaProperties.ATALANTA_CONNECTION_POOL_NAME, "cp");
        properties.put(AtalantaProperties.ATALANTA_CONNECTION_POOL_DISCONNECT_TIMEOUT_MS, "60000");
        properties.put(AtalantaProperties.ATALANTA_STOP_REGION_TIMEOUT_MS, "600000");
        properties.put(AtalantaProperties.ATALANTA_SHUTDOWN_RS_TIMEOUT_MS, "630000");
        properties.put(AtalantaProperties.ATALANTA_START_REGION_TIMEOUT_MS, "12000");
        properties.put(AtalantaProperties.ATALANTA_CLIENT_GET_TABLE_RETRY, "30");
        properties.put(AtalantaProperties.ATALANTA_CLIENT_GET_TABLE_TIMEOUT_MS, "10000");
        properties.put(AtalantaProperties.ATALANTA_HBASE_SCAN_FILTER, "true");
        properties.put(AtalantaProperties.ATALANTA_CLIENT_MIN_BATCH_SIZE, "100");
        properties.put(AtalantaProperties.ATALANTA_CLIENT_AUTO_FLUSH_SIZE, "500");

    }

    public static Configuration create(Map<String, String> map) {
        Configuration configuration = new Configuration();
        configuration.properties.putAll(map);
        configuration.loadSubsystem();
        return configuration;
    }

    public static Configuration create(File... resources) {
        Configuration configuration = new Configuration();
        for (File f : resources) {
            try {
                configuration.addResource(f);
            } catch (Exception e) {
                LOGGER.error("Configuration {} can't be loaded", f);
            }
        }
        configuration.loadSubsystem();
        return configuration;
    }

    public static Configuration create(String... resources) {
        Configuration configuration = new Configuration();
        for (String str : resources) {
            try {
                configuration.addResource(str);
            } catch (Exception e) {
                LOGGER.error("Configuration {} can't be loaded", str);
            }
        }
        configuration.loadSubsystem();
        return configuration;
    }

    public static Configuration create() {
        Configuration configuration = new Configuration();
        configuration.loadSubsystem();
        return configuration;
    }

    public static Configuration copyConfiguration(DbConfiguration configuration) {
        Configuration copy = new Configuration();
        configuration.iterator().forEachRemaining(c -> copy.set(c.getKey(), c.getValue()));
        copy.checkConfiguration();
        return copy;
    }

    @Override
    public String get(String key) {
        return properties.get(key);
    }

    public Integer getInt(String key) {
        return Integer.valueOf(properties.get(key));
    }

    public Long getLong(String key) {
        return Long.valueOf(properties.get(key));
    }

    public Boolean getBoolean(String key) {
        return Boolean.valueOf(properties.get(key));
    }


    @Override
    public void set(String key, String value) {
        properties.put(key, value);
        if (SubsystemKey.isSubsystemKey(key)) {
            loadSubsystem();
        }
    }

    public void set(String key, Object value) {
        properties.put(key, Objects.toString(value));
        if (SubsystemKey.isSubsystemKey(key)) {
            loadSubsystem();
        }
    }

    @Override
    public void addResource(File resource) throws IOException {
        Exception e = parseXml(resource);
        if (e != null) {
            e = parseProperties(resource);
        }
        if (e != null) {
            throw new RuntimeException("Cannot load cfg: " + resource, e);
        }
        checkConfiguration();
        loadSubsystem();
    }

    @Override
    public void addResource(String resource) throws IOException {
        addResource(new File(resource));
    }

    private Exception parseProperties(File resource) {
        try {
            Properties p = new Properties();
            p.load(new FileReader(resource));
            p.forEach((k, v) -> properties.put(k.toString(), v.toString()));
        } catch (IOException e) {
            return e;
        }
        return null;
    }

    private Exception parseXml(File resource) {
        DocumentBuilder db;
        try {
            db = documentBuilder();
            Document doc = db.parse(resource);
            parse(doc);
        } catch (ParserConfigurationException | IOException | SAXException e) {
            return e;
        }
        return null;
    }

    private DocumentBuilder documentBuilder() throws ParserConfigurationException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        return dbf.newDocumentBuilder();
    }

    private void parse(Document doc) {
        doc.getDocumentElement().normalize();
        NodeList list = doc.getElementsByTagName("property");
        for (int temp = 0; temp < list.getLength(); temp++) {
            Node node = list.item(temp);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Element element = (Element) node;
                String name = element.getElementsByTagName("name").item(0).getTextContent();
                String value = element.getElementsByTagName("value").item(0).getTextContent();
                properties.put(name, value);
            }
        }
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return properties.entrySet().iterator();
    }

    // TODO: Reduce visibility
    public void checkConfiguration() {
        checkProperty(AtalantaProperties.ATALANTA_ZOOKEEPER_CONNECTION_STRING);
        checkProperty(AtalantaProperties.ATALANTA_ZOOKEEPER_PATH);
        checkProperty(AtalantaProperties.ATALANTA_ZOOKEEPER_RETRY_COUNT);
        checkProperty(AtalantaProperties.ATALANTA_ZOOKEEPER_RETRY_DELAY_MS);
        checkProperty(AtalantaProperties.ATALANTA_MASTER_CONNECTION_TIMEOUT_MS);
        checkProperty(AtalantaProperties.ATALANTA_MASTER_DISCONNECTION_TIMEOUT_MS);
        checkProperty(AtalantaProperties.ATALANTA_CREATE_NAMESPACE_TIMEOUT_MS);
        checkProperty(AtalantaProperties.ATALANTA_CREATE_TABLE_TIMEOUT_MS);
        checkProperty(AtalantaProperties.ATALANTA_ENABLE_TIMEOUT_MS);
        checkProperty(AtalantaProperties.ATALANTA_DISABLE_TABLE_TIMEOUT_MS);
        checkProperty(AtalantaProperties.ATALANTA_DROP_TABLE_TIMEOUT_MS);
        checkProperty(AtalantaProperties.ATALANTA_MOVE_REGION_TIMEOUT_MS);
        checkProperty(AtalantaProperties.ATALANTA_SPLIT_REGION_TIMEOUT_MS);
        checkProperty(AtalantaProperties.ATALANTA_REBALANCE_REGION_START_TIMEOUT_MS);
        checkProperty(AtalantaProperties.ATALANTA_STOP_REBALANCE_TASKS_TIMEOUT_MS);

        LOGGER.debug("ATALANTA configuration was filled completely");
    }

    private void checkProperty(String propertyName) {
        Objects.requireNonNull(properties.get(propertyName), "conf key '" + propertyName + "' must not be null");
    }

    @Override
    public String toString() {
        return "Client config: " + System.lineSeparator() + logWithoutPassowrd();
    }

    private String logWithoutPassowrd() {
        return properties.entrySet().stream().map( e -> {
            if ( e.getKey().toLowerCase().contains("pass") ) {
                return e.getKey() +  ": ***";
            } else {
                return e.getKey() + ": " + e.getValue();
            }
        }).collect(Collectors.joining(System.lineSeparator()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Configuration)) {
            return false;
        }
        Configuration entries = (Configuration) o;
        return properties.equals(entries.properties);
    }

    @Override
    public int hashCode() {
        return properties.hashCode();
    }

    protected void loadSubsystem() {
        properties.forEach((k, v) -> {
            if (SubsystemKey.isSubsystemKey(k) && !exposedKeys.contains(k)) {
                SubsystemKey subsystemKey = new SubsystemKey(k);
                if (subsystemMap.get(subsystemKey.getSubsystemName()) == null) {
                    subsystemMap.put(subsystemKey.getSubsystemName(), new DefaultSubsystemConfiguration(subsystemKey.getPath()));
                }
                subsystemMap.get(subsystemKey.getSubsystemName()).putOption(subsystemKey.getSubsystemKey(), v);
            }
            exposedKeys.add(k);
        });
    }

    @Override
    public Map<String, Object> getSubsystemOptions(String subsystemName) {
        SubsystemConfiguration configuration = subsystemMap.get(subsystemName);
        return configuration != null ? configuration.getOptions() : Collections.emptyMap();
    }
}
