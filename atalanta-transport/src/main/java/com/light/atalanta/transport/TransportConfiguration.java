/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport;

import com.light.atalanta.DbConfiguration;
import com.light.atalanta.Pair;

import java.util.*;

public class TransportConfiguration {
    public static final String ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY = "atalanta.transport.socket.timeout";
    public static final String ATALANTA_TRANSPORT_SOCKET_RECEIVE_BUFFER_SIZE_PROPERTY = "atalanta.transport.socket.receive.buffer.size";
    public static final String ATALANTA_TRANSPORT_SOCKET_SEND_BUFFER_SIZE_PROPERTY = "atalanta.transport.socket.send.buffer.size";
    public static final String ATALANTA_TRANSPORT_SOCKET_TCP_NO_DELAY_PROPERTY = "atalanta.transport.socket.tcp.no.delay";
    public static final String ATALANTA_TRANSPORT_SOCKET_RETRIES_NUMBER_PROPERTY = "atalanta.transport.socket.retries.number";
    public static final String ATALANTA_TRANSPORT_SOCKET_RETRIES_TIMEOUT_PROPERTY = "atalanta.transport.socket.retries.timeout";
    public static final String ATALANTA_TRANSPORT_CORE_UNAVAILABLE_TIMEOUT_PROPERTY = "atalanta.transport.core.unavailable.timeout";
    public static final String ATALANTA_HBASE_SCAN_FILTER = "atalanta.hbase.scan.filter";
    public static final String ATALANTA_TRANSPORT_BULK_CHUNK_SIZE = "atalanta.transport.bulk.chunk.size";
    public static final String ATALANTA_TRANSPORT_TABLE_MAP_TIMEOUT_ENABLE = "atalanta.transport.enable.table.map.timeout";

    private static final List<Pair<String, Object>> ALL_PROPERTIES = Arrays.asList(
            new Pair<>(ATALANTA_TRANSPORT_SOCKET_TIMEOUT_PROPERTY, 120_000),
            new Pair<>(ATALANTA_TRANSPORT_SOCKET_RECEIVE_BUFFER_SIZE_PROPERTY, 0),
            new Pair<>(ATALANTA_TRANSPORT_SOCKET_SEND_BUFFER_SIZE_PROPERTY, 0),
            new Pair<>(ATALANTA_TRANSPORT_SOCKET_TCP_NO_DELAY_PROPERTY, false),
            new Pair<>(ATALANTA_TRANSPORT_SOCKET_RETRIES_NUMBER_PROPERTY, 5),
            new Pair<>(ATALANTA_TRANSPORT_SOCKET_RETRIES_TIMEOUT_PROPERTY, 1_000),
            new Pair<>(ATALANTA_HBASE_SCAN_FILTER, true),
            new Pair<>(ATALANTA_TRANSPORT_CORE_UNAVAILABLE_TIMEOUT_PROPERTY, 60_000),
            new Pair<>(ATALANTA_TRANSPORT_BULK_CHUNK_SIZE, 8192),
            new Pair<>(ATALANTA_TRANSPORT_TABLE_MAP_TIMEOUT_ENABLE, true)
            );

    private final Map<String, String> configuration;

    private TransportConfiguration(Map<String, String> configuration) {
        this.configuration = configuration;
    }

    public static TransportConfiguration defaultConfiguration() {
        Map<String, String> props = new HashMap<>();
        for (Pair<String, Object> property : ALL_PROPERTIES) {
            props.put(property.getFirst(), property.getSecond().toString());
        }
        return new TransportConfiguration(props);
    }

    public static TransportConfiguration fromConfiguration(DbConfiguration configuration) {
        Map<String, String> props = new HashMap<>();
        for (Pair<String, Object> property : ALL_PROPERTIES) {
            String val = configuration.getOrDefault(property.getFirst(), property.getSecond().toString());
            props.put(property.getFirst(), val);
        }
        return new TransportConfiguration(props);
    }

    public static TransportConfiguration fromConfiguration(Map<String, String> configuration) {
        Map<String, String> props = new HashMap<>();
        for (Pair<String, Object> property : ALL_PROPERTIES) {
            String val = configuration.getOrDefault(property.getFirst(), property.getSecond().toString());
            props.put(property.getFirst(), val);
        }
        return new TransportConfiguration(props);
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    public String get(String property) {
        return configuration.get(property);
    }

    public TransportConfiguration set(String propertyName, String propertyValue) {
        configuration.put(propertyName, propertyValue);
        return this;
    }
}
