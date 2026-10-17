/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static com.light.atalanta.ATALANTAConstants.DEFAULT_NAMESPACE;

/**
 * Table name: namespace and table qualifier, validated on creation.
 */
public class TableName implements NameOfTable {
    //Allows only letters, digits and '_'
    private static final String VALID_NAMESPACE_REGEX =
            "(?:[_\\p{Digit}\\p{IsAlphabetic}]+)";

    private static final Pattern VALID_NAMESPACE_PATTERN = Pattern.compile(VALID_NAMESPACE_REGEX);

    //Allows only letters, digits, '_', '-' and '.'
    private static final String VALID_TABLE_QUALIFIER_REGEX =
            "(?:[_\\p{Digit}\\p{IsAlphabetic}][-_.\\p{Digit}\\p{IsAlphabetic}]*)";

    private static final Pattern VALID_TABLE_QUALIFIER_PATTERN = Pattern.compile(VALID_TABLE_QUALIFIER_REGEX);

    private final String table;

    private final String namespace;

    private static final Map<String, TableName> CACHE = new ConcurrentHashMap<>();

    public TableName() {
        table = null;
        namespace = null;
    }

    public TableName(String namespace, String table) {
        Objects.requireNonNull(table, "Table qualifier can't be null");
        if (namespace == null || namespace.equals(StringUtil.EMPTY_STRING)) {
            this.namespace = DEFAULT_NAMESPACE;
        } else {
            this.namespace = checkAndGetNamespace(namespace);
        }
        this.table = checkAndGetTable(table);
    }

    public static TableName valueOf(String namespace, String table) {
        final String tableName = namespace + SEPARATOR + table;
        return CACHE.computeIfAbsent(tableName, any -> new TableName(namespace, table));
    }

    public static TableName valueOf(String tableName) {
        String[] parts = tableName.split(SEPARATOR);
        if (parts.length > 2) {
            throw new RuntimeException("Table name string " + tableName + " is damaged");
        }

        return parts.length == 1 ? valueOf("default", tableName) : valueOf(parts[0], parts[1]);
    }

    public static TableName fromString(String tableName) {
        if (tableName.contains(SEPARATOR)) {
            String[] parts = tableName.split(SEPARATOR);
            Preconditions.checkArgument(parts.length == 2, "Table name string " + tableName + " is damaged");
            return CACHE.computeIfAbsent(tableName, any -> new TableName(parts[0], parts[1]));
        }

        return CACHE.computeIfAbsent(DEFAULT_NAMESPACE + SEPARATOR + tableName, any -> new TableName(DEFAULT_NAMESPACE, tableName));
    }

    public static TableName valueOf(TableName nt) {
        return valueOf(nt.getNamespace(), nt.getTableName());
    }

    public String getTableName() {
        return table;
    }

    public String getNamespace() {
        return namespace;
    }

    // Validates the namespace (no special characters, etc.).
    // Returns the namespace on success; throws IllegalArgumentException otherwise.
    private String checkAndGetNamespace(String namespace) throws IllegalArgumentException {
        Preconditions.checkArgument(VALID_NAMESPACE_PATTERN.matcher(namespace).matches(),
                "Namespace has invalid symbols. Allows only letters, digits and '_'");
        return namespace;
    }

    // Same as checkAndGetNamespace()
    private String checkAndGetTable(String table) throws IllegalArgumentException {
        Preconditions.checkArgument(VALID_TABLE_QUALIFIER_PATTERN.matcher(table).matches(),
                "Table has invalid symbols. Allows only letters, digits, '_', '-' and '.'");
        return table;
    }

    @Override
    public String toString() {
        return String.join(SEPARATOR, namespace, table);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableName tableName = (TableName) o;
        if (!table.equals(tableName.table)) {
            return false;
        }
        return namespace.equals(tableName.namespace);
    }

    @Override
    public int hashCode() {
        int result = table.hashCode();
        result = 31 * result + namespace.hashCode();
        result = result & 0x7FFFFFFF;
        return result;
    }
}
