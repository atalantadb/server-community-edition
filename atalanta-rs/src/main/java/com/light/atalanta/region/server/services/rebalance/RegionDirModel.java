/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.rebalance;

import java.util.Objects;

public class RegionDirModel {

    private final String name;

    private final String ns;
    private final String table;
    private final String path;
    private final Long size;

    public RegionDirModel(String name, String ns, String table, String path, Long size) {
        this.size = size;
        this.path = path;
        this.name = name;
        this.table = table;
        this.ns = ns;
    }

    public Long getSize() {
        return size;
    }

    public String getPath() {
        return path;
    }

    public String getName() {
        return name;
    }

    public String getNs() {
        return ns;
    }

    public String getTable() {
        return table;
    }

    @Override
    public String toString() {
        return "RegionDirModel{" +
                "name='" + name + '\'' +
                ", ns='" + ns + '\'' +
                ", table='" + table + '\'' +
                ", path='" + path + '\'' +
                ", size=" + size +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegionDirModel that = (RegionDirModel) o;
        return Objects.equals(name, that.name)
                && Objects.equals(ns, that.ns)
                && Objects.equals(table, that.table)
                && Objects.equals(path, that.path)
                && Objects.equals(size, that.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, ns, table, path, size);
    }
}
