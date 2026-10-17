/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.model;

import com.light.atalanta.RegionServer;
import com.light.atalanta.Preconditions;
import com.light.atalanta.metrics.RegionServerMetrics;

import static com.light.atalanta.ATALANTAConstants.HOSTNAME_SEPARATOR;

/** Location of the region server API. */
public class RegionServerModel implements RegionServer {

    private final String name;

    private final String host;

    private final int javaPort;

    private final int coreClientPort;

    private final int jmxPort;

    private final String locationString;

    private final String toString;

    private RegionServerMetrics regionServerMetrics = new RegionServerMetrics();

    public RegionServerModel(String name, String host, int javaPort, int coreClientPort, int jmxPort) {
        Preconditions.checkNotNullOrEmpty(name, "Region server's name can't be null or empty");
        Preconditions.checkNotNullOrEmpty(host, "Region server's host can't be null or empty");
        Preconditions.checkArgument(javaPort > 0, "Region server's java port must be greater than zero");
        Preconditions.checkArgument(coreClientPort > 0, "Region server's core client port must be greater than zero");
        this.name = name;
        this.host = host;
        this.javaPort = javaPort;
        this.coreClientPort = coreClientPort;
        this.jmxPort = jmxPort;
        this.locationString = String.join(HOSTNAME_SEPARATOR, host,
                String.valueOf(javaPort), String.valueOf(coreClientPort), String.valueOf(jmxPort));
        this.toString = String.join(HOSTNAME_SEPARATOR, name, host,
                String.valueOf(javaPort), String.valueOf(coreClientPort), String.valueOf(jmxPort));
    }

    public String getName() {
        return name;
    }

    public int getJavaPort() {
        return javaPort;
    }

    public int getCoreClientPort() {
        return coreClientPort;
    }

    public String getHost() {
        return host;
    }

    public int getJmxPort() {
        return jmxPort;
    }

    public String getLocationString() {
        return locationString;
    }

    @Override
    public String toString() {
        return toString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RegionServerModel that = (RegionServerModel) o;

        if (javaPort != that.javaPort) {
            return false;
        }
        if (coreClientPort != that.coreClientPort) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        return host.equals(that.host);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + host.hashCode();
        result = 31 * result + javaPort;
        result = 31 * result + coreClientPort;
        return result;
    }

    @Override
    public int compareTo(RegionServer other) {
        int compare = this.getHost().compareToIgnoreCase(other.getHost());
        if (compare != 0) return compare;
        compare = this.getJavaPort() - other.getJavaPort();
        if (compare != 0) return compare;
        compare = this.getCoreClientPort() - other.getCoreClientPort();
        if (compare != 0) return compare;
        return this.getJavaPort() - other.getJmxPort();
    }

    public RegionServerMetrics getRegionServerMetrics() {
        return regionServerMetrics;
    }

    public void setRegionServerMetrics(RegionServerMetrics rsm) {
        regionServerMetrics = rsm;
    }
}
