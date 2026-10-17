/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services;

import com.light.atalanta.Environment;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import com.light.atalanta.services.ZookeeperHelperExtended;

import java.net.UnknownHostException;
import java.util.UUID;

import static com.light.atalanta.ATALANTAConstants.HOSTNAME_SEPARATOR;

public class RegionServerInfoSupplier {

    private final String regionServerId;

    private final ZookeeperHelperExtended zookeeperHelperExtended;

    private final RegionServerConfiguration conf;

    private final int jmxPort;

    private final String profile;

    public RegionServerInfoSupplier(ZookeeperHelperExtended zookeeperHelperExtended, RegionServerConfiguration conf, String profile) throws Exception {
        this.zookeeperHelperExtended = zookeeperHelperExtended;
        this.conf = conf;
        this.profile = profile;
        this.jmxPort = Integer.parseInt(System.getProperty("com.sun.management.jmxremote.port", "0"));
        regionServerId = initId();
    }

    public String regionServerId() {
        return regionServerId;
    }

    public String hostPorts() throws Exception {
        return String.join(HOSTNAME_SEPARATOR,
                regionServerHost(profile),
                String.valueOf(clientPort()),
                String.valueOf(coreClientPort()),
                String.valueOf(jmxPort));
    }

    // Lowercase the host name: Linux may change its case, for example 'fedora' to 'Fedora'
    public String regionServerHost(final String profile) throws UnknownHostException {
        return Environment.getLocalHost(profile);
    }

    public Integer coreClientPort() {
        return conf.getCoreConfiguration().getClientPort();
    }

    public int clientPort() {
        return conf.getGrpcConfiguration().getRegionServerPort();
    }

    public int getJmxPort() {
        return jmxPort;
    }

    private String initId() throws Exception {
        for (String regId : zookeeperHelperExtended.getAllRegionServerInfoIds()) {
            String regHostPort = zookeeperHelperExtended.getRegionServerInfo(regId);
            if (regHostPort.equals(hostPorts())) {
                return regId;
            }
        }
        return UUID.randomUUID().toString();
    }

    public RegionServerModel getMe(final String profile) throws Exception {
        return new RegionServerModel(
                regionServerId,
                regionServerHost(profile),
                clientPort(),
                coreClientPort(),
                getJmxPort());
    }

}
