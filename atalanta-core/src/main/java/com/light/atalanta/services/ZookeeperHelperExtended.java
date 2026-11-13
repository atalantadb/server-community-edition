/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.services;

import com.light.atalanta.util.ShutdownManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.light.atalanta.ATALANTAConstants.F_MASTER_NODE;
import static com.light.atalanta.ATALANTAConstants.REGION_SERVER_DISABLED;
import static com.light.atalanta.ATALANTAConstants.REGION_SERVER_DISABLING;
import static com.light.atalanta.ATALANTAConstants.REGION_SERVER_EPHEMERAL_SUBPATH;
import static com.light.atalanta.ATALANTAConstants.REGION_SERVER_INFO_SUBPATH;
import static com.light.atalanta.ATALANTAConstants.REGION_SERVER_INITIALIZING;
import static com.light.atalanta.ATALANTAConstants.REGION_SERVER_RUNNING;
import static com.light.atalanta.ATALANTAConstants.ZK_SEPARATOR;

public class ZookeeperHelperExtended {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperHelperExtended.class);

    private final ZookeeperHelper zooHelper;

    public ZookeeperHelperExtended(ZookeeperHelper zooHelper) {
        this.zooHelper = zooHelper;
    }

    public String getMasterHostPort() throws Exception {
        return zooHelper.get(F_MASTER_NODE).orElseThrow(IllegalArgumentException::new);
    }

    public String getRegionServerInfo(String regionServerId) throws Exception {
        return zooHelper.get(REGION_SERVER_INFO_SUBPATH + ZK_SEPARATOR + regionServerId).orElseThrow(IllegalArgumentException::new);
    }

    public String getRegionServerStatus(String regionServerId) throws Exception {
        return zooHelper.get(REGION_SERVER_EPHEMERAL_SUBPATH + ZK_SEPARATOR + regionServerId).orElseThrow(IllegalArgumentException::new);
    }

    public void registerRs(String regionServerId, String hostInfo, String status) throws Exception {
        LOGGER.info("Registering RS in ZooKeeper with status: {} ...", status);
        final String rsEphemeralPath = REGION_SERVER_EPHEMERAL_SUBPATH + ZK_SEPARATOR + regionServerId;
        if (zooHelper.checkExists(rsEphemeralPath)) {
            ShutdownManager.exitWithError("RS with ID: " + regionServerId + " already running");
        } else {
            zooHelper.createEphemeral(rsEphemeralPath, status);
        }
        final String rsPath = REGION_SERVER_INFO_SUBPATH + ZK_SEPARATOR + regionServerId;
        if (zooHelper.checkExists(rsPath)) {
            zooHelper.setData(rsPath,  hostInfo);
        } else {
            zooHelper.create(rsPath, hostInfo);
        }
        LOGGER.info("RS has been registered. ID: {}' ", regionServerId);
    }

    public void updateRsStatus(String regionServerId, String status) throws Exception {
        LOGGER.info("Updating RS ZooKeeper status to: {} ...", status);
        final String rsEphemeralPath = REGION_SERVER_EPHEMERAL_SUBPATH + ZK_SEPARATOR + regionServerId;
        zooHelper.setData(rsEphemeralPath, status);
        LOGGER.info("RS Zookeeper status was updated");
    }

    public void unregisterRs(String regionServerId) throws Exception {
        LOGGER.info("Unregistering RS from ZooKeeper ...");
        final String rsEphemeralPath = REGION_SERVER_EPHEMERAL_SUBPATH + ZK_SEPARATOR + regionServerId;
        if (zooHelper.checkExists(rsEphemeralPath)) {
            zooHelper.delete(rsEphemeralPath);
            LOGGER.info("RS has been unregistered. id: {}' ", regionServerId);
        } else {
            LOGGER.info("RS with path: '{}' does not exist in ZK.", regionServerId);
        }
    }

    // Region servers with ephemeral nodes (INITIALIZING and RUNNING statuses)
    public List<String> getAliveRegionServerIds() throws Exception {
        List<String> runningIds = new ArrayList<>();
        try {
            if (zooHelper.checkExists(REGION_SERVER_EPHEMERAL_SUBPATH)) {
                for (String rs : zooHelper.getChildren(REGION_SERVER_EPHEMERAL_SUBPATH)) {
                    Optional<String> status = zooHelper.get(String.join("/", REGION_SERVER_EPHEMERAL_SUBPATH, rs));
                    if (status.isPresent() && (status.get().equals(REGION_SERVER_RUNNING) || status.get().equals(REGION_SERVER_INITIALIZING))) {
                        runningIds.add(rs);
                    }
                }
            }
            return runningIds;
        } catch (Exception ex) {
            throw new Exception("Unable to read ZK", ex);
        }
    }

    public List<String> getDisablingRegionServerIds() throws Exception {
        List<String> runningIds = new ArrayList<>();
        try {
            if (zooHelper.checkExists(REGION_SERVER_EPHEMERAL_SUBPATH)) {
                for (String rs : zooHelper.getChildren(REGION_SERVER_EPHEMERAL_SUBPATH)) {
                    Optional<String> status = zooHelper.get(String.join("/", REGION_SERVER_EPHEMERAL_SUBPATH, rs));
                    if (status.isPresent() && (status.get().equals(REGION_SERVER_DISABLING))) {
                        runningIds.add(rs);
                    }
                }
            }
            return runningIds;
        } catch (Exception ex) {
            throw new Exception("Unable to read ZK", ex);
        }
    }

    public List<String> getAliveOrDisabledRegionServerIds() throws Exception {
        List<String> runningIds = new ArrayList<>();
        try {
            if (zooHelper.checkExists(REGION_SERVER_EPHEMERAL_SUBPATH)) {
                for (String rs : zooHelper.getChildren(REGION_SERVER_EPHEMERAL_SUBPATH)) {
                    Optional<String> status = zooHelper.get(String.join("/", REGION_SERVER_EPHEMERAL_SUBPATH, rs));
                    if (status.isPresent() && (status.get().equals(REGION_SERVER_RUNNING)
                            || status.get().equals(REGION_SERVER_INITIALIZING)
                            || status.get().equals(REGION_SERVER_DISABLED))
                            || status.get().equals(REGION_SERVER_DISABLING)) {
                        runningIds.add(rs);
                    }
                }
            }
            return runningIds;
        } catch (Exception ex) {
            throw new Exception("Unable to read ZK", ex);
        }
    }

    // Running region servers (RUNNING status)
    public List<String> getRunningRegionServerIds() throws Exception {
        List<String> runningIds = new ArrayList<>();
        try {
            if (zooHelper.checkExists(REGION_SERVER_EPHEMERAL_SUBPATH)) {
                for (String rs : zooHelper.getChildren(REGION_SERVER_EPHEMERAL_SUBPATH)) {
                    Optional<String> status = zooHelper.get(String.join("/", REGION_SERVER_EPHEMERAL_SUBPATH, rs));
                    if (status.isPresent()
                            && ( status.get().equals(REGION_SERVER_RUNNING) || status.get().equals(REGION_SERVER_DISABLING))) {
                        runningIds.add(rs);
                    }
                }
            }
            return runningIds;
        } catch (Exception ex) {
            throw new Exception("Unable to read ZK", ex);
        }
    }

    public List<String> getAllRegionServerInfoIds() throws Exception {
        try {
            return zooHelper.checkExists(REGION_SERVER_INFO_SUBPATH) ? zooHelper.getChildren(REGION_SERVER_INFO_SUBPATH) : Collections.emptyList();
        } catch (Exception ex) {
            throw new Exception("Cannot read ZK", ex);
        }
    }
}
