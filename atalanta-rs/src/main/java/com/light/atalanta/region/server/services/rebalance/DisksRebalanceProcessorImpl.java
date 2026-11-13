/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.rebalance;

import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.light.atalanta.region.server.services.ReplicaMapperProcessor.REBALANCING_REPLICA_POSTFIX;

public class DisksRebalanceProcessorImpl implements DisksRebalanceProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DisksRebalanceProcessorImpl.class);

    private final RegionServerConfiguration conf;

    private final DiskMappingService diskMappingService;
    private final DisksRebalancePlanner disksRebalancePlanner;

    public DisksRebalanceProcessorImpl(RegionServerConfiguration regionServerConfiguration,
                                       DiskMappingService diskMappingService,
                                       DisksRebalancePlanner disksRebalancePlanner) {
        this.conf = regionServerConfiguration;
        this.diskMappingService = diskMappingService;
        this.disksRebalancePlanner = disksRebalancePlanner;
    }

    @Override
    public void rebalance() {
        if (conf.getDataConfiguration().getRebalanceDiskDiffMb() > 0
                && conf.getRsDirs().size() > 1
                && getStoreCount() > 1) {
            LOGGER.info("Start disks rebalancing");
            rebalanceDisks();
            LOGGER.info("Complete disks rebalancing");
        } else {
            LOGGER.info("Disks rebalancing is disabled!");
        }
    }

    private int getStoreCount() {
        return conf.getRsDirs().stream().map(p -> {
                    try {
                        return Files.getFileStore(Paths.get(p)).name();
                    } catch (IOException e) {
                        LOGGER.error("cannot get store for path " + p);
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toSet())
                .size();
    }

    private void rebalanceDisks() {
        Map<FileStoreInfo, List<RegionDirModel>> diskMapsBefore = diskMappingService.getDiskMapping(conf.getRsDirs());
        diskMapsBefore.forEach((key, value) -> LOGGER.info("Before rebalance disk: {},  regions count: {}",
                key.getName(), value.size()));
        MovePlan movePlan = disksRebalancePlanner.calcPlan(diskMapsBefore,
                conf.getDataConfiguration().getRebalanceDiskDiffMb() * 1024 * 1024);
        moveData(movePlan);
        Map<FileStoreInfo, List<RegionDirModel>> diskMapsAfter = diskMappingService.getDiskMapping(conf.getRsDirs());
        diskMapsAfter.forEach((key, value) -> LOGGER.info("After rebalance disk: {},  regions count: {}",
                key.getName(), value.size()));

    }

    private void moveData(MovePlan movePlan) {
        movePlan.parallelStream().forEach( moveFromTo -> {
            try {
                LOGGER.info("rebalance move: from {}, to {}", moveFromTo.getFrom().getPath(), moveFromTo.getTo().getPath());
                File destDir = new File(moveFromTo.getTo().getPath() + REBALANCING_REPLICA_POSTFIX);
                File srcDir = new File(moveFromTo.getFrom().getPath());
                FileUtils.copyDirectory(srcDir, destDir);
                FileUtils.moveDirectory(destDir, new File(moveFromTo.getTo().getPath()));
                FileUtils.deleteDirectory(srcDir);
            } catch (IOException e) {
                LOGGER.error("cannot copy data: {}", moveFromTo, e);
                throw new RuntimeException(e);
            }
        });
    }

}
