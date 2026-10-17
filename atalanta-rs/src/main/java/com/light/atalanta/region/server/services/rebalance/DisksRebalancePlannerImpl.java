/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.rebalance;

import com.light.atalanta.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class DisksRebalancePlannerImpl implements DisksRebalancePlanner{
    private static final Logger LOGGER = LoggerFactory.getLogger(DisksRebalancePlannerImpl.class);
    @Override
    public MovePlan calcPlan(Map<FileStoreInfo, List<RegionDirModel>> diskMapsBefore,
                                                               long rebalanceDiskDiff) {
        MovePlan res = calcNextMove(diskMapsBefore, rebalanceDiskDiff, new MovePlan());
        long size = diskMapsBefore.size();
        // While there are more than two disks, drop the fullest one and extend the move plan
        // (needed for disks of different capacity or holding data other than AtalantaDB)
        while (size > 2){
            TreeMap<FileStoreInfo, List<RegionDirModel>> sortedDisksMap = new TreeMap<>(Comparator.comparingLong(FileStoreInfo::getFreeDiskSpace));
            sortedDisksMap.putAll(diskMapsBefore);
            sortedDisksMap.remove(sortedDisksMap.firstKey());
            res = calcNextMove(sortedDisksMap, rebalanceDiskDiff, res);
            diskMapsBefore = sortedDisksMap;
            --size;
        }
        return res;
    }

    // Called recursively
    private MovePlan calcNextMove(Map<FileStoreInfo, List<RegionDirModel>> diskMapsBefore,
                                                                    long rebalanceDiskDiff,
                                                                    MovePlan res) {
        // sortedDisksMap: disks sorted by free space, descending
        TreeMap<FileStoreInfo, List<RegionDirModel>> sortedDisksMap = new TreeMap<>(Comparator.comparingLong(FileStoreInfo::getFreeDiskSpace));
        sortedDisksMap.putAll(diskMapsBefore);
        // from: the disk with the least free space; regions are moved from it
        Map.Entry<FileStoreInfo, List<RegionDirModel>> from = sortedDisksMap.firstEntry();
        // to: the disk with the most free space; regions are moved to it
        Map.Entry<FileStoreInfo, List<RegionDirModel>> to = sortedDisksMap.lastEntry();
        long fromSize = from.getKey().getFreeDiskSpace();
        long toSize = to.getKey().getFreeDiskSpace();
        final long diff =  toSize - fromSize;
        // Stop if one disk is left or the spread is already within the norm
        if (sortedDisksMap.size() < 2 || diff < rebalanceDiskDiff) {
            LOGGER.info("Disk rebelance complete. Disk count: {}, diff {}", sortedDisksMap.size(), diff);
            return res;
        }

        // Sort the regions on the fullest disk by size, descending
        List<RegionDirModel> fromSorted = new ArrayList<>(from.getValue().stream()
                .sorted(Comparator.comparingLong(RegionDirModel::getSize).reversed()).collect(Collectors.toList()));
        sortedDisksMap.put(from.getKey(), fromSorted);
        if (fromSorted.isEmpty()) {
            LOGGER.info("Disk from: {}, free space: {}, no regions!", from.getKey().getName(), from.getKey().getFreeDiskSpace());
        } else {
            LOGGER.info("Disk from: {}, free space: {}, max region size: {}, min region size: {}",
                    from.getKey().getName(),
                    from.getKey().getFreeDiskSpace(),
                    fromSorted.get(0).getSize(),
                    fromSorted.get(fromSorted.size() - 1).getSize());
        }
        boolean moved = false;
        for(RegionDirModel f : new ArrayList<>(fromSorted)) {
            long fromSizeAfter = fromSize + f.getSize();
            long toSizeAfter = toSize - f.getSize();
            // If the move reduces the difference in free space between from and to
            if (toSizeAfter > 0 && Math.abs(toSizeAfter - fromSizeAfter) < toSize - fromSize) {
                // ... add the move to the plan
                RegionDirModel target = new RegionDirModel(f.getName(), f.getNs(), f.getTable(),
                        to.getKey().getRandomDir() + "/" + f.getNs() + "/" +  f.getTable() + "/" + f.getName(), f.getSize());
                res.addToPlan(f, target);
                // ... move the region from from to to
                fromSorted.remove(f);
                to.getValue().add(target);
                moved = true;
                toSize = toSizeAfter;
                fromSize = fromSizeAfter;
                // ... update the free space of from and to
                from.getKey().setFreeDiskSpace(fromSize);
                to.getKey().setFreeDiskSpace(toSize);
                // If the target disk still has excess free space, try again recursively
                if (toSize < fromSize) {
                    return calcNextMove(sortedDisksMap, rebalanceDiskDiff, res);
                }
            }
        }
        if (!moved) {
            final long diffNew =  toSize - fromSize;
            LOGGER.info("Disk rebalance complete. Disk count: {}, diff after: {}", sortedDisksMap.size(), diffNew);
        }
        return moved ? calcNextMove(sortedDisksMap, rebalanceDiskDiff, res) : res;
    }

}
