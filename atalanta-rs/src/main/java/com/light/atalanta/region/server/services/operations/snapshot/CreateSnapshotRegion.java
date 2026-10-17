/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.operations.snapshot;

import com.light.atalanta.TableName;
import com.light.atalanta.grpc.ATALANTAProto;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.region.server.services.operations.progressed.DistributeRegion; import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

import static com.light.atalanta. ATALANTAConstants.SNAPSHOT_ZNODE;
public class CreateSnapshotRegion {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateSnapshotRegion.class.getName());
    private final RegionServerConfiguration regionServerConfiguration;
    private final DistributeRegion distributeRegion;
    private final FileSystem fileSystem;

    public CreateSnapshotRegion(RegionServerConfiguration regionServerConfiguration, DistributeRegion distributeRegion, FileSystem fileSystem) {
        this.regionServerConfiguration = regionServerConfiguration;
        this.distributeRegion = distributeRegion;
        this.fileSystem = fileSystem;
    }

    public void createSnapshotRegion(String name, ATALANTAProto.ProtoTableName tableName,
                                     String region, int targetReplicaNum) throws IOException {
        String relDir = "/" + SNAPSHOT_ZNODE + "/" + name + "/" +
                tableName.getNamespace() + "/" + tableName.getTable() + region + "_" + targetReplicaNum;
        for (String dir : regionServerConfiguration.getRsDirs()) {
            final String snapshotDir = dir + "/" + relDir;
            fileSystem.makeSureDirRemoved(snapshotDir);
        }
        String regionDir = distributeRegion.chooseDataDir(TableName.valueOf(SNAPSHOT_ZNODE, name), new RegionModel(region, targetReplicaNum, 1, new byte[]{0}, new byte[]{1}));
        fileSystem.makeSureDirExists(regionDir + relDir);
        LOGGER.info("Created snapshot region dir: {}", regionDir + relDir);
    }
}