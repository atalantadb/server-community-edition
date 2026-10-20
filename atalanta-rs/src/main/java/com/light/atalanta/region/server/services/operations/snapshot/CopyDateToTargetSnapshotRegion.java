/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.operations.snapshot;


import com.light.atalanta.grpc.ATALANTAProto;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.region.server.vo.RegionServerConfiguration; import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import static com.light.atalanta.ATALANTAConstants.SNAPSHOT_ZNODE;
public class CopyDateToTargetSnapshotRegion {
    private static final Logger LOGGER = LoggerFactory.getLogger(CopyDateToTargetSnapshotRegion.class.getName());
    private final RegionServerConfiguration regionServerConfiguration;
    private final FileSystem fileSystem;

    public CopyDateToTargetSnapshotRegion(RegionServerConfiguration regionServerConfiguration, FileSystem fileSystem) {
        this.regionServerConfiguration = regionServerConfiguration;
        this.fileSystem = fileSystem;
    }

    public void copyDataToTargetSnapshotRegion(String name, ATALANTAProto.ProtoTableName tableName,
                                               String region, int targetReplicaNum, String fileName, ByteBuffer array) throws IOException {
        String relDir = "/" + SNAPSHOT_ZNODE + "/" + name + "/" + tableName.getNamespace() + "/" + tableName.getTable() + region + "_" + targetReplicaNum;
        for (String dir : regionServerConfiguration.getRsDirs()) {
            final String snapshotDir = dir + "/" + relDir;
            if (fileSystem.isPathExist(snapshotDir)) {
                byte[] data = new byte[array.remaining()];
                array.get(data);
                Files.write(Paths.get(snapshotDir, fileName), data, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                LOGGER.info("copy snapshot data: path: {}, appended size: {}", snapshotDir, data.length);
                break;
            }
        }
    }
}
