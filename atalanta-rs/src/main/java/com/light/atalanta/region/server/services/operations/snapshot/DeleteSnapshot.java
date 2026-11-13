/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.operations.snapshot;


import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.light.atalanta.ATALANTAConstants.SNAPSHOT_ZNODE;
public class DeleteSnapshot {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteSnapshot.class.getName());
    private final RegionServerConfiguration regionServerConfiguration;
    private final FileSystem fileSystem;

    public DeleteSnapshot(RegionServerConfiguration regionServerConfiguration, FileSystem fileSystem) {
        this.regionServerConfiguration = regionServerConfiguration;
        this.fileSystem = fileSystem;
    }

    public void deleteSnapshot(String name,
                               StreamObserver<RegionServerProto.RegionSnapshotOperationProgress> responseObserver) throws Exception {
        for (String dir : regionServerConfiguration.getRsDirs()) {
            String snapshotDir = dir + "/" + SNAPSHOT_ZNODE + "/" + name;
            LOGGER.info("Delete snapshot dir: {}", snapshotDir);
            fileSystem.makeSureDirRemoved(snapshotDir);
        }
    }
}