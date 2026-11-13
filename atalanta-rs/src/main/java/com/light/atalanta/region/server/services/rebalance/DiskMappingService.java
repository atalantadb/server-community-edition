/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.rebalance;

import java.nio.file.FileStore;
import java.util.List;
import java.util.Map;

public interface DiskMappingService {
    Map<FileStoreInfo, List<RegionDirModel>> getDiskMapping(List<String> dirs);
}
