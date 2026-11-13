/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.operations.progressed;

public enum RegionModifyStatus {
    NEW,
    MODIFYING,
    READY_TO_BACKUP,
    READY_TO_COPY,
    READY_TO_ENABLED,
}
