/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

public enum RegionTaskType {
    /**
     * Creates (distributes) a region.
     * Used when a table is created.
     */
    DISTRIBUTE,

    /**
     * Enables a region.
     * Used when a table is enabled.
     * Works like DISTRIBUTE, but does not create the actual files (they must already exist)
     * and does not add the region to the internal RS structure.
     */
    ENABLE,

    /**
     * Disables a region.
     * Used when a table is disabled.
     */
    DISABLE,

    /**
     * Deletes a region.
     * Used when a table is dropped.
     */
    DROP,

    /**
     * Allows processing of requests to the region.
     *
     * Used when a region is moved.
     * Served by the RS that leads the region.
     */
    START,

    /**
     * Forbids processing of requests to the region.
     *
     * Used when a region is moved.
     * Served by the RS that leads the region.
     */
    STOP,

    /**
     * Disables a region.
     *
     * Used when a region is moved.
     * On the RS it works exactly like DISABLE; only logging differs. The RS does not know this endpoint:
     * RegionServerInteraction.java converts the command to DISABLE.
     * Served by the source RS.
     */
    UNASSIGN,

    /**
     * Starts moving a region.
     *
     * Served by the target RS.
     * On such a request the target RS contacts the source RS and sends it requests in this order:
     * UNASSIGN -> LIST_FILES -> SEND_BATCH -> SEND_BATCH -> ... SEND_BATCH.
     */
    MOVE,

    /**
     * Starts a region split, step 1.
     */
    SPLIT1,

    /**
     * Starts a region split, step 2.
     */
    SPLIT2,

    /**
     * Starts region recovery.
     *
     * Similar to MOVE.
     */
    RECOVER,

    /**
     * Returns a part of a region file of the given size.
     *
     * Used when a region is moved.
     * Served by the source RS.
     */
    SEND_FILE,

    /**
     * Returns the list of region files split into parts of the given size.
     *
     * Used when a region is moved, as the basis for SEND_BATCH requests.
     * Served by the source RS.
     */
    LIST_FILES
}
