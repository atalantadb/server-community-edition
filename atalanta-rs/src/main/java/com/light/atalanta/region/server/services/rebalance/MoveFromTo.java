/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.rebalance;

import java.util.Objects;

public class MoveFromTo {

    private final RegionDirModel from;

    private final RegionDirModel to;

    public MoveFromTo(RegionDirModel from, RegionDirModel to) {
        this.from = from;
        this.to = to;
    }

    public RegionDirModel getFrom() {
        return from;
    }

    public RegionDirModel getTo() {
        return to;
    }

    @Override
    public String toString() {
        return "MoveFromTo{" +
                "from=" + from +
                ", to=" + to +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MoveFromTo that = (MoveFromTo) o;
        return Objects.equals(from, that.from) && Objects.equals(to, that.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }
}
