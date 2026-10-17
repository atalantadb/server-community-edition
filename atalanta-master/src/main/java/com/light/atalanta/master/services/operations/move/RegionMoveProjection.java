/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations.move;

import com.light.atalanta.model.RegionDistributionModel;

import java.util.Objects;

public class RegionMoveProjection {
    private RegionDistributionModel source;
    private RegionDistributionModel target;

    public RegionMoveProjection(RegionDistributionModel source, RegionDistributionModel target) {
        this.source = source;
        this.target = target;
    }

    public RegionDistributionModel getSource() {
        return source;
    }

    public void setSource(RegionDistributionModel source) {
        this.source = source;
    }

    public RegionDistributionModel getTarget() {
        return target;
    }

    public void setTarget(RegionDistributionModel target) {
        this.target = target;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegionMoveProjection that = (RegionMoveProjection) o;
        return Objects.equals(source, that.source) && Objects.equals(target, that.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, target);
    }

    @Override
    public String toString() {
        return String.format("%n{source=%s, target=%s}", source.toShortString(), target.toShortString());
    }
}
