/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport;

import java.util.Objects;

public class RegionPosition {
   public static final RegionPosition UNSET = new RegionPosition(-1);

    private final int position;

    public RegionPosition(int regionPosition) {
        this.position = regionPosition;
    }

    public int getPosition() {
        return position;
    }

    public boolean isRegionPositionSet() {
        return position != -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegionPosition that = (RegionPosition) o;
        return position == that.position;
    }

    @Override
    public int hashCode() {
        return Objects.hash(position);
    }

    @Override
    public String toString() {
        return "RegionPosition[" +
                "position=" + position +
                ']';
    }
}
