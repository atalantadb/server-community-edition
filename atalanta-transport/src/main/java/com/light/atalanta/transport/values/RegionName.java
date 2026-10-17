/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.values;

import com.google.common.base.Preconditions;

import java.util.Objects;

public class RegionName {
    public static final int REGION_NAME_LENGTH = 16;

    private final String name;

    public RegionName(String name) {
        validateName(name);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegionName that = (RegionName) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "RegionName=" + name;
    }

    private void validateName(String name) {
        Objects.requireNonNull(name);
        Preconditions.checkArgument(name.contains("_") && name.substring(0, name.indexOf("_")).length() == REGION_NAME_LENGTH,
                "Region name must be " + REGION_NAME_LENGTH + "-symbols string and contain replica number");
    }
}
