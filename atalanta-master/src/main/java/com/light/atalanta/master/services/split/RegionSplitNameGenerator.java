/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.split;

import com.light.atalanta.model.RegionModel;

import java.util.HashMap;
import java.util.Map;

/**
 * Generates a new name for the splitted copy of a region.
 * If the name has already been generated, returns it.
 */
public class RegionSplitNameGenerator {
    private final Map<String, String> cache = new HashMap<>();

    public String generate(String splittingName) {
        return cache.computeIfAbsent(splittingName, sn -> RegionModel.makeRandomRegionName());
    }
}
