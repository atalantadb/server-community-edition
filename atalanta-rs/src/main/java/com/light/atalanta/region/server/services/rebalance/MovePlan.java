/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.rebalance;

import java.util.ArrayList;
import java.util.List;

public class MovePlan extends ArrayList<MoveFromTo> {
    public void addToPlan(RegionDirModel from, RegionDirModel to) {
        for(MoveFromTo cur : new ArrayList<>(this)) {
            if (cur.getFrom().getName().equals(from.getName())) {
                remove(cur);
                if ( !cur.getFrom().equals(to)) {
                    add(new MoveFromTo(cur.getFrom(), to));
                }
                return;
            }
        }
        if (!from.equals(to)) {
            add(new MoveFromTo(from, to));
        }
    }
}
