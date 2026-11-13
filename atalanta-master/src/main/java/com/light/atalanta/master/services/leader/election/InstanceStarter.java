/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.leader.election;

import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

public interface InstanceStarter extends Cloneable {

    boolean start(LeaderLatchListener leaderLatchListener) throws Exception;
}
