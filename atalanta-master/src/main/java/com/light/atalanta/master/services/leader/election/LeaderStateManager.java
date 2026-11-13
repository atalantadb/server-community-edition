/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.leader.election;

import com.light.atalanta.master.enums.LeaderState;

public interface LeaderStateManager {

    void start() throws Exception;

    void toFollowerMode();

    LeaderState getState();

    void shutdown(int exitCode);
}
