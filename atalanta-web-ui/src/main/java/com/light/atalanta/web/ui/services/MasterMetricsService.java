/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.web.ui.services;

import com.light.atalanta.grpc.MasterProto;
import com.light.atalanta.metrics.VersionInfo;

public interface MasterMetricsService {

    MasterProto.PingResponse getMasterLeaderPingResponse();

    MasterProto.InfoResponse info();

    MasterProto.RecoveryMonitoringInfo recoveryInfo();

    MasterProto.MasterStartMonitoringInfo getMasterStartInfo();

    VersionInfo getMasterVersionInfo();


}
