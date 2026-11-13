/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.metrics;

import javax.management.DynamicMBean;

public interface JmxRsMetrics extends DynamicMBean {

    void addCoreStat(Object pingResponse);

}
