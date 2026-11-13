/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
============================================================================
 Author: Danil Lipovoy
 Notice: Certain algorithms, data structures, and methods implemented in this software are disclosed in U.S. Provisional Patent Application No. 64/161,408, filed September 24, 2026.
 This software is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0) and nothing in this notice is intended to modify the rights granted under that license.
============================================================================

 */
#define _POSIX_C_SOURCE 200809L
#include "time_utils.h"
#include "time.h"
long getCurrentTimeMs() {
    long nowTs;
    struct timespec spec;
    long ms;
    clock_gettime(CLOCK_REALTIME, &spec);
    nowTs = (intmax_t)spec.tv_sec;
    ms = round(spec.tv_nsec / 1.0e6);

    if (ms > 999) {
        nowTs++;
        ms = 0;
    }
    nowTs = nowTs * 1000 + ms;
    return nowTs;
}
