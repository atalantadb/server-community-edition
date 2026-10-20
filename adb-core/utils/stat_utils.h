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
#include "../all_headers.h"
#include "time_utils.h"
#define DT_LOG_SIZE 32
uint32_t g_coll_counter;
_Atomic int total_clients;
double mem, ins_ps, upd_ps, get_ps, del_ps, scn_ps;
uint64_t total_client_request_count;
uint32_t total_replica_request_count;
uint32_t total_ins_counter;
uint32_t total_upd_counter;
uint32_t total_get_counter;
uint32_t total_del_counter;
uint32_t total_scn_counter;
uint32_t total_rep_counter;
long start_ts, stop_ts;
char *curr_dt(char *buf);
size_t total_memory;
void rate_rtt_us(uint32_t rtt_us, int *score, const char **grade);
int get_tcp_rtt_us_by_fd(int fd, uint32_t *rtt_us);
void printStat();
_Noreturn void *pthread_stat();
