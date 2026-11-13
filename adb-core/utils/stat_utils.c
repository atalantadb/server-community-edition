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
#include <stdarg.h>
#include "../all_headers.h"
#include "stat_utils.h"
#include "../index/f_index.h"
#include <sys/time.h>
#include <time.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "read_prop.h"
#define TCPF_ALL 0xFFF
char *curr_dt(char *buf) {
    struct tm tm_info;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    uint32_t millisec = lrint(tv.tv_usec / 1000.0);

    if (millisec >= 1000) {
        millisec -= 1000;
        tv.tv_sec++;
    }
    localtime_r(&tv.tv_sec, &tm_info);
    strftime(buf, DT_LOG_SIZE - 4, "%Y-%m-%d %H:%M:%S", &tm_info);
    snprintf(buf + strlen(buf), DT_LOG_SIZE - strlen(buf), ",%03d ", millisec);
    return buf;
}


size_t get_process_memory() {
    FILE *file = fopen("/proc/self/status", "r");

    if (!file) {
        perror("fopen");
        return 0;
    }
    char line[256];
    size_t memory = 0;

    while (fgets(line, sizeof(line), file)) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            sscanf(line, "VmRSS: %zu kB", &memory);
            break;
        }
    }
    fclose(file);
    return memory;
}

size_t get_total_memory() {
    FILE *file = fopen("/proc/meminfo", "r");

    if (!file) {
        perror("fopen");
        return 0;
    }
    char line[256];
    size_t total_memory = 0;

    while (fgets(line, sizeof(line), file)) {
        if (sscanf(line, "MemTotal: %zu kB", &total_memory) == 1) {
            break;
        }
    }
    fclose(file);
    return total_memory;
}

void printStat(struct table_info_t *t_info, struct region_info_t *r_info, char *dt, uint64_t cid) {
    total_ins_counter = 0;
    total_upd_counter = 0;
    total_get_counter = 0;
    total_del_counter = 0;
    total_scn_counter = 0;
    total_rep_counter = 0;
    g_coll_counter = 0;
    total_replica_request_count = 0;

    while (atomic_load(&is_shutting_down))
        sleep(10);
    pthread_mutex_lock(&tables_lock);

    for (int t = 0; t < table_count_curr; t++) {
        t_info = table_info[t];
        pthread_mutex_unlock(&tables_lock);
        get_t_lock(t_info, 11, false, cid);
        uint32_t pos = -1;
        uint8_t new_log_level = 0;

        for (uint32_t i = 0; i < online_debug_tables_count; i++) {
            if (strcmp(online_debug_info[i]->namespace, t_info->ns) == 0) {
                if (strcmp(online_debug_info[i]->table_name, t_info->table) == 0) {
                    new_log_level = online_debug_info[i]->log_level;
                    pos = i;
                    break;
                }
            }
        }

        for (int r = 0; r < t_info->region_count_curr; r++) {
            r_info = t_info->region_info[r];

            if (pos != -1) {
                if (online_debug_info[pos]->uuid != NULL && r_info->uuid != NULL) {
                    if (strcmp(online_debug_info[pos]->uuid, r_info->uuid) == 0)
                        r_info->log_level = online_debug_info[pos]->log_level;
                } else {
                    r_info->log_level = new_log_level;
                }
            } else {
                r_info->log_level = new_log_level;
            }

            if (r_info->is_leader) {
                add_path_to_drv_arr(r_info->r_fname, cid);
                uint64_t activity_sum = (uint64_t)atomic_load(&r_info->req_put_counter_sum) + (uint64_t)atomic_load(&r_info->req_get_counter_sum) +
                                        (uint64_t)atomic_load(&r_info->req_del_counter_sum) + (uint64_t)atomic_load(&r_info->req_scn_counter_sum);
                uint64_t last_activity_sum = atomic_load(&r_info->append_compact_last_activity_sum);

                if (activity_sum != last_activity_sum) {
                    atomic_store(&r_info->append_compact_idle_streak, 0);
                    atomic_store(&r_info->append_compact_last_activity_sum, activity_sum);
                } else {
                    uint32_t idle_streak = atomic_load(&r_info->append_compact_idle_streak);

                    if (idle_streak < UINT32_MAX)
                        idle_streak++;
                    atomic_store(&r_info->append_compact_idle_streak, idle_streak);
                }
                total_ins_counter += r_info->req_ins_counter;
                total_upd_counter += r_info->req_upd_counter;
                total_get_counter += r_info->req_get_counter;
                total_del_counter += r_info->req_del_counter;
                total_scn_counter += r_info->req_scn_counter;
                total_client_request_count = total_ins_counter + total_upd_counter + total_get_counter + total_del_counter + total_scn_counter;
                r_info->req_counter = 0;
                r_info->req_ins_counter = 0;
                r_info->req_upd_counter = 0;
                r_info->req_get_counter = 0;
                r_info->req_del_counter = 0;
                r_info->req_scn_counter = 0;

                if (log_level > 0)
                    for (int bs = 0; bs < r_info->max_idx_b; bs++)
                        for (int b = 0; b < BUCKETS; b++)
                            g_coll_counter += r_info->r_idx_b[bs]->coll_count_curr[b];
            } else {
                total_replica_request_count += r_info->req_rep_counter;
                r_info->req_rep_counter = 0;
            }
        }
        do_t_unlock(t_info, 12, false, cid);

        while (atomic_load(&is_shutting_down))
            sleep(10);
        pthread_mutex_lock(&tables_lock);
    }
    pthread_mutex_unlock(&tables_lock);
    stop_ts = getCurrentTimeMs(); // TODO first stat will be not fair (move to worker thread)
    uint64_t current_ops = (uint64_t)llround((double)total_client_request_count * 1000.0 / (double)(stop_ts - start_ts));
    atomic_store(&ops, current_ops);
    int compact_ops_limit = atomic_load(&append_compact_ops_limit);

    if (compact_ops_limit > 0 && current_ops < (uint64_t)compact_ops_limit) {
        uint32_t low_ops_streak = atomic_load(&append_compact_low_ops_streak);

        if (low_ops_streak < 2U)
            low_ops_streak++;
        atomic_store(&append_compact_low_ops_streak, low_ops_streak);
        atomic_store(&append_compact_truncate_allowed, low_ops_streak >= 2U);
    } else {
        atomic_store(&append_compact_low_ops_streak, 0);
        atomic_store(&append_compact_truncate_allowed, false);
    }
    int sleeping_ops_limit = atomic_load(&append_compact_sleeping_ops_limit);

    if (sleeping_ops_limit > 0 && current_ops < (uint64_t)sleeping_ops_limit) {
        uint32_t sleeping_ops_streak = atomic_load(&append_compact_sleeping_ops_streak);

        if (sleeping_ops_streak < APPEND_COMPACTION_IDLE_STREAK_REQUIRED)
            sleeping_ops_streak++;
        atomic_store(&append_compact_sleeping_ops_streak, sleeping_ops_streak);
    } else {
        atomic_store(&append_compact_sleeping_ops_streak, 0);
    }
    uint64_t rpl = (uint64_t)llround((double)total_replica_request_count * 1000.0 / (double)(stop_ts - start_ts));
    ins_ps = (double)total_ins_counter * 1000 / (double)(stop_ts - start_ts);
    upd_ps = (double)total_upd_counter * 1000 / (double)(stop_ts - start_ts);
    get_ps = (double)total_get_counter * 1000 / (double)(stop_ts - start_ts);
    del_ps = (double)total_del_counter * 1000 / (double)(stop_ts - start_ts);
    scn_ps = (double)total_scn_counter * 1000 / (double)(stop_ts - start_ts);
    size_t memory = get_process_memory();

    if (total_memory != 0)
        mem = (double)memory * 100 / (double)total_memory;

    if (log_level == 0)
        fprintf(stderr, "%s INFO [STAT] mem=%04.1f(%%), cli=%-4d ins=%-8.0f upd=%-8.0f get=%-8.0f del=%-8.0f scn=%-8.0f ops=%lu\n", curr_dt(dt), mem, total_clients, ins_ps,
                upd_ps, get_ps, del_ps, scn_ps, current_ops);
    else
        fprintf(stderr, "%s INFO [STAT] mem=%04.1f(%%), cli=%-4d coll=%-4u rpl=%-7lu ins=%-8.0f upd=%-8.0f get=%-8.0f del=%-8.0f scn=%-8.0f ops=%lu\n", curr_dt(dt), mem,
                total_clients, g_coll_counter, rpl, ins_ps, upd_ps, get_ps, del_ps, scn_ps, current_ops);

    if (log_level > 1) {
        uint64_t compact_generated = atomic_exchange(&append_compact_metric_garbage_generated_bytes, 0);
        uint64_t compact_removed = atomic_exchange(&append_compact_metric_garbage_removed_bytes, 0);
        uint64_t compact_lock_attempts = atomic_exchange(&append_compact_metric_lock_attempts, 0);
        uint64_t compact_lock_misses = atomic_exchange(&append_compact_metric_lock_misses, 0);
        uint64_t compact_lock_acquired = atomic_exchange(&append_compact_metric_lock_acquired, 0);
        uint64_t compact_wait_ms = atomic_exchange(&append_compact_metric_request_wait_ms, 0);
        uint64_t compact_wait_max_ms = atomic_exchange(&append_compact_metric_request_wait_max_ms, 0);
        uint64_t compact_wait_samples = atomic_exchange(&append_compact_metric_request_wait_samples, 0);
        uint64_t compact_lock_hold_us = atomic_exchange(&append_compact_metric_lock_hold_us, 0);
        uint64_t compact_build_us = atomic_exchange(&append_compact_metric_build_us, 0);
        uint64_t compact_apply_us = atomic_exchange(&append_compact_metric_apply_us, 0);
        uint64_t compact_segments = atomic_exchange(&append_compact_metric_segments, 0);
        uint64_t compact_client_acquires = atomic_exchange(&append_compact_metric_client_acquires_waiting, 0);
        uint64_t compact_changed_blocks = atomic_exchange(&append_compact_metric_changed_blocks, 0);
        uint64_t compact_sent_blocks = atomic_exchange(&append_compact_metric_sent_blocks, 0);
        uint64_t compact_build_prepare_us = atomic_exchange(&append_compact_metric_build_prepare_us, 0);
        uint64_t compact_build_source_read_us = atomic_exchange(&append_compact_metric_build_source_read_us, 0);
        uint64_t compact_build_source_lookup_us = atomic_exchange(&append_compact_metric_build_source_lookup_us, 0);
        uint64_t compact_build_source_pack_us = atomic_exchange(&append_compact_metric_build_source_pack_us, 0);
        uint64_t compact_build_tail_read_us = atomic_exchange(&append_compact_metric_build_tail_read_us, 0);
        uint64_t compact_build_tail_lookup_us = atomic_exchange(&append_compact_metric_build_tail_lookup_us, 0);
        uint64_t compact_build_tail_pack_us = atomic_exchange(&append_compact_metric_build_tail_pack_us, 0);
        uint64_t compact_build_verify_us = atomic_exchange(&append_compact_metric_build_verify_us, 0);
        uint64_t compact_build_hash_us = atomic_exchange(&append_compact_metric_build_hash_us, 0);
        uint64_t compact_build_source_bytes = atomic_exchange(&append_compact_metric_build_source_bytes, 0);
        uint64_t compact_build_tail_bytes = atomic_exchange(&append_compact_metric_build_tail_bytes, 0);
        uint64_t compact_build_records = atomic_exchange(&append_compact_metric_build_records, 0);
        uint64_t compact_build_index_lookups = atomic_exchange(&append_compact_metric_build_index_lookups, 0);
        uint64_t compact_apply_flush_us = atomic_exchange(&append_compact_metric_apply_flush_us, 0);
        uint64_t compact_apply_validate_us = atomic_exchange(&append_compact_metric_apply_validate_us, 0);
        uint64_t compact_apply_replica_us = atomic_exchange(&append_compact_metric_apply_replica_us, 0);
        uint64_t compact_apply_index_us = atomic_exchange(&append_compact_metric_apply_index_us, 0);
        uint64_t compact_apply_scans_us = atomic_exchange(&append_compact_metric_apply_scans_us, 0);
        uint64_t compact_apply_zindex_us = atomic_exchange(&append_compact_metric_apply_zindex_us, 0);
        uint64_t compact_apply_truncate_us = atomic_exchange(&append_compact_metric_apply_truncate_us, 0);
        uint64_t compact_apply_accounting_us = atomic_exchange(&append_compact_metric_apply_accounting_us, 0);
        uint64_t compact_replica_send_us = atomic_exchange(&append_compact_metric_replica_send_us, 0);
        uint64_t compact_replica_local_write_us = atomic_exchange(&append_compact_metric_replica_local_write_us, 0);
        uint64_t compact_replica_recv_us = atomic_exchange(&append_compact_metric_replica_recv_us, 0);
        uint64_t compact_replica_retries = atomic_exchange(&append_compact_metric_replica_retries, 0);
        uint64_t compact_replica_bytes = atomic_exchange(&append_compact_metric_replica_bytes, 0);
        uint64_t compact_client_lock_attempts = atomic_exchange(&append_compact_metric_client_lock_attempts, 0);
        uint64_t compact_client_lock_contended = atomic_exchange(&append_compact_metric_client_lock_contended, 0);
        uint64_t compact_client_lock_contended_by_compactor = atomic_exchange(&append_compact_metric_client_lock_contended_by_compactor, 0);
        uint64_t compact_client_lock_wait_us = atomic_exchange(&append_compact_metric_client_lock_wait_us, 0);
        uint64_t compact_client_lock_wait_compactor_us = atomic_exchange(&append_compact_metric_client_lock_wait_compactor_us, 0);
        uint64_t compact_client_lock_wait_max_us = atomic_exchange(&append_compact_metric_client_lock_wait_max_us, 0);
        uint64_t compact_client_lock_hold_us = atomic_exchange(&append_compact_metric_client_lock_hold_us, 0);
        uint64_t compact_client_lock_hold_max_us = atomic_exchange(&append_compact_metric_client_lock_hold_max_us, 0);
        uint64_t compact_client_lock_hold_samples = atomic_exchange(&append_compact_metric_client_lock_hold_samples, 0);
        uint64_t compact_client_block_maker_us = atomic_exchange(&append_compact_metric_client_block_maker_us, 0);
        uint64_t compact_client_bulk_process_us = atomic_exchange(&append_compact_metric_client_bulk_process_us, 0);
        uint64_t compact_client_bulk_replica_us = atomic_exchange(&append_compact_metric_client_bulk_replica_us, 0);
        uint64_t compact_v2_source_matches = atomic_exchange(&append_compact_metric_v2_source_matches, 0);
        uint64_t compact_v2_source_mismatches = atomic_exchange(&append_compact_metric_v2_source_mismatches, 0);
        uint64_t compact_v2_replay_matches = atomic_exchange(&append_compact_metric_v2_replay_matches, 0);
        uint64_t compact_v2_state_failures = atomic_exchange(&append_compact_metric_v2_state_failures, 0);
        uint64_t compact_v2_final_probe_calls = atomic_exchange(&append_compact_metric_v2_final_probe_calls, 0);
        uint64_t compact_v2_commands = atomic_exchange(&append_compact_metric_v2_commands, 0);
        uint64_t compact_v2_total_us = atomic_exchange(&append_compact_metric_v2_total_us, 0);
        uint64_t compact_v2_setup_us = atomic_exchange(&append_compact_metric_v2_setup_us, 0);
        uint64_t compact_v2_source_verify_us = atomic_exchange(&append_compact_metric_v2_source_verify_us, 0);
        uint64_t compact_v2_raw_read_us = atomic_exchange(&append_compact_metric_v2_raw_read_us, 0);
        uint64_t compact_v2_raw_read_calls = atomic_exchange(&append_compact_metric_v2_raw_read_calls, 0);
        uint64_t compact_v2_raw_read_bytes = atomic_exchange(&append_compact_metric_v2_raw_read_bytes, 0);
        uint64_t compact_v2_block_verify_us = atomic_exchange(&append_compact_metric_v2_block_verify_us, 0);
        uint64_t compact_v2_rebuild_us = atomic_exchange(&append_compact_metric_v2_rebuild_us, 0);
        uint64_t compact_v2_result_hash_us = atomic_exchange(&append_compact_metric_v2_result_hash_us, 0);
        uint64_t compact_v2_expected_hash_us = atomic_exchange(&append_compact_metric_v2_expected_hash_us, 0);
        uint64_t compact_v2_final_probe_us = atomic_exchange(&append_compact_metric_v2_final_probe_us, 0);
        uint64_t compact_v2_compact_write_us = atomic_exchange(&append_compact_metric_v2_compact_write_us, 0);
        uint64_t compact_v2_truncate_us = atomic_exchange(&append_compact_metric_v2_truncate_us, 0);
        uint64_t compact_v2_ordinary_write_us = atomic_exchange(&append_compact_metric_v2_ordinary_write_us, 0);
        uint64_t compact_v2_fstat_us = atomic_exchange(&append_compact_metric_v2_fstat_us, 0);
        uint64_t compact_v2_context_init_us = atomic_exchange(&append_compact_metric_v2_context_init_us, 0);
        uint64_t compact_v2_validate_us = atomic_exchange(&append_compact_metric_v2_validate_us, 0);
        uint64_t compact_v2_region_check_us = atomic_exchange(&append_compact_metric_v2_region_check_us, 0);
        uint64_t compact_v2_target_us = atomic_exchange(&append_compact_metric_v2_target_us, 0);
        uint64_t compact_v2_preflush_us = atomic_exchange(&append_compact_metric_v2_preflush_us, 0);
        uint64_t compact_v2_write_copy_us = atomic_exchange(&append_compact_metric_v2_write_copy_us, 0);
        uint64_t compact_v2_ordinary_write_call_us = atomic_exchange(&append_compact_metric_v2_ordinary_write_call_us, 0);
        uint64_t compact_v2_postflush_us = atomic_exchange(&append_compact_metric_v2_postflush_us, 0);
        uint64_t compact_v2_initial_fstat_us = atomic_exchange(&append_compact_metric_v2_initial_fstat_us, 0);
        uint64_t compact_v2_replay_fstat_us = atomic_exchange(&append_compact_metric_v2_replay_fstat_us, 0);
        uint64_t compact_v2_final_fstat_us = atomic_exchange(&append_compact_metric_v2_final_fstat_us, 0);
        uint64_t compact_v2_cleanup_us = atomic_exchange(&append_compact_metric_v2_cleanup_us, 0);
        uint64_t compact_v2_fail_handler_us = atomic_exchange(&append_compact_metric_v2_fail_handler_us, 0);
        uint64_t compact_v2_workspace_contexts = atomic_exchange(&append_compact_metric_v2_workspace_contexts, 0);
        uint64_t compact_v2_heap_contexts = atomic_exchange(&append_compact_metric_v2_heap_contexts, 0);
        uint64_t compact_v2_heap_write_copies = atomic_exchange(&append_compact_metric_v2_heap_write_copies, 0);
        uint64_t compact_v2_compact_write_calls = atomic_exchange(&append_compact_metric_v2_compact_write_calls, 0);
        uint64_t compact_v2_compact_write_bytes = atomic_exchange(&append_compact_metric_v2_compact_write_bytes, 0);
        uint64_t compact_v2_ordinary_write_calls = atomic_exchange(&append_compact_metric_v2_ordinary_write_calls, 0);
        uint64_t compact_v2_ordinary_write_bytes = atomic_exchange(&append_compact_metric_v2_ordinary_write_bytes, 0);
        uint64_t compact_client_wait_hist[8];

        for (uint32_t i = 0; i < 8; i++)
            compact_client_wait_hist[i] = atomic_exchange(&append_compact_metric_client_lock_wait_hist[i], 0);

        if (compact_generated != 0 || compact_removed != 0 || compact_lock_attempts != 0 || compact_segments != 0)
            fprintf(stderr,
                    "%s INFO [COMPACT_STAT] generated=%lu removed=%lu segments=%lu blocks_changed=%lu blocks_sent=%lu changed_percent=%.2f lock_attempts=%lu lock_misses=%lu "
                    "lock_acquired=%lu wait_samples=%lu wait_avg_ms=%.2f wait_max_ms=%lu lock_hold_ms=%.2f build_ms=%.2f apply_ms=%.2f client_acquires_waiting=%lu\n",
                    curr_dt(dt), compact_generated, compact_removed, compact_segments, compact_changed_blocks, compact_sent_blocks,
                    compact_sent_blocks == 0 ? 0.0 : (double)compact_changed_blocks * 100.0 / (double)compact_sent_blocks, compact_lock_attempts, compact_lock_misses,
                    compact_lock_acquired, compact_wait_samples, compact_wait_samples == 0 ? 0.0 : (double)compact_wait_ms / (double)compact_wait_samples, compact_wait_max_ms,
                    (double)compact_lock_hold_us / 1000.0, (double)compact_build_us / 1000.0, (double)compact_apply_us / 1000.0, compact_client_acquires);

        if (compact_build_us != 0 || compact_apply_us != 0)
            fprintf(stderr,
                    "%s INFO [COMPACT_PHASE] build_ms=%.2f prepare=%.2f source_read=%.2f source_lookup=%.2f source_pack=%.2f tail_read=%.2f tail_lookup=%.2f tail_pack=%.2f "
                    "verify=%.2f hash=%.2f source_bytes=%lu tail_bytes=%lu records=%lu lookups=%lu apply_ms=%.2f flush=%.2f validate=%.2f replica=%.2f index=%.2f scans=%.2f "
                    "zindex=%.2f truncate=%.2f accounting=%.2f replica_send=%.2f replica_local=%.2f replica_recv=%.2f replica_retries=%lu replica_bytes=%lu\n",
                    curr_dt(dt), (double)compact_build_us / 1000.0, (double)compact_build_prepare_us / 1000.0, (double)compact_build_source_read_us / 1000.0,
                    (double)compact_build_source_lookup_us / 1000.0, (double)compact_build_source_pack_us / 1000.0, (double)compact_build_tail_read_us / 1000.0,
                    (double)compact_build_tail_lookup_us / 1000.0, (double)compact_build_tail_pack_us / 1000.0, (double)compact_build_verify_us / 1000.0,
                    (double)compact_build_hash_us / 1000.0, compact_build_source_bytes, compact_build_tail_bytes, compact_build_records, compact_build_index_lookups,
                    (double)compact_apply_us / 1000.0, (double)compact_apply_flush_us / 1000.0, (double)compact_apply_validate_us / 1000.0,
                    (double)compact_apply_replica_us / 1000.0, (double)compact_apply_index_us / 1000.0, (double)compact_apply_scans_us / 1000.0,
                    (double)compact_apply_zindex_us / 1000.0, (double)compact_apply_truncate_us / 1000.0, (double)compact_apply_accounting_us / 1000.0,
                    (double)compact_replica_send_us / 1000.0, (double)compact_replica_local_write_us / 1000.0, (double)compact_replica_recv_us / 1000.0, compact_replica_retries,
                    compact_replica_bytes);

        if (compact_client_lock_attempts != 0 || compact_client_lock_contended != 0 || compact_client_lock_hold_samples != 0)
            fprintf(stderr,
                    "%s INFO [COMPACT_CLIENT] sampled_attempts=%lu contended=%lu by_compactor=%lu wait_ms=%.2f wait_compactor_ms=%.2f wait_max_us=%lu hold_samples=%lu "
                    "hold_ms=%.2f hold_avg_us=%.2f hold_max_us=%lu block_ms=%.2f bulk_ms=%.2f bulk_replica_ms=%.2f "
                    "wait_hist_lt10_50_100_250_500_1000_5000_ge5000=%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu\n",
                    curr_dt(dt), compact_client_lock_attempts, compact_client_lock_contended, compact_client_lock_contended_by_compactor,
                    (double)compact_client_lock_wait_us / 1000.0, (double)compact_client_lock_wait_compactor_us / 1000.0, compact_client_lock_wait_max_us,
                    compact_client_lock_hold_samples, (double)compact_client_lock_hold_us / 1000.0,
                    compact_client_lock_hold_samples == 0 ? 0.0 : (double)compact_client_lock_hold_us / (double)compact_client_lock_hold_samples, compact_client_lock_hold_max_us,
                    (double)compact_client_block_maker_us / 1000.0, (double)compact_client_bulk_process_us / 1000.0, (double)compact_client_bulk_replica_us / 1000.0,
                    compact_client_wait_hist[0], compact_client_wait_hist[1], compact_client_wait_hist[2], compact_client_wait_hist[3], compact_client_wait_hist[4],
                    compact_client_wait_hist[5], compact_client_wait_hist[6], compact_client_wait_hist[7]);

        if (compact_v2_source_matches != 0 || compact_v2_source_mismatches != 0 || compact_v2_replay_matches != 0 || compact_v2_state_failures != 0 ||
            compact_v2_final_probe_calls != 0)
            fprintf(stderr, "%s INFO [COMPACT_V2] source_matches=%lu source_mismatches=%lu replay_matches=%lu state_failures=%lu final_probe_calls=%lu\n", curr_dt(dt),
                    compact_v2_source_matches, compact_v2_source_mismatches, compact_v2_replay_matches, compact_v2_state_failures, compact_v2_final_probe_calls);

        if (compact_v2_commands != 0)
            fprintf(stderr,
                    "%s INFO [COMPACT_V2_PHASE] commands=%lu total_ms=%.2f avg_us=%.2f setup_ms=%.2f source_verify_ms=%.2f raw_read_ms=%.2f raw_read_calls=%lu "
                    "raw_read_bytes=%lu block_verify_ms=%.2f rebuild_ms=%.2f result_hash_ms=%.2f expected_hash_ms=%.2f final_probe_ms=%.2f compact_write_ms=%.2f "
                    "truncate_ms=%.2f ordinary_write_ms=%.2f fstat_ms=%.2f\n",
                    curr_dt(dt), compact_v2_commands, (double)compact_v2_total_us / 1000.0, (double)compact_v2_total_us / (double)compact_v2_commands,
                    (double)compact_v2_setup_us / 1000.0, (double)compact_v2_source_verify_us / 1000.0, (double)compact_v2_raw_read_us / 1000.0, compact_v2_raw_read_calls,
                    compact_v2_raw_read_bytes, (double)compact_v2_block_verify_us / 1000.0, (double)compact_v2_rebuild_us / 1000.0, (double)compact_v2_result_hash_us / 1000.0,
                    (double)compact_v2_expected_hash_us / 1000.0, (double)compact_v2_final_probe_us / 1000.0, (double)compact_v2_compact_write_us / 1000.0,
                    (double)compact_v2_truncate_us / 1000.0, (double)compact_v2_ordinary_write_us / 1000.0, (double)compact_v2_fstat_us / 1000.0);

        if (compact_v2_commands != 0) {
            uint64_t compact_v2_accounted_us = compact_v2_context_init_us + compact_v2_validate_us + compact_v2_region_check_us + compact_v2_target_us + compact_v2_preflush_us +
                                               compact_v2_initial_fstat_us + compact_v2_source_verify_us + compact_v2_expected_hash_us + compact_v2_final_probe_us +
                                               compact_v2_write_copy_us + compact_v2_compact_write_us + compact_v2_truncate_us + compact_v2_ordinary_write_call_us +
                                               compact_v2_postflush_us + compact_v2_replay_fstat_us + compact_v2_final_fstat_us + compact_v2_cleanup_us +
                                               compact_v2_fail_handler_us;
            uint64_t compact_v2_residual_us = compact_v2_total_us > compact_v2_accounted_us ? compact_v2_total_us - compact_v2_accounted_us : 0;
            fprintf(stderr,
                    "%s INFO [COMPACT_V2_INTERNAL] commands=%lu accounted_ms=%.2f residual_ms=%.2f init_ms=%.2f validate_ms=%.2f region_check_ms=%.2f target_ms=%.2f "
                    "preflush_ms=%.2f initial_fstat_ms=%.2f source_state_ms=%.2f expected_hash_ms=%.2f final_probe_ms=%.2f write_copy_ms=%.2f compact_write_ms=%.2f "
                    "truncate_ms=%.2f ordinary_write_call_ms=%.2f postflush_ms=%.2f replay_fstat_ms=%.2f final_fstat_ms=%.2f cleanup_ms=%.2f fail_handler_ms=%.2f "
                    "contexts_workspace=%lu contexts_heap=%lu write_copies_heap=%lu compact_calls=%lu compact_bytes=%lu ordinary_calls=%lu ordinary_bytes=%lu\n",
                    curr_dt(dt), compact_v2_commands, (double)compact_v2_accounted_us / 1000.0, (double)compact_v2_residual_us / 1000.0,
                    (double)compact_v2_context_init_us / 1000.0, (double)compact_v2_validate_us / 1000.0, (double)compact_v2_region_check_us / 1000.0,
                    (double)compact_v2_target_us / 1000.0, (double)compact_v2_preflush_us / 1000.0, (double)compact_v2_initial_fstat_us / 1000.0,
                    (double)compact_v2_source_verify_us / 1000.0, (double)compact_v2_expected_hash_us / 1000.0, (double)compact_v2_final_probe_us / 1000.0,
                    (double)compact_v2_write_copy_us / 1000.0, (double)compact_v2_compact_write_us / 1000.0, (double)compact_v2_truncate_us / 1000.0,
                    (double)compact_v2_ordinary_write_call_us / 1000.0, (double)compact_v2_postflush_us / 1000.0, (double)compact_v2_replay_fstat_us / 1000.0,
                    (double)compact_v2_final_fstat_us / 1000.0, (double)compact_v2_cleanup_us / 1000.0, (double)compact_v2_fail_handler_us / 1000.0,
                    compact_v2_workspace_contexts, compact_v2_heap_contexts, compact_v2_heap_write_copies, compact_v2_compact_write_calls, compact_v2_compact_write_bytes,
                    compact_v2_ordinary_write_calls, compact_v2_ordinary_write_bytes);
        }
    }
    start_ts = stop_ts;
}

uint32_t read_debug_file(online_debug_info_t **online_debug_info, uint32_t online_debug_tables_count, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    struct stat fileStatBefore;

    if (stat(debug_file_name, &fileStatBefore) == -1) {
        for (uint32_t i = 0; i < online_debug_tables_count; i++) {
            free((*online_debug_info)[i].namespace);
            free((*online_debug_info)[i].table_name);
            free((*online_debug_info)[i].uuid);
        }
        free(*online_debug_info);
        *online_debug_info = NULL;
        return 0;
    }
    FILE *file = fopen(debug_file_name, "r");

    if (!file) {
        fprintf(stderr, "%s WARN [HEALTH=%015lu] Failed to open debug file %s: %s\n", curr_dt(dt), cid, debug_file_name, strerror(errno));
        return online_debug_tables_count;
    }
    online_debug_info_t *new_list = NULL;
    uint32_t new_count = 0;
    char line[256];
    size_t line_num = 0;

    while (fgets(line, sizeof(line), file)) {
        line_num++;
        line[strcspn(line, "\r\n")] = 0;

        if (strlen(line) == 0)
            continue;

        if (line[0] == '#')
            continue;
        char *equal = strchr(line, '=');

        if (!equal) {
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Malformed line %zu in debug file %s: %s\n", curr_dt(dt), cid, line_num, debug_file_name, line);
            continue;
        }
        *equal = '\0';
        uint8_t value = (uint8_t)atoi(equal + 1);

        if (strcmp(line, "log_level") == 0) {
            log_level = value;
            continue;
        }
        char *parts[3] = {0};
        int part_count = 0;
        char *token = strtok(line, ":");

        while (token && part_count < 3) {
            parts[part_count++] = token;
            token = strtok(NULL, ":");
        }

        if (part_count < 2 || part_count > 3) {
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Malformed line %zu in debug file %s: %s\n", curr_dt(dt), cid, line_num, debug_file_name, line);
            continue;
        }
        online_debug_info_t *tmp = realloc(new_list, sizeof(online_debug_info_t) * (new_count + 1));

        if (!tmp) {
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Out of memory while reading %s\n", curr_dt(dt), cid, debug_file_name);
            fclose(file);
            return online_debug_tables_count;
        }
        new_list = tmp;
        new_list[new_count].namespace = strdup(parts[0]);
        new_list[new_count].table_name = strdup(parts[1]);
        new_list[new_count].uuid = (part_count == 3) ? strdup(parts[2]) : NULL;
        new_list[new_count].log_level = value;
        new_count++;
    }

    if (ferror(file)) {
        fprintf(stderr, "%s WARN [HEALTH=%015lu] Error reading debug file %s: %s (errno=%d)\n", curr_dt(dt), cid, debug_file_name, strerror(errno), errno);
        fclose(file);
        return online_debug_tables_count;
    }
    fclose(file);

    for (uint32_t i = 0; i < online_debug_tables_count; i++) {
        int found = 0;

        for (uint32_t j = 0; j < new_count; j++) {
            if (strcmp((*online_debug_info)[i].namespace, new_list[j].namespace) == 0 && strcmp((*online_debug_info)[i].table_name, new_list[j].table_name) == 0 &&
                (((*online_debug_info)[i].uuid == NULL && new_list[j].uuid == NULL) ||
                 ((*online_debug_info)[i].uuid && new_list[j].uuid && strcmp((*online_debug_info)[i].uuid, new_list[j].uuid) == 0))) {
                found = 1;
                break;
            }
        }
        free((*online_debug_info)[i].namespace);
        free((*online_debug_info)[i].table_name);
        free((*online_debug_info)[i].uuid);
    }
    free(*online_debug_info);
    *online_debug_info = new_list;
    return new_count;
}

void rate_rtt_us(uint32_t rtt_us, int *score, const char **grade) {
    if (rtt_us < 100) {
        *score = 5;
        *grade = "Perfect";
    } else if (rtt_us < 200) {
        *score = 4;
        *grade = "Good";
    } else if (rtt_us < 500) {
        *score = 3;
        *grade = "OK";
    } else if (rtt_us < 1000) {
        *score = 2;
        *grade = "Bad";
    } else {
        *score = 1;
        *grade = "Awful";
    }
}

int get_tcp_rtt_us_by_fd(int fd, uint32_t *rtt_us) {
    if (fd < 0)
        return -1;
    struct tcp_info ti;
    socklen_t ti_len = sizeof(ti);
    memset(&ti, 0, sizeof(ti));

    if (getsockopt(fd, IPPROTO_TCP, TCP_INFO, &ti, &ti_len) != 0) {
        return -1;
    }
    *rtt_us = ti.tcpi_rtt;
    return 0;
}

_Noreturn void *pthread_stat() {
    char dt[DT_LOG_SIZE];
    uint64_t cid = get_new_cid();
    fprintf(stderr, "%s INFO [STAT=%015lu] Statistics thread started\n", curr_dt(dt), cid);
    total_memory = get_total_memory();
    start_ts = getCurrentTimeMs();
    sleep(1);
    struct table_info_t *t_info;
    struct region_info_t *r_info;
    online_debug_tables_count = 1;
    online_debug_info = malloc(online_debug_tables_count * sizeof(struct online_debug_info_t *));

    if (online_debug_info == NULL) {
        fprintf(stderr, "%s FATAL [STAT=%015lu] Failed malloc online_debug_info=%lu\n", curr_dt(dt), cid, online_debug_tables_count * sizeof(struct online_debug_info_t *));
        exit(-1);
    }
    online_debug_info[0] = malloc(sizeof(struct online_debug_info_t));

    if (online_debug_info[0] == NULL) {
        fprintf(stderr, "%s FATAL [STAT=%015lu] Failed malloc online_debug_info[0]=%lu\n", curr_dt(dt), cid, sizeof(struct online_debug_info_t));
        exit(-1);
    }
    online_debug_info[0]->namespace = malloc(1);

    if (online_debug_info[0]->namespace == NULL) {
        fprintf(stderr, "%s FATAL [STAT=%015lu] Failed malloc online_debug_info[0]->namespace=1\n", curr_dt(dt), cid);
        exit(-1);
    }
    online_debug_info[0]->table_name = malloc(1);

    if (online_debug_info[0]->table_name == NULL) {
        fprintf(stderr, "%s FATAL [STAT=%015lu] Failed malloc online_debug_info[0]->table_name=1\n", curr_dt(dt), cid);
        exit(-1);
    }
    online_debug_info[0]->uuid = malloc(1);

    if (online_debug_info[0]->uuid == NULL) {
        fprintf(stderr, "%s FATAL [STAT=%015lu] Failed malloc online_debug_info[0]->uuid=1\n", curr_dt(dt), cid);
        exit(-1);
    }
    online_debug_info[0]->log_level = 0;

    for (;;) {
        read_file_prop(prop_file_name, false);
        online_debug_tables_count = read_debug_file(online_debug_info, online_debug_tables_count, cid);
        printStat(t_info, r_info, dt, cid);
        sleep(1);
    }
}
