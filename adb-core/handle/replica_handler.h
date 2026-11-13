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
#ifndef ADB_REPLICA_HANDLER_H
#define ADB_REPLICA_HANDLER_H
#include "../all_headers.h"
#include "../index/index_builder.h"
#define WAITING_CONN -2
#define REPLICA_REPLY_CHECK_REQUIRED (UINT64_C(1) << 32)
#define REPLICA_REPLY_ERROR (UINT64_C(2) << 32)
struct append_compaction_context_t;
struct append_compaction_plan_t;
int init_replica_conn(struct region_info_t *r_info, char *curr_uuid, uint8_t replica_num, bool force, table_handler_t *t_handler);
bool create_reserve_region_map(char *cbuff, region_info_t *r_info, uint8_t rep, table_handler_t *t_handler);
bool replica_refresh_region_map(struct region_info_t *r_info, uint8_t replica_num, bool force, table_handler_t *t_handler);
void waiting_connection(struct region_info_t *r_info, uint8_t replica_num, table_handler_t *t_handler);
bool replica_data_check(struct region_info_t *r_info, uint8_t replica_num, char *buff_reserve, uint32_t msg_size, table_handler_t *t_handler);
void finish_thread(region_info_t *r_info, int call_id, bool do_unlock, table_handler_t *t_handler);
void send_to_replicas(struct region_info_t *r_info, char *new_buff, uint8_t b, uint64_t rw_off, uint32_t new_rsize, bool do_truncate, uint8_t bs, uint8_t replica_msg_type,
                      table_handler_t *t_handler);
bool apply_write_compact_transaction(region_info_t *r_info, const uint8_t *payload, uint32_t payload_size, table_handler_t *t_handler);
void recv_from_replicas(region_info_t *r_info, bool do_retry, uint32_t req_count_chk1, uint32_t req_count_chk2, table_handler_t *t_handler);
void replicas_writer(region_info_t *r_info, char *rbuff, uint8_t b, uint64_t write_off, uint32_t new_rsize, bool do_truncate, uint8_t bs, bool do_retry,
                     table_handler_t *t_handler);
void bulk_replicas_writer(region_info_t *r_info, table_handler_t *t_handler);
bool try_replicas_idle_commit(table_info_t *table, region_info_t *region, table_handler_t *handler);
bool get_all_local_ip();
#endif
