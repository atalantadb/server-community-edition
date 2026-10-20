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
#include "../regions-adm/region_utils.h"
#include "../handle/replica_handler.h"
#include "index_builder.h"
#include "../utils/voids_service.h"
#include "index_resizer.h"
#include "../handle/lob.h"
#define MAX_TRY_MEM_ALLOCATE 10
#define MAGIC_NUM 0xF19889A3067dcc81 // TODO Сделать нормально
#define INSERT_OK 0
#define GET_OK 0
#define UPDATE_OK 0
#define DEL_OK 0
#define UPDATE_COLLISION 1
#define UPDATE_TOO_BIG -1
#define INSERT_TOO_BIG -1
#define PUT_FAILED -2
#define GET_PUT_EXTRACT_ERROR -7
#define GET_DOUBLE_NOT_FOUND_IN_COLL -3
#define REC_NOT_FOUND 2
#define REGION_HANDLER_LOCK 100
#define NEW_REGION_HANDLER_LOCK 101
#define NEW_REGION_LOCK 102
#define FIND_REGION_LOCK 103
#define REGION_ADMIN_START_STOP_LOCK 104
#define REGION_ADMIN_UNASSIGN_LOCK 105
#define RESIZE_MOVE_IDX_LOCK 106
#define SET_REPLICA_LOCK 107
#define REGION_HEALTH_LOCK 108
#define TRY_REGION_ADMIN_UNASSIGN_LOCK 109
#define TRY_REGION_ADMIN_START_STOP_LOCK 110
#define IDX_BUILDER_LOCK 111
#define IDX_RESIZER_LOCK 112
#define IDX_BUILDER_WAITING_LOCK 113
#define APPEND_COMPACTION_LOCK 116
#define DATA_CHK_REGION_LOCK 117
#define FIND_REGION_T_LOCK 28
int write_qfl_dict(struct region_info_t *r_info, table_handler_t *t_handler);
int calc_extract_qfls_size(region_info_t *r_info, char *src_buff, uint32_t msg_size, uint32_t key_size, uint32_t cf_size, uint32_t qv_size, uint32_t dst_capacity,
                           uint32_t *required_size, uint8_t bs, uint64_t cid);
int prepare_extract_region_buff(region_info_t *r_info, char *src_buff, uint32_t msg_size, uint32_t key_size, uint32_t cf_size, uint32_t qv_size, uint8_t bs,
                                uint8_t **dst_buff, uint64_t *dst_capacity, uint64_t cid);
void pack_qfl(region_info_t *r_info, uint8_t *buff, msg_cli_t *msg_cli, uint32_t *src_qfl_offset, uint32_t *dst_qfl_offset, uint16_t s2b, uint32_t qfl_to_del);
uint32_t extract_qfls(struct region_info_t *r_info, char *src_buff, char *dst_buff, uint32_t dst_capacity, uint32_t msg_size, uint32_t key_size, uint32_t cf_size,
                      uint32_t qv_size, uint8_t bs, bool force, uint64_t cid);
void del_from_index(struct region_info_t *r_info, uint32_t read_block, msg_cli_t *msg_cli, table_handler_t *t_handler, uint32_t coll_pos, uint8_t bs);
int8_t get_put(region_info_t *r_info, uint32_t read_block, msg_cli_t *msg_cli, uint8_t *buff, table_handler_t *t_handler, uint32_t coll_pos, uint8_t bs);
uint32_t del_in_chain(struct region_info_t *r_info, uint32_t k_idx, uint16_t j_recurse, uint8_t bs, table_handler_t *t_handler);
uint16_t empty_forward_s_ref(struct region_info_t *r_info, uint32_t k_idx, uint16_t s_index_orig, bool r_flag, uint16_t r_count, uint8_t bs);
void empty_s_ref(struct region_info_t *r_info, uint32_t k_idx, uint16_t new_s_idx, uint16_t j_count_bkp, uint16_t j_count_orig, uint8_t bs);
uint32_t s_index_put(struct region_info_t *r_info, msg_cli_t *msg_cli, uint8_t *buff, msg_srv_t *msg_srv, uint64_t cid);
uint32_t index_put(struct region_info_t *r_info, msg_cli_t *msg_cli, uint8_t *buff, table_handler_t *t_handler, uint8_t bs, bool no_insert_coll);
uint32_t s_insert(struct region_info_t *r_info, msg_cli_t *msg_cli, uint8_t *buff, msg_srv_t *msg_srv, bool save2idx, int32_t kpos, uint32_t coll_pos, uint64_t cid);
int get_upd_coll(struct region_info_t *r_info, msg_cli_t *msg_cli, uint8_t *buff, table_handler_t *t_handler, uint8_t bs, bool no_insert_coll, uint32_t k_idx);
uint32_t insert(struct region_info_t *r_info, msg_cli_t *msg_cli, uint8_t *buff, table_handler_t *t_handler, bool save2idx, int32_t kpos, uint32_t coll_pos,
                uint32_t read_block, uint8_t bs);
void oc_index_corrector(struct region_info_t *r_info, uint8_t b, msg_cli_t *msg_get, uint32_t b_offset, uint32_t k_idx, uint8_t bs, table_handler_t *t_handler);
void got_error(region_info_t *r_info, uint8_t *buff, msg_cli_t *msg_cli, uint8_t bs, int result, int call, table_handler_t *t_handler);
bool prepare_append_compaction_dirty_map(region_info_t *r_info, uint32_t block, uint64_t cid);
void mark_append_compaction_segment(region_info_t *r_info, uint32_t block, uint64_t garbage_bytes, uint8_t bs);
bool mark_append_compaction_free_space(region_info_t *r_info, uint32_t first_block, uint32_t end_block, uint8_t bs, uint64_t cid);
void account_append_compaction_block_change(region_info_t *r_info, uint8_t b, uint8_t bs, uint32_t block, uint32_t old_z_index, uint64_t cid);
