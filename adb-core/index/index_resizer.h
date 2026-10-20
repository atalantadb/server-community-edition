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
#ifndef ADB_INDEX_RESIZER_H
#define ADB_INDEX_RESIZER_H
#include "../utils/voids_service.h"
#define RESIZE_IDX_DONE 0
#define RESIZE_IDX_PREPARING 1
#define RESIZE_IDX_PREPARED 2
#define RESIZE_IDX_RESIZED 3
typedef struct {
    uint32_t f_idx_real_size;
    uint32_t num_chunks;
    uint32_t chunk_size;
} idx_resize_chunks_t;
typedef struct {
    region_info_t *r_info;
    uint8_t b;
    uint8_t bs;
    uint32_t f_idx_size_x2;
    uint32_t f_idx_size;
    uint32_t f_idx_real_size;
    uint32_t num_chunks;
    uint32_t chunk_index;
    uint32_t pure_start;
    uint32_t pure_end;
    uint8_t *idx_moved_flag;
    table_handler_t *t_handler;
    int result;
} resize_index_chunk_thread_arg_t;
bool resize_index_chunk(region_info_t *r_info, uint8_t b, uint32_t f_idx_size_x2, uint8_t bs, uint32_t f_idx_size, uint32_t f_idx_real_size, uint32_t num_chunks,
                        uint32_t chunk_index, uint32_t pure_start, uint32_t pure_end, uint8_t *idx_moved_flag, table_handler_t *t_handler);
bool resize_index_chunk_finalize(region_info_t *r_info, uint8_t b, uint32_t f_idx_size_x2, uint8_t bs, uint32_t f_idx_size, uint32_t f_idx_real_size, uint32_t num_chunks,
                                 uint8_t *idx_moved_flag, table_handler_t *t_handler, uint64_t cid);
void resize_index_create_copy(region_info_t *r_info, uint8_t b, uint8_t bs, idx_t **idx, idx_t **idx_x2, uint8_t **k1_index, uint8_t **k1_index_x2, uint8_t **k2_index,
                              uint8_t **k2_index_x2, uint32_t f_idx_size, const idx_resize_chunks_t *plan, uint64_t cid);
bool resize_thread_runner(struct region_info_t *r_info, uint32_t b, uint8_t bs, table_handler_t *t_handler);
void pthread_idx_resize_service(void *arguments);
void move_index(region_info_t *r_info, uint8_t b, uint32_t i, int16_t jump, uint16_t *shift_keeper, uint8_t *idx_moved_flag, uint8_t bs, idx_t *idx_x3, uint8_t *k1_index_x3,
                uint8_t *k2_index_x3, uint32_t f_idx_size, table_handler_t *t_handler);
bool find_k_idx_x2_local(region_info_t *r_info, uint8_t b, uint32_t *k_idx_io, uint32_t c_idx, uint32_t curr_k_idx, uint8_t bs, table_handler_t *t_handler);
uint8_t check_k_idx(region_info_t *r_info, bool is_keys, uint8_t *hsh, uint8_t *volatile record_keeper, uint32_t new_b_offset, uint8_t bs, table_handler_t *t_handler);
#define INS_IDX_CHECK_ERROR UINT8_MAX
uint32_t resize_index_prepare(region_info_t *r_info, uint8_t b, uint8_t bs, uint64_t cid);
void resize_index_free_prepared(region_info_t *r_info, uint8_t b, uint8_t bs);
bool resize_index(region_info_t *r_info, uint8_t b, uint32_t f_idx_size_x2, uint8_t bs, uint32_t f_idx_size, table_handler_t *t_handler);
bool write_idx_meta(region_info_t *r_info, uint8_t b, uint8_t bs, table_handler_t *t_handler);
void print_idx(char *label, region_info_t *r_info, uint8_t b, uint8_t bs, uint64_t cid);
void calc_skew_idx(region_info_t *r_info, uint8_t b, uint8_t bs, uint64_t cid);
void *pthread_idx_resize_coordinator(void *arg);
#endif
