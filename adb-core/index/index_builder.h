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
#ifndef ADB_INDEX_BUILDER_H
#define ADB_INDEX_BUILDER_H
#include "../all_headers.h"
#include "../utils/msg_utils.h"
#include "../utils/voids_service.h"
#include "../utils/health.h"
struct arg_struct {
    struct region_info_t *r_info;
    uint32_t b;
    uint8_t bs;
    struct table_handler_t *t_handler;
};
inline static uint8_t calc_zip_record_size(struct region_info_t *r_info, uint32_t vs, uint8_t bs) {
    // TODO Из-за +1 мы теряем 0.3% места в конце каждого 4096 блока. С другой стороны можно использовать это место для хранения инфы о блоке
    return (uint8_t)((vs - 1) / r_info->r_idx_b[bs]->bsize_factor + 1);
}


inline static uint32_t calc_zip_record_size_fix(uint32_t vs) {
    // TODO Из-за +1 мы теряем 0.3% места в конце каждого 4096 блока. С другой стороны можно использовать это место для хранения инфы о блоке
    if (vs == 0)
        return 0;
    return (uint32_t)((vs - 1) / FIX_BSIZE_FACTOR + 1);
}

bool check_meta_quality(const uint8_t *meta_buff, uint32_t meta_size, uint32_t block_size, uint64_t cid);
int8_t get_bs_map_pos(region_info_t *r_info, uint8_t bs, bool create, table_handler_t *t_handler);
bool coll_handler(region_info_t *r_info, msg_cli_t *msg_cli, uint8_t b, uint8_t bs, uint64_t cid);
void build_hash_params(msg_cli_t *msg, region_info_t *r_info, uint8_t bs);
bool build_idx_vars_old(region_info_t *r_info, msg_cli_t *msg_get, uint8_t bs, table_handler_t *t_handler);
int build_idx_vars(region_info_t *r_info, msg_cli_t *msg_get, uint8_t *bs, table_handler_t *t_handler);
bool scan_index(region_info_t *r_info, uint8_t b, uint64_t file_size, uint8_t bs, table_handler_t *t_handler);
bool obsolete_marker_has_winner(region_info_t *r_info, uint8_t *record, size_t available, table_handler_t *t_handler);
uint32_t prepare_idx_pointers(struct region_info_t *r_info, uint8_t bs, uint64_t cid);
uint32_t build_index(struct region_info_t *r_info, uint8_t bs, table_handler_t *t_handler);
bool resize_coll_keeper(struct region_info_t *r_info, uint32_t b, uint8_t bs, uint64_t cid);
bool resize_coll_keeper_x2(struct region_info_t *r_info, uint32_t b, uint8_t bs, uint64_t cid);
bool resize_key_keeper(struct region_info_t *r_info, uint32_t b, uint8_t bs, uint64_t cid);
void free_region(struct region_info_t *r_info, uint64_t cid);
void free_index(struct region_info_t *r_info);
inline static uint8_t calc_zip_record_size(struct region_info_t *r_info, uint32_t vs, uint8_t bs);
inline static uint32_t calc_zip_record_size_fix(uint32_t vs);
bool find_k_idx(region_info_t *r_info, bool save2idx, uint32_t curr_k_idx, uint8_t bs, table_handler_t *t_handler);
bool find_k_idx_x2(region_info_t *r_info, uint32_t curr_k_idx, uint8_t bs, table_handler_t *t_handler);
uint32_t calc_k_idx_x2(region_info_t *r_info, uint8_t b, uint32_t k_idx, uint32_t c_idx, uint8_t bs, uint64_t cid);
int load_idx_x3_from_file(region_info_t *r_info, uint8_t bs);
#endif
