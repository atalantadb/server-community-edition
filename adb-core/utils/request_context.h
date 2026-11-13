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
#ifndef ADB_REQUEST_CONTEXT_H
#define ADB_REQUEST_CONTEXT_H
#include <stdbool.h>
#include <stdint.h>
union s2b_u {
    uint16_t short_val;
    uint8_t byte[2];
};
typedef struct scan_keys_list_t {
    uint64_t cid;
    uint8_t bs;
    uint32_t curr_offset;
    uint64_t k_count;
    uint64_t k_shift_global;
    union s2b_u key_size;
    uint64_t keys_mem_usage_limit;
    uint64_t max_key_count;
    char *keys;
    char *key_sizes;
    uint64_t last_scan_ts;
    uint32_t scan_block_num;
    uint32_t client_scan_block_num;
    uint32_t last_client_scan_block_num;
    uint32_t last_client_scan_block_count;
    uint32_t scan_block_num_rewind;
    uint32_t scan_block_shift;
} scan_keys_list_t;
typedef struct msg_srv_t {
    uint32_t msg_size;
    uint8_t *resp_buff;
    uint8_t *resp_buff_base;
    uint32_t scan_keys_list_pos;
    scan_keys_list_t *p_scan_keys_list;
    uint32_t curr_max_msg_size;
    uint32_t total_max_msg_size;
    bool allow_multiple_block_scan;
    uint32_t msg_size_zip;
    uint8_t msg_type;
} msg_srv_t;
static inline uint8_t msg_srv_read_access(const msg_srv_t *msg_srv) {
    (void)msg_srv;
    return 0;
}


static inline uint8_t msg_srv_write_access(const msg_srv_t *msg_srv) {
    (void)msg_srv;
    return 0;
}
#endif
