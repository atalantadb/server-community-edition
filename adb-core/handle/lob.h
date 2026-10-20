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
#ifndef KING_SIZE_H
#define KING_SIZE_H
#include "../all_headers.h"
#include "../handle/block_handler.h"
#define WAL_INIT 0
#define WAL_CLEAN 0
#define WAL_INS_LINK_READY 1
#define WAL_INS_LOB_READY 2
#define WAL_DEL_LINK_READY 3
#define WAL_DEL_LOB_READY 4
#define WAL_UPD_INS_LINK_READY 5
#define WAL_UPD_INS_LOB_READY 6
#define WAL_UPD_DEL_LOB_READY 8
#define WAL_UPD_DEL_LINK_READY 10
#define WAL_DEL_DBL_LINK_READY 12
#define WAL_UPD_LINK_TO_DATA_READY 13
#define CRC_HEADER_SIZE 20U
#define HEADER_SIZE 24U
int copy_fd_to_path(int src_fd, const char *dst_path);
bool verify_lob_chains(int fd, uint64_t total_chunks, region_info_t *r_info, uint64_t cid);
uint32_t chk_wal_lob(region_info_t *r_info, uint8_t *buff, table_handler_t *t_handler);
bool wal_lob(region_info_t *r_info, uint32_t msg_size, uint8_t *buff, uint8_t wal_type_chk, uint8_t wal_type, table_handler_t *t_handler);
bool insert_lob(struct region_info_t *r_info, u_int32_t msg_size, uint8_t *buff, table_handler_t *t_handler);
bool cmp_lob_key(region_info_t *r_info, uint32_t small_msg_size, uint8_t *small_msg_buff, uint32_t key_size, table_handler_t *t_handler);
uint32_t get_lob(region_info_t *r_info, uint32_t small_msg_size, uint8_t *small_msg_buff, table_handler_t *t_handler);
#define LOB_MOVE_RECOVERY_FAILED (-1)
#define LOB_MOVE_RECOVERY_NONE 0
#define LOB_MOVE_RECOVERY_DONE 1
int recover_lob_move(region_info_t *r_info, uint32_t wal_size, uint8_t *wal_buff, table_handler_t *t_handler);
bool del_lob(region_info_t *r_info, uint32_t small_msg_size, uint8_t *small_msg_buff, bool repair, table_handler_t *t_handler);
#endif
