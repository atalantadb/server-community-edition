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
#include "../index/f_index.h"
#include "../utils/stat_utils.h"
#include "../index/index_builder.h"
#include "../handle/lob.h"
void swapbytes(uint8_t *p);
void error_replica_handler(msg_srv_t *msg_srv, region_info_t *r_info, uint64_t cid);
int block_maker(region_info_t *r_info, uint8_t *buff, uint32_t rsize, table_handler_t *t_handler);
int scan_handler_complicated(struct region_info_t *r_info, uint8_t *buff, uint32_t rsize, table_handler_t *t_handler);
int scan_rows(struct region_info_t *r_info, uint8_t *buff, uint32_t rsize, table_handler_t *t_handler);
