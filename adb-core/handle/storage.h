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
#ifndef ADB_HANDLE_STORAGE_H
#define ADB_HANDLE_STORAGE_H
#include "../all_headers.h"
#include "../regions-adm/region_utils.h"
#include "../utils/stat_utils.h"
int open_file(char *fn, uint64_t cid);
void write_file(int fd, uint64_t offset, uint8_t *record, uint32_t write_size, uint16_t bsize_factor, region_info_t *r_info, uint64_t cid);
void flush_write_4K_buffer(region_info_t *r_info, uint64_t cid);
bool flush_write_4K_buffer_on_exit(region_info_t *r_info);
int truncate_file(region_info_t *r_info, int fd, uint64_t length, uint64_t cid);
int read_file(int fd, uint8_t *rbuff, uint64_t offset, uint32_t read_size, region_info_t *r_info, uint64_t cid);
void bind_data_000_fd(region_info_t *r_info, int fd, uint64_t cid);
void unbind_data_000_fd(region_info_t *r_info, int fd, uint64_t cid);
uint64_t get_file_size(char *fn);
uint64_t get_file_mtime(char *fn);
void close_data_file(region_info_t *r_info, uint8_t bs, uint8_t b, uint64_t cid);
#endif
