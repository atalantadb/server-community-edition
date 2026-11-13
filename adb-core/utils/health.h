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
#ifndef ADB_HEALTH_H
#define ADB_HEALTH_H
#include "../all_headers.h"
#include "../regions-adm/rs_processor.h"
#include "../regions-adm/region_utils.h"
struct region_info_t;
struct data_keeper_t;
struct table_handler_t;
struct msg_files_list_t;
struct msg_file_info_t;
struct table_handler_t;
#ifdef COLL_LOB_TEST
#define CHUNK_SIZE (1 * 1024 * 1024)
#else
#define CHUNK_SIZE (100 * 1024 * 1024)
#endif
extern uint64_t need_data_chk;
extern _Atomic int replica_need_data_chk_count;
extern uint32_t replica_need_data_chk_capacity;
extern struct region_info_t **r_info_replicas_chk;
extern pthread_mutex_t r_info_chk_mutex;
bool apply_replica_reply_timeout(struct region_info_t *r_info, uint8_t replica_num, int fd, uint64_t cid);
void grow_replica_reply_timeout(struct region_info_t *r_info, uint8_t replica_num, int fd, uint64_t cid);
void reset_replica_reply_timeout(struct region_info_t *r_info, uint8_t replica_num, uint64_t cid);
void del_r_info_chk_element(struct region_info_t *r_info_chk);
void calculate_crc(const char *filename);
int get_month_number(const char *month);
uint64_t calc_num_blocks(long file_size);
uint16_t get_bsize_factor(const char *filename);
void data_fix(struct region_info_t *r_info, uint8_t bs, uint32_t read_block, uint16_t r_real_offset, struct table_handler_t *t_handler);
typedef enum { DATA_QUALITY_ERROR = -1, DATA_QUALITY_BAD = 0, DATA_QUALITY_OK = 1 } data_quality_result_t;
void stop_region_after_key_error(struct region_info_t *r_info, int code, struct table_handler_t *t_handler);
data_quality_result_t check_data_quality(struct region_info_t *r_info, uint8_t bs, uint64_t file_chk_offset, uint64_t file_size, uint8_t *chk_buff, uint64_t cid);
uint32_t build_rsc_msg(struct region_info_t *r_info, uint8_t replica_num, uint8_t msg_type, uint64_t rw_off, uint8_t bs, uint32_t chunk_size);
bool reload_index(struct region_info_t *r_info, uint8_t bs, uint32_t file_size, struct table_handler_t *t_handler);
int get_open_files_limit(rlim_t *soft, rlim_t *hard);
int replica_data_fix_exchange(struct region_info_t *r_info, uint8_t replica_num, uint32_t rsc_msg_size, uint8_t *recv_buff, uint32_t wait_recv_timeout,
                              struct table_handler_t *t_handler);
bool compare_fix_files_lists(struct region_info_t *r_info, uint8_t replica_num, struct msg_files_list_t *files_list1, struct msg_files_list_t *files_list2,
                             struct table_handler_t *t_handler);
bool check_replica(struct region_info_t *r_info, int replica_num, struct table_handler_t *t_handler);
bool check_remote_replica(struct region_info_t *r_info, uint8_t replica_num, struct msg_files_list_t *files_list, struct table_handler_t *t_handler);
bool calculate_xxh128_for_file(struct region_info_t *r_info, bool calc_xxh, const char *filename, struct msg_file_info_t *file_info, uint64_t cid);
struct msg_files_list_t *get_files_list(struct region_info_t *r_info, bool calc_xxh, struct table_handler_t *t_handler);
uint8_t *serialize_files_list(struct msg_files_list_t *files_list, uint32_t *output_size, uint64_t cid);
uint8_t *serialize_files_list_calc_size(const struct msg_files_list_t *files_list, uint32_t *output_size, uint64_t cid);
void free_files_list(struct msg_files_list_t *files_list);
struct msg_files_list_t *deserialize_files_list(const uint8_t *buffer, uint32_t size, uint64_t cid);
void print_files_list(const struct msg_files_list_t *files_list, char *uuid, uint64_t cid);
bool set_region_recovery_info(struct region_info_t *r_info, uint64_t data_chk_ts, struct table_handler_t *t_handler);
bool region_needs_restart_marker(const char *dir_path, uint64_t health_ts, uint64_t cid);
void drop_region_recovery_info(struct region_info_t *r_info, uint64_t cid);
bool shutdown_recovery_marker_valid(struct region_info_t *r_info, uint64_t *marker_ts);
bool shutdown_close_locked_region(struct region_info_t *r_info, uint64_t cid);
bool shutdown_close_locked_group(struct region_info_t *leader, uint64_t cid);
bool shutdown_regions_parallel(struct table_handler_t *t_handler);
void add_data_record_keeper(struct region_info_t *r_info, const char *file_name, uint32_t file_name_size, uint32_t file_pos, const uint8_t *buffer_key, uint32_t buffer_key_size,
                            const uint8_t *buffer_block_data, uint32_t buffer_block_data_size, uint64_t cid);
void save_data_record_keepers_to_file(struct region_info_t *r_info, uint64_t cid);
void free_data_keeper(struct data_keeper_t *data_keeper_record);
void free_all_data_keeper_records(struct region_info_t *r_info);
uint64_t get_data_check_ts(struct region_info_t *r_info, struct table_handler_t *t_handler, char *filename, uint64_t cid);
uint64_t get_last_modify_ts(const char *filename, uint64_t cid);
bool save_data_check(uint64_t cid, bool print_ts_info);
void *pthread_health();
void graceful_exit(int exit_code);
void add_path_to_drv_arr(char *path, uint64_t cid);
void data_care(uint64_t cid);
bool procces_replicas_data_chk(struct region_info_t *r_info, struct table_handler_t *t_handler);
bool process_admin_replicas_data_chk(struct region_info_t *r_info, struct table_handler_t *t_handler);
void *pthread_replicas_chk(void *arg);
int mem_check_before_alloc(size_t bytes_to_alloc, int mem_free_limit_percentage, unsigned int *out_free_after);
void *pthread_data_care();
void *pthread_flush_write_4K_buffers(void *arg);
void migrate_all_workers();
int reopen_stderr(uint64_t cid);
#endif
