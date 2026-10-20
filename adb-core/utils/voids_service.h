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
#ifndef ADB_VOIDS_SERVICE_H
#define ADB_VOIDS_SERVICE_H
#include "../all_headers.h"
#include "../regions-adm/region_utils.h"
#include "../handle/storage.h"
#include "../handle/replica_handler.h"
#include "../index/f_index.h"
typedef struct append_compaction_segment_stats_t {
    uint32_t blocks_checked;
    uint32_t records_checked;
    uint32_t live_records;
    uint32_t stale_records;
    uint64_t live_bytes;
    uint64_t stale_bytes;
} append_compaction_segment_stats_t;
typedef struct append_compaction_record_move_t {
    char digest[DIGEST_SIZE_FULL];
    uint32_t old_block;
    uint32_t new_block;
    uint8_t is_collision;
} append_compaction_record_move_t;
typedef struct append_compaction_plan_t {
    uint8_t b;
    uint8_t bs;
    uint32_t segment;
    uint32_t first_block;
    uint32_t source_blocks;
    uint32_t source_used_blocks;
    uint32_t result_used_blocks;
    uint32_t freed_blocks;
    uint32_t result_free_blocks;
    uint32_t original_b_offset;
    uint32_t tail_range_first_block;
    uint32_t tail_first_block;
    uint32_t tail_blocks;
    uint32_t tail_live_records;
    uint32_t tail_stale_records;
    uint32_t new_b_offset;
    uint32_t truncate_blocks;
    uint64_t result_write_offset;
    uint32_t result_write_size;
    uint64_t original_file_size;
    uint64_t new_file_size;
    uint32_t live_records;
    uint32_t stale_records;
    uint32_t source_record_count;
    uint32_t record_decision_count;
    uint32_t changed_blocks;
    uint64_t removed_garbage_bytes;
    uint64_t source_block_hash[APPEND_COMPACTION_BLOCKS_PER_SEGMENT];
    msg_write_compact_block_meta_t block_meta[APPEND_COMPACTION_BLOCKS_PER_SEGMENT * 2U];
    uint32_t block_meta_count;
    uint8_t result_hash[MSG_WRITE_COMPACT_HASH_SIZE];
    uint8_t source_z_index[APPEND_COMPACTION_BLOCKS_PER_SEGMENT];
    uint8_t result_z_index[APPEND_COMPACTION_BLOCKS_PER_SEGMENT];
    uint8_t tail_z_index[APPEND_COMPACTION_BLOCKS_PER_SEGMENT];
    uint8_t *live_bitmap;
    append_compaction_record_move_t *records;
    uint32_t records_count;
    uint32_t *delete_offsets;
    uint32_t delete_count;
    uint64_t gc_safe_revision, gc_generation, gc_confirmed;
    uint64_t gc_removed_bytes;
    record_gc_marker_stats_t gc_marker_stats;
    uint64_t metric_prepare_us;
    uint64_t metric_source_read_us;
    uint64_t metric_source_lookup_us;
    uint64_t metric_source_pack_us;
    uint64_t metric_tail_read_us;
    uint64_t metric_tail_lookup_us;
    uint64_t metric_tail_pack_us;
    uint64_t metric_verify_us;
    uint64_t metric_hash_us;
    uint64_t metric_source_bytes;
    uint64_t metric_tail_bytes;
    uint64_t metric_index_lookups;
} append_compaction_plan_t;
typedef struct append_compaction_context_t {
    uint8_t *source_buffer;
    uint8_t *result_buffer;
    bool owns_work_buffers;
    uint8_t *live_bitmap;
    uint32_t live_bitmap_capacity_bits;
    append_compaction_record_move_t *records;
    uint32_t records_capacity;
    uint32_t *delete_offsets;
    uint32_t delete_capacity;
} append_compaction_context_t;
typedef struct append_compaction_piggyback_t {
    bool active;
    bool local_applied;
    bool attached_to_write;
    bool truncate_applied;
    region_info_t *r_info;
    append_compaction_context_t context;
    append_compaction_plan_t plan;
} append_compaction_piggyback_t;
#define APPEND_COMPACTION_ANALYZE_OK 0
#define APPEND_COMPACTION_ANALYZE_EMPTY 1
#define APPEND_COMPACTION_ANALYZE_ERROR -1
#define APPEND_COMPACTION_PIGGYBACK_ERROR -1
#define APPEND_COMPACTION_PIGGYBACK_NONE 0
#define APPEND_COMPACTION_PIGGYBACK_PREPARED 1
uint32_t get_append_compaction_budget(region_info_t *r_info, uint8_t bs);
uint32_t get_append_compaction_idle_budget(region_info_t *r_info, uint8_t bs);
int request_append_compaction_shutdown_drain(region_info_t *r_info);
bool replicas_idle_commit_ready(region_info_t *r_info);
bool replicas_idle_commit_load_ready(void);
bool append_compaction_piggyback_ready(region_info_t *r_info, uint8_t b, uint8_t bs, uint8_t request_type);
int prepare_append_compaction_piggyback(region_info_t *r_info, uint8_t request_type, table_handler_t *t_handler);
void free_append_compaction_piggyback(table_handler_t *t_handler);
void complete_append_compaction_piggyback(region_info_t *r_info, table_handler_t *t_handler);
bool get_current_record_block(region_info_t *r_info, msg_cli_t *msg_cli, uint8_t bs, uint32_t *current_block, table_handler_t *t_handler);
int analyze_append_compaction_segment(region_info_t *r_info, uint8_t b, uint8_t bs, uint32_t segment, uint8_t *segment_buff, append_compaction_segment_stats_t *stats,
                                      table_handler_t *t_handler);
bool init_append_compaction_context(append_compaction_context_t *context);
bool init_append_compaction_context_with_workspace(append_compaction_context_t *context, uint8_t *workspace, size_t workspace_size);
void free_append_compaction_context(append_compaction_context_t *context);
int build_append_compaction_plan(region_info_t *r_info, uint8_t b, uint8_t bs, uint32_t segment, append_compaction_context_t *context, append_compaction_plan_t *plan,
                                 table_handler_t *t_handler);
int build_append_compaction_result_from_bitmap(region_info_t *r_info, uint8_t b, uint8_t bs, append_compaction_context_t *context, append_compaction_plan_t *plan,
                                               bool append_tail, bool update_moves, table_handler_t *t_handler);
int verify_append_compaction_replica_plan(region_info_t *r_info, uint8_t b, uint8_t bs, const msg_write_compact_v2_t *header, const uint8_t *block_meta,
                                          const uint8_t *live_bitmap, append_compaction_context_t *context, append_compaction_plan_t *plan, table_handler_t *t_handler);
void log_append_compaction_result_probe(region_info_t *r_info, const append_compaction_context_t *context, const append_compaction_plan_t *plan, const char *stage, uint64_t cid);
void log_append_compaction_disk_probe(region_info_t *r_info, const append_compaction_plan_t *plan, int fd, const char *stage, uint64_t cid);
bool apply_append_compaction_plan_local(region_info_t *r_info, append_compaction_context_t *context, append_compaction_plan_t *plan, table_handler_t *t_handler);
bool apply_append_compaction_plan(region_info_t *r_info, append_compaction_context_t *context, append_compaction_plan_t *plan, table_handler_t *t_handler);
enum { RECORD_GC_RETRY = -1, RECORD_GC_BLOCKED = 0, RECORD_GC_PROGRESS = 1, RECORD_GC_DONE = 2 };
int process_record_gc_visit(region_info_t *, append_compaction_context_t *, table_handler_t *);
uint64_t get_record_gc_safe_revision(region_info_t *);
int process_record_gc_cleanup_visit(region_info_t *, append_compaction_context_t *, table_handler_t *);
bool record_gc_candidate(region_info_t *, bool high_load, uint64_t now_ms);
int process_record_gc_background_visit(region_info_t *, append_compaction_context_t *, table_handler_t *, bool sleeping);
uint32_t use_void(struct region_info_t *r_info, uint8_t b, uint32_t zip_val_size, uint32_t b_offset, uint8_t bs);
uint32_t use_void_block(struct region_info_t *r_info, uint8_t b, uint32_t zip_val_size, uint32_t b_offset, uint8_t bs);
void remove_void(struct region_info_t *r_info, uint8_t b, uint32_t void_pos_this, uint32_t read_off, uint8_t bs);
void add_void(struct region_info_t *r_info, uint8_t b, uint32_t void_pos, uint32_t read_off, uint8_t bs);
int merge_voids(int v, struct region_info_t *r_info, uint8_t b, uint8_t bs, table_handler_t *t_handler);
int merge_compressed_voids(int v, struct region_info_t *r_info, uint8_t b, uint8_t bs, table_handler_t *t_handler);
void move_compressed_void(struct region_info_t *r_info, uint8_t b, uint8_t bs, table_handler_t *t_handler);
void move_void(struct region_info_t *r_info, uint8_t b, uint8_t bs, table_handler_t *t_handler);
void handle_scans(struct region_info_t *r_info, uint8_t b, uint8_t bs, uint32_t l_offset, uint32_t r_offset, msg_cli_t *msg_cli, uint64_t cid);
_Noreturn void *pthread_void_service();
void *pthread_compaction_service(void *arg);
void *pthread_compaction_supervisor(void *arg);
void *pthread_sleeping_compaction_service(void *arg);
#endif
