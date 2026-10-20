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
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "voids_service.h"
#include "../crypto/xxhash.h"
#define APPEND_COMPACTION_RETRY_DELAY_MS 1000U
static uint64_t append_compaction_monotonic_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000U + (uint64_t)ts.tv_nsec / 1000U;
}


static void append_compaction_update_max(_Atomic uint64_t *value, uint64_t candidate) {
    uint64_t current = atomic_load(value);

    while (current < candidate && !atomic_compare_exchange_weak(value, &current, candidate)) {
    }
}
#include "health.h"
static uint64_t append_compaction_free_bytes(const region_info_t *r_info, uint8_t bs) {
    const idx_region_info_t *idx = r_info->r_idx_b[bs];
    uint32_t last = idx->b_offset[FBP];
    uint64_t capacity = (uint64_t)idx->block_size_zip * idx->bsize_factor;

    if (r_info->enable_block_compression != BLOCK_COMPRESSION_NONE || last == 0 || last >= idx->f_idx_size[FBP] || idx->idx == NULL || idx->idx[FBP] == NULL || capacity == 0 ||
        capacity > FIX_BLOCK_SIZE)
        return 0;
    uint64_t last_used = (uint64_t)idx->idx[FBP][last].z_index * idx->bsize_factor;

    if (last_used > capacity)
        return 0;
    uint64_t logical_capacity = (uint64_t)(last - 1U) * capacity + last_used;
    return logical_capacity > r_info->append_compact_occupied_bytes ? logical_capacity - r_info->append_compact_occupied_bytes : 0;
}

static uint32_t get_append_compaction_budget_internal(region_info_t *r_info, uint8_t bs, bool idle_region) {
    if (r_info == NULL || r_info->r_idx_b == NULL || bs > BLOCK_ARRAY_LAST_POS || r_info->r_idx_b[bs] == NULL || r_info->r_idx_b[bs]->fd[FBP] != r_info->data_000_fd ||
        r_info->r_idx_b[bs]->block_size != FIX_BLOCK_SIZE)
        return 0;
    uint64_t occupied_bytes = r_info->append_compact_occupied_bytes;
    uint64_t garbage_bytes = r_info->append_compact_garbage_bytes;

    if (garbage_bytes > occupied_bytes)
        garbage_bytes = occupied_bytes;
    uint64_t live_bytes = occupied_bytes - garbage_bytes;
    uint64_t free_bytes = append_compaction_free_bytes(r_info, bs);

    if (garbage_bytes == 0 && free_bytes == 0)
        return 0;
    int start_percent = atomic_load(&append_compact_start_percent);
    int stop_percent = atomic_load(&append_compact_client_stop_percent);
    int max_segments_per_visit = atomic_load(&append_compact_max_segment_per_visit);
    int ops_limit = atomic_load(&append_compact_ops_limit);
    uint64_t current_ops = atomic_load(&ops);
    __uint128_t garbage_percent_left = ((__uint128_t)garbage_bytes + free_bytes) * 100U;

    if (max_segments_per_visit < 1)
        max_segments_per_visit = 1;

    if (idle_region || (ops_limit > 0 && current_ops < (uint64_t)ops_limit)) {
        int min_percent = atomic_load(&append_compact_min_percent);

        if (min_percent < 0)
            min_percent = 0;

        if (garbage_percent_left <= (__uint128_t)live_bytes * (uint32_t)min_percent)
            return 0;
        return (uint32_t)max_segments_per_visit;
    } else {
        if (start_percent < 0)
            return 0;

        if (stop_percent < 0)
            stop_percent = 0;

        if (stop_percent > start_percent)
            stop_percent = start_percent;

        if (atomic_load(&r_info->append_compaction_requested)) {
            if (garbage_percent_left <= (__uint128_t)live_bytes * (uint32_t)stop_percent)
                return 0;
        } else if (garbage_percent_left < (__uint128_t)live_bytes * (uint32_t)start_percent) {
            return 0;
        }
    }
    uint64_t start_threshold = (uint32_t)start_percent;
    uint32_t budget = 1;

    if (garbage_percent_left >= (__uint128_t)live_bytes * (start_threshold + 20U))
        budget = (uint32_t)max_segments_per_visit;
    else if (garbage_percent_left >= (__uint128_t)live_bytes * (start_threshold + 15U))
        budget = 8;
    else if (garbage_percent_left >= (__uint128_t)live_bytes * (start_threshold + 10U))
        budget = 4;
    else if (garbage_percent_left >= (__uint128_t)live_bytes * (start_threshold + 5U))
        budget = 2;

    if (budget > (uint32_t)max_segments_per_visit)
        budget = (uint32_t)max_segments_per_visit;
    return budget;
}

uint32_t get_append_compaction_budget(region_info_t *r_info, uint8_t bs) {
    return get_append_compaction_budget_internal(r_info, bs, false);
}

uint32_t get_append_compaction_idle_budget(region_info_t *r_info, uint8_t bs) {
    return get_append_compaction_budget_internal(r_info, bs, true);
}

static bool get_current_record_info(region_info_t *r_info, msg_cli_t *msg_cli, uint8_t bs, uint32_t *current_block, bool *is_collision, char *full_digest,
                                    table_handler_t *t_handler) {
    if (r_info == NULL || msg_cli == NULL || msg_cli->kv == NULL || msg_cli->kv->key == NULL || current_block == NULL || t_handler == NULL || bs > BLOCK_ARRAY_LAST_POS ||
        r_info->r_idx_b[bs] == NULL)
        return false;
    char digest[DIGEST_SIZE_FULL] = {0};
    bool is_full_digest_calc = false;
    calc_hash(msg_cli->kv->key, msg_cli->kv->key_size, digest);

    if (full_digest != NULL) {
        memset(full_digest, 0, DIGEST_SIZE_FULL);
        memcpy(full_digest, digest, DIGEST_SIZE_SMALL);
    }

    if (is_collision != NULL)
        *is_collision = false;
    uint8_t b = digest[BUCKET_POS] % BUCKETS;
    int coll_count = atomic_load(&r_info->r_idx_b[bs]->coll_count_curr[b]);

    if (coll_count > 0 && r_info->r_idx_b[bs]->coll_keeper != NULL && r_info->r_idx_b[bs]->coll_keeper[b] != NULL) {
        for (uint32_t c = 0; c < (uint32_t)coll_count * (uint32_t)COLL_KEEPER_SIZE; c += (uint32_t)COLL_KEEPER_SIZE) {
            if ((uint8_t)digest[0] == r_info->r_idx_b[bs]->coll_keeper[b][c] && (uint8_t)digest[1] == r_info->r_idx_b[bs]->coll_keeper[b][c + 1]) {
                if (!is_full_digest_calc) {
                    calc_hash_full(msg_cli->kv->key, msg_cli->kv->key_size, digest, DIGEST_SIZE_SMALL);
                    is_full_digest_calc = true;

                    if (full_digest != NULL)
                        memcpy(full_digest, digest, DIGEST_SIZE_FULL);
                }
            }

            if (is_full_digest_calc && memcmp(digest, r_info->r_idx_b[bs]->coll_keeper[b] + c, (uint32_t)DIGEST_SIZE_FULL) == 0) {
                union i2b_u i2b;
                i2b.integer_val = 0;
                memcpy(i2b.byte, r_info->r_idx_b[bs]->coll_keeper[b] + c + (uint32_t)DIGEST_SIZE_FULL, (uint32_t)OFFSET_COLL_KEEPER_SIZE);

                if (i2b.integer_val == 0)
                    return false;
                *current_block = i2b.integer_val;

                if (is_collision != NULL)
                    *is_collision = true;
                return true;
            }
        }
    }
    memcpy(r_info->digest, digest, DIGEST_SIZE_SMALL);
    r_info->has_digest_calc = true;
    build_hash_params(msg_cli, r_info, bs);
    r_info->has_digest_calc = false;

    if (!find_k_idx(r_info, false, 0, bs, t_handler))
        return false;
    b = r_info->r_idx_b[bs]->idx_r_help->b;
    uint32_t k_idx = r_info->r_idx_b[bs]->idx_r_help->k_idx;
    uint32_t c_idx = r_info->r_idx_b[bs]->idx_r_help->c_idx;

    if (k_idx >= r_info->r_idx_b[bs]->f_idx_size[b] || r_info->r_idx_b[bs]->idx[b][k_idx].c_index != c_idx || r_info->r_idx_b[bs]->idx[b][k_idx].o_index == 0)
        return false;
    *current_block = r_info->r_idx_b[bs]->idx[b][k_idx].o_index;
    return true;
}

bool get_current_record_block(region_info_t *r_info, msg_cli_t *msg_cli, uint8_t bs, uint32_t *current_block, table_handler_t *t_handler) {
    return get_current_record_info(r_info, msg_cli, bs, current_block, NULL, NULL, t_handler);
}

static int append_compaction_segment_error(region_info_t *r_info, uint8_t b, uint8_t bs, uint32_t segment, uint32_t block, uint32_t offset, const char *reason,
                                           table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Append compaction analyze failed for %s[%u], bs=%u, segment=%u, block=%u, offset=%u: %s\n", curr_dt(dt), t_handler->cid,
            r_info->r_idx_b[bs]->fname, b, bs, segment, block, offset, reason);
    return APPEND_COMPACTION_ANALYZE_ERROR;
}


static int append_compaction_replica_state_mismatch(region_info_t *r_info, uint8_t b, uint8_t bs, uint32_t segment, uint32_t block, uint32_t offset, const char *reason,
                                                    table_handler_t *t_handler) {
    if (log_level > 1) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr,
                "%s DEBUG [COMPACT_SERVICE=%015lu] Append compaction replica source state differs for %s[%u], bs=%u, segment=%u, block=%u, offset=%u: %s; checking final state\n",
                curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, b, bs, segment, block, offset, reason);
    }
    return APPEND_COMPACTION_ANALYZE_ERROR;
}

int analyze_append_compaction_segment(region_info_t *r_info, uint8_t b, uint8_t bs, uint32_t segment, uint8_t *segment_buff, append_compaction_segment_stats_t *stats,
                                      table_handler_t *t_handler) {
    if (r_info == NULL || segment_buff == NULL || stats == NULL || t_handler == NULL || bs > BLOCK_ARRAY_LAST_POS || r_info->r_idx_b[bs] == NULL || b >= BUCKETS ||
        r_info->r_idx_b[bs]->idx[b] == NULL)
        return APPEND_COMPACTION_ANALYZE_ERROR;
    memset(stats, 0, sizeof(*stats));

    if (r_info->r_idx_b[bs]->fd[b] != r_info->data_000_fd || r_info->r_idx_b[bs]->block_size != FIX_BLOCK_SIZE || r_info->enable_block_compression != 0 ||
        r_info->r_idx_b[bs]->bsize_factor == 0)
        return append_compaction_segment_error(r_info, b, bs, segment, 0, 0, "unsupported data file format", t_handler);

    if (segment > UINT32_MAX / APPEND_COMPACTION_BLOCKS_PER_SEGMENT)
        return append_compaction_segment_error(r_info, b, bs, segment, 0, 0, "segment number overflow", t_handler);
    uint32_t first_block = segment * APPEND_COMPACTION_BLOCKS_PER_SEGMENT;
    uint32_t last_file_block = r_info->r_idx_b[bs]->b_offset[b];

    if (first_block > last_file_block)
        return APPEND_COMPACTION_ANALYZE_EMPTY;

    if (last_file_block >= r_info->r_idx_b[bs]->f_idx_size[b])
        return append_compaction_segment_error(r_info, b, bs, segment, last_file_block, 0, "last block exceeds index size", t_handler);
    uint32_t blocks_to_read = APPEND_COMPACTION_BLOCKS_PER_SEGMENT;

    if (blocks_to_read > last_file_block - first_block + 1)
        blocks_to_read = last_file_block - first_block + 1;
    uint32_t read_size = blocks_to_read * FIX_BLOCK_SIZE;
    int bytes_read = read_file(r_info->r_idx_b[bs]->fd[b], segment_buff, (uint64_t)first_block * FIX_BLOCK_SIZE, read_size, r_info, t_handler->cid);

    if (bytes_read < 0)
        return append_compaction_segment_error(r_info, b, bs, segment, first_block, 0, "read_file failed", t_handler);
    msg_cli_t msg_cli = {0};
    msg_kv_t msg_kv = {0};
    struct msg_qv_t msg_qv = {0};
    msg_cli.kv = &msg_kv;
    msg_kv.qv = &msg_qv;

    for (uint32_t block_pos = 0; block_pos < blocks_to_read; block_pos++) {
        uint32_t block = first_block + block_pos;

        if (block == 0)
            continue;
        uint32_t used_bytes = (uint32_t)r_info->r_idx_b[bs]->idx[b][block].z_index * r_info->r_idx_b[bs]->bsize_factor;

        if (used_bytes > FIX_BLOCK_SIZE)
            return append_compaction_segment_error(r_info, b, bs, segment, block, 0, "z_index exceeds 4-KB block", t_handler);

        if ((uint64_t)block_pos * FIX_BLOCK_SIZE + used_bytes > (uint32_t)bytes_read)
            return append_compaction_segment_error(r_info, b, bs, segment, block, 0, "z_index exceeds bytes read", t_handler);
        stats->blocks_checked++;
        uint32_t block_offset = block_pos * FIX_BLOCK_SIZE;
        uint32_t record_offset = 0;

        while (record_offset < used_bytes) {
            if (used_bytes - record_offset < KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET)
                return append_compaction_segment_error(r_info, b, bs, segment, block, record_offset, "record header exceeds z_index", t_handler);
            uint8_t *record = segment_buff + block_offset + record_offset;
            uint32_t msg_size = 0;
            uint32_t key_size = 0;
            uint32_t cf_size = 0;
            memcpy(&msg_size, record, sizeof(msg_size));
            memcpy(&key_size, record + KEY_SIZE_RECORD_OFFSET, sizeof(key_size));
            memcpy(&cf_size, record + CF_SIZE_RECORD_OFFSET, sizeof(cf_size));

            if (msg_size == 0)
                return append_compaction_segment_error(r_info, b, bs, segment, block, record_offset, "zero-sized record inside used block part", t_handler);

            if (msg_size < KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET || key_size > msg_size - (KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET) ||
                cf_size > msg_size - (KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET + key_size))
                return append_compaction_segment_error(r_info, b, bs, segment, block, record_offset, "record key/cf exceeds msg_size", t_handler);
            uint32_t aligned_size = (uint32_t)calc_zip_record_size(r_info, msg_size, bs) * r_info->r_idx_b[bs]->bsize_factor;

            if (aligned_size == 0 || aligned_size > used_bytes - record_offset || aligned_size > FIX_BLOCK_SIZE - record_offset)
                return append_compaction_segment_error(r_info, b, bs, segment, block, record_offset, "aligned record exceeds block or z_index", t_handler);
            msg_cli.msg_size = msg_size;
            msg_kv.key_size = key_size;
            msg_kv.cf_size = cf_size;
            msg_kv.key = record + KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET;
            uint32_t current_block = 0;
            bool is_live = get_current_record_block(r_info, &msg_cli, bs, &current_block, t_handler) && current_block == block;
            stats->records_checked++;

            if (is_live) {
                stats->live_records++;
                stats->live_bytes += aligned_size;
            } else {
                stats->stale_records++;
                stats->stale_bytes += aligned_size;
            }
            record_offset += aligned_size;
        }

        if (record_offset != used_bytes)
            return append_compaction_segment_error(r_info, b, bs, segment, block, record_offset, "records do not end at z_index", t_handler);
    }
    return APPEND_COMPACTION_ANALYZE_OK;
}

bool init_append_compaction_context(append_compaction_context_t *context) {
    if (context == NULL)
        return false;
    memset(context, 0, sizeof(*context));
    context->source_buffer = malloc(APPEND_COMPACTION_SEGMENT_SIZE);

    if (context->source_buffer == NULL)
        return false;
    context->result_buffer = malloc(APPEND_COMPACTION_SEGMENT_SIZE);

    if (context->result_buffer == NULL) {
        free(context->source_buffer);
        context->source_buffer = NULL;
        return false;
    }
    context->owns_work_buffers = true;
    return true;
}


bool init_append_compaction_context_with_workspace(append_compaction_context_t *context, uint8_t *workspace, size_t workspace_size) {
    if (context == NULL || workspace == NULL || workspace_size < 2U * (size_t)APPEND_COMPACTION_SEGMENT_SIZE)
        return false;
    memset(context, 0, sizeof(*context));
    context->source_buffer = workspace;
    context->result_buffer = workspace + APPEND_COMPACTION_SEGMENT_SIZE;
    context->owns_work_buffers = false;
    return true;
}

void free_append_compaction_context(append_compaction_context_t *context) {
    if (context == NULL)
        return;

    if (context->owns_work_buffers) {
        free(context->source_buffer);
        free(context->result_buffer);
    }
    free(context->live_bitmap);
    free(context->records);
    free(context->delete_offsets);
    memset(context, 0, sizeof(*context));
}

static bool reserve_append_compaction_record(append_compaction_context_t *context, uint32_t required, region_info_t *r_info, table_handler_t *t_handler) {
    if (required <= context->records_capacity)
        return true;
    uint32_t new_capacity = context->records_capacity == 0 ? 256U : context->records_capacity * 2U;

    if (new_capacity < required)
        new_capacity = required;

    if (new_capacity > APPEND_COMPACTION_SEGMENT_SIZE / (KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET))
        return false;
    append_compaction_record_move_t *records = realloc(context->records, (size_t)new_capacity * sizeof(*records));

    if (records == NULL) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Failed allocate append compaction plan for %s, records=%u\n", curr_dt(dt), t_handler->cid, r_info->uuid, new_capacity);
        return false;
    }
    context->records = records;
    context->records_capacity = new_capacity;
    return true;
}


static bool parse_append_record(uint8_t *record, uint32_t available, region_info_t *r_info, uint8_t bs, msg_cli_t *msg_cli, uint32_t *aligned_size) {
    disk_record_header_t h;

    if (!disk_record_decode_header(record, available, &h))
        return false;
    uint32_t record_size = (uint32_t)calc_zip_record_size(r_info, h.msg_size, bs) * r_info->r_idx_b[bs]->bsize_factor;

    if (record_size == 0 || record_size > available)
        return false;
    msg_cli->msg_size = h.msg_size;
    msg_cli->kv->key_size = h.key_size;
    msg_cli->kv->cf_size = h.cf_size;
    msg_cli->kv->qv_size = h.qv_size;
    msg_cli->kv->key = record + KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET;
    *aligned_size = record_size;
    return true;
}

static bool append_block_has_key(const uint8_t *block, uint32_t used, const msg_cli_t *msg, uint32_t factor) {
    for (uint32_t off = 0; off < used;) {
        disk_record_header_t h;

        if (!disk_record_decode_header(block + off, used - off, &h))
            return true;

        if (h.key_size == msg->kv->key_size && memcmp(block + off + DISK_RECORD_HEADER_SIZE, msg->kv->key, h.key_size) == 0)
            return true;
        off += (uint32_t)align_up(h.msg_size, factor);
    }
    return false;
}

static bool reserve_append_compaction_decision(append_compaction_context_t *context, uint32_t required_bits, region_info_t *r_info, table_handler_t *t_handler) {
    if (required_bits <= context->live_bitmap_capacity_bits)
        return true;
    uint32_t maximum_bits = (2U * APPEND_COMPACTION_SEGMENT_SIZE) / (KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET);

    if (required_bits > maximum_bits)
        return false;
    uint32_t new_capacity_bits = context->live_bitmap_capacity_bits == 0 ? 1024U : context->live_bitmap_capacity_bits * 2U;

    if (new_capacity_bits < required_bits)
        new_capacity_bits = required_bits;

    if (new_capacity_bits > maximum_bits)
        new_capacity_bits = maximum_bits;
    size_t old_size = ((size_t)context->live_bitmap_capacity_bits + 7U) / 8U;
    size_t new_size = ((size_t)new_capacity_bits + 7U) / 8U;
    uint8_t *live_bitmap = realloc(context->live_bitmap, new_size);

    if (live_bitmap == NULL) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Failed allocate append compaction bitmap for %s, bits=%u\n", curr_dt(dt), t_handler->cid, r_info->uuid,
                new_capacity_bits);
        return false;
    }
    memset(live_bitmap + old_size, 0, new_size - old_size);
    context->live_bitmap = live_bitmap;
    context->live_bitmap_capacity_bits = new_capacity_bits;
    return true;
}

static bool append_compaction_add_decision(append_compaction_context_t *context, append_compaction_plan_t *plan, bool is_live, region_info_t *r_info,
                                           table_handler_t *t_handler) {
    if (!reserve_append_compaction_decision(context, plan->record_decision_count + 1U, r_info, t_handler))
        return false;
    uint32_t decision = plan->record_decision_count++;
    uint8_t mask = (uint8_t)(1U << (decision & 7U));

    if (is_live)
        context->live_bitmap[decision / 8U] |= mask;
    else
        context->live_bitmap[decision / 8U] &= (uint8_t)~mask;
    plan->live_bitmap = context->live_bitmap;
    return true;
}

static bool append_compaction_decision_is_live(const append_compaction_plan_t *plan, uint32_t decision) {
    return decision < plan->record_decision_count && (plan->live_bitmap[decision / 8U] & (uint8_t)(1U << (decision & 7U))) != 0;
}

static void append_compaction_hash128(const uint8_t *data, size_t size, uint8_t hash[MSG_WRITE_COMPACT_HASH_SIZE]) {
    XXH128_canonical_t canonical;
    XXH128_canonicalFromHash(&canonical, XXH3_128bits(data, size));
    memcpy(hash, canonical.digest, MSG_WRITE_COMPACT_HASH_SIZE);
}

static void append_compaction_init_block_meta(msg_write_compact_block_meta_t *meta, uint32_t block, uint32_t used_bytes, const uint8_t *block_data) {
    memset(meta, 0, sizeof(*meta));
    meta->block = block;
    meta->used_bytes = (uint16_t)used_bytes;
    append_compaction_hash128(block_data, FIX_BLOCK_SIZE, meta->input_hash);
}


static bool append_compaction_add_block_record(msg_write_compact_block_meta_t *meta) {
    if (meta->record_count == UINT16_MAX)
        return false;
    meta->record_count++;
    return true;
}


static bool append_compaction_finalize_bitmap_metadata(append_compaction_plan_t *plan, append_compaction_context_t *context) {
    if (plan->source_blocks == 0 || plan->source_blocks > APPEND_COMPACTION_BLOCKS_PER_SEGMENT || plan->tail_blocks > APPEND_COMPACTION_BLOCKS_PER_SEGMENT ||
        plan->source_blocks > UINT32_MAX - plan->tail_blocks)
        return false;
    plan->block_meta_count = plan->source_blocks + plan->tail_blocks;
    uint64_t source_records = 0;
    uint64_t total_records = 0;

    for (uint32_t i = 0; i < plan->block_meta_count; i++) {
        const msg_write_compact_block_meta_t *meta = &plan->block_meta[i];
        uint32_t expected_block = i < plan->source_blocks ? plan->first_block + i : plan->tail_first_block + (i - plan->source_blocks);

        if (meta->block != expected_block || meta->used_bytes > FIX_BLOCK_SIZE || (meta->block == 0 && (meta->used_bytes != FIX_BLOCK_SIZE || meta->record_count != 0)) ||
            (meta->block != 0 && ((meta->used_bytes == 0) != (meta->record_count == 0))))
            return false;
        total_records += meta->record_count;

        if (i < plan->source_blocks)
            source_records += meta->record_count;
    }

    if (source_records != plan->source_record_count || total_records != plan->record_decision_count)
        return false;
    append_compaction_hash128(context->result_buffer, plan->result_write_size, plan->result_hash);
    return true;
}

int build_append_compaction_result_from_bitmap(region_info_t *r_info, uint8_t b, uint8_t bs, append_compaction_context_t *context, append_compaction_plan_t *plan,
                                               bool append_tail, bool update_moves, table_handler_t *t_handler) {
    if (r_info == NULL || context == NULL || plan == NULL || t_handler == NULL || context->source_buffer == NULL || context->result_buffer == NULL || bs > BLOCK_ARRAY_LAST_POS ||
        r_info->r_idx_b == NULL || r_info->r_idx_b[bs] == NULL || r_info->r_idx_b[bs]->bsize_factor == 0 || plan->source_blocks > APPEND_COMPACTION_BLOCKS_PER_SEGMENT ||
        plan->tail_blocks > APPEND_COMPACTION_BLOCKS_PER_SEGMENT || plan->source_record_count > plan->record_decision_count || plan->tail_live_records > plan->live_records ||
        (plan->record_decision_count != 0 && plan->live_bitmap == NULL) || (update_moves && plan->records_count != 0 && plan->records == NULL))
        return APPEND_COMPACTION_ANALYZE_ERROR;
    msg_cli_t msg_cli = {0};
    msg_kv_t msg_kv = {0};
    struct msg_qv_t msg_qv = {0};
    msg_cli.kv = &msg_kv;
    msg_kv.qv = &msg_qv;
    uint32_t first_data_pos = plan->first_block == 0 ? 1U : 0U;
    uint32_t destination_pos = first_data_pos;
    uint32_t destination_used = 0;

    if (!append_tail) {
        memset(plan->result_z_index, 0, sizeof(plan->result_z_index));

        if (plan->first_block == 0)
            memcpy(context->result_buffer, context->source_buffer, FIX_BLOCK_SIZE);
    } else {
        for (uint32_t block_pos = first_data_pos; block_pos < plan->source_blocks; block_pos++) {
            if (plan->result_z_index[block_pos] != 0) {
                destination_pos = block_pos;
                destination_used = (uint32_t)plan->result_z_index[block_pos] * r_info->r_idx_b[bs]->bsize_factor;
            }
        }
    }
    uint64_t result_capacity_calc = (uint64_t)r_info->r_idx_b[bs]->block_size_zip * r_info->r_idx_b[bs]->bsize_factor;

    if (result_capacity_calc == 0 || result_capacity_calc > FIX_BLOCK_SIZE)
        return append_compaction_segment_error(r_info, b, bs, plan->segment, plan->first_block, 0, "bitmap result block capacity is invalid", t_handler);
    uint32_t result_capacity = (uint32_t)result_capacity_calc;
    uint32_t decision = append_tail ? plan->source_record_count : 0U;
    uint32_t live_record = append_tail ? plan->live_records - plan->tail_live_records : 0U;
    uint32_t range_first_block = append_tail ? plan->tail_first_block : plan->first_block;
    uint32_t range_blocks = append_tail ? plan->tail_blocks : plan->source_blocks;
    uint8_t *range_z_index = append_tail ? plan->tail_z_index : plan->source_z_index;

    for (uint32_t block_pos = 0; block_pos < range_blocks; block_pos++) {
        uint32_t source_block = range_first_block + block_pos;

        if (source_block == 0)
            continue;
        uint32_t used_bytes = (uint32_t)range_z_index[block_pos] * r_info->r_idx_b[bs]->bsize_factor;

        if (used_bytes > FIX_BLOCK_SIZE)
            return append_compaction_segment_error(r_info, b, bs, plan->segment, source_block, 0, "bitmap input z_index exceeds block", t_handler);
        uint32_t record_offset = 0;

        while (record_offset < used_bytes) {
            uint32_t aligned_size = 0;
            uint8_t *input_record = context->source_buffer + block_pos * FIX_BLOCK_SIZE + record_offset;

            if (!parse_append_record(input_record, used_bytes - record_offset, r_info, bs, &msg_cli, &aligned_size) || aligned_size > FIX_BLOCK_SIZE - record_offset)
                return append_compaction_segment_error(r_info, b, bs, plan->segment, source_block, record_offset, "bitmap input record exceeds block or z_index", t_handler);
            uint32_t expected_decisions = append_tail ? plan->record_decision_count : plan->source_record_count;

            if (decision >= expected_decisions)
                return append_compaction_segment_error(r_info, b, bs, plan->segment, source_block, record_offset, "bitmap has fewer decisions than input records", t_handler);

            if (append_compaction_decision_is_live(plan, decision)) {
                if (aligned_size > result_capacity)
                    return append_compaction_segment_error(r_info, b, bs, plan->segment, source_block, record_offset, "bitmap live record exceeds result block capacity",
                                                           t_handler);

                if (destination_used + aligned_size > result_capacity ||
                    append_block_has_key(context->result_buffer + destination_pos * FIX_BLOCK_SIZE, destination_used, &msg_cli, r_info->r_idx_b[bs]->bsize_factor)) {
                    destination_pos++;
                    destination_used = 0;
                }

                if (destination_pos >= plan->source_blocks || (update_moves && live_record >= plan->records_count))
                    return append_compaction_segment_error(r_info, b, bs, plan->segment, source_block, record_offset, "bitmap live records exceed result plan", t_handler);
                uint32_t new_block = plan->first_block + destination_pos;
                uint8_t *result_record = context->result_buffer + destination_pos * FIX_BLOCK_SIZE + destination_used;
                memcpy(result_record, input_record, msg_cli.msg_size);

                if (aligned_size > msg_cli.msg_size)
                    memset(result_record + msg_cli.msg_size, 0, aligned_size - msg_cli.msg_size);
                destination_used += aligned_size;
                plan->result_z_index[destination_pos] = (uint8_t)(destination_used / r_info->r_idx_b[bs]->bsize_factor);

                if (update_moves)
                    plan->records[live_record].new_block = new_block;
                live_record++;
            }
            decision++;
            record_offset += aligned_size;
        }

        if (record_offset != used_bytes)
            return append_compaction_segment_error(r_info, b, bs, plan->segment, source_block, record_offset, "bitmap records do not end at z_index", t_handler);
    }
    uint32_t expected_decisions = append_tail ? plan->record_decision_count : plan->source_record_count;
    uint32_t expected_live_records = append_tail ? plan->live_records : plan->live_records - plan->tail_live_records;

    if (decision != expected_decisions || live_record != expected_live_records || (update_moves && live_record != plan->records_count))
        return append_compaction_segment_error(r_info, b, bs, plan->segment, plan->first_block, 0, "bitmap result counts differ from plan", t_handler);
    plan->result_used_blocks = 0;

    for (uint32_t block_pos = first_data_pos; block_pos < plan->source_blocks; block_pos++)
        if (plan->result_z_index[block_pos] != 0)
            plan->result_used_blocks++;
    return APPEND_COMPACTION_ANALYZE_OK;
}

static int finalize_append_compaction_result_buffer(region_info_t *r_info, uint8_t b, uint8_t bs, append_compaction_context_t *context, append_compaction_plan_t *plan,
                                                    table_handler_t *t_handler) {
    if (r_info == NULL || context == NULL || plan == NULL || t_handler == NULL || context->result_buffer == NULL || r_info->r_idx_b == NULL || bs > BLOCK_ARRAY_LAST_POS ||
        r_info->r_idx_b[bs] == NULL || r_info->r_idx_b[bs]->bsize_factor == 0 || plan->source_blocks > APPEND_COMPACTION_BLOCKS_PER_SEGMENT)
        return APPEND_COMPACTION_ANALYZE_ERROR;

    for (uint32_t block_pos = 0; block_pos < plan->source_blocks; block_pos++) {
        uint32_t block = plan->first_block + block_pos;
        uint32_t used_bytes = block == 0 ? FIX_BLOCK_SIZE : (uint32_t)plan->result_z_index[block_pos] * r_info->r_idx_b[bs]->bsize_factor;

        if (used_bytes > FIX_BLOCK_SIZE)
            return append_compaction_segment_error(r_info, b, bs, plan->segment, block, used_bytes, "result z_index exceeds block before finalization", t_handler);

        if (used_bytes < FIX_BLOCK_SIZE)
            memset(context->result_buffer + block_pos * FIX_BLOCK_SIZE + used_bytes, 0, FIX_BLOCK_SIZE - used_bytes);
    }
    return APPEND_COMPACTION_ANALYZE_OK;
}

static int append_compaction_pread_exact(int fd, uint8_t *buffer, uint64_t offset, uint32_t size) {
    if (buffer == NULL || fd < 0 || offset > (uint64_t)INT64_MAX || size > (uint64_t)INT64_MAX - offset) {
        errno = EOVERFLOW;
        return -1;
    }
    uint32_t bytes_read = 0;

    while (bytes_read < size) {
        ssize_t result = pread(fd, buffer + bytes_read, size - bytes_read, (off_t)(offset + bytes_read));

        if (result < 0 && errno == EINTR)
            continue;

        if (result < 0)
            return -1;

        if (result == 0)
            break;
        bytes_read += (uint32_t)result;
    }
    return (int)bytes_read;
}

static void append_compaction_probe_block(const uint8_t *block, uint32_t *nonzero_bytes, int32_t *first_nonzero, uint64_t *hash) {
    *nonzero_bytes = 0;
    *first_nonzero = -1;

    for (uint32_t i = 0; i < FIX_BLOCK_SIZE; i++) {
        if (block[i] != 0) {
            if (*first_nonzero < 0)
                *first_nonzero = (int32_t)i;
            (*nonzero_bytes)++;
        }
    }
    *hash = XXH3_64bits(block, FIX_BLOCK_SIZE);
}


static void append_compaction_print_probe(region_info_t *r_info, const append_compaction_plan_t *plan, const char *stage, uint64_t cid, uint64_t full_hash,
                                          const uint8_t *first_block, const uint8_t *last_block) {
    if (log_level <= 2 || r_info == NULL || plan == NULL || stage == NULL || first_block == NULL || last_block == NULL || plan->source_blocks == 0)
        return;
    uint32_t last_pos = plan->source_blocks - 1U;
    uint32_t first_nonzero_count = 0;
    uint32_t last_nonzero_count = 0;
    int32_t first_nonzero_offset = -1;
    int32_t last_nonzero_offset = -1;
    uint64_t first_hash = 0;
    uint64_t last_hash = 0;
    append_compaction_probe_block(first_block, &first_nonzero_count, &first_nonzero_offset, &first_hash);
    append_compaction_probe_block(last_block, &last_nonzero_count, &last_nonzero_offset, &last_hash);
    uint32_t last_used = (uint32_t)plan->result_z_index[last_pos] * r_info->r_idx_b[plan->bs]->bsize_factor;
    uint32_t sample_offset = last_nonzero_offset < 0 ? 0U : (uint32_t)last_nonzero_offset;

    if (sample_offset > FIX_BLOCK_SIZE - 8U)
        sample_offset = FIX_BLOCK_SIZE - 8U;
    uint64_t last_block_offset = plan->result_write_offset + (uint64_t)last_pos * FIX_BLOCK_SIZE;
    uint64_t buffered_end = r_info->write_4K_file_offset + r_info->write_4K_buff_used;
    bool buffered_overlaps_last = r_info->write_4K_buff_used != 0 && r_info->write_4K_file_offset < last_block_offset + FIX_BLOCK_SIZE && buffered_end > last_block_offset;
    char dt[DT_LOG_SIZE];
    fprintf(stderr,
            "%s TRACE [COMPACT_PROBE=%015lu] stage=%s uuid=%s segment=%u range=[%u,%u) "
            "result_blocks=%u last_z=%u last_used=%u full_hash=%016lx first_hash=%016lx "
            "first_nonzero=%u/%d last_hash=%016lx last_nonzero=%u/%d "
            "last_sample_offset=%u last_sample=%02x%02x%02x%02x%02x%02x%02x%02x "
            "write4k_fd=%d write4k_offset=%lu write4k_used=%u write4k_overlaps_last=%d "
            "result_hash=%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x\n",
            curr_dt(dt), cid, stage, r_info->uuid, plan->segment, plan->first_block, plan->first_block + plan->source_blocks, plan->result_used_blocks,
            plan->result_z_index[last_pos], last_used, (unsigned long)full_hash, (unsigned long)first_hash, first_nonzero_count, first_nonzero_offset, (unsigned long)last_hash,
            last_nonzero_count, last_nonzero_offset, sample_offset, last_block[sample_offset], last_block[sample_offset + 1U], last_block[sample_offset + 2U],
            last_block[sample_offset + 3U], last_block[sample_offset + 4U], last_block[sample_offset + 5U], last_block[sample_offset + 6U], last_block[sample_offset + 7U],
            r_info->write_4K_fd, r_info->write_4K_file_offset, r_info->write_4K_buff_used, buffered_overlaps_last, plan->result_hash[0], plan->result_hash[1],
            plan->result_hash[2], plan->result_hash[3], plan->result_hash[4], plan->result_hash[5], plan->result_hash[6], plan->result_hash[7], plan->result_hash[8],
            plan->result_hash[9], plan->result_hash[10], plan->result_hash[11], plan->result_hash[12], plan->result_hash[13], plan->result_hash[14], plan->result_hash[15]);
}

void log_append_compaction_result_probe(region_info_t *r_info, const append_compaction_context_t *context, const append_compaction_plan_t *plan, const char *stage,
                                        uint64_t cid) {
    if (log_level <= 2 || context == NULL || context->result_buffer == NULL || plan == NULL || plan->source_blocks == 0 ||
        plan->result_write_size != plan->source_blocks * FIX_BLOCK_SIZE)
        return;
    const uint8_t *first_block = context->result_buffer;
    const uint8_t *last_block = context->result_buffer + (plan->source_blocks - 1U) * FIX_BLOCK_SIZE;
    uint64_t full_hash = XXH3_64bits(context->result_buffer, plan->result_write_size);
    append_compaction_print_probe(r_info, plan, stage, cid, full_hash, first_block, last_block);
}


void log_append_compaction_disk_probe(region_info_t *r_info, const append_compaction_plan_t *plan, int fd, const char *stage, uint64_t cid) {
    if (log_level <= 2 || r_info == NULL || plan == NULL || stage == NULL || fd < 0 || plan->source_blocks == 0)
        return;
    uint8_t blocks[FIX_BLOCK_SIZE * 2U];
    uint64_t first_offset = plan->result_write_offset;
    uint64_t last_offset = first_offset + (uint64_t)(plan->source_blocks - 1U) * FIX_BLOCK_SIZE;
    int first_read = append_compaction_pread_exact(fd, blocks, first_offset, FIX_BLOCK_SIZE);
    int last_read = append_compaction_pread_exact(fd, blocks + FIX_BLOCK_SIZE, last_offset, FIX_BLOCK_SIZE);

    if (first_read != FIX_BLOCK_SIZE || last_read != FIX_BLOCK_SIZE) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr,
                "%s TRACE [COMPACT_PROBE=%015lu] stage=%s uuid=%s segment=%u disk_probe_failed "
                "first_offset=%lu first_read=%d last_offset=%lu last_read=%d errno=%d\n",
                curr_dt(dt), cid, stage, r_info->uuid, plan->segment, first_offset, first_read, last_offset, last_read, errno);
        return;
    }
    append_compaction_print_probe(r_info, plan, stage, cid, 0, blocks, blocks + FIX_BLOCK_SIZE);
}

static int load_append_compaction_replica_range(region_info_t *r_info, uint8_t b, uint8_t bs, const uint8_t *block_meta, uint32_t meta_first, uint32_t first_block,
                                                uint32_t blocks, uint8_t *z_index, append_compaction_context_t *context, append_compaction_plan_t *plan,
                                                table_handler_t *t_handler) {
    uint32_t parsed_records = 0;
    msg_cli_t msg_cli = {0};
    msg_kv_t msg_kv = {0};
    struct msg_qv_t msg_qv = {0};
    msg_cli.kv = &msg_kv;
    msg_kv.qv = &msg_qv;
    uint32_t alignment = r_info->r_idx_b[bs]->bsize_factor;
    uint32_t range_read_size = 0;

    for (uint32_t block_pos = 0; block_pos < blocks; block_pos++) {
        msg_write_compact_block_meta_t meta;
        memcpy(&meta, block_meta + (size_t)(meta_first + block_pos) * sizeof(meta), sizeof(meta));
        uint32_t block = first_block + block_pos;

        if (meta.block != block || meta.used_bytes > FIX_BLOCK_SIZE || meta.used_bytes % alignment != 0 ||
            (block == 0 && (meta.used_bytes != FIX_BLOCK_SIZE || meta.record_count != 0)) || (block != 0 && ((meta.used_bytes == 0) != (meta.record_count == 0))))
            return append_compaction_replica_state_mismatch(r_info, b, bs, plan->segment, block, 0, "replica block metadata is invalid", t_handler);
        uint32_t block_data_end = block_pos * FIX_BLOCK_SIZE + meta.used_bytes;

        if (block_data_end > range_read_size)
            range_read_size = block_data_end;
        z_index[block_pos] = (uint8_t)(meta.used_bytes / alignment);
    }

    if (range_read_size > APPEND_COMPACTION_SEGMENT_SIZE)
        return append_compaction_replica_state_mismatch(r_info, b, bs, plan->segment, first_block, 0, "replica input range exceeds service buffer", t_handler);
    uint64_t range_offset = (uint64_t)first_block * FIX_BLOCK_SIZE;
    uint64_t raw_read_started_us = append_compaction_monotonic_us();
    int range_bytes_read = range_read_size == 0 ? 0 : append_compaction_pread_exact(r_info->r_idx_b[bs]->fd[b], context->source_buffer, range_offset, range_read_size);

    if (range_read_size != 0) {
        atomic_fetch_add(&append_compact_metric_v2_raw_read_calls, 1);
        atomic_fetch_add(&append_compact_metric_v2_raw_read_bytes, range_read_size);
        atomic_fetch_add(&append_compact_metric_v2_raw_read_us, append_compaction_monotonic_us() - raw_read_started_us);
    }

    if (range_bytes_read != (int)range_read_size)
        return append_compaction_replica_state_mismatch(r_info, b, bs, plan->segment, first_block, range_bytes_read < 0 ? 0U : (uint32_t)range_bytes_read,
                                                        "replica input range short read", t_handler);
    uint64_t block_verify_started_us = append_compaction_monotonic_us();

    for (uint32_t block_pos = 0; block_pos < blocks; block_pos++) {
        msg_write_compact_block_meta_t meta;
        memcpy(&meta, block_meta + (size_t)(meta_first + block_pos) * sizeof(meta), sizeof(meta));
        uint32_t block = first_block + block_pos;
        uint8_t *block_data = context->source_buffer + block_pos * FIX_BLOCK_SIZE;

        if (meta.used_bytes < FIX_BLOCK_SIZE)
            memset(block_data + meta.used_bytes, 0, FIX_BLOCK_SIZE - meta.used_bytes);
        uint8_t input_hash[MSG_WRITE_COMPACT_HASH_SIZE];
        append_compaction_hash128(block_data, FIX_BLOCK_SIZE, input_hash);

        if (memcmp(input_hash, meta.input_hash, sizeof(input_hash)) != 0)
            return append_compaction_replica_state_mismatch(r_info, b, bs, plan->segment, block, 0, "replica input block hash differs", t_handler);

        if (block == 0)
            continue;
        uint32_t block_records = 0;
        uint32_t record_offset = 0;

        while (record_offset < meta.used_bytes) {
            uint32_t aligned_size = 0;
            uint8_t *record = block_data + record_offset;

            if (!parse_append_record(record, meta.used_bytes - record_offset, r_info, bs, &msg_cli, &aligned_size) || aligned_size > FIX_BLOCK_SIZE - record_offset)
                return append_compaction_replica_state_mismatch(r_info, b, bs, plan->segment, block, record_offset, "replica input record exceeds block metadata", t_handler);

            if (block_records == UINT16_MAX)
                return append_compaction_replica_state_mismatch(r_info, b, bs, plan->segment, block, record_offset, "replica input record count overflow", t_handler);
            block_records++;
            record_offset += aligned_size;
        }

        if (record_offset != meta.used_bytes || block_records != meta.record_count)
            return append_compaction_replica_state_mismatch(r_info, b, bs, plan->segment, block, record_offset, "replica input record count differs", t_handler);

        if (parsed_records > UINT32_MAX - block_records)
            return append_compaction_replica_state_mismatch(r_info, b, bs, plan->segment, block, record_offset, "replica total record count overflow", t_handler);
        parsed_records += block_records;
    }
    atomic_fetch_add(&append_compact_metric_v2_block_verify_us, append_compaction_monotonic_us() - block_verify_started_us);
    return (int)parsed_records;
}


static uint32_t count_append_compaction_live_bits(const uint8_t *live_bitmap, uint32_t first, uint32_t count) {
    uint32_t live = 0;

    for (uint32_t i = 0; i < count; i++)
        live += (live_bitmap[(first + i) / 8U] >> ((first + i) & 7U)) & 1U;
    return live;
}

int verify_append_compaction_replica_plan(region_info_t *r_info, uint8_t b, uint8_t bs, const msg_write_compact_v2_t *header, const uint8_t *block_meta,
                                          const uint8_t *live_bitmap, append_compaction_context_t *context, append_compaction_plan_t *plan, table_handler_t *t_handler) {
    if (r_info == NULL || header == NULL || block_meta == NULL || live_bitmap == NULL || context == NULL || plan == NULL || t_handler == NULL || context->source_buffer == NULL ||
        context->result_buffer == NULL || b >= BUCKETS || bs > BLOCK_ARRAY_LAST_POS || r_info->r_idx_b == NULL || r_info->r_idx_b[bs] == NULL || r_info->r_idx_b[bs]->fd[b] < 0 ||
        r_info->r_idx_b[bs]->bsize_factor == 0 || header->source_blocks == 0 || header->source_blocks > APPEND_COMPACTION_BLOCKS_PER_SEGMENT ||
        header->tail_blocks > APPEND_COMPACTION_BLOCKS_PER_SEGMENT || header->source_record_count > header->record_count)
        return APPEND_COMPACTION_ANALYZE_ERROR;
    memset(plan, 0, sizeof(*plan));
    plan->b = b;
    plan->bs = bs;
    plan->segment = header->segment;
    plan->first_block = header->first_block;
    plan->source_blocks = header->source_blocks;
    plan->tail_range_first_block = header->tail_range_first_block;
    plan->tail_first_block = header->tail_first_block;
    plan->tail_blocks = header->tail_blocks;
    plan->original_b_offset = header->original_b_offset;
    plan->new_b_offset = header->new_b_offset;
    plan->truncate_blocks = header->original_b_offset - header->new_b_offset;
    plan->result_write_offset = (uint64_t)header->first_block * FIX_BLOCK_SIZE;
    plan->result_write_size = header->source_blocks * FIX_BLOCK_SIZE;
    plan->original_file_size = header->compact_old_file_size;
    plan->new_file_size = header->compact_new_file_size;
    plan->source_record_count = header->source_record_count;
    plan->record_decision_count = header->record_count;
    plan->live_bitmap = (uint8_t *)live_bitmap;
    plan->block_meta_count = header->block_meta_count;
    plan->live_records = count_append_compaction_live_bits(live_bitmap, 0, header->record_count);
    plan->tail_live_records = count_append_compaction_live_bits(live_bitmap, header->source_record_count, header->record_count - header->source_record_count);
    int source_records =
        load_append_compaction_replica_range(r_info, b, bs, block_meta, 0, header->first_block, header->source_blocks, plan->source_z_index, context, plan, t_handler);

    if (source_records < 0)
        return APPEND_COMPACTION_ANALYZE_ERROR;

    if ((uint32_t)source_records != header->source_record_count)
        return append_compaction_replica_state_mismatch(r_info, b, bs, header->segment, header->first_block, 0, "replica source record total differs", t_handler);
    uint32_t first_data_pos = header->first_block == 0 ? 1U : 0U;

    for (uint32_t i = first_data_pos; i < header->source_blocks; i++)
        if (plan->source_z_index[i] != 0)
            plan->source_used_blocks++;
    uint64_t rebuild_started_us = append_compaction_monotonic_us();
    int build_result = build_append_compaction_result_from_bitmap(r_info, b, bs, context, plan, false, false, t_handler);
    atomic_fetch_add(&append_compact_metric_v2_rebuild_us, append_compaction_monotonic_us() - rebuild_started_us);

    if (build_result != APPEND_COMPACTION_ANALYZE_OK)
        return build_result;

    if (plan->source_used_blocks < plan->result_used_blocks)
        return append_compaction_segment_error(r_info, b, bs, header->segment, header->first_block, 0, "replica result uses more blocks than source", t_handler);
    uint32_t source_data_blocks = header->source_blocks - first_data_pos;

    if (plan->result_used_blocks > source_data_blocks)
        return append_compaction_segment_error(r_info, b, bs, header->segment, header->first_block, 0, "replica result uses more blocks than source range", t_handler);
    uint32_t freed_blocks = source_data_blocks - plan->result_used_blocks;

    if (freed_blocks > header->original_b_offset)
        return append_compaction_segment_error(r_info, b, bs, header->segment, header->first_block, 0, "replica freed block count reaches meta block", t_handler);
    uint32_t source_last_block = header->first_block + header->source_blocks - 1U;
    uint32_t expected_tail_range_first = 0;
    uint32_t expected_tail_first = 0;
    uint32_t expected_tail_blocks = 0;

    if (freed_blocks != 0) {
        expected_tail_range_first = header->original_b_offset - freed_blocks + 1U;
        expected_tail_first = expected_tail_range_first;

        if (expected_tail_first <= source_last_block && source_last_block != UINT32_MAX)
            expected_tail_first = source_last_block + 1U;
        expected_tail_blocks = expected_tail_first <= header->original_b_offset ? header->original_b_offset - expected_tail_first + 1U : 0U;
    }

    if (header->tail_range_first_block != expected_tail_range_first || header->tail_first_block != (expected_tail_blocks == 0 ? 0U : expected_tail_first) ||
        header->tail_blocks != expected_tail_blocks)
        return append_compaction_segment_error(r_info, b, bs, header->segment, header->first_block, 0, "replica tail range differs from bitmap result", t_handler);

    if (header->tail_blocks != 0) {
        int tail_records = load_append_compaction_replica_range(r_info, b, bs, block_meta, header->source_blocks, header->tail_first_block, header->tail_blocks,
                                                                plan->tail_z_index, context, plan, t_handler);

        if (tail_records < 0)
            return APPEND_COMPACTION_ANALYZE_ERROR;

        if ((uint32_t)tail_records != header->record_count - header->source_record_count)
            return append_compaction_replica_state_mismatch(r_info, b, bs, header->segment, header->tail_first_block, 0, "replica tail record total differs", t_handler);
        rebuild_started_us = append_compaction_monotonic_us();
        build_result = build_append_compaction_result_from_bitmap(r_info, b, bs, context, plan, true, false, t_handler);
        atomic_fetch_add(&append_compact_metric_v2_rebuild_us, append_compaction_monotonic_us() - rebuild_started_us);

        if (build_result != APPEND_COMPACTION_ANALYZE_OK)
            return build_result;
    }
    int finalize_result = finalize_append_compaction_result_buffer(r_info, b, bs, context, plan, t_handler);

    if (finalize_result != APPEND_COMPACTION_ANALYZE_OK)
        return finalize_result;
    uint32_t result_last_block = header->first_block == 0 ? 0U : header->first_block - 1U;

    for (uint32_t i = first_data_pos; i < header->source_blocks; i++)
        if (plan->result_z_index[i] != 0)
            result_last_block = header->first_block + i;
    uint32_t expected_new_b_offset;

    if (freed_blocks == 0)
        expected_new_b_offset = header->original_b_offset;
    else if (expected_tail_range_first > source_last_block + 1U)
        expected_new_b_offset = expected_tail_range_first - 1U;
    else
        expected_new_b_offset = result_last_block;
    uint64_t expected_new_file_size =
        expected_new_b_offset == header->original_b_offset ? header->compact_old_file_size : ((uint64_t)expected_new_b_offset + 1U) * FIX_BLOCK_SIZE;

    if (header->new_b_offset != expected_new_b_offset || header->compact_new_file_size != expected_new_file_size)
        return append_compaction_segment_error(r_info, b, bs, header->segment, header->first_block, 0, "replica logical result boundary differs", t_handler);
    uint8_t result_hash[MSG_WRITE_COMPACT_HASH_SIZE];
    uint64_t result_hash_started_us = append_compaction_monotonic_us();
    append_compaction_hash128(context->result_buffer, plan->result_write_size, result_hash);
    atomic_fetch_add(&append_compact_metric_v2_result_hash_us, append_compaction_monotonic_us() - result_hash_started_us);

    if (memcmp(result_hash, header->result_hash, sizeof(result_hash)) != 0)
        return append_compaction_segment_error(r_info, b, bs, header->segment, header->first_block, 0, "replica result hash differs", t_handler);
    memcpy(plan->result_hash, result_hash, sizeof(result_hash));
    return APPEND_COMPACTION_ANALYZE_OK;
}


static bool append_compaction_range_is_zero(const uint8_t *buff, uint32_t size) {
    for (uint32_t i = 0; i < size; i++) {
        if (buff[i] != 0)
            return false;
    }
    return true;
}


static int verify_append_compaction_plan(region_info_t *r_info, uint8_t b, uint8_t bs, append_compaction_context_t *context, append_compaction_plan_t *plan,
                                         table_handler_t *t_handler) {
    msg_cli_t msg_cli = {0};
    msg_kv_t msg_kv = {0};
    struct msg_qv_t msg_qv = {0};
    msg_cli.kv = &msg_kv;
    msg_kv.qv = &msg_qv;
    uint32_t record_no = 0;

    for (uint32_t block_pos = 0; block_pos < plan->source_blocks; block_pos++) {
        uint32_t block = plan->first_block + block_pos;

        if (block == 0)
            continue;
        uint32_t used_bytes = (uint32_t)plan->result_z_index[block_pos] * r_info->r_idx_b[bs]->bsize_factor;
        uint32_t record_offset = 0;

        while (record_offset < used_bytes) {
            uint32_t aligned_size = 0;
            uint8_t *record = context->result_buffer + block_pos * FIX_BLOCK_SIZE + record_offset;

            if (!parse_append_record(record, used_bytes - record_offset, r_info, bs, &msg_cli, &aligned_size))
                return append_compaction_segment_error(r_info, b, bs, plan->segment, block, record_offset, "result record is invalid", t_handler);

            if (aligned_size > FIX_BLOCK_SIZE - record_offset || record_no >= plan->records_count)
                return append_compaction_segment_error(r_info, b, bs, plan->segment, block, record_offset, "result record exceeds plan", t_handler);

            if (!append_compaction_range_is_zero(record + msg_cli.msg_size, aligned_size - msg_cli.msg_size))
                return append_compaction_segment_error(r_info, b, bs, plan->segment, block, record_offset, "result record padding is not zero", t_handler);
            char digest[DIGEST_SIZE_FULL];
            uint32_t current_block = 0;
            bool is_collision = false;
            append_compaction_record_move_t *move = &plan->records[record_no];

            if (!get_current_record_info(r_info, &msg_cli, bs, &current_block, &is_collision, digest, t_handler) || current_block != move->old_block ||
                move->new_block != block || move->is_collision != (uint8_t)is_collision || memcmp(move->digest, digest, DIGEST_SIZE_FULL) != 0)
                return append_compaction_segment_error(r_info, b, bs, plan->segment, block, record_offset, "result record does not match source plan", t_handler);
            record_no++;
            record_offset += aligned_size;
        }

        if (record_offset != used_bytes)
            return append_compaction_segment_error(r_info, b, bs, plan->segment, block, record_offset, "result records do not end at z_index", t_handler);

        if (!append_compaction_range_is_zero(context->result_buffer + block_pos * FIX_BLOCK_SIZE + used_bytes, FIX_BLOCK_SIZE - used_bytes))
            return append_compaction_segment_error(r_info, b, bs, plan->segment, block, used_bytes, "result block remainder is not zero", t_handler);
    }

    if (record_no != plan->records_count || record_no != plan->live_records)
        return append_compaction_segment_error(r_info, b, bs, plan->segment, plan->first_block, 0, "result live record count differs from source", t_handler);
    return APPEND_COMPACTION_ANALYZE_OK;
}


static bool append_gc_delete(append_compaction_context_t *ctx, append_compaction_plan_t *plan, uint32_t offset) {
    if (plan->delete_count == ctx->delete_capacity) {
        uint32_t capacity = ctx->delete_capacity ? ctx->delete_capacity * 2U : 64U;

        if (capacity > APPEND_COMPACTION_SEGMENT_SIZE / DISK_RECORD_HEADER_SIZE)
            capacity = APPEND_COMPACTION_SEGMENT_SIZE / DISK_RECORD_HEADER_SIZE;

        if (capacity <= ctx->delete_capacity)
            return false;
        uint32_t *tmp = realloc(ctx->delete_offsets, capacity * sizeof(*tmp));

        if (tmp == NULL)
            return false;
        ctx->delete_offsets = tmp;
        ctx->delete_capacity = capacity;
    }
    plan->delete_offsets = ctx->delete_offsets;
    plan->delete_offsets[plan->delete_count++] = offset;
    return true;
}


static void record_gc_version_range(uint64_t version, uint64_t *min, uint64_t *max) {
    if (*min == 0 || version < *min)
        *min = version;

    if (version > *max)
        *max = version;
}


static void record_gc_plan_marker_stats(append_compaction_plan_t *plan, const uint8_t *record, bool live, bool erase) {
    uint64_t raw = record_header_load_le(record + RECORD_VERSION_OFFSET, 8);

    if (!(raw & RECORD_VERSION_TAG))
        return;
    uint64_t version = raw & RECORD_VERSION_MAX;
    record_gc_marker_stats_t *s = &plan->gc_marker_stats;

    if (erase) {
        ++s->removed;
        record_gc_version_range(version, &s->removed_min, &s->removed_max);
    } else if (live) {
        ++s->retained;
        record_gc_version_range(version, &s->retained_min, &s->retained_max);
    } else {
        ++s->obsolete;
    }
}


static int build_append_compaction_plan_internal(region_info_t *r_info, uint8_t b, uint8_t bs, uint32_t segment, append_compaction_context_t *context,
                                                 append_compaction_plan_t *plan, table_handler_t *t_handler, bool source_only, uint64_t purge_before) {
    if (r_info == NULL || context == NULL || plan == NULL || t_handler == NULL || context->source_buffer == NULL || context->result_buffer == NULL || bs > BLOCK_ARRAY_LAST_POS ||
        r_info->r_idx_b[bs] == NULL || b >= BUCKETS || r_info->r_idx_b[bs]->idx[b] == NULL)
        return APPEND_COMPACTION_ANALYZE_ERROR;
    memset(plan, 0, sizeof(*plan));
    uint64_t phase_started_us = append_compaction_monotonic_us();
    plan->b = b;
    plan->bs = bs;
    plan->segment = segment;
    plan->records = context->records;
    plan->live_bitmap = context->live_bitmap;

    if (purge_before != 0) {
        if (!source_only || purge_before != get_record_gc_safe_revision(r_info))
            return APPEND_COMPACTION_ANALYZE_ERROR;
        plan->gc_safe_revision = purge_before;
        plan->gc_generation = atomic_load(&r_info->record_gc_generation);
        plan->gc_confirmed = r_info->record_versions.confirmed;
    }

    if (r_info->r_idx_b[bs]->fd[b] != r_info->data_000_fd || r_info->r_idx_b[bs]->block_size != FIX_BLOCK_SIZE || r_info->enable_block_compression != 0 ||
        r_info->r_idx_b[bs]->bsize_factor == 0)
        return append_compaction_segment_error(r_info, b, bs, segment, 0, 0, "unsupported data file format", t_handler);

    if (segment > UINT32_MAX / APPEND_COMPACTION_BLOCKS_PER_SEGMENT)
        return append_compaction_segment_error(r_info, b, bs, segment, 0, 0, "segment number overflow", t_handler);
    struct stat file_stat;

    if (fstat(r_info->r_idx_b[bs]->fd[b], &file_stat) != 0 || file_stat.st_size < 0)
        return append_compaction_segment_error(r_info, b, bs, segment, 0, 0, "failed to get source file size", t_handler);
    plan->original_file_size = (uint64_t)file_stat.st_size;

    if (r_info->write_4K_fd == r_info->r_idx_b[bs]->fd[b] && r_info->write_4K_buff_used != 0) {
        if (r_info->write_4K_file_offset > UINT64_MAX - r_info->write_4K_buff_used)
            return append_compaction_segment_error(r_info, b, bs, segment, 0, 0, "write buffer range overflow", t_handler);
        uint64_t buffered_end = r_info->write_4K_file_offset + r_info->write_4K_buff_used;

        if (buffered_end > plan->original_file_size)
            plan->original_file_size = buffered_end;
    }
    plan->first_block = segment * APPEND_COMPACTION_BLOCKS_PER_SEGMENT;
    uint32_t last_file_block = r_info->r_idx_b[bs]->b_offset[b];
    plan->original_b_offset = last_file_block;

    if (plan->first_block > last_file_block)
        return APPEND_COMPACTION_ANALYZE_EMPTY;

    if (last_file_block >= r_info->r_idx_b[bs]->f_idx_size[b])
        return append_compaction_segment_error(r_info, b, bs, segment, last_file_block, 0, "last block exceeds index size", t_handler);
    plan->source_blocks = APPEND_COMPACTION_BLOCKS_PER_SEGMENT;

    if (plan->source_blocks > last_file_block - plan->first_block + 1)
        plan->source_blocks = last_file_block - plan->first_block + 1;
    uint32_t read_size = plan->source_blocks * FIX_BLOCK_SIZE;
    plan->result_write_offset = (uint64_t)plan->first_block * FIX_BLOCK_SIZE;
    plan->result_write_size = read_size;
    plan->metric_prepare_us += append_compaction_monotonic_us() - phase_started_us;
    phase_started_us = append_compaction_monotonic_us();
    uint32_t source_used_bytes[APPEND_COMPACTION_BLOCKS_PER_SEGMENT] = {0};
    uint32_t source_range_read_size = 0;

    for (uint32_t block_pos = 0; block_pos < plan->source_blocks; block_pos++) {
        uint32_t source_block = plan->first_block + block_pos;
        uint32_t block_read_size = source_block == 0 ? FIX_BLOCK_SIZE : (uint32_t)r_info->r_idx_b[bs]->idx[b][source_block].z_index * r_info->r_idx_b[bs]->bsize_factor;

        if (block_read_size > FIX_BLOCK_SIZE)
            return append_compaction_segment_error(r_info, b, bs, segment, source_block, 0, "source z_index exceeds 4-KB block before read", t_handler);
        source_used_bytes[block_pos] = block_read_size;
        uint32_t block_end = block_pos * FIX_BLOCK_SIZE + block_read_size;

        if (block_end > source_range_read_size)
            source_range_read_size = block_end;
        plan->metric_source_bytes += block_read_size;
    }

    if (source_range_read_size > read_size)
        return append_compaction_segment_error(r_info, b, bs, segment, plan->first_block, 0, "source read range exceeds service buffer", t_handler);

    if (source_range_read_size != 0) {
        int range_bytes_read =
            read_file(r_info->r_idx_b[bs]->fd[b], context->source_buffer, (uint64_t)plan->first_block * FIX_BLOCK_SIZE, source_range_read_size, r_info, t_handler->cid);

        if (range_bytes_read != (int)source_range_read_size) {
            uint32_t available = range_bytes_read < 0 ? 0U : (uint32_t)range_bytes_read;
            uint32_t failed_pos = 0;

            while (failed_pos < plan->source_blocks && failed_pos * FIX_BLOCK_SIZE + source_used_bytes[failed_pos] <= available)
                failed_pos++;

            if (failed_pos >= plan->source_blocks)
                failed_pos = plan->source_blocks - 1U;
            uint32_t failed_block_begin = failed_pos * FIX_BLOCK_SIZE;
            uint32_t available_in_block = available > failed_block_begin ? available - failed_block_begin : 0U;

            if (available_in_block > source_used_bytes[failed_pos])
                available_in_block = source_used_bytes[failed_pos];
            return append_compaction_segment_error(r_info, b, bs, segment, plan->first_block + failed_pos, available_in_block, "source range short read", t_handler);
        }
    }

    for (uint32_t block_pos = 0; block_pos < plan->source_blocks; block_pos++) {
        uint32_t block_read_size = source_used_bytes[block_pos];

        if (block_read_size < FIX_BLOCK_SIZE)
            memset(context->source_buffer + block_pos * FIX_BLOCK_SIZE + block_read_size, 0, FIX_BLOCK_SIZE - block_read_size);
    }
    int bytes_read = (int)read_size;
    plan->metric_source_read_us += append_compaction_monotonic_us() - phase_started_us;
    phase_started_us = append_compaction_monotonic_us();

    for (uint32_t block_pos = 0; block_pos < plan->source_blocks; block_pos++) {
        uint32_t source_block = plan->first_block + block_pos;
        uint32_t used_bytes = source_block == 0 ? FIX_BLOCK_SIZE : (uint32_t)r_info->r_idx_b[bs]->idx[b][source_block].z_index * r_info->r_idx_b[bs]->bsize_factor;
        plan->source_block_hash[block_pos] = XXH3_64bits(context->source_buffer + block_pos * FIX_BLOCK_SIZE, FIX_BLOCK_SIZE);
        append_compaction_init_block_meta(&plan->block_meta[block_pos], source_block, used_bytes, context->source_buffer + block_pos * FIX_BLOCK_SIZE);
    }
    plan->block_meta_count = plan->source_blocks;
    plan->metric_hash_us += append_compaction_monotonic_us() - phase_started_us;

    if (plan->first_block == 0) {
        if (bytes_read < FIX_BLOCK_SIZE)
            return append_compaction_segment_error(r_info, b, bs, segment, 0, 0, "meta block is incomplete", t_handler);
        memcpy(context->result_buffer, context->source_buffer, FIX_BLOCK_SIZE);
    }
    msg_cli_t msg_cli = {0};
    msg_kv_t msg_kv = {0};
    struct msg_qv_t msg_qv = {0};
    msg_cli.kv = &msg_kv;
    msg_kv.qv = &msg_qv;
    phase_started_us = append_compaction_monotonic_us();

    for (uint32_t block_pos = 0; block_pos < plan->source_blocks; block_pos++) {
        uint32_t source_block = plan->first_block + block_pos;

        if (source_block == 0)
            continue;
        uint32_t used_bytes = (uint32_t)r_info->r_idx_b[bs]->idx[b][source_block].z_index * r_info->r_idx_b[bs]->bsize_factor;
        plan->source_z_index[block_pos] = r_info->r_idx_b[bs]->idx[b][source_block].z_index;

        if (used_bytes > FIX_BLOCK_SIZE || (uint64_t)block_pos * FIX_BLOCK_SIZE + used_bytes > (uint32_t)bytes_read)
            return append_compaction_segment_error(r_info, b, bs, segment, source_block, 0, "source z_index exceeds block or bytes read", t_handler);

        if (used_bytes != 0)
            plan->source_used_blocks++;
        uint32_t source_offset = 0;

        while (source_offset < used_bytes) {
            uint32_t aligned_size = 0;
            uint8_t *source_record = context->source_buffer + block_pos * FIX_BLOCK_SIZE + source_offset;

            if (!parse_append_record(source_record, used_bytes - source_offset, r_info, bs, &msg_cli, &aligned_size) || aligned_size > FIX_BLOCK_SIZE - source_offset)
                return append_compaction_segment_error(r_info, b, bs, segment, source_block, source_offset, "source record exceeds block or z_index", t_handler);

            if (!append_compaction_add_block_record(&plan->block_meta[block_pos]))
                return append_compaction_segment_error(r_info, b, bs, segment, source_block, source_offset, "source record count exceeds wire format", t_handler);
            uint32_t current_block = 0;
            bool is_collision = false;
            char digest[DIGEST_SIZE_FULL];
            uint64_t lookup_started_us = append_compaction_monotonic_us();
            bool is_live = get_current_record_info(r_info, &msg_cli, bs, &current_block, &is_collision, digest, t_handler) && current_block == source_block;
            plan->metric_source_lookup_us += append_compaction_monotonic_us() - lookup_started_us;
            plan->metric_index_lookups++;
            uint64_t marker_version = 0;
            bool erase_marker = is_live && msg_cli.kv->qv_size == 0 && purge_before != 0 && disk_record_version_read(source_record, aligned_size, &marker_version) &&
                                marker_version != 0 && marker_version < purge_before;

            if (msg_cli.kv->qv_size == 0)
                record_gc_plan_marker_stats(plan, source_record, is_live, erase_marker);

            if (erase_marker) {
                if (!append_gc_delete(context, plan, block_pos * FIX_BLOCK_SIZE + source_offset))
                    return APPEND_COMPACTION_ANALYZE_ERROR;
                plan->gc_removed_bytes += aligned_size;
                is_live = false;
            }

            if (!is_live) {
                if (!erase_marker && msg_cli.kv->qv_size == 0 && !obsolete_marker_has_winner(r_info, source_record, aligned_size, t_handler)) {
                    stop_region_after_key_error(r_info, KEY_EXTRACT_INVALID_STATE, t_handler);
                    return APPEND_COMPACTION_ANALYZE_ERROR;
                }

                if (!append_compaction_add_decision(context, plan, false, r_info, t_handler))
                    return APPEND_COMPACTION_ANALYZE_ERROR;
                plan->stale_records++;
                plan->removed_garbage_bytes += aligned_size;
                source_offset += aligned_size;
                continue;
            }

            if (!append_compaction_add_decision(context, plan, true, r_info, t_handler))
                return APPEND_COMPACTION_ANALYZE_ERROR;

            if (!reserve_append_compaction_record(context, plan->records_count + 1U, r_info, t_handler))
                return APPEND_COMPACTION_ANALYZE_ERROR;
            plan->records = context->records;
            append_compaction_record_move_t *move = &plan->records[plan->records_count++];
            memcpy(move->digest, digest, DIGEST_SIZE_FULL);
            move->old_block = source_block;
            move->new_block = 0;
            move->is_collision = (uint8_t)is_collision;
            plan->live_records++;
            source_offset += aligned_size;
        }

        if (source_offset != used_bytes)
            return append_compaction_segment_error(r_info, b, bs, segment, source_block, source_offset, "source records do not end at z_index", t_handler);
    }
    plan->source_record_count = plan->record_decision_count;
    int source_build_result = build_append_compaction_result_from_bitmap(r_info, b, bs, context, plan, false, true, t_handler);

    if (source_build_result != APPEND_COMPACTION_ANALYZE_OK)
        return source_build_result;
    plan->metric_source_pack_us += append_compaction_monotonic_us() - phase_started_us - plan->metric_source_lookup_us;
    uint32_t first_data_pos = plan->first_block == 0 ? 1U : 0U;

    if (plan->source_used_blocks < plan->result_used_blocks)
        return append_compaction_segment_error(r_info, b, bs, segment, plan->first_block, 0, "result uses more blocks than source", t_handler);
    uint32_t source_data_blocks = plan->source_blocks - first_data_pos;
    plan->freed_blocks = source_data_blocks - plan->result_used_blocks;

    if (source_only)
        plan->freed_blocks = 0;
    uint32_t selected_last_block = plan->first_block + plan->source_blocks - 1U;
    uint32_t tail_range_first = 0;

    if (plan->freed_blocks != 0) {
        if (plan->freed_blocks > last_file_block)
            return append_compaction_segment_error(r_info, b, bs, segment, last_file_block, 0, "freed block count reaches meta block", t_handler);
        tail_range_first = last_file_block - plan->freed_blocks + 1U;
        plan->tail_range_first_block = tail_range_first;
        uint32_t external_tail_first = tail_range_first;

        if (external_tail_first <= selected_last_block)
            external_tail_first = selected_last_block + 1U;

        if (external_tail_first <= last_file_block) {
            plan->tail_first_block = external_tail_first;
            plan->tail_blocks = last_file_block - external_tail_first + 1U;

            if (plan->tail_blocks > APPEND_COMPACTION_BLOCKS_PER_SEGMENT)
                return append_compaction_segment_error(r_info, b, bs, segment, external_tail_first, 0, "tail exceeds service buffer", t_handler);
            uint32_t tail_read_size = plan->tail_blocks * FIX_BLOCK_SIZE;
            phase_started_us = append_compaction_monotonic_us();
            uint32_t tail_used_bytes[APPEND_COMPACTION_BLOCKS_PER_SEGMENT] = {0};
            uint32_t tail_range_read_size = 0;

            for (uint32_t tail_pos = 0; tail_pos < plan->tail_blocks; tail_pos++) {
                uint32_t tail_block = external_tail_first + tail_pos;
                uint32_t block_read_size = (uint32_t)r_info->r_idx_b[bs]->idx[b][tail_block].z_index * r_info->r_idx_b[bs]->bsize_factor;

                if (block_read_size > FIX_BLOCK_SIZE)
                    return append_compaction_segment_error(r_info, b, bs, segment, tail_block, 0, "tail z_index exceeds 4-KB block before read", t_handler);
                tail_used_bytes[tail_pos] = block_read_size;
                uint32_t block_end = tail_pos * FIX_BLOCK_SIZE + block_read_size;

                if (block_end > tail_range_read_size)
                    tail_range_read_size = block_end;
                plan->metric_tail_bytes += block_read_size;
            }

            if (tail_range_read_size > tail_read_size)
                return append_compaction_segment_error(r_info, b, bs, segment, external_tail_first, 0, "tail read range exceeds service buffer", t_handler);

            if (tail_range_read_size != 0) {
                int range_bytes_read =
                    read_file(r_info->r_idx_b[bs]->fd[b], context->source_buffer, (uint64_t)external_tail_first * FIX_BLOCK_SIZE, tail_range_read_size, r_info, t_handler->cid);

                if (range_bytes_read != (int)tail_range_read_size) {
                    uint32_t available = range_bytes_read < 0 ? 0U : (uint32_t)range_bytes_read;
                    uint32_t failed_pos = 0;

                    while (failed_pos < plan->tail_blocks && failed_pos * FIX_BLOCK_SIZE + tail_used_bytes[failed_pos] <= available)
                        failed_pos++;

                    if (failed_pos >= plan->tail_blocks)
                        failed_pos = plan->tail_blocks - 1U;
                    uint32_t failed_block_begin = failed_pos * FIX_BLOCK_SIZE;
                    uint32_t available_in_block = available > failed_block_begin ? available - failed_block_begin : 0U;

                    if (available_in_block > tail_used_bytes[failed_pos])
                        available_in_block = tail_used_bytes[failed_pos];
                    return append_compaction_segment_error(r_info, b, bs, segment, external_tail_first + failed_pos, available_in_block, "tail range short read", t_handler);
                }
            }

            for (uint32_t tail_pos = 0; tail_pos < plan->tail_blocks; tail_pos++) {
                uint32_t block_read_size = tail_used_bytes[tail_pos];

                if (block_read_size < FIX_BLOCK_SIZE)
                    memset(context->source_buffer + tail_pos * FIX_BLOCK_SIZE + block_read_size, 0, FIX_BLOCK_SIZE - block_read_size);
            }
            int tail_bytes_read = (int)tail_read_size;
            plan->metric_tail_read_us += append_compaction_monotonic_us() - phase_started_us;
            phase_started_us = append_compaction_monotonic_us();

            for (uint32_t tail_pos = 0; tail_pos < plan->tail_blocks; tail_pos++) {
                uint32_t tail_block = external_tail_first + tail_pos;
                uint32_t used_bytes = (uint32_t)r_info->r_idx_b[bs]->idx[b][tail_block].z_index * r_info->r_idx_b[bs]->bsize_factor;
                append_compaction_init_block_meta(&plan->block_meta[plan->source_blocks + tail_pos], tail_block, used_bytes, context->source_buffer + tail_pos * FIX_BLOCK_SIZE);
            }
            plan->block_meta_count = plan->source_blocks + plan->tail_blocks;
            plan->metric_hash_us += append_compaction_monotonic_us() - phase_started_us;
            phase_started_us = append_compaction_monotonic_us();

            for (uint32_t tail_pos = 0; tail_pos < plan->tail_blocks; tail_pos++) {
                uint32_t tail_block = external_tail_first + tail_pos;
                uint32_t used_bytes = (uint32_t)r_info->r_idx_b[bs]->idx[b][tail_block].z_index * r_info->r_idx_b[bs]->bsize_factor;
                plan->tail_z_index[tail_pos] = r_info->r_idx_b[bs]->idx[b][tail_block].z_index;

                if (used_bytes > FIX_BLOCK_SIZE || (uint64_t)tail_pos * FIX_BLOCK_SIZE + used_bytes > (uint32_t)tail_bytes_read) {
                    struct stat tail_file_stat;
                    int tail_fstat_result = fstat(r_info->r_idx_b[bs]->fd[b], &tail_file_stat);
                    uint64_t tail_read_offset = (uint64_t)external_tail_first * FIX_BLOCK_SIZE;
                    uint64_t required_end_in_buffer = (uint64_t)tail_pos * FIX_BLOCK_SIZE + used_bytes;
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr,
                            "%s ERROR [COMPACT_SERVICE=%015lu] Tail block diagnostic for %s[%u], bs=%u, segment=%u: tail_block=%u, last_file_block=%u, external_tail_first=%u, "
                            "tail_pos=%u/%u, z_index=%u, bsize_factor=%u, used_bytes=%u, tail_read_offset=%lu, tail_read_size=%u, tail_bytes_read=%d, "
                            "required_end_in_buffer=%lu, original_file_size=%lu, physical_file_size=%ld, physical_remainder=%ld, write_4K_fd=%d, data_fd=%d, "
                            "write_4K_offset=%lu, write_4K_used=%u, write_4K_cache_ok=%d\n",
                            curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, b, bs, segment, tail_block, last_file_block, external_tail_first, tail_pos,
                            plan->tail_blocks, r_info->r_idx_b[bs]->idx[b][tail_block].z_index, r_info->r_idx_b[bs]->bsize_factor, used_bytes, tail_read_offset, tail_read_size,
                            tail_bytes_read, required_end_in_buffer, plan->original_file_size, tail_fstat_result == 0 ? (long)tail_file_stat.st_size : -1L,
                            tail_fstat_result == 0 ? (long)(tail_file_stat.st_size % FIX_BLOCK_SIZE) : -1L, r_info->write_4K_fd, r_info->r_idx_b[bs]->fd[b],
                            r_info->write_4K_file_offset, r_info->write_4K_buff_used, r_info->write_4K_cache_ok);
                    return append_compaction_segment_error(r_info, b, bs, segment, tail_block, 0, "tail z_index exceeds block or bytes read", t_handler);
                }
                uint32_t tail_offset = 0;

                while (tail_offset < used_bytes) {
                    uint32_t aligned_size = 0;
                    uint8_t *tail_record = context->source_buffer + tail_pos * FIX_BLOCK_SIZE + tail_offset;

                    if (!parse_append_record(tail_record, used_bytes - tail_offset, r_info, bs, &msg_cli, &aligned_size) || aligned_size > FIX_BLOCK_SIZE - tail_offset)
                        return append_compaction_segment_error(r_info, b, bs, segment, tail_block, tail_offset, "tail record exceeds block or z_index", t_handler);

                    if (!append_compaction_add_block_record(&plan->block_meta[plan->source_blocks + tail_pos]))
                        return append_compaction_segment_error(r_info, b, bs, segment, tail_block, tail_offset, "tail record count exceeds wire format", t_handler);
                    uint32_t current_block = 0;
                    bool is_collision = false;
                    char digest[DIGEST_SIZE_FULL];
                    uint64_t lookup_started_us = append_compaction_monotonic_us();
                    bool is_live = get_current_record_info(r_info, &msg_cli, bs, &current_block, &is_collision, digest, t_handler) && current_block == tail_block;
                    plan->metric_tail_lookup_us += append_compaction_monotonic_us() - lookup_started_us;
                    plan->metric_index_lookups++;

                    if (msg_cli.kv->qv_size == 0)
                        record_gc_plan_marker_stats(plan, tail_record, is_live, false);

                    if (!is_live) {
                        if (msg_cli.kv->qv_size == 0 && !obsolete_marker_has_winner(r_info, tail_record, aligned_size, t_handler)) {
                            stop_region_after_key_error(r_info, KEY_EXTRACT_INVALID_STATE, t_handler);
                            return APPEND_COMPACTION_ANALYZE_ERROR;
                        }

                        if (!append_compaction_add_decision(context, plan, false, r_info, t_handler))
                            return APPEND_COMPACTION_ANALYZE_ERROR;
                        plan->tail_stale_records++;
                        plan->stale_records++;
                        plan->removed_garbage_bytes += aligned_size;
                        tail_offset += aligned_size;
                        continue;
                    }

                    if (!append_compaction_add_decision(context, plan, true, r_info, t_handler))
                        return APPEND_COMPACTION_ANALYZE_ERROR;

                    if (!reserve_append_compaction_record(context, plan->records_count + 1U, r_info, t_handler))
                        return APPEND_COMPACTION_ANALYZE_ERROR;
                    plan->records = context->records;
                    append_compaction_record_move_t *move = &plan->records[plan->records_count++];
                    memcpy(move->digest, digest, DIGEST_SIZE_FULL);
                    move->old_block = tail_block;
                    move->new_block = 0;
                    move->is_collision = (uint8_t)is_collision;
                    plan->tail_live_records++;
                    plan->live_records++;
                    tail_offset += aligned_size;
                }

                if (tail_offset != used_bytes)
                    return append_compaction_segment_error(r_info, b, bs, segment, tail_block, tail_offset, "tail records do not end at z_index", t_handler);
            }
            int final_build_result = build_append_compaction_result_from_bitmap(r_info, b, bs, context, plan, true, true, t_handler);

            if (final_build_result != APPEND_COMPACTION_ANALYZE_OK)
                return final_build_result;
            plan->metric_tail_pack_us += append_compaction_monotonic_us() - phase_started_us - plan->metric_tail_lookup_us;
        }
    }
    int finalize_result = finalize_append_compaction_result_buffer(r_info, b, bs, context, plan, t_handler);

    if (finalize_result != APPEND_COMPACTION_ANALYZE_OK)
        return finalize_result;
    plan->result_used_blocks = 0;
    uint32_t result_last_block = plan->first_block == 0 ? 0U : plan->first_block - 1U;

    for (uint32_t block_pos = first_data_pos; block_pos < plan->source_blocks; block_pos++) {
        if (plan->result_z_index[block_pos] != 0) {
            plan->result_used_blocks++;
            result_last_block = plan->first_block + block_pos;
        }
    }

    if (plan->result_used_blocks > source_data_blocks)
        return append_compaction_segment_error(r_info, b, bs, segment, plan->first_block, 0, "tail result uses more blocks than source range", t_handler);
    plan->result_free_blocks = source_data_blocks - plan->result_used_blocks;

    if (plan->freed_blocks == 0) {
        plan->new_b_offset = last_file_block;
    } else if (tail_range_first > selected_last_block + 1U) {
        plan->new_b_offset = tail_range_first - 1U;
    } else {
        plan->new_b_offset = result_last_block;
    }

    if (plan->new_b_offset > last_file_block)
        return append_compaction_segment_error(r_info, b, bs, segment, plan->new_b_offset, 0, "new b_offset exceeds old b_offset", t_handler);
    plan->truncate_blocks = last_file_block - plan->new_b_offset;
    plan->new_file_size = plan->truncate_blocks == 0 ? plan->original_file_size : ((uint64_t)plan->new_b_offset + 1U) * FIX_BLOCK_SIZE;

    if ((plan->record_decision_count & 7U) != 0)
        plan->live_bitmap[plan->record_decision_count / 8U] &= (uint8_t)((1U << (plan->record_decision_count & 7U)) - 1U);
    phase_started_us = append_compaction_monotonic_us();

    if (!append_compaction_finalize_bitmap_metadata(plan, context))
        return append_compaction_segment_error(r_info, b, bs, segment, plan->first_block, 0, "bitmap block metadata differs from parsed records", t_handler);
    plan->metric_hash_us += append_compaction_monotonic_us() - phase_started_us;
    phase_started_us = append_compaction_monotonic_us();
    int verify_result = verify_append_compaction_plan(r_info, b, bs, context, plan, t_handler);
    plan->metric_verify_us += append_compaction_monotonic_us() - phase_started_us;

    if (verify_result != APPEND_COMPACTION_ANALYZE_OK)
        return verify_result;
    phase_started_us = append_compaction_monotonic_us();

    for (uint32_t block_pos = 0; block_pos < plan->source_blocks; block_pos++)
        if (plan->source_block_hash[block_pos] != XXH3_64bits(context->result_buffer + block_pos * FIX_BLOCK_SIZE, FIX_BLOCK_SIZE))
            plan->changed_blocks++;
    plan->metric_hash_us += append_compaction_monotonic_us() - phase_started_us;

    if (log_level > 1) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr,
                "%s DEBUG [COMPACT_SERVICE=%015lu] Append compaction plan %s[%u], bs=%u, segment=%u: live=%u, stale=%u, tail=%u/%u, garbage=%lu, source_blocks=%u, "
                "result_blocks=%u, changed_blocks=%u/%u, freed_blocks=%u, tail_blocks=%u, tail_first_block=%u, tail_range_first_block=%u, original_b_offset=%u, "
                "truncate_blocks=%u, new_b_offset=%u, new_file_size=%lu\n",
                curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, b, bs, segment, plan->live_records, plan->stale_records, plan->tail_live_records,
                plan->tail_stale_records, plan->removed_garbage_bytes, plan->source_used_blocks, plan->result_used_blocks, plan->changed_blocks, plan->source_blocks,
                plan->freed_blocks, plan->tail_blocks, plan->tail_first_block, plan->tail_range_first_block, plan->original_b_offset, plan->truncate_blocks, plan->new_b_offset,
                plan->new_file_size);
    }
    log_append_compaction_result_probe(r_info, context, plan, "leader_built", t_handler->cid);
    return APPEND_COMPACTION_ANALYZE_OK;
}

int build_append_compaction_plan(region_info_t *r, uint8_t b, uint8_t bs, uint32_t segment, append_compaction_context_t *context, append_compaction_plan_t *plan,
                                 table_handler_t *h) {
    return build_append_compaction_plan_internal(r, b, bs, segment, context, plan, h, false, 0);
}


static bool append_compaction_region_ready_internal(region_info_t *r_info, uint8_t b, uint8_t bs, bool allow_recovery_marker) {
    if (atomic_load(&rs_shutdown_state) >= RS_SHUTDOWN_WAITING_RPS)
        return false;
    return (rf == 1 || data_check_enabled == 1) && r_info != NULL && r_info->is_leader && !r_info->is_freed && !r_info->stop && !r_info->bad_data &&
           (r_info->status == STATUS_ASSIGN_REGION || r_info->status == STATUS_START_REGION) && r_info->idx_resizer == RESIZE_IDX_DONE &&
           (allow_recovery_marker || atomic_load(&r_info->r_need_data_chk) == 0) && atomic_load(&r_info->recovery_path_ready) != 0 && r_info->r_fname != NULL &&
           r_info->replica != NULL && r_info->rsc_msg != NULL && r_info->bulk_mode == BUIK_RELICA_OFF && r_info->r_idx_b != NULL && bs < r_info->max_idx_b &&
           r_info->r_idx_b[bs] != NULL && b < BUCKETS && r_info->r_idx_b[bs]->idx[b] != NULL && r_info->r_idx_b[bs]->fd[b] == r_info->data_000_fd &&
           r_info->r_idx_b[bs]->block_size == FIX_BLOCK_SIZE && r_info->enable_block_compression == 0;
}

static bool append_compaction_region_ready(region_info_t *r_info, uint8_t b, uint8_t bs) {
    return append_compaction_region_ready_internal(r_info, b, bs, false);
}


static const char *record_gc_block_reason(region_info_t *r, uint8_t bs) {
    if (!append_compaction_region_ready(r, FBP, bs))
        return "region_state";

    if (atomic_load(&is_shutting_down) || atomic_load(&rs_shutdown_state) != RS_SHUTDOWN_RUNNING)
        return "shutdown";

    if (r->has_parent_replica)
        return "parent_replica";

    if (r->already_finishing)
        return "finishing";

    if (r->k_dbl != NULL)
        return "duplicates_pending";

    if (r->waiting_count_admin)
        return "admin_waiter";

    if (r->waiting_count_lock)
        return "lock_waiter";

    if (r->waiting_count_reconnect)
        return "reconnect_waiter";

    if (r->is_write)
        return "write_active";

    if (r->is_replicate)
        return "replication_active";

    if (r->bulk_buff_offset1 || r->bulk_buff_offset2)
        return "bulk_pending";

    if (r->replica->replica_control)
        return "replica_control";

    if (!record_version_state_valid(&r->record_versions))
        return "version_state";

    if (r->record_versions.pending)
        return "version_pending";

    if (rf < 1 || rf > 3)
        return "replication_factor";

    for (int n = 1; n < rf; ++n) {
        region_info_t *child = n == 1 ? r->replica->r_info1 : r->replica->r_info2;

        if (child != NULL) {
            if (child->is_freed || child->stop || child->bad_data || child->r_need_data_chk)
                return n == 1 ? "local_replica1" : "local_replica2";
        } else {
            bool connected = (n == 1 ? r->replica->replica1_sock_fd : r->replica->replica2_sock_fd) > 0;

            if ((n == 1 ? r->replica->replica1_port : r->replica->replica2_port) <= 0 || !connected)
                return n == 1 ? "remote_replica1_disconnected" : "remote_replica2_disconnected";
        }
    }
    return NULL;
}

static bool record_gc_ready(region_info_t *r, uint8_t bs) {
    return record_gc_block_reason(r, bs) == NULL;
}

static void record_gc_account_tail(region_info_t *r, uint8_t bs, uint32_t old_last, uint32_t new_last) {
    record_gc_state_t *gc = &r->record_gc;

    if (gc->phase == RECORD_GC_IDLE || gc->generation != atomic_load(&r->record_gc_generation))
        return;

    if (gc->bs != bs || gc->fd != r->data_000_fd || old_last < gc->expected_last_block) {
        invalidate_record_gc(r);
        return;
    }
    gc->expected_last_block = new_last;
}

uint64_t get_record_gc_safe_revision(region_info_t *r) {
    if (r == NULL || r->record_gc.phase != RECORD_GC_COMPLETE || r->record_gc.generation != atomic_load(&r->record_gc_generation) || !record_gc_ready(r, r->record_gc.bs) ||
        r->record_gc.fd != r->data_000_fd || atomic_load(&r->append_compact_truncate_pending) || r->r_idx_b[r->record_gc.bs]->b_offset[FBP] < r->record_gc.expected_last_block)
        return 0;
    return r->record_gc.safe_revision;
}


static bool record_gc_delete_keys(region_info_t *r, append_compaction_context_t *ctx, append_compaction_plan_t *plan, table_handler_t *h, bool apply) {
    if (plan->delete_count == 0)
        return true;

    if (plan->delete_offsets != ctx->delete_offsets || plan->tail_blocks != 0 || plan->gc_safe_revision == 0 || plan->gc_generation != atomic_load(&r->record_gc_generation) ||
        plan->gc_confirmed != r->record_versions.confirmed || r->pending_delete < plan->delete_count)
        return false;
    msg_kv_t kv = {0};
    msg_cli_t key = {.kv = &kv};

    for (uint32_t n = 0; n < plan->delete_count; ++n) {
        uint32_t off = plan->delete_offsets[n];

        if (off >= plan->source_blocks * FIX_BLOCK_SIZE || (n && off <= plan->delete_offsets[n - 1U]))
            return false;
        disk_record_header_t header;

        if (!disk_record_decode_header(ctx->source_buffer + off, FIX_BLOCK_SIZE - off % FIX_BLOCK_SIZE, &header) || header.kind != DISK_RECORD_TOMBSTONE ||
            header.version >= plan->gc_safe_revision)
            return false;
        kv.key = ctx->source_buffer + off + DISK_RECORD_HEADER_SIZE;
        kv.key_size = header.key_size;
        kv.cf = kv.key + kv.key_size;
        kv.cf_size = header.cf_size;
        key.msg_size = header.msg_size;
        uint32_t expected = plan->first_block + off / FIX_BLOCK_SIZE, current;
        r->has_digest_calc = false;
        build_hash_params(&key, r, plan->bs);

        if (!get_current_record_block(r, &key, plan->bs, &current, h) || current != expected)
            return false;

        if (apply) {
            del_from_index(r, current, &key, h, UINT32_MAX, plan->bs);
            --r->pending_delete;
        }
    }
    return true;
}


bool append_compaction_piggyback_ready(region_info_t *r_info, uint8_t b, uint8_t bs, uint8_t request_type) {
    (void)r_info;
    (void)b;
    (void)bs;
    (void)request_type;
    return false;
}

static bool append_compaction_plan_is_client_actionable(const append_compaction_plan_t *plan) {
    return plan != NULL && plan->truncate_blocks != 0;
}


static bool append_compaction_plan_is_client_order_safe(const append_compaction_plan_t *plan) {
    if (plan == NULL || plan->source_blocks == 0)
        return false;
    uint64_t source_first = plan->first_block;
    uint64_t source_end = source_first + plan->source_blocks;
    uint64_t append_first = plan->new_b_offset;
    uint64_t append_end = append_first + 2U;
    return source_first >= append_end || append_first >= source_end;
}

static bool append_compaction_plan_is_service_actionable(const append_compaction_plan_t *plan) {
    if (plan == NULL)
        return false;

    if (plan->truncate_blocks != 0)
        return true;

    if (plan->stale_records == 0 || plan->changed_blocks == 0)
        return false;
    return plan->freed_blocks == 0 && plan->tail_blocks == 0 && plan->new_b_offset == plan->original_b_offset && plan->new_file_size == plan->original_file_size;
}


static bool append_compaction_segment_is_dirty(const region_info_t *r_info, uint32_t segment) {
    if (r_info == NULL || r_info->append_compact_dirty_map == NULL)
        return false;
    uint32_t byte = segment / 8U;
    uint8_t mask = (uint8_t)(1U << (segment & 7U));
    return byte < r_info->append_compact_dirty_map_size && (r_info->append_compact_dirty_map[byte] & mask) != 0;
}

static bool validate_append_compaction_plan_for_apply(region_info_t *r_info, append_compaction_context_t *context, append_compaction_plan_t *plan, table_handler_t *t_handler,
                                                      bool allow_recovery_marker, const char **reason, uint64_t *physical_file_size) {
    *reason = "invalid arguments";
    *physical_file_size = UINT64_MAX;

    if (r_info == NULL || context == NULL || plan == NULL || t_handler == NULL || context->result_buffer == NULL)
        return false;

    if (plan->records != context->records) {
        *reason = "record array changed";
        return false;
    }

    if (!append_compaction_region_ready_internal(r_info, plan->b, plan->bs, allow_recovery_marker)) {
        *reason = "region is no longer ready";
        return false;
    }

    if (plan->gc_safe_revision != 0 && (plan->gc_safe_revision != get_record_gc_safe_revision(r_info) || !record_gc_delete_keys(r_info, context, plan, t_handler, false))) {
        *reason = "GC permission or marker changed";
        return false;
    }

    if (plan->source_blocks == 0 || plan->source_blocks > APPEND_COMPACTION_BLOCKS_PER_SEGMENT) {
        *reason = "invalid source block count";
        return false;
    }

    if (plan->result_write_offset != (uint64_t)plan->first_block * FIX_BLOCK_SIZE || plan->result_write_size != plan->source_blocks * FIX_BLOCK_SIZE) {
        *reason = "result write range changed";
        return false;
    }

    if (plan->original_b_offset != r_info->r_idx_b[plan->bs]->b_offset[plan->b]) {
        *reason = "b_offset changed";
        return false;
    }

    if (plan->new_b_offset > plan->original_b_offset) {
        *reason = "new b_offset exceeds original";
        return false;
    }

    if (plan->truncate_blocks != plan->original_b_offset - plan->new_b_offset) {
        *reason = "truncate block count changed";
        return false;
    }

    if (!append_compaction_plan_is_service_actionable(plan)) {
        *reason = "plan does not remove stale records";
        return false;
    }

    if (plan->truncate_blocks != 0) {
        if (plan->new_file_size != ((uint64_t)plan->new_b_offset + 1U) * FIX_BLOCK_SIZE) {
            *reason = "new file size does not match b_offset";
            return false;
        }
    } else if (plan->new_b_offset != plan->original_b_offset || plan->new_file_size != plan->original_file_size || plan->freed_blocks != 0 || plan->tail_blocks != 0) {
        *reason = "invalid rewrite-only plan";
        return false;
    }
    struct stat file_stat;

    if (fstat(r_info->r_idx_b[plan->bs]->fd[plan->b], &file_stat) != 0 || file_stat.st_size < 0) {
        *reason = "cannot get current file size";
        return false;
    }
    *physical_file_size = (uint64_t)file_stat.st_size;

    if (*physical_file_size != plan->original_file_size) {
        *reason = "physical file size changed";
        return false;
    }

    if (plan->truncate_blocks != 0) {
        if (plan->new_file_size >= plan->original_file_size) {
            *reason = "plan does not reduce physical file size";
            return false;
        }
    } else if (plan->new_file_size != plan->original_file_size) {
        *reason = "rewrite-only plan changes physical file size";
        return false;
    }

    for (uint32_t block_pos = 0; block_pos < plan->source_blocks; block_pos++) {
        uint32_t block = plan->first_block + block_pos;

        if (block > plan->original_b_offset) {
            *reason = "source block exceeds original b_offset";
            return false;
        }

        if (block != 0 && r_info->r_idx_b[plan->bs]->idx[plan->b][block].z_index != plan->source_z_index[block_pos]) {
            *reason = "source z_index changed";
            return false;
        }
    }

    for (uint32_t tail_pos = 0; tail_pos < plan->tail_blocks; tail_pos++) {
        uint32_t block = plan->tail_first_block + tail_pos;

        if (block > plan->original_b_offset) {
            *reason = "tail block exceeds original b_offset";
            return false;
        }

        if (r_info->r_idx_b[plan->bs]->idx[plan->b][block].z_index != plan->tail_z_index[tail_pos]) {
            *reason = "tail z_index changed";
            return false;
        }
    }

    if (verify_append_compaction_plan(r_info, plan->b, plan->bs, context, plan, t_handler) != APPEND_COMPACTION_ANALYZE_OK) {
        *reason = "result content verification failed";
        return false;
    }
    *reason = "ok";
    return true;
}


static bool update_append_compaction_indexes(region_info_t *r_info, append_compaction_context_t *context, append_compaction_plan_t *plan, table_handler_t *t_handler) {
    msg_cli_t msg_cli = {0};
    msg_kv_t msg_kv = {0};
    struct msg_qv_t msg_qv = {0};
    msg_cli.kv = &msg_kv;
    msg_kv.qv = &msg_qv;
    uint32_t record_no = 0;
    uint64_t scans_us = 0;

    for (uint32_t block_pos = 0; block_pos < plan->source_blocks; block_pos++) {
        uint32_t new_block = plan->first_block + block_pos;

        if (new_block == 0)
            continue;
        uint32_t used_bytes = (uint32_t)plan->result_z_index[block_pos] * r_info->r_idx_b[plan->bs]->bsize_factor;
        uint32_t record_offset = 0;

        while (record_offset < used_bytes) {
            uint32_t aligned_size = 0;
            uint8_t *record = context->result_buffer + block_pos * FIX_BLOCK_SIZE + record_offset;

            if (!parse_append_record(record, used_bytes - record_offset, r_info, plan->bs, &msg_cli, &aligned_size) || record_no >= plan->records_count)
                return false;
            append_compaction_record_move_t *move = &plan->records[record_no++];

            if (move->new_block != new_block)
                return false;

            if (move->old_block != move->new_block) {
                memcpy(r_info->digest, move->digest, DIGEST_SIZE_SMALL);
                r_info->has_digest_calc = true;
                oc_index_corrector(r_info, plan->b, &msg_cli, move->new_block, (uint32_t)-1, plan->bs, t_handler);
                r_info->has_digest_calc = false;
                record_gc_state_t *gc = &r_info->record_gc;

                if (msg_cli.kv->qv_size == 0 && gc->erasing && gc->generation == atomic_load(&r_info->record_gc_generation)) {
                    uint64_t version;
                    uint32_t segment = move->new_block / APPEND_COMPACTION_BLOCKS_PER_SEGMENT;

                    if (disk_record_version_read(record, aligned_size, &version) && version < gc->safe_revision && segment < gc->erase_segment) {
                        gc->erase_segment = segment;
                        ++gc->erase_rewinds;
                    }
                }
                uint64_t scans_started_us = append_compaction_monotonic_us();

                if (msg_cli.kv->qv_size != 0)
                    handle_scans(r_info, plan->b, plan->bs, move->new_block, move->old_block, &msg_cli, t_handler->cid);
                scans_us += append_compaction_monotonic_us() - scans_started_us;
            }
            record_offset += aligned_size;
        }
    }
    atomic_fetch_add(&append_compact_metric_apply_scans_us, scans_us);
    return record_no == plan->records_count;
}

static void clear_append_compaction_dirty_segment(region_info_t *r_info, uint32_t segment) {
    uint32_t byte = segment / 8U;
    uint8_t mask = (uint8_t)(1U << (segment & 7U));

    if (byte < r_info->append_compact_dirty_map_size && (r_info->append_compact_dirty_map[byte] & mask) != 0) {
        r_info->append_compact_dirty_map[byte] &= (uint8_t)~mask;

        if (r_info->append_compact_dirty_segments != 0)
            r_info->append_compact_dirty_segments--;
    }
}

static uint32_t refresh_append_compaction_request(region_info_t *r_info, uint8_t bs) {
    uint64_t current_ms = (uint64_t)getCurrentTimeMs();
    uint32_t budget =
        r_info->append_compact_dirty_segments == 0 || current_ms < atomic_load(&r_info->append_compaction_retry_after_ms) ? 0 : get_append_compaction_budget(r_info, bs);

    if (budget != 0) {
        if (!atomic_exchange(&r_info->append_compaction_requested, true))
            atomic_store(&r_info->append_compaction_requested_since_ms, current_ms);
    } else {
        atomic_store(&r_info->append_compaction_requested, false);
        atomic_store(&r_info->append_compaction_requested_since_ms, 0);
    }
    return budget;
}


static uint8_t find_append_compaction_data_bs(region_info_t *r_info);
int request_append_compaction_shutdown_drain(region_info_t *r_info) {
    if (r_info == NULL)
        return -1;
    uint8_t bs = find_append_compaction_data_bs(r_info);
    bool dirty = atomic_load(&r_info->append_compact_dirty_segments) != 0;
    bool truncate_pending = atomic_load(&r_info->append_compact_truncate_pending);

    if (!dirty && !truncate_pending)
        return 0;

    if (bs == UINT8_MAX || r_info->bad_data || r_info->stop || atomic_load(&r_info->r_need_data_chk) != 0)
        return -1;

    if (r_info->idx_resizer != RESIZE_IDX_DONE || r_info->bulk_mode != BUIK_RELICA_OFF)
        return 1;

    if (!append_compaction_region_ready(r_info, FBP, bs))
        return -1;
    bool needs_compaction = dirty && get_append_compaction_idle_budget(r_info, bs) != 0;

    if (needs_compaction && !atomic_exchange(&r_info->append_compaction_requested, true))
        atomic_store(&r_info->append_compaction_requested_since_ms, (uint64_t)getCurrentTimeMs());
    return needs_compaction || truncate_pending ? 1 : 0;
}


static void finish_append_compaction_accounting(region_info_t *r_info, append_compaction_plan_t *plan) {
    atomic_store(&r_info->append_compaction_retry_after_ms, 0);
    clear_append_compaction_dirty_segment(r_info, plan->segment);
    uint32_t first_removed_segment = plan->new_b_offset / APPEND_COMPACTION_BLOCKS_PER_SEGMENT + 1U;
    uint32_t map_segments = r_info->append_compact_dirty_map_size * 8U;

    for (uint32_t segment = first_removed_segment; segment < map_segments; segment++)
        clear_append_compaction_dirty_segment(r_info, segment);
    uint64_t stale_bytes = plan->removed_garbage_bytes - plan->gc_removed_bytes;

    if (r_info->append_compact_garbage_bytes > stale_bytes)
        r_info->append_compact_garbage_bytes -= stale_bytes;
    else
        r_info->append_compact_garbage_bytes = 0;

    if (r_info->append_compact_occupied_bytes > plan->removed_garbage_bytes)
        r_info->append_compact_occupied_bytes -= plan->removed_garbage_bytes;
    else
        r_info->append_compact_occupied_bytes = 0;

    if (r_info->append_compact_dirty_segments == 0)
        r_info->append_compact_garbage_bytes = 0;

    if ((plan->truncate_blocks != 0 || plan->gc_safe_revision != 0) && plan->result_free_blocks != 0) {
        for (uint32_t pos = plan->first_block == 0 ? 1U : 0U; pos < plan->source_blocks && plan->first_block + pos < plan->new_b_offset; pos++) {
            if (plan->result_z_index[pos] == 0) {
                mark_append_compaction_segment(r_info, plan->first_block + pos, 0, plan->bs);
                break;
            }
        }
    }
    uint32_t remaining_segments = plan->new_b_offset / APPEND_COMPACTION_BLOCKS_PER_SEGMENT + 1U;
    r_info->append_compact_cursor_segment = remaining_segments == 0 ? 0 : (plan->segment + 1U) % remaining_segments;
    refresh_append_compaction_request(r_info, plan->bs);
}

static bool apply_append_compaction_plan_internal(region_info_t *r_info, append_compaction_context_t *context, append_compaction_plan_t *plan, table_handler_t *t_handler,
                                                  bool replicate) {
    bool allow_recovery_marker = !replicate;

    if (r_info == NULL || context == NULL || plan == NULL || t_handler == NULL || !append_compaction_region_ready_internal(r_info, plan->b, plan->bs, allow_recovery_marker))
        return false;
    uint64_t phase_started_us = append_compaction_monotonic_us();
    flush_write_4K_buffer(r_info, t_handler->cid);
    atomic_fetch_add(&append_compact_metric_apply_flush_us, append_compaction_monotonic_us() - phase_started_us);
    const char *validation_reason = NULL;
    uint64_t physical_file_size = UINT64_MAX;
    phase_started_us = append_compaction_monotonic_us();

    if (!validate_append_compaction_plan_for_apply(r_info, context, plan, t_handler, allow_recovery_marker, &validation_reason, &physical_file_size)) {
        atomic_fetch_add(&append_compact_metric_apply_validate_us, append_compaction_monotonic_us() - phase_started_us);
        char dt[DT_LOG_SIZE];
        uint32_t current_b_offset =
            plan->bs <= BLOCK_ARRAY_LAST_POS && r_info->r_idx_b[plan->bs] != NULL && plan->b < BUCKETS ? r_info->r_idx_b[plan->bs]->b_offset[plan->b] : UINT32_MAX;
        fprintf(stderr,
                "%s WARN [COMPACT_SERVICE=%015lu] Append compaction plan rejected before apply for %s, segment=%u, reason=%s, b_offset=%u/%u->%u, truncate_blocks=%u, "
                "physical_file=%lu, original_file=%lu, new_file=%lu, source_blocks=%u, tail_blocks=%u\n",
                curr_dt(dt), t_handler->cid, r_info->uuid, plan->segment, validation_reason, current_b_offset, plan->original_b_offset, plan->new_b_offset, plan->truncate_blocks,
                physical_file_size, plan->original_file_size, plan->new_file_size, plan->source_blocks, plan->tail_blocks);
        return false;
    }
    atomic_fetch_add(&append_compact_metric_apply_validate_us, append_compaction_monotonic_us() - phase_started_us);
    log_append_compaction_result_probe(r_info, context, plan, replicate ? "service_leader_prewrite" : "client_leader_prewrite", t_handler->cid);
    phase_started_us = append_compaction_monotonic_us();

    if (replicate) {
        replicas_writer(r_info, (char *)context->result_buffer, plan->b, plan->result_write_offset, plan->result_write_size, false, plan->bs, true, t_handler);
        atomic_fetch_add(&append_compact_metric_apply_replica_us, append_compaction_monotonic_us() - phase_started_us);
    } else {
        write_file(r_info->r_idx_b[plan->bs]->fd[plan->b], plan->result_write_offset, context->result_buffer, plan->result_write_size, r_info->r_idx_b[plan->bs]->bsize_factor,
                   r_info, t_handler->cid);
    }
    log_append_compaction_disk_probe(r_info, plan, r_info->r_idx_b[plan->bs]->fd[plan->b], replicate ? "service_leader_postwrite" : "client_leader_postwrite", t_handler->cid);

    if (plan->gc_safe_revision != 0 && (plan->gc_generation != atomic_load(&r_info->record_gc_generation) || !record_gc_ready(r_info, plan->bs)))
        return false;
    phase_started_us = append_compaction_monotonic_us();

    if (!update_append_compaction_indexes(r_info, context, plan, t_handler)) {
        atomic_fetch_add(&append_compact_metric_apply_index_us, append_compaction_monotonic_us() - phase_started_us);
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Append compaction index apply failed for %s, segment=%u; recovery marker preserved\n", curr_dt(dt), t_handler->cid,
                r_info->uuid, plan->segment);
        r_info->stop = true;
        r_info->bad_data = true;
        return false;
    }
    atomic_fetch_add(&append_compact_metric_apply_index_us, append_compaction_monotonic_us() - phase_started_us);
    phase_started_us = append_compaction_monotonic_us();

    for (uint32_t block_pos = 0; block_pos < plan->source_blocks; block_pos++) {
        uint32_t block = plan->first_block + block_pos;

        if (block != 0)
            r_info->r_idx_b[plan->bs]->idx[plan->b][block].z_index = plan->result_z_index[block_pos];
    }

    for (uint32_t block = plan->new_b_offset + 1U; block <= plan->original_b_offset; block++)
        r_info->r_idx_b[plan->bs]->idx[plan->b][block].z_index = 0;
    atomic_fetch_add(&append_compact_metric_apply_zindex_us, append_compaction_monotonic_us() - phase_started_us);
    bool did_truncate = false;

    if (plan->truncate_blocks == 0) {
        did_truncate = false;
    } else if (!APPEND_COMPACTION_SKIP_TRUNCATE_POC && atomic_load(&append_compact_truncate_allowed)) {
        phase_started_us = append_compaction_monotonic_us();

        if (replicate)
            replicas_writer(r_info, NULL, plan->b, plan->new_file_size, 0, true, plan->bs, true, t_handler);
        else
            truncate_file(r_info, r_info->r_idx_b[plan->bs]->fd[plan->b], plan->new_file_size, t_handler->cid);
        atomic_fetch_add(&append_compact_metric_apply_truncate_us, append_compaction_monotonic_us() - phase_started_us);
        struct stat file_stat;

        if (fstat(r_info->r_idx_b[plan->bs]->fd[plan->b], &file_stat) != 0 || file_stat.st_size < 0 || (uint64_t)file_stat.st_size != plan->new_file_size) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Append compaction truncate verification failed for %s, expected=%lu; recovery marker preserved\n", curr_dt(dt),
                    t_handler->cid, r_info->uuid, plan->new_file_size);
            r_info->stop = true;
            r_info->bad_data = true;
            return false;
        }
        atomic_store(&r_info->append_compact_truncate_pending, false);
        did_truncate = true;
    } else {
        atomic_store(&r_info->append_compact_truncate_pending, true);
    }
    phase_started_us = append_compaction_monotonic_us();
    r_info->r_idx_b[plan->bs]->b_offset[plan->b] = plan->new_b_offset;
    finish_append_compaction_accounting(r_info, plan);
    atomic_fetch_add(&append_compact_metric_garbage_removed_bytes, plan->removed_garbage_bytes);
    atomic_fetch_add(&append_compact_metric_segments, 1);
    atomic_fetch_add(&append_compact_metric_changed_blocks, plan->changed_blocks);

    if (replicate)
        atomic_fetch_add(&append_compact_metric_sent_blocks, plan->source_blocks);
    atomic_fetch_add(&append_compact_metric_apply_accounting_us, append_compaction_monotonic_us() - phase_started_us);

    if (replicate && atomic_load(&r_info->r_need_data_chk) != 0) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Append compaction recovery marker was not removed for %s, segment=%u; stop current batch\n", curr_dt(dt),
                t_handler->cid, r_info->uuid, plan->segment);
        return false;
    }

    if (log_level > 1) {
        char dt[DT_LOG_SIZE];

        if (!did_truncate)
            fprintf(stderr,
                    "%s INFO [COMPACT_SERVICE=%015lu] Append compaction applied for %s, segment=%u, removed=%lu bytes, physical_file=%lu retained, logical_file=%lu, "
                    "b_offset=%u->%u\n",
                    curr_dt(dt), t_handler->cid, r_info->uuid, plan->segment, plan->removed_garbage_bytes, plan->original_file_size, plan->new_file_size, plan->original_b_offset,
                    plan->new_b_offset);
        else
            fprintf(stderr, "%s INFO [COMPACT_SERVICE=%015lu] Append compaction applied for %s, segment=%u, removed=%lu bytes, file=%lu->%lu, b_offset=%u->%u\n", curr_dt(dt),
                    t_handler->cid, r_info->uuid, plan->segment, plan->removed_garbage_bytes, plan->original_file_size, plan->new_file_size, plan->original_b_offset,
                    plan->new_b_offset);
    }
    record_gc_account_tail(r_info, plan->bs, plan->original_b_offset, plan->new_b_offset);
    return true;
}

bool apply_append_compaction_plan_local(region_info_t *r_info, append_compaction_context_t *context, append_compaction_plan_t *plan, table_handler_t *t_handler) {
    if (r_info == NULL)
        return false;
    return apply_append_compaction_plan_internal(r_info, context, plan, t_handler, false);
}


bool apply_append_compaction_plan(region_info_t *r_info, append_compaction_context_t *context, append_compaction_plan_t *plan, table_handler_t *t_handler) {
    return apply_append_compaction_plan_internal(r_info, context, plan, t_handler, true);
}


static uint64_t append_compaction_region_activity_sum(region_info_t *r_info) {
    return (uint64_t)atomic_load(&r_info->req_put_counter_sum) + (uint64_t)atomic_load(&r_info->req_get_counter_sum) + (uint64_t)atomic_load(&r_info->req_del_counter_sum) +
           (uint64_t)atomic_load(&r_info->req_scn_counter_sum);
}

static bool append_compaction_region_idle(region_info_t *r_info) {
    if (r_info == NULL)
        return false;
    uint64_t sampled_activity = atomic_load(&r_info->append_compact_last_activity_sum);
    return atomic_load(&r_info->append_compact_idle_streak) >= APPEND_COMPACTION_IDLE_STREAK_REQUIRED && append_compaction_region_activity_sum(r_info) == sampled_activity;
}

bool replicas_idle_commit_ready(region_info_t *r_info) {
    if (r_info == NULL || atomic_load(&is_shutting_down) != 0 || atomic_load(&rs_shutdown_state) != RS_SHUTDOWN_RUNNING)
        return false;

    if (!r_info->is_leader || r_info->has_parent_replica || r_info->is_freed || r_info->stop || r_info->bad_data || r_info->already_finishing ||
        (r_info->status != STATUS_ASSIGN_REGION && r_info->status != STATUS_START_REGION) || atomic_load(&r_info->r_need_data_chk) != 0 || !r_info->recovery_path_ready ||
        r_info->r_fname == NULL || r_info->replica == NULL || r_info->rsc_msg == NULL || r_info->r_idx_b == NULL || r_info->data_000_fd < 0 || r_info->max_idx_b <= 0 ||
        r_info->max_idx_b > BLOCK_ARRAY_LAST_POS + 1 || r_info->idx_resizer != RESIZE_IDX_DONE || r_info->bulk_mode != BUIK_RELICA_OFF || r_info->bulk_buff_offset1 != 0 ||
        r_info->bulk_buff_offset2 != 0 || r_info->is_write || r_info->is_replicate || r_info->replica->replica_control != 0 || r_info->waiting_count_lock != 0 ||
        r_info->waiting_count_admin != 0 || r_info->waiting_count_reconnect != 0 || atomic_load(&r_info->append_compact_truncate_pending) ||
        !append_compaction_region_idle(r_info))
        return false;

    if (atomic_load(&r_info->append_compact_dirty_segments) == 0)
        return true;
    uint8_t bs = find_append_compaction_data_bs(r_info);

    if (bs == UINT8_MAX || r_info->r_idx_b[bs]->idx == NULL || !append_compaction_region_ready(r_info, FBP, bs))
        return false;
    return get_append_compaction_idle_budget(r_info, bs) == 0;
}

bool replicas_idle_commit_load_ready(void) {
    int sleeping_ops_limit = atomic_load(&append_compact_sleeping_ops_limit);

    if (sleeping_ops_limit <= 0 || atomic_load(&append_compact_sleeping_ops_streak) < APPEND_COMPACTION_IDLE_STREAK_REQUIRED)
        return false;
    uint64_t current_ops = atomic_load(&ops);
    return current_ops < (uint64_t)sleeping_ops_limit;
}


static bool append_compaction_sleeping_load_ready(void) {
    if (!replicas_idle_commit_load_ready())
        return false;
    uint64_t current_ops = atomic_load(&ops);
    int ops_limit = atomic_load(&append_compact_ops_limit);
    return ops_limit <= 0 || current_ops > (uint64_t)ops_limit;
}

static bool append_compaction_sleeping_region_ready(region_info_t *r_info) {
    return append_compaction_sleeping_load_ready() && append_compaction_region_idle(r_info);
}

static bool finish_append_compaction_truncate_internal(region_info_t *r_info, uint8_t b, uint8_t bs, table_handler_t *t_handler, bool force_for_idle_region, bool for_gc) {
    bool truncate_allowed =
        for_gc ? record_gc_ready(r_info, bs) : (force_for_idle_region ? append_compaction_sleeping_region_ready(r_info) : atomic_load(&append_compact_truncate_allowed));

    if ((!for_gc && !atomic_load(&r_info->append_compact_truncate_pending)) || !truncate_allowed || !append_compaction_region_ready(r_info, b, bs))
        return false;
    flush_write_4K_buffer(r_info, t_handler->cid);
    truncate_allowed =
        for_gc ? record_gc_ready(r_info, bs) : (force_for_idle_region ? append_compaction_sleeping_region_ready(r_info) : atomic_load(&append_compact_truncate_allowed));

    if (!truncate_allowed || !append_compaction_region_ready(r_info, b, bs))
        return false;
    uint32_t original_last_block = r_info->r_idx_b[bs]->b_offset[b];
    uint32_t last_block = original_last_block;

    while (last_block > 0 && r_info->r_idx_b[bs]->idx[b][last_block].z_index == 0)
        last_block--;
    uint32_t last_block_used = last_block == 0 ? FIX_BLOCK_SIZE : (uint32_t)r_info->r_idx_b[bs]->idx[b][last_block].z_index * r_info->r_idx_b[bs]->bsize_factor;

    if (last_block_used > FIX_BLOCK_SIZE) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Last block z_index exceeds block size before deferred truncate for %s, block=%u, z_index=%u, bsize_factor=%u\n",
                curr_dt(dt), t_handler->cid, r_info->uuid, last_block, r_info->r_idx_b[bs]->idx[b][last_block].z_index, r_info->r_idx_b[bs]->bsize_factor);
        r_info->stop = true;
        r_info->bad_data = true;
        return false;
    }
    uint64_t logical_file_size = (uint64_t)last_block * FIX_BLOCK_SIZE + last_block_used;
    struct stat file_stat;

    if (fstat(r_info->r_idx_b[bs]->fd[b], &file_stat) != 0 || file_stat.st_size < 0) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Failed to get physical size before deferred truncate for %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
        return false;
    }
    uint64_t physical_file_size = (uint64_t)file_stat.st_size;

    if (physical_file_size < logical_file_size) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Physical file is shorter than logical file for %s, physical=%lu, logical=%lu\n", curr_dt(dt), t_handler->cid,
                r_info->uuid, physical_file_size, logical_file_size);
        r_info->stop = true;
        r_info->bad_data = true;
        return false;
    }

    if (physical_file_size > logical_file_size || for_gc) {
        replicas_writer(r_info, NULL, b, logical_file_size, 0, true, bs, true, t_handler);

        if (fstat(r_info->r_idx_b[bs]->fd[b], &file_stat) != 0 || file_stat.st_size < 0 || (uint64_t)file_stat.st_size != logical_file_size) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Deferred truncate verification failed for %s, expected=%lu\n", curr_dt(dt), t_handler->cid, r_info->uuid,
                    logical_file_size);
            r_info->stop = true;
            r_info->bad_data = true;
            return false;
        }
    }

    if (for_gc) {
        for (int n = 1; n < rf; ++n) {
            region_info_t *child = n == 1 ? r_info->replica->r_info1 : r_info->replica->r_info2;

            if (child == NULL)
                continue;
            uint8_t child_bs = find_append_compaction_data_bs(child);

            if (child_bs == UINT8_MAX || fstat(child->r_idx_b[child_bs]->fd[b], &file_stat) != 0 || file_stat.st_size < 0 || (uint64_t)file_stat.st_size != logical_file_size) {
                invalidate_record_gc(r_info);
                return false;
            }
        }
    }
    r_info->r_idx_b[bs]->b_offset[b] = last_block;
    record_gc_account_tail(r_info, bs, original_last_block, last_block);
    atomic_store(&r_info->append_compact_truncate_pending, false);

    if (log_level > 1 || (for_gc && log_level >= 1)) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s INFO [COMPACT_SERVICE=%015lu] Deferred truncate applied for %s, file=%lu->%lu, b_offset=%u->%u\n", curr_dt(dt), t_handler->cid, r_info->uuid,
                physical_file_size, logical_file_size, original_last_block, last_block);
    }
    return true;
}

static bool finish_pending_append_compaction_truncate(region_info_t *r_info, uint8_t b, uint8_t bs, table_handler_t *t_handler, bool force_for_idle_region) {
    return finish_append_compaction_truncate_internal(r_info, b, bs, t_handler, force_for_idle_region, false);
}

static bool record_gc_plan_witness(region_info_t *r, append_compaction_context_t *context, const append_compaction_plan_t *plan, uint64_t *version) {
    *version = 0;
    uint32_t factor = r->r_idx_b[plan->bs]->bsize_factor;

    for (uint32_t pos = 0; pos < plan->source_blocks; ++pos) {
        if (plan->first_block + pos == 0)
            continue;
        uint32_t used = (uint32_t)plan->result_z_index[pos] * factor;

        for (uint32_t off = 0; off < used;) {
            disk_record_header_t h;

            if (!disk_record_decode_header(context->result_buffer + pos * FIX_BLOCK_SIZE + off, used - off, &h))
                return false;
            uint32_t size = (uint32_t)align_up(h.msg_size, factor);

            if (size == 0 || size > used - off)
                return false;

            if (h.version > *version)
                *version = h.version;
            off += size;
        }
    }
    return true;
}


static void record_gc_log_state(region_info_t *r, table_handler_t *h, const char *stage, const char *reason) {
    if (log_level < 1)
        return;
    record_gc_state_t *gc = &r->record_gc;
    uint8_t bs = find_append_compaction_data_bs(r);
    uint32_t last = bs == UINT8_MAX ? UINT32_MAX : r->r_idx_b[bs]->b_offset[FBP];
    char dt[DT_LOG_SIZE];
    fprintf(stderr,
            "%s INFO [RECORD_GC=%015lu] stage=%s uuid=%s reason=%s generation=%lu/%lu phase=%u boundary=%lu confirmed=%lu witness=%lu safe_revision=%lu pending_begin=%lu "
            "pending_delete=%lu history_next=%u history_end=%u erase_next=%u erase_end=%u last_block=%u erasing=%u erased=%u truncate_pending=%u history_segments=%lu "
            "history_applied=%lu history_stale=%lu history_obsolete_mk=%lu erase_segments=%lu erase_applied=%lu erase_stale=%lu erase_obsolete_mk=%lu removed_mk=%lu "
            "removed_min=%lu removed_max=%lu retained_mk_observations=%lu retained_min=%lu retained_max=%lu erase_rewinds=%lu stop=%u bad_data=%u status=%u resize=%u "
            "recovery=%u version_pending=%lu data_check_enabled=%u\n",
            curr_dt(dt), h->cid, stage, r->uuid, reason, gc->generation, atomic_load(&r->record_gc_generation), (unsigned)gc->phase, gc->boundary, r->record_versions.confirmed,
            gc->witness_version, gc->safe_revision, gc->pending_begin, atomic_load(&r->pending_delete), gc->next_segment, gc->end_block, gc->erase_segment, gc->erase_end_block,
            last, (unsigned)gc->erasing, (unsigned)gc->erased, (unsigned)atomic_load(&r->append_compact_truncate_pending), gc->history_stats.segments, gc->history_stats.applied,
            gc->history_stats.stale_records, gc->history_stats.markers.obsolete, gc->erase_stats.segments, gc->erase_stats.applied, gc->erase_stats.stale_records,
            gc->erase_stats.markers.obsolete, gc->erase_stats.markers.removed, gc->erase_stats.markers.removed_min, gc->erase_stats.markers.removed_max,
            gc->erase_stats.markers.retained, gc->erase_stats.markers.retained_min, gc->erase_stats.markers.retained_max, gc->erase_rewinds, (unsigned)r->stop,
            (unsigned)r->bad_data, (unsigned)r->status, (unsigned)r->idx_resizer, (unsigned)atomic_load(&r->r_need_data_chk), r->record_versions.pending,
            (unsigned)data_check_enabled);
}

static int record_gc_pause(region_info_t *r, table_handler_t *h, const char *reason, int result) {
    if (log_level >= 1 && (r->record_gc.pause_reason == NULL || strcmp(r->record_gc.pause_reason, reason) != 0)) {
        record_gc_log_state(r, h, "pause", reason);
        r->record_gc.pause_reason = reason;
    }
    return result;
}

static void record_gc_resume(region_info_t *r, table_handler_t *h) {
    if (r->record_gc.pause_reason != NULL) {
        record_gc_log_state(r, h, "resume", r->record_gc.pause_reason);
        r->record_gc.pause_reason = NULL;
    }
}


static void record_gc_report_empty(region_info_t *r, table_handler_t *h) {
    if (r->record_gc.phase != RECORD_GC_IDLE && !r->record_gc.empty_reported) {
        record_gc_resume(r, h);
        record_gc_log_state(r, h, "idle", "no_pending_mk");
        r->record_gc.empty_reported = true;
    }
}

static void record_gc_account_plan(record_gc_pass_stats_t *s, const append_compaction_plan_t *plan) {
    ++s->segments;

    if (plan->stale_records != 0)
        ++s->applied;
    s->stale_records += plan->stale_records;
    const record_gc_marker_stats_t *p = &plan->gc_marker_stats;
    s->markers.obsolete += p->obsolete;
    s->markers.removed += p->removed;
    s->markers.retained += p->retained;

    if (p->removed) {
        record_gc_version_range(p->removed_min, &s->markers.removed_min, &s->markers.removed_max);
        record_gc_version_range(p->removed_max, &s->markers.removed_min, &s->markers.removed_max);
    }

    if (p->retained) {
        record_gc_version_range(p->retained_min, &s->markers.retained_min, &s->markers.retained_max);
        record_gc_version_range(p->retained_max, &s->markers.retained_min, &s->markers.retained_max);
    }
}

int process_record_gc_visit(region_info_t *r, append_compaction_context_t *context, table_handler_t *h) {
    if (r == NULL || context == NULL || h == NULL)
        return RECORD_GC_BLOCKED;

    if (h->record_mutation != NULL || (h->append_compaction_piggyback != NULL && h->append_compaction_piggyback->active))
        return record_gc_pause(r, h, "mutation_or_piggyback", RECORD_GC_BLOCKED);
    uint8_t bs = find_append_compaction_data_bs(r);
    const char *blocked = bs == UINT8_MAX ? "no_data_file" : record_gc_block_reason(r, bs);

    if (blocked != NULL)
        return record_gc_pause(r, h, blocked, RECORD_GC_BLOCKED);
    record_gc_state_t *gc = &r->record_gc;
    uint64_t generation = atomic_load(&r->record_gc_generation);
    uint32_t last = r->r_idx_b[bs]->b_offset[FBP];

    if (gc->generation != generation || gc->fd != r->data_000_fd || gc->bs != bs || last < gc->expected_last_block) {
        if (gc->phase != RECORD_GC_IDLE)
            record_gc_log_state(r, h, "reset",
                                gc->generation != generation ? "generation_changed" : (gc->fd != r->data_000_fd || gc->bs != bs ? "data_file_changed" : "unexpected_shrink"));
        *gc = (record_gc_state_t){0};
    }

    if (gc->phase == RECORD_GC_COMPLETE) {
        if (!atomic_load(&r->append_compact_truncate_pending))
            return RECORD_GC_DONE;
        gc->phase = RECORD_GC_FINISH;
    }

    if (gc->phase == RECORD_GC_IDLE) {
        if (r->record_versions.confirmed == 0)
            return record_gc_pause(r, h, "no_confirmed_revision", RECORD_GC_BLOCKED);
        *gc = (record_gc_state_t){.generation = generation,
                                  .boundary = r->record_versions.confirmed,
                                  .end_block = last,
                                  .expected_last_block = last,
                                  .fd = r->data_000_fd,
                                  .bs = bs,
                                  .phase = RECORD_GC_SWEEP,
                                  .pending_begin = atomic_load(&r->pending_delete)};
        record_gc_log_state(r, h, "history_start", "new_pass");
    }
    gc->expected_last_block = last;

    if (gc->phase == RECORD_GC_SWEEP && gc->next_segment <= gc->end_block / APPEND_COMPACTION_BLOCKS_PER_SEGMENT &&
        gc->next_segment <= last / APPEND_COMPACTION_BLOCKS_PER_SEGMENT) {
        append_compaction_plan_t plan;
        int result = build_append_compaction_plan(r, FBP, bs, gc->next_segment, context, &plan, h);

        if (!r->stop && !r->bad_data && (result != APPEND_COMPACTION_ANALYZE_OK || (plan.stale_records != 0 && !append_compaction_plan_is_service_actionable(&plan))))
            result = build_append_compaction_plan_internal(r, FBP, bs, gc->next_segment, context, &plan, h, true, 0);

        if (result != APPEND_COMPACTION_ANALYZE_OK)
            return record_gc_pause(r, h, "history_build", RECORD_GC_RETRY);
        uint64_t witness;

        if (!record_gc_plan_witness(r, context, &plan, &witness))
            return record_gc_pause(r, h, "history_witness", RECORD_GC_RETRY);

        if (plan.stale_records != 0 || plan.tail_stale_records != 0) {
            if (!append_compaction_plan_is_service_actionable(&plan) || !apply_append_compaction_plan(r, context, &plan, h))
                return record_gc_pause(r, h, "history_apply", RECORD_GC_RETRY);
        }

        if (generation != atomic_load(&r->record_gc_generation) || !record_gc_ready(r, bs))
            return record_gc_pause(r, h, "history_state_changed", RECORD_GC_RETRY);

        if (witness > gc->witness_version)
            gc->witness_version = witness;
        record_gc_account_plan(&gc->history_stats, &plan);
        ++gc->next_segment;
        record_gc_resume(r, h);
        return RECORD_GC_PROGRESS;
    }
    gc->phase = RECORD_GC_FINISH;

    if (!finish_append_compaction_truncate_internal(r, FBP, bs, h, false, true) || generation != atomic_load(&r->record_gc_generation) || !record_gc_ready(r, bs))
        return record_gc_pause(r, h, "history_truncate_or_state", RECORD_GC_RETRY);
    gc->safe_revision = gc->boundary < gc->witness_version ? gc->boundary : gc->witness_version;
    gc->phase = RECORD_GC_COMPLETE;
    record_gc_resume(r, h);
    record_gc_log_state(r, h, "history_complete", "truncate_confirmed");
    return RECORD_GC_DONE;
}


int process_record_gc_cleanup_visit(region_info_t *r, append_compaction_context_t *ctx, table_handler_t *h) {
    if (r == NULL || ctx == NULL || h == NULL)
        return RECORD_GC_BLOCKED;

    if (h->record_mutation != NULL || (h->append_compaction_piggyback && h->append_compaction_piggyback->active))
        return record_gc_pause(r, h, "mutation_or_piggyback", RECORD_GC_BLOCKED);
    uint8_t bs = find_append_compaction_data_bs(r);
    const char *blocked = bs == UINT8_MAX ? "no_data_file" : record_gc_block_reason(r, bs);

    if (blocked != NULL)
        return record_gc_pause(r, h, blocked, RECORD_GC_BLOCKED);
    record_gc_state_t *gc = &r->record_gc;
    uint64_t generation = atomic_load(&r->record_gc_generation);

    if (gc->erased && gc->generation == generation && gc->boundary == r->record_versions.confirmed) {
        record_gc_resume(r, h);
        return RECORD_GC_DONE;
    }

    if (r->pending_delete == 0) {
        record_gc_report_empty(r, h);
        return RECORD_GC_DONE;
    }

    if (gc->erased) {
        record_gc_log_state(r, h, "reset", "new_revision_or_generation");
        *gc = (record_gc_state_t){0};
    }
    uint64_t safe_revision = get_record_gc_safe_revision(r);

    if (safe_revision == 0) {
        int result = process_record_gc_visit(r, ctx, h);

        if (result != RECORD_GC_DONE)
            return result;

        if (gc->safe_revision == 0)
            record_gc_pause(r, h, "no_safe_revision", RECORD_GC_PROGRESS);
        return RECORD_GC_PROGRESS;
    }

    if (!gc->erasing) {
        gc->erasing = true;
        gc->erase_segment = 0;
        gc->erase_end_block = r->r_idx_b[bs]->b_offset[FBP];
        record_gc_log_state(r, h, "erase_start", "history_confirmed");
    }
    uint32_t segment = gc->erase_segment;

    if (segment > gc->erase_end_block / APPEND_COMPACTION_BLOCKS_PER_SEGMENT || segment > r->r_idx_b[bs]->b_offset[FBP] / APPEND_COMPACTION_BLOCKS_PER_SEGMENT) {
        gc->erasing = false;
        gc->erased = true;
        record_gc_resume(r, h);
        record_gc_log_state(r, h, "erase_complete", "range_exhausted");
        return RECORD_GC_DONE;
    }
    append_compaction_plan_t plan;
    int built = build_append_compaction_plan_internal(r, FBP, bs, segment, ctx, &plan, h, true, safe_revision);

    if (built != APPEND_COMPACTION_ANALYZE_OK)
        return record_gc_pause(r, h, "erase_build", RECORD_GC_RETRY);

    if (generation != atomic_load(&r->record_gc_generation) || !record_gc_ready(r, bs))
        return record_gc_pause(r, h, "erase_state_changed", RECORD_GC_RETRY);
    uint64_t pending_before = atomic_load(&r->pending_delete);

    if (plan.stale_records != 0) {
        if (!record_gc_delete_keys(r, ctx, &plan, h, false))
            return record_gc_pause(r, h, "erase_index_check", RECORD_GC_RETRY);
        record_mutation_t guard = {0};
        h->record_mutation = &guard;
        bool ok = apply_append_compaction_plan(r, ctx, &plan, h) && finish_append_compaction_truncate_internal(r, FBP, bs, h, false, true) &&
                  generation == atomic_load(&r->record_gc_generation) && record_gc_ready(r, bs) && record_gc_delete_keys(r, ctx, &plan, h, true);
        h->record_mutation = NULL;

        if (!ok) {
            if (guard.changed)
                stop_region_after_key_error(r, KEY_EXTRACT_INVALID_STATE, h);
            return record_gc_pause(r, h, "erase_apply_truncate_or_index", RECORD_GC_RETRY);
        }
    }
    record_gc_account_plan(&gc->erase_stats, &plan);
    gc->empty_reported = false;
    gc->erase_segment = segment + 1U;
    record_gc_resume(r, h);

    if (log_level >= 1 && plan.delete_count != 0) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr,
                "%s INFO [RECORD_GC=%015lu] stage=erase_applied uuid=%s generation=%lu boundary=%lu segment=%u removed_mk=%u removed_min=%lu removed_max=%lu safe_revision=%lu "
                "pending_before=%lu pending_after=%lu truncate_pending=%u\n",
                curr_dt(dt), h->cid, r->uuid, gc->generation, gc->boundary, segment, plan.delete_count, plan.gc_marker_stats.removed_min, plan.gc_marker_stats.removed_max,
                safe_revision, pending_before, atomic_load(&r->pending_delete), (unsigned)atomic_load(&r->append_compact_truncate_pending));
    }

    if (r->pending_delete == 0)
        record_gc_report_empty(r, h);
    return RECORD_GC_PROGRESS;
}


bool record_gc_candidate(region_info_t *r, bool high_load, uint64_t now_ms) {
    if (r == NULL || atomic_load(&r->pending_delete) == 0 || now_ms < atomic_load(&r->record_gc_next_visit_ms))
        return false;
    int limit = atomic_load(&append_compact_pending_delete_limit);
    return !high_load || atomic_load(&r->record_gc_forced) || atomic_load(&r->pending_delete) > (uint64_t)(limit < 0 ? 0 : limit);
}


int process_record_gc_background_visit(region_info_t *r, append_compaction_context_t *ctx, table_handler_t *h, bool sleeping) {
    uint64_t now = append_compaction_monotonic_us() / 1000U;
    int limit = atomic_load(&append_compact_ops_limit);
    bool high = limit > 0 && atomic_load(&ops) > (uint64_t)limit;

    if ((sleeping ? atomic_load(&append_compact_sleeping_thread_count) <= 0 : atomic_load(&append_compact_thread_count) <= 0) ||
        !record_gc_candidate(r, !sleeping && high, now) || (sleeping && !append_compaction_sleeping_region_ready(r)))
        return RECORD_GC_BLOCKED;
    int pressure = atomic_load(&append_compact_pending_delete_limit);

    if (!sleeping && r->pending_delete > (uint64_t)(pressure < 0 ? 0 : pressure))
        atomic_store(&r->record_gc_forced, true);
    int result = process_record_gc_cleanup_visit(r, ctx, h);

    if (result == RECORD_GC_DONE || r->pending_delete == 0)
        atomic_store(&r->record_gc_forced, false);
    uint64_t delay = result == RECORD_GC_PROGRESS ? (high ? 100U : 10U) : 1000U;
    atomic_store(&r->record_gc_next_visit_ms, now + delay);
    return result;
}

static bool find_next_append_compaction_segment(region_info_t *r_info, uint32_t last_block, uint32_t *segment) {
    if (r_info->append_compact_dirty_map == NULL || r_info->append_compact_dirty_map_size == 0 || r_info->append_compact_dirty_segments == 0)
        return false;
    uint32_t segment_count = last_block / APPEND_COMPACTION_BLOCKS_PER_SEGMENT + 1U;
    uint32_t start = r_info->append_compact_cursor_segment < segment_count ? r_info->append_compact_cursor_segment : 0;

    for (uint32_t i = 0; i < segment_count; i++) {
        uint32_t current = (start + i) % segment_count;

        if (append_compaction_segment_is_dirty(r_info, current)) {
            *segment = current;
            return true;
        }
    }
    return false;
}


static void append_compaction_retry_later(region_info_t *r_info) {
    atomic_store(&r_info->append_compaction_requested, false);
    atomic_store(&r_info->append_compaction_requested_since_ms, 0);
    uint64_t current_ms = (uint64_t)getCurrentTimeMs();
    atomic_store(&r_info->append_compaction_retry_after_ms,
                 current_ms > UINT64_MAX - APPEND_COMPACTION_RETRY_DELAY_MS ? UINT64_MAX : current_ms + APPEND_COMPACTION_RETRY_DELAY_MS);
}

void free_append_compaction_piggyback(table_handler_t *t_handler) {
    if (t_handler == NULL || t_handler->append_compaction_piggyback == NULL)
        return;
    append_compaction_piggyback_t *piggyback = t_handler->append_compaction_piggyback;
    free_append_compaction_context(&piggyback->context);
    free(piggyback);
    t_handler->append_compaction_piggyback = NULL;
}

void complete_append_compaction_piggyback(region_info_t *r_info, table_handler_t *t_handler) {
    if (r_info == NULL || t_handler == NULL || t_handler->append_compaction_piggyback == NULL)
        return;
    append_compaction_piggyback_t *piggyback = t_handler->append_compaction_piggyback;

    if (!piggyback->active || !piggyback->attached_to_write || piggyback->r_info != r_info)
        return;
    piggyback->active = false;
    piggyback->local_applied = false;
    piggyback->attached_to_write = false;
    piggyback->truncate_applied = false;
    piggyback->r_info = NULL;
    memset(&piggyback->plan, 0, sizeof(piggyback->plan));
}

static uint8_t find_append_compaction_data_bs(region_info_t *r_info) {
    if (r_info == NULL || r_info->r_idx_b == NULL)
        return UINT8_MAX;

    for (uint8_t bs = 0; bs < r_info->max_idx_b; bs++)
        if (r_info->r_idx_b[bs] != NULL && r_info->r_idx_b[bs]->fd[FBP] == r_info->data_000_fd && r_info->r_idx_b[bs]->block_size == FIX_BLOCK_SIZE)
            return bs;
    return UINT8_MAX;
}


int prepare_append_compaction_piggyback(region_info_t *r_info, uint8_t request_type, table_handler_t *t_handler) {
    if (r_info == NULL || t_handler == NULL)
        return APPEND_COMPACTION_PIGGYBACK_NONE;
    append_compaction_piggyback_t *piggyback = t_handler->append_compaction_piggyback;

    if (piggyback != NULL && piggyback->active)
        return APPEND_COMPACTION_PIGGYBACK_NONE;
    uint8_t bs = find_append_compaction_data_bs(r_info);

    if (bs == UINT8_MAX || !append_compaction_piggyback_ready(r_info, FBP, bs, request_type))
        return APPEND_COMPACTION_PIGGYBACK_NONE;

    if (piggyback == NULL) {
        piggyback = calloc(1, sizeof(*piggyback));

        if (piggyback == NULL || !init_append_compaction_context(&piggyback->context)) {
            if (piggyback != NULL) {
                free_append_compaction_context(&piggyback->context);
                free(piggyback);
            }
            append_compaction_retry_later(r_info);
            return APPEND_COMPACTION_PIGGYBACK_NONE;
        }
        t_handler->append_compaction_piggyback = piggyback;
    }
    uint32_t segment = 0;

    if (!find_next_append_compaction_segment(r_info, r_info->r_idx_b[bs]->b_offset[FBP], &segment)) {
        atomic_store(&r_info->append_compaction_requested, false);
        atomic_store(&r_info->append_compaction_requested_since_ms, 0);
        return APPEND_COMPACTION_PIGGYBACK_NONE;
    }
    memset(&piggyback->plan, 0, sizeof(piggyback->plan));
    uint64_t build_started_us = append_compaction_monotonic_us();
    int result = build_append_compaction_plan(r_info, FBP, bs, segment, &piggyback->context, &piggyback->plan, t_handler);
    atomic_fetch_add(&append_compact_metric_build_us, append_compaction_monotonic_us() - build_started_us);
    atomic_fetch_add(&append_compact_metric_build_prepare_us, piggyback->plan.metric_prepare_us);
    atomic_fetch_add(&append_compact_metric_build_source_read_us, piggyback->plan.metric_source_read_us);
    atomic_fetch_add(&append_compact_metric_build_source_lookup_us, piggyback->plan.metric_source_lookup_us);
    atomic_fetch_add(&append_compact_metric_build_source_pack_us, piggyback->plan.metric_source_pack_us);
    atomic_fetch_add(&append_compact_metric_build_tail_read_us, piggyback->plan.metric_tail_read_us);
    atomic_fetch_add(&append_compact_metric_build_tail_lookup_us, piggyback->plan.metric_tail_lookup_us);
    atomic_fetch_add(&append_compact_metric_build_tail_pack_us, piggyback->plan.metric_tail_pack_us);
    atomic_fetch_add(&append_compact_metric_build_verify_us, piggyback->plan.metric_verify_us);
    atomic_fetch_add(&append_compact_metric_build_hash_us, piggyback->plan.metric_hash_us);
    atomic_fetch_add(&append_compact_metric_build_source_bytes, piggyback->plan.metric_source_bytes);
    atomic_fetch_add(&append_compact_metric_build_tail_bytes, piggyback->plan.metric_tail_bytes);
    atomic_fetch_add(&append_compact_metric_build_records, (uint64_t)piggyback->plan.live_records + piggyback->plan.stale_records);
    atomic_fetch_add(&append_compact_metric_build_index_lookups, piggyback->plan.metric_index_lookups);
    bool client_actionable = result == APPEND_COMPACTION_ANALYZE_OK && append_compaction_plan_is_client_actionable(&piggyback->plan);
    bool client_order_safe = client_actionable && append_compaction_plan_is_client_order_safe(&piggyback->plan);

    if (result != APPEND_COMPACTION_ANALYZE_OK || !client_actionable || !client_order_safe) {
        uint32_t segment_count = r_info->r_idx_b[bs]->b_offset[FBP] / APPEND_COMPACTION_BLOCKS_PER_SEGMENT + 1U;
        r_info->append_compact_cursor_segment = segment_count == 0 ? 0 : (segment + 1U) % segment_count;

        if (client_actionable && !client_order_safe && log_level > 1) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Skip client append compaction for %s, segment=%u: source_blocks=[%u,%lu), possible_write_blocks=[%u,%lu)\n",
                    curr_dt(dt), t_handler->cid, r_info->uuid, segment, piggyback->plan.first_block, (uint64_t)piggyback->plan.first_block + piggyback->plan.source_blocks,
                    piggyback->plan.new_b_offset, (uint64_t)piggyback->plan.new_b_offset + 2U);
        }

        if (result == APPEND_COMPACTION_ANALYZE_OK && piggyback->plan.stale_records == 0 && !client_actionable) {
            clear_append_compaction_dirty_segment(r_info, segment);

            if (atomic_load(&r_info->append_compact_dirty_segments) == 0)
                atomic_store(&r_info->append_compact_garbage_bytes, 0);
            refresh_append_compaction_request(r_info, bs);
        } else if (result == APPEND_COMPACTION_ANALYZE_OK) {
            refresh_append_compaction_request(r_info, bs);
        } else {
            append_compaction_retry_later(r_info);
        }
        return APPEND_COMPACTION_PIGGYBACK_NONE;
    }
    atomic_store(&r_info->is_replicate, 1);
    uint64_t apply_started_us = append_compaction_monotonic_us();

    if (!apply_append_compaction_plan_local(r_info, &piggyback->context, &piggyback->plan, t_handler)) {
        atomic_fetch_add(&append_compact_metric_apply_us, append_compaction_monotonic_us() - apply_started_us);
        r_info->stop = true;
        r_info->bad_data = true;
        return APPEND_COMPACTION_PIGGYBACK_ERROR;
    }
    atomic_fetch_add(&append_compact_metric_apply_us, append_compaction_monotonic_us() - apply_started_us);
    piggyback->truncate_applied = piggyback->plan.truncate_blocks != 0 && !atomic_load(&r_info->append_compact_truncate_pending);
    piggyback->r_info = r_info;
    piggyback->local_applied = true;
    piggyback->attached_to_write = false;
    piggyback->active = true;
    return APPEND_COMPACTION_PIGGYBACK_PREPARED;
}


static void log_append_compaction_candidate(region_info_t *r, bool sleeping, bool selected, bool probe, uint32_t worker, uint64_t now_ms, uint64_t cid) {
    if (log_level < 1 || r == NULL || r->is_freed || !r->is_leader)
        return;
    unsigned pool = sleeping ? 1U : 0U;

    if (now_ms < r->append_compact_diag_next_ms[pool])
        return;
    r->append_compact_diag_next_ms[pool] = now_ms + 60000U;
    const char *pool_name = sleeping ? "sleeping" : "service";
    char dt[DT_LOG_SIZE];

    if (pthread_mutex_trylock(&r->rlock) != 0) {
        fprintf(stderr, "%s INFO [COMPACT_DIAG=%015lu] region=%p pool=%s worker=%u selected=%u snapshot=rlock_busy\n", curr_dt(dt), cid, (void *)r, pool_name, worker,
                (unsigned)selected);
        return;
    }

    if (pthread_mutex_trylock(&r->control_mutex) != 0) {
        pthread_mutex_unlock(&r->rlock);
        fprintf(stderr, "%s INFO [COMPACT_DIAG=%015lu] region=%p pool=%s worker=%u selected=%u snapshot=control_busy\n", curr_dt(dt), cid, (void *)r, pool_name, worker,
                (unsigned)selected);
        return;
    }

    if (r->is_freed || r->r_lock_id != 0 || r->waiting_count_lock != 0) {
        fprintf(stderr, "%s INFO [COMPACT_DIAG=%015lu] region=%p pool=%s worker=%u selected=%u snapshot=control_state freed=%u lock_id=%u waiters=%u\n", curr_dt(dt), cid,
                (void *)r, pool_name, worker, (unsigned)selected, (unsigned)r->is_freed, (unsigned)r->r_lock_id, (unsigned)r->waiting_count_lock);
        pthread_mutex_unlock(&r->control_mutex);
        pthread_mutex_unlock(&r->rlock);
        return;
    }
    pthread_mutex_unlock(&r->control_mutex);
    uint8_t bs = find_append_compaction_data_bs(r);
    idx_region_info_t *idx = bs == UINT8_MAX ? NULL : r->r_idx_b[bs];
    bool valid_index = idx != NULL && idx->idx != NULL && idx->idx[FBP] != NULL;
    uint32_t last = valid_index ? idx->b_offset[FBP] : 0;
    uint64_t logical = 0;
    bool logical_valid = valid_index && last < idx->f_idx_size[FBP];

    if (logical_valid)
        logical = last == 0 ? FIX_BLOCK_SIZE : (uint64_t)last * FIX_BLOCK_SIZE + (uint64_t)idx->idx[FBP][last].z_index * idx->bsize_factor;
    struct stat st;
    int stat_error = 0;
    int64_t physical = -1;

    if (valid_index) {
        if (fstat(idx->fd[FBP], &st) == 0)
            physical = st.st_size;
        else
            stat_error = errno;
    }
    uint32_t map_bits = 0;

    if (r->append_compact_dirty_map != NULL)
        for (uint32_t i = 0; i < r->append_compact_dirty_map_size; ++i)
            map_bits += (uint32_t)__builtin_popcount((unsigned)r->append_compact_dirty_map[i]);
    uint64_t wall_ms = (uint64_t)getCurrentTimeMs();
    uint64_t retry_at = atomic_load(&r->append_compaction_retry_after_ms);
    uint64_t retry_ms = retry_at > wall_ms ? retry_at - wall_ms : 0;
    uint32_t budget = valid_index ? get_append_compaction_budget(r, bs) : 0;
    uint32_t idle_budget = valid_index ? get_append_compaction_idle_budget(r, bs) : 0;
    const char *reason = "ready";

    if (atomic_load(&rs_shutdown_state) >= RS_SHUTDOWN_WAITING_RPS)
        reason = "shutdown";
    else if (rf != 1 && data_check_enabled != 1)
        reason = "data_check_disabled";
    else if (r->stop)
        reason = "stop";
    else if (r->bad_data)
        reason = "bad_data";
    else if (r->status != STATUS_ASSIGN_REGION && r->status != STATUS_START_REGION)
        reason = "status";
    else if (r->idx_resizer != RESIZE_IDX_DONE)
        reason = "resize";
    else if (atomic_load(&r->r_need_data_chk))
        reason = "recovery";
    else if (!atomic_load(&r->recovery_path_ready) || r->r_fname == NULL)
        reason = "recovery_path";
    else if (r->replica == NULL || r->rsc_msg == NULL)
        reason = "replica_state";
    else if (r->bulk_mode != BUIK_RELICA_OFF)
        reason = "bulk";
    else if (!valid_index)
        reason = "data_index";
    else if (r->enable_block_compression != 0)
        reason = "block_compression";
    else if (sleeping && !append_compaction_sleeping_load_ready())
        reason = "pool_load";
    else if (sleeping && !append_compaction_region_idle(r))
        reason = "region_active";
    else if (retry_ms != 0)
        reason = "retry_delay";
    else if (atomic_load(&r->append_compact_dirty_segments) == 0)
        reason = "no_dirty";
    else if ((sleeping ? idle_budget : budget) == 0)
        reason = "budget_zero";
    fprintf(stderr,
            "%s INFO [COMPACT_DIAG=%015lu] uuid=%s region=%p pool=%s worker=%u selected=%u probe=%u reason=%s ops=%lu limits=%d/%d workers=%d/%d idle=%u streak=%u requested=%u "
            "dirty=%u map_bits=%u occupied=%lu garbage=%lu free=%lu budget=%u/%u retry_ms=%lu cursor=%u last=%u logical=%lu logical_valid=%u physical=%ld stat_errno=%d "
            "truncate=%u/%u admin=%u pending_delete=%lu attempts=%lu applied=%lu last_segment=%u last_result=%d\n",
            curr_dt(dt), cid, r->uuid != NULL ? r->uuid : "-", (void *)r, pool_name, worker, (unsigned)selected, (unsigned)probe, reason, (uint64_t)atomic_load(&ops),
            atomic_load(&append_compact_ops_limit), atomic_load(&append_compact_sleeping_ops_limit), atomic_load(&append_compact_thread_count),
            atomic_load(&append_compact_sleeping_thread_count), (unsigned)append_compaction_region_idle(r), atomic_load(&r->append_compact_idle_streak),
            (unsigned)atomic_load(&r->append_compaction_requested), atomic_load(&r->append_compact_dirty_segments), map_bits, r->append_compact_occupied_bytes,
            r->append_compact_garbage_bytes, valid_index ? append_compaction_free_bytes(r, bs) : 0, budget, idle_budget, retry_ms, r->append_compact_cursor_segment, last,
            logical, (unsigned)logical_valid, physical, stat_error, (unsigned)atomic_load(&r->append_compact_truncate_pending),
            (unsigned)atomic_load(&append_compact_truncate_allowed), (unsigned)r->waiting_count_admin, (uint64_t)atomic_load(&r->pending_delete), r->append_compact_diag_attempts,
            r->append_compact_diag_applied, r->append_compact_diag_segment, r->append_compact_diag_result);
    pthread_mutex_unlock(&r->rlock);
}


static uint32_t process_append_compaction_region(region_info_t *r_info, append_compaction_context_t *context, table_handler_t *t_handler, bool sleeping_mode) {
    if (r_info == NULL || r_info->r_idx_b == NULL)
        return 0;
    uint8_t bs = find_append_compaction_data_bs(r_info);

    if (bs == UINT8_MAX || !append_compaction_region_ready(r_info, FBP, bs))
        return 0;

    if (sleeping_mode && !append_compaction_sleeping_region_ready(r_info))
        return 0;
    uint32_t processed = !sleeping_mode && finish_pending_append_compaction_truncate(r_info, FBP, bs, t_handler, false) ? 1U : 0U;

    if (atomic_load(&r_info->append_compact_dirty_segments) == 0) {
        if (sleeping_mode && finish_pending_append_compaction_truncate(r_info, FBP, bs, t_handler, true))
            processed++;
        return processed;
    }
    uint64_t current_ms = (uint64_t)getCurrentTimeMs();
    uint32_t visit_budget = sleeping_mode ? (current_ms < atomic_load(&r_info->append_compaction_retry_after_ms) ? 0 : get_append_compaction_idle_budget(r_info, bs))
                                          : refresh_append_compaction_request(r_info, bs);

    if (sleeping_mode && visit_budget > 1U)
        visit_budget = 1U;

    if (visit_budget == 0) {
        if (sleeping_mode && finish_pending_append_compaction_truncate(r_info, FBP, bs, t_handler, true))
            processed++;
        return processed;
    }
    flush_write_4K_buffer(r_info, t_handler->cid);

    if (!append_compaction_region_ready(r_info, FBP, bs) || (sleeping_mode && !append_compaction_sleeping_region_ready(r_info)))
        return 0;
    uint32_t plan_attempts = 0;

    while (plan_attempts < visit_budget) {
        if (atomic_load(&is_shutting_down) != 0 || r_info->stop || r_info->bad_data || (!sleeping_mode && !atomic_load(&r_info->append_compaction_requested)) ||
            !append_compaction_region_ready(r_info, FBP, bs) || (sleeping_mode && !append_compaction_sleeping_region_ready(r_info)))
            break;
        uint32_t current_budget = sleeping_mode ? get_append_compaction_idle_budget(r_info, bs) : refresh_append_compaction_request(r_info, bs);

        if (sleeping_mode && current_budget > 1U)
            current_budget = 1U;

        if (current_budget == 0 || plan_attempts >= current_budget)
            break;
        uint32_t segment = 0;

        if (!find_next_append_compaction_segment(r_info, r_info->r_idx_b[bs]->b_offset[FBP], &segment)) {
            atomic_store(&r_info->append_compaction_requested, false);
            break;
        }
        plan_attempts++;
        append_compaction_plan_t plan;
        uint64_t build_started_us = append_compaction_monotonic_us();
        int result = build_append_compaction_plan(r_info, FBP, bs, segment, context, &plan, t_handler);
        r_info->append_compact_diag_attempts++;
        r_info->append_compact_diag_segment = segment;
        r_info->append_compact_diag_result = result;
        atomic_fetch_add(&append_compact_metric_build_us, append_compaction_monotonic_us() - build_started_us);
        atomic_fetch_add(&append_compact_metric_build_prepare_us, plan.metric_prepare_us);
        atomic_fetch_add(&append_compact_metric_build_source_read_us, plan.metric_source_read_us);
        atomic_fetch_add(&append_compact_metric_build_source_lookup_us, plan.metric_source_lookup_us);
        atomic_fetch_add(&append_compact_metric_build_source_pack_us, plan.metric_source_pack_us);
        atomic_fetch_add(&append_compact_metric_build_tail_read_us, plan.metric_tail_read_us);
        atomic_fetch_add(&append_compact_metric_build_tail_lookup_us, plan.metric_tail_lookup_us);
        atomic_fetch_add(&append_compact_metric_build_tail_pack_us, plan.metric_tail_pack_us);
        atomic_fetch_add(&append_compact_metric_build_verify_us, plan.metric_verify_us);
        atomic_fetch_add(&append_compact_metric_build_hash_us, plan.metric_hash_us);
        atomic_fetch_add(&append_compact_metric_build_source_bytes, plan.metric_source_bytes);
        atomic_fetch_add(&append_compact_metric_build_tail_bytes, plan.metric_tail_bytes);
        atomic_fetch_add(&append_compact_metric_build_records, (uint64_t)plan.live_records + plan.stale_records);
        atomic_fetch_add(&append_compact_metric_build_index_lookups, plan.metric_index_lookups);

        if (result != APPEND_COMPACTION_ANALYZE_OK || !append_compaction_plan_is_service_actionable(&plan)) {
            uint32_t segment_count = r_info->r_idx_b[bs]->b_offset[FBP] / APPEND_COMPACTION_BLOCKS_PER_SEGMENT + 1U;
            r_info->append_compact_cursor_segment = segment_count == 0 ? 0 : (segment + 1U) % segment_count;

            if (result == APPEND_COMPACTION_ANALYZE_OK && plan.stale_records == 0) {
                clear_append_compaction_dirty_segment(r_info, segment);

                if (atomic_load(&r_info->append_compact_dirty_segments) == 0)
                    atomic_store(&r_info->append_compact_garbage_bytes, 0);
                refresh_append_compaction_request(r_info, bs);

                if (sleeping_mode)
                    break;
                continue;
            }
            break;
        }
        uint64_t apply_started_us = append_compaction_monotonic_us();
        bool applied = apply_append_compaction_plan(r_info, context, &plan, t_handler);

        if (applied)
            r_info->append_compact_diag_applied++;
        atomic_fetch_add(&append_compact_metric_apply_us, append_compaction_monotonic_us() - apply_started_us);

        if (!applied) {
            uint32_t segment_count = r_info->r_idx_b[bs]->b_offset[FBP] / APPEND_COMPACTION_BLOCKS_PER_SEGMENT + 1U;
            r_info->append_compact_cursor_segment = segment_count == 0 ? 0 : (segment + 1U) % segment_count;
            atomic_store(&r_info->append_compaction_requested, false);
            atomic_store(&r_info->append_compaction_requested_since_ms, 0);
            uint64_t current_ms = (uint64_t)getCurrentTimeMs();
            atomic_store(&r_info->append_compaction_retry_after_ms,
                         current_ms > UINT64_MAX - APPEND_COMPACTION_RETRY_DELAY_MS ? UINT64_MAX : current_ms + APPEND_COMPACTION_RETRY_DELAY_MS);
            break;
        }
        processed++;
    }

    if (sleeping_mode && finish_pending_append_compaction_truncate(r_info, FBP, bs, t_handler, true))
        processed++;
    return processed;
}

static void free_compaction_worker_context(void *arg) {
    free_append_compaction_context(arg);
}

typedef struct {
    pthread_t thread;
    bool running;
    bool started;
    int last_error;
} compaction_worker_slot_t;
static void poll_compaction_workers(compaction_worker_slot_t *slots, uint32_t count, uint64_t cid) {
    char dt[DT_LOG_SIZE];

    for (uint32_t id = 0; id < count; ++id) {
        compaction_worker_slot_t *slot = &slots[id];

        if (slot->running) {
            int rc = pthread_tryjoin_np(slot->thread, NULL);

            if (rc == EBUSY)
                continue;

            if (rc != 0) {
                if (slot->last_error != rc)
                    fprintf(stderr, "%s ERROR [COMPACT_SUPERVISOR=%015lu] Cannot join worker=%u: %s; replacement deferred\n", curr_dt(dt), cid, id, strerror(rc));
                slot->last_error = rc;
                continue;
            }
            slot->running = false;
            slot->last_error = 0;
            fprintf(stderr, "%s INFO [COMPACT_SUPERVISOR=%015lu] Worker=%u exited\n", curr_dt(dt), cid, id);
        }
        int active_workers = atomic_load(&append_compact_thread_count);

        if (atomic_load(&is_shutting_down) || atomic_load(&rs_shutdown_state) != RS_SHUTDOWN_RUNNING || active_workers <= 0 || id >= (uint32_t)active_workers)
            continue;
        int rc = pthread_create(&slot->thread, NULL, pthread_compaction_service, (void *)(uintptr_t)id);

        if (rc != 0) {
            if (slot->last_error != rc)
                fprintf(stderr, "%s ERROR [COMPACT_SUPERVISOR=%015lu] Cannot start worker=%u: %s; retry in 1s\n", curr_dt(dt), cid, id, strerror(rc));
            slot->last_error = rc;
            continue;
        }
        slot->running = true;
        slot->last_error = 0;
        fprintf(stderr, "%s INFO [COMPACT_SUPERVISOR=%015lu] Worker=%u %s\n", curr_dt(dt), cid, id, slot->started ? "restarted" : "started");
        slot->started = true;
    }
}


void *pthread_compaction_supervisor(void *arg) {
    uint32_t count = (uint32_t)(uintptr_t)arg;

    if (count == 0)
        return NULL;
    uint64_t cid = get_new_cid();
    compaction_worker_slot_t *slots = NULL;
    bool allocation_reported = false;

    while (!atomic_load(&is_shutting_down)) {
        if (slots == NULL) {
            slots = calloc(count, sizeof(*slots));

            if (slots == NULL) {
                if (!allocation_reported) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s ERROR [COMPACT_SUPERVISOR=%015lu] Cannot allocate %u worker slots; retry in 1s\n", curr_dt(dt), cid, count);
                    allocation_reported = true;
                }
                sleep(1);
                continue;
            }
        }
        poll_compaction_workers(slots, count, cid);
        sleep(1);
    }

    if (slots != NULL) {
        for (uint32_t id = 0; id < count; ++id) {
            if (!slots[id].running)
                continue;
            int rc = pthread_detach(slots[id].thread);

            if (rc != 0) {
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s ERROR [COMPACT_SUPERVISOR=%015lu] Cannot detach worker=%u at shutdown: %s\n", curr_dt(dt), cid, id, strerror(rc));
            }
        }
        free(slots);
    }
    return NULL;
}

void *pthread_compaction_service(void *arg) {
    uint32_t worker_id = (uint32_t)(uintptr_t)arg;

    if (worker_id >= append_compact_thread_count)
        return NULL;
    const uint8_t table_lock_id = 31;
    const struct timespec busy_interval = {.tv_sec = 0, .tv_nsec = 100000};
    const struct timespec idle_interval = {.tv_sec = 0, .tv_nsec = 1000000};
    uint64_t last_low_load_ops = UINT64_MAX;
    table_handler_t *t_handler;

    while ((t_handler = create_t_handler(0)) == NULL)
        sleep(1);
    t_handler->cid = get_new_cid();
    append_compaction_context_t context;

    if (!init_append_compaction_context(&context)) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Cannot allocate append compaction buffers\n", curr_dt(dt), t_handler->cid);
        free_t_handler(t_handler);
        return NULL;
    }
    tables_snapshot_t snapshot = {0};
    pthread_cleanup_push(free_compaction_worker_context, &context);
    pthread_cleanup_push(free_tables_snapshot, &snapshot);

    while (atomic_load(&is_shutting_down) == 0) {
        int active_workers = atomic_load(&append_compact_thread_count);

        if (active_workers <= 0 || worker_id >= (uint32_t)active_workers) {
            sleep(1);
            continue;
        }

        if (atomic_load(&rs_shutdown_state) >= RS_SHUTDOWN_WAITING_RPS) {
            sleep(1);
            continue;
        }
        int ops_limit = atomic_load(&append_compact_ops_limit);
        uint64_t current_ops = atomic_load(&ops);
        bool high_load = ops_limit > 0 && current_ops > (uint64_t)ops_limit;

        if (high_load) {
            const struct timespec pressure_interval = {.tv_sec = 0, .tv_nsec = 100000000};
            nanosleep(&pressure_interval, NULL);
        }
        bool work_pending = false;
        bool compaction_done = false;
        bool probe_low_load = ops_limit > 0 && current_ops < (uint64_t)ops_limit && current_ops != last_low_load_ops;
        bool low_load_probe_incomplete = false;

        if (!take_tables_snapshot(&snapshot, t_handler->cid)) {
            sleep(1);
            continue;
        }
        uint64_t diag_visit_ms = log_level >= 1 ? append_compaction_monotonic_us() / 1000U : 0;

        for (int t = 0; t < snapshot.count && atomic_load(&is_shutting_down) == 0; t++) {
            table_info_t *t_info = snapshot.tables[t];

            if (t_info == NULL)
                continue;
            uint32_t table_worker_offset = (uint32_t)((uint64_t)(uint32_t)t * 31U % (uint32_t)active_workers);
            int first_region = (int)((worker_id + (uint32_t)active_workers - table_worker_offset) % (uint32_t)active_workers);

            for (int r = first_region; atomic_load(&is_shutting_down) == 0; r += active_workers) {
                if (pthread_mutex_trylock(&t_info->tlock) != 0) {
                    work_pending = true;

                    if (probe_low_load)
                        low_load_probe_incomplete = true;
                    break;
                }

                if (t_info->t_lock_id != 0) {
                    work_pending = true;

                    if (probe_low_load)
                        low_load_probe_incomplete = true;
                    pthread_mutex_unlock(&t_info->tlock);
                    break;
                }
                t_info->t_lock_id = (char)table_lock_id;

                if (t_info->retired || r >= t_info->region_count_curr) {
                    do_t_unlock(t_info, 33, false, t_handler->cid);
                    break;
                }
                region_info_t *r_info = t_info->region_info[r];
                bool truncate_pending = r_info != NULL && atomic_load(&append_compact_truncate_allowed) && atomic_load(&r_info->append_compact_truncate_pending);
                uint64_t current_ms = (uint64_t)getCurrentTimeMs();
                bool retry_due = r_info != NULL && atomic_load(&r_info->append_compact_dirty_segments) != 0 && atomic_load(&r_info->append_compaction_retry_after_ms) != 0 &&
                                 current_ms >= atomic_load(&r_info->append_compaction_retry_after_ms);
                bool gc_pending = record_gc_candidate(r_info, high_load, append_compaction_monotonic_us() / 1000U);
                bool selected = r_info != NULL && !(high_load ? !gc_pending
                                                              : (!gc_pending && !truncate_pending && !retry_due && !atomic_load(&r_info->append_compaction_requested) &&
                                                                 (!probe_low_load || atomic_load(&r_info->append_compact_dirty_segments) == 0)));
                log_append_compaction_candidate(r_info, false, selected, probe_low_load, worker_id, diag_visit_ms, t_handler->cid);

                if (!selected) {
                    do_t_unlock(t_info, 33, false, t_handler->cid);
                    continue;
                }
                work_pending = true;
                atomic_fetch_add(&append_compact_metric_lock_attempts, 1);

                if (pthread_mutex_trylock(&r_info->rlock) != 0) {
                    atomic_fetch_add(&append_compact_metric_lock_misses, 1);

                    if (probe_low_load)
                        low_load_probe_incomplete = true;
                    do_t_unlock(t_info, 33, false, t_handler->cid);
                    continue;
                }
                pthread_mutex_lock(&r_info->control_mutex);

                if (r_info->is_freed || r_info->r_lock_id != 0 || r_info->waiting_count_lock != 0) {
                    if (probe_low_load)
                        low_load_probe_incomplete = true;
                    pthread_mutex_unlock(&r_info->control_mutex);
                    pthread_mutex_unlock(&r_info->rlock);
                    do_t_unlock(t_info, 33, false, t_handler->cid);
                    continue;
                }
                r_info->r_lock_id = APPEND_COMPACTION_LOCK;
                r_info->curr_cid_lock = t_handler->cid;
                atomic_store(&r_info->append_compaction_lock_held, true);
                pthread_mutex_unlock(&r_info->control_mutex);
                do_t_unlock(t_info, 33, false, t_handler->cid);
                atomic_fetch_add(&append_compact_metric_lock_acquired, 1);
                uint64_t requested_since_ms = atomic_exchange(&r_info->append_compaction_requested_since_ms, 0);

                if (requested_since_ms != 0) {
                    uint64_t current_ms = getCurrentTimeMs();
                    uint64_t wait_ms = current_ms >= requested_since_ms ? current_ms - requested_since_ms : 0;
                    atomic_fetch_add(&append_compact_metric_request_wait_ms, wait_ms);
                    atomic_fetch_add(&append_compact_metric_request_wait_samples, 1);
                    append_compaction_update_max(&append_compact_metric_request_wait_max_ms, wait_ms);
                }
                uint64_t lock_started_us = append_compaction_monotonic_us();
                int gc_result = process_record_gc_background_visit(r_info, &context, t_handler, false);
                uint32_t processed_segments = gc_result == RECORD_GC_PROGRESS ? 1U : (high_load ? 0U : process_append_compaction_region(r_info, &context, t_handler, false));

                if (processed_segments != 0)
                    compaction_done = true;

                if (atomic_load(&r_info->append_compaction_requested) && atomic_load(&r_info->append_compaction_requested_since_ms) == 0)
                    atomic_store(&r_info->append_compaction_requested_since_ms, getCurrentTimeMs());
                uint64_t lock_hold_us = append_compaction_monotonic_us() - lock_started_us;
                char metric_uuid[UUID_SIZE];
                memcpy(metric_uuid, r_info->uuid, UUID_SIZE);
                metric_uuid[UUID_SIZE - 1U] = 0;
                uint32_t metric_dirty_segments = atomic_load(&r_info->append_compact_dirty_segments);
                bool metric_requested = atomic_load(&r_info->append_compaction_requested);
                atomic_store(&r_info->append_compaction_lock_held, false);
                do_r_unlock(r_info, 133, false, t_handler);
                atomic_fetch_add(&append_compact_metric_lock_hold_us, lock_hold_us);

                if (log_level > 1 && lock_hold_us >= 2000U) {
                    uint64_t now_ms = getCurrentTimeMs();
                    uint64_t last_ms = atomic_load(&append_compact_metric_slow_log_last_ms);

                    if (now_ms >= last_ms + 100U && atomic_compare_exchange_strong(&append_compact_metric_slow_log_last_ms, &last_ms, now_ms)) {
                        char dt[DT_LOG_SIZE];
                        fprintf(stderr, "%s INFO [COMPACT_LOCK=%015lu] Slow compaction lock uuid=%s hold_us=%lu processed=%u dirty_segments=%u requested=%u\n", curr_dt(dt),
                                t_handler->cid, metric_uuid, lock_hold_us, processed_segments, metric_dirty_segments, metric_requested);
                    }
                }
            }
        }
        last_low_load_ops = ops_limit > 0 && current_ops < (uint64_t)ops_limit && !low_load_probe_incomplete ? current_ops : UINT64_MAX;

        if (atomic_load(&is_shutting_down) != 0)
            break;

        if (!compaction_done)
            nanosleep(work_pending ? &busy_interval : &idle_interval, NULL);
    }
    pthread_cleanup_pop(1);
    pthread_cleanup_pop(1);
    free_t_handler(t_handler);
    return NULL;
}


void *pthread_sleeping_compaction_service(void *arg) {
    uint32_t worker_id = (uint32_t)(uintptr_t)arg;

    if (worker_id >= APPEND_COMPACTION_SLEEPING_MAX_THREADS)
        return NULL;
    const uint8_t table_lock_id = 32;
    const struct timespec busy_interval = {.tv_sec = 0, .tv_nsec = 100000};
    const struct timespec idle_interval = {.tv_sec = 0, .tv_nsec = 1000000};
    table_handler_t *t_handler;

    while ((t_handler = create_t_handler(0)) == NULL)
        sleep(1);
    t_handler->cid = get_new_cid();
    append_compaction_context_t context;

    if (!init_append_compaction_context(&context)) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Cannot allocate sleeping append compaction buffers\n", curr_dt(dt), t_handler->cid);
        free_t_handler(t_handler);
        return NULL;
    }
    tables_snapshot_t snapshot = {0};
    pthread_cleanup_push(free_compaction_worker_context, &context);
    pthread_cleanup_push(free_tables_snapshot, &snapshot);

    while (atomic_load(&is_shutting_down) == 0) {
        int active_workers = atomic_load(&append_compact_sleeping_thread_count);

        if (atomic_load(&rs_shutdown_state) >= RS_SHUTDOWN_WAITING_RPS) {
            sleep(1);
            continue;
        }

        if (active_workers < 0)
            active_workers = 0;

        if (active_workers > (int)APPEND_COMPACTION_SLEEPING_MAX_THREADS)
            active_workers = (int)APPEND_COMPACTION_SLEEPING_MAX_THREADS;

        if (worker_id >= (uint32_t)active_workers) {
            sleep(1);
            continue;
        }

        if (!replicas_idle_commit_load_ready()) {
            nanosleep(&idle_interval, NULL);
            continue;
        }
        bool candidate_found = false;
        bool lock_busy = false;
        bool compaction_done = false;

        if (!take_tables_snapshot(&snapshot, t_handler->cid)) {
            sleep(1);
            continue;
        }
        uint64_t visit_ms = append_compaction_monotonic_us() / 1000U;

        for (int t = 0; t < snapshot.count && atomic_load(&is_shutting_down) == 0 && replicas_idle_commit_load_ready(); t++) {
            table_info_t *t_info = snapshot.tables[t];

            if (t_info == NULL)
                continue;

            for (int r = 0; atomic_load(&is_shutting_down) == 0 && replicas_idle_commit_load_ready(); r++) {
                if (pthread_mutex_trylock(&t_info->tlock) != 0) {
                    lock_busy = true;
                    break;
                }

                if (t_info->t_lock_id != 0) {
                    lock_busy = true;
                    pthread_mutex_unlock(&t_info->tlock);
                    break;
                }
                t_info->t_lock_id = (char)table_lock_id;

                if (t_info->retired || r >= t_info->region_count_curr) {
                    do_t_unlock(t_info, 34, false, t_handler->cid);
                    break;
                }
                region_info_t *r_info = t_info->region_info[r];
                bool has_work = r_info != NULL && (atomic_load(&r_info->append_compact_dirty_segments) != 0 || atomic_load(&r_info->append_compact_truncate_pending));
                bool gc_pending = record_gc_candidate(r_info, false, visit_ms);
                bool needs_commit = r_info != NULL && r_info->is_leader && !r_info->is_freed && !atomic_load(&r_info->replicas_idle_committed) &&
                                    atomic_load(&rs_shutdown_state) == RS_SHUTDOWN_RUNNING && visit_ms >= atomic_load(&r_info->replicas_idle_retry_after_ms);
                bool selected = append_compaction_region_idle(r_info) && (((has_work || gc_pending) && append_compaction_sleeping_load_ready()) || needs_commit);
                log_append_compaction_candidate(r_info, true, selected, false, worker_id, visit_ms, t_handler->cid);

                if (!selected) {
                    do_t_unlock(t_info, 34, false, t_handler->cid);
                    continue;
                }
                candidate_found = true;
                atomic_fetch_add(&append_compact_metric_lock_attempts, 1);

                if (pthread_mutex_trylock(&r_info->rlock) != 0) {
                    atomic_fetch_add(&append_compact_metric_lock_misses, 1);
                    lock_busy = true;
                    do_t_unlock(t_info, 34, false, t_handler->cid);
                    continue;
                }
                pthread_mutex_lock(&r_info->control_mutex);

                if (r_info->is_freed || r_info->r_lock_id != 0 || r_info->waiting_count_lock != 0 || r_info->waiting_count_admin != 0) {
                    lock_busy = true;
                    pthread_mutex_unlock(&r_info->control_mutex);
                    pthread_mutex_unlock(&r_info->rlock);
                    do_t_unlock(t_info, 34, false, t_handler->cid);
                    continue;
                }
                r_info->r_lock_id = APPEND_COMPACTION_LOCK;
                r_info->curr_cid_lock = t_handler->cid;
                atomic_store(&r_info->append_compaction_lock_held, true);
                pthread_mutex_unlock(&r_info->control_mutex);
                do_t_unlock(t_info, 34, false, t_handler->cid);
                atomic_fetch_add(&append_compact_metric_lock_acquired, 1);
                uint64_t lock_started_us = append_compaction_monotonic_us();
                int gc_result = process_record_gc_background_visit(r_info, &context, t_handler, true);
                uint32_t processed_segments = gc_result == RECORD_GC_PROGRESS ? 1U : process_append_compaction_region(r_info, &context, t_handler, true);

                if (processed_segments != 0)
                    compaction_done = true;

                if (try_replicas_idle_commit(t_info, r_info, t_handler))
                    compaction_done = true;
                uint64_t lock_hold_us = append_compaction_monotonic_us() - lock_started_us;
                atomic_store(&r_info->append_compaction_lock_held, false);
                do_r_unlock(r_info, 134, false, t_handler);
                atomic_fetch_add(&append_compact_metric_lock_hold_us, lock_hold_us);
            }
        }

        if (atomic_load(&is_shutting_down) != 0)
            break;

        if (!compaction_done)
            nanosleep(candidate_found && lock_busy ? &busy_interval : &idle_interval, NULL);
    }
    pthread_cleanup_pop(1);
    pthread_cleanup_pop(1);
    free_t_handler(t_handler);
    return NULL;
}


uint32_t use_void(struct region_info_t *r_info, uint8_t b, uint32_t zip_val_size, uint32_t b_offset, uint8_t bs) {
    for (int v = 0; v < VOID_ELEMENTS_TRY_COUNT; v++) {
        if (zip_val_size + v > r_info->r_idx_b[bs]->block_size_zip)
            break;

        if (r_info->r_idx_b[bs]->void_info[b][zip_val_size + v].void_count_curr > 0) {
            uint32_t void_count_curr = r_info->r_idx_b[bs]->void_info[b][zip_val_size + v].void_count_curr - 1;
            b_offset = r_info->r_idx_b[bs]->void_info[b][zip_val_size + v].void_arr[void_count_curr];
            r_info->r_idx_b[bs]->void_info[b][zip_val_size + v].void_arr[void_count_curr] = 0;
            r_info->r_idx_b[bs]->void_info[b][zip_val_size + v].void_count_tot--;
            r_info->r_idx_b[bs]->void_info[b][zip_val_size + v].void_count_curr--;

            if (r_info->r_idx_b[bs]->max_void_pos == zip_val_size + v && r_info->r_idx_b[bs]->max_void_count != 0)
                r_info->r_idx_b[bs]->max_void_count--;
            return b_offset;
        }
    }
    return b_offset;
}


uint32_t use_void_block(struct region_info_t *r_info, uint8_t b, uint32_t zip_val_size, uint32_t b_offset, uint8_t bs) {
    for (int v = 3; v < VOID_ELEMENTS_TRY_COUNT + 3; v++) {
        if (zip_val_size + v > r_info->r_idx_b[bs]->block_size_zip)
            break;

        if (r_info->r_idx_b[bs]->void_info[b][zip_val_size + v].void_count_curr > 0) {
            uint32_t void_count_curr = r_info->r_idx_b[bs]->void_info[b][zip_val_size + v].void_count_curr - 1;

            if (log_level > 3) {
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Use %s voids void_info[0][%u]=%u, b_offset=%u, b_offset2=%u\n", curr_dt(dt), 0L, r_info->uuid,
                        zip_val_size + v, void_count_curr, b_offset, r_info->r_idx_b[bs]->void_info[b][zip_val_size + v].void_arr[void_count_curr]);
            }
            b_offset = r_info->r_idx_b[bs]->void_info[b][zip_val_size + v].void_arr[void_count_curr];
            r_info->r_idx_b[bs]->void_info[b][zip_val_size + v].void_arr[void_count_curr] = 0;
            r_info->r_idx_b[bs]->void_info[b][zip_val_size + v].void_count_tot--;
            r_info->r_idx_b[bs]->void_info[b][zip_val_size + v].void_count_curr--;

            if (r_info->r_idx_b[bs]->max_void_pos == zip_val_size + v && r_info->r_idx_b[bs]->max_void_count != 0)
                r_info->r_idx_b[bs]->max_void_count--;
            return b_offset;
        }
    }
    return b_offset;
}

void remove_void(struct region_info_t *r_info, uint8_t b, uint32_t void_pos_this, uint32_t block_no, uint8_t bs) {
    if (log_level > 3) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Remove %s voids void_pos_this=%u, block_no=%u\n", curr_dt(dt), 0L, r_info->uuid, void_pos_this, block_no);
    }

    for (int i = 0; i < r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_count_curr; i++) {
        if (log_level > 3) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Remove %s voids try void_pos_this[%u]=%u, void_arr[%u]=%u\n", curr_dt(dt), 0L, r_info->uuid, i, void_pos_this, i,
                    r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_arr[i]);
        }

        if (r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_arr[i] == block_no) {
            for (int j = i; j < r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_count_curr - 1; j++) {
                if (log_level > 3) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Remove %s voids void_arr[%u]=%u set void_arr[%u]=%u\n", curr_dt(dt), 0L, r_info->uuid, j,
                            r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_arr[j], j + 1, r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_arr[j + 1]);
                }
                r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_arr[j] = r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_arr[j + 1];
            }
            r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_arr[r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_count_curr - 1] = 0;
            r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_count_curr--;
            r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_count_tot--;

            if (r_info->r_idx_b[bs]->max_void_pos == void_pos_this && r_info->r_idx_b[bs]->max_void_count != 0)
                r_info->r_idx_b[bs]->max_void_count--;

            if (log_level > 3) {
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Remove %s voids void_pos_this[%u]=%u, block_no=%u\n", curr_dt(dt), 0L, r_info->uuid, i, void_pos_this,
                        block_no);
            }
            break;
        }
    }
}

// TODO  void_pos_this > 1 => void_pos_this >= 1
bool change_block_void(struct region_info_t *r_info, uint8_t b, uint32_t void_pos_this, uint32_t read_block_old, uint32_t read_block_new, uint8_t bs) {
    if (log_level > 3) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s TRACE [COMPACT_SERVICE=%015lu] Change %s block void_pos_this=%u, block_size_zip=%u, read_block_old=%u, read_block_new=%u\n", curr_dt(dt), 0L,
                r_info->uuid, void_pos_this, r_info->r_idx_b[bs]->block_size_zip, read_block_old, read_block_new);
    }

    if (void_pos_this > 1 && void_pos_this <= r_info->r_idx_b[bs]->block_size_zip) {
        if (log_level > 3) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s TRACE [COMPACT_SERVICE=%015lu] Change %s block void_count_curr=%u\n", curr_dt(dt), 0L, r_info->uuid,
                    r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_count_curr);
        }

        for (int i = 0; i < r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_count_curr; i++) {
            if (log_level > 3) {
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s TRACE [COMPACT_SERVICE=%015lu] Change %s block void_pos_this[%u]=%u, read_block_old=%u, void_arr[%u]=%u\n", curr_dt(dt), 0L, r_info->uuid, i,
                        void_pos_this, read_block_old, i, r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_arr[i]);
            }

            if (r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_arr[i] == read_block_old) {
                r_info->r_idx_b[bs]->void_info[b][void_pos_this].void_arr[i] = read_block_new;

                if (log_level > 3) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s TRACE [COMPACT_SERVICE=%015lu] Change %s block void_pos_this[%u]=%u, read_block_old=%u, read_block_new=%u\n", curr_dt(dt), 0L,
                            r_info->uuid, i, void_pos_this, read_block_old, read_block_new);
                }
                return true;
            }
        }
    }

    if (log_level > 3) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s TRACE [COMPACT_SERVICE=%015lu] Failed %s change block void_pos_this=%u, block_size_zip=%u, read_block_old=%u, read_block_new=%u\n", curr_dt(dt), 0L,
                r_info->uuid, void_pos_this, r_info->r_idx_b[bs]->block_size_zip, read_block_old, read_block_new);
    }
    return false; // TODO Smth
}

void add_void(struct region_info_t *r_info, uint8_t b, uint32_t void_pos, uint32_t read_off, uint8_t bs) {
    // TODO Не нужно добавлять в список слишком маленький войд (?)
    if (log_level > 3) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Add %s voids void_pos=%u, read_off=%u\n", curr_dt(dt), 0L, r_info->uuid, void_pos, read_off);
    }

    if (void_pos > 1 && void_pos <= r_info->r_idx_b[bs]->block_size_zip && r_info->r_idx_b[bs]->b_offset[b] != read_off) {
        if (log_level > 3) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Add %s voids try void_pos=%u, b_offset=%u\n", curr_dt(dt), 0L, r_info->uuid, void_pos,
                    r_info->r_idx_b[bs]->b_offset[b]);
        }
        r_info->r_idx_b[bs]->void_info[b][void_pos].void_count_tot++;
        uint32_t void_count_curr = r_info->r_idx_b[bs]->void_info[b][void_pos].void_count_curr + 1;

        if (void_count_curr < MAX_VOID_ELEMENTS) {
            r_info->r_idx_b[bs]->void_info[b][void_pos].void_count_curr++;
            r_info->r_idx_b[bs]->void_info[b][void_pos].void_arr[void_count_curr - 1] = read_off;
        }

        if (log_level > 3) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Add %s voids void_count_tot[%u]=%u, void_count_curr=%u, read_off=%u\n", curr_dt(dt), 0L, r_info->uuid, void_pos,
                    r_info->r_idx_b[bs]->void_info[b][void_pos].void_count_tot, void_count_curr, read_off);
        }

        if (void_pos > VOIDS / 2 && r_info->r_idx_b[bs]->max_void_count < r_info->r_idx_b[bs]->void_info[b][void_pos].void_count_tot) {
            r_info->r_idx_b[bs]->max_void_pos = void_pos;
            r_info->r_idx_b[bs]->max_void_count = r_info->r_idx_b[bs]->void_info[b][void_pos].void_count_tot;
        }

        if (log_level > 3) {
            if (r_info->r_idx_b[bs]->void_info[b][void_pos].void_count_tot > MAX_VOID_ELEMENTS && log_level > 0) {
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Voids %s void_count_tot[%u]=%u\n", curr_dt(dt), 0L, r_info->uuid, void_pos,
                        r_info->r_idx_b[bs]->void_info[b][void_pos].void_count_tot);
            }
        }
    }
}


static int16_t extract_void_key(region_info_t *r_info, const uint8_t *bytes, table_handler_t *t_handler) {
    int16_t result = get_key_from_msg_bytes_checked(r_info->msg_get, bytes, t_handler->cid);

    if (result < 0) {
        stop_region_after_key_error(r_info, result, t_handler);
        finish_thread(r_info, 113, true, t_handler);
    }
    return result;
}

int merge_compressed_voids(int v, struct region_info_t *r_info, uint8_t b, uint8_t bs, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    const uint32_t local_block_size = r_info->r_idx_b[bs]->block_size;
    const uint16_t local_bsize_factor = r_info->r_idx_b[bs]->bsize_factor;
    uint32_t crc_block;

    for (int v2 = VOIDS - v; v2 < VOIDS - 1; v2++) {
        if (v2 > VOIDS / 2 && r_info->r_idx_b[bs]->max_void_count < r_info->r_idx_b[bs]->void_info[b][v2].void_count_tot) {
            r_info->r_idx_b[bs]->max_void_pos = v2;
            r_info->r_idx_b[bs]->max_void_count = r_info->r_idx_b[bs]->void_info[b][v2].void_count_tot;
        }

        if (v2 != v && ((v - 2) + (v2 - 2)) > VOIDS) {
            if (r_info->r_idx_b[bs]->void_info[b][v].void_count_curr > 0 && r_info->r_idx_b[bs]->void_info[b][v2].void_count_curr > 0) {
                int32_t void_count_curr_l = r_info->r_idx_b[bs]->void_info[b][v].void_count_curr - 1;
                int32_t void_count_curr_r = r_info->r_idx_b[bs]->void_info[b][v2].void_count_curr - 1;

                if (void_count_curr_l < 0 || void_count_curr_r < 0) {
                    continue;
                }
                uint32_t read_block_num_l = r_info->r_idx_b[bs]->void_info[b][v].void_arr[void_count_curr_l];
                uint32_t read_block_num_r = r_info->r_idx_b[bs]->void_info[b][v2].void_arr[void_count_curr_r];

                if (read_block_num_l == read_block_num_r) {
                    read_block_num_l = read_block_num_r;
                }
                uint64_t rw_off_l = (uint64_t)read_block_num_l * local_block_size;
                uint64_t rw_off_r = (uint64_t)read_block_num_r * local_block_size;
                uint8_t z_units_l = r_info->r_idx_b[bs]->idx[b][read_block_num_l].z_index;
                uint8_t z_units_r = r_info->r_idx_b[bs]->idx[b][read_block_num_r].z_index;
                uint32_t compressed_size_l = (uint32_t)z_units_l * (uint32_t)local_bsize_factor;

                if (compressed_size_l == 0)
                    continue; // TODO Check is it ok?

                if (log_level > 0) {
                    fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Ready to compact %s l_read_block=%u, r_read_block=%u, z_units_l=%u, z_units_r=%u\n", curr_dt(dt),
                            t_handler->cid, r_info->uuid, read_block_num_l, read_block_num_r, z_units_l, z_units_r);
                }
                uint32_t read_l = read_file(r_info->r_idx_b[bs]->fd[b], r_info->zbuff, rw_off_l, compressed_size_l, r_info, t_handler->cid);

                if (read_l == 0) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Read zero bytes %s ar rw_off_l=%lu, block=%u where compressed_size_r=%u\n", curr_dt(dt), t_handler->cid,
                            r_info->uuid, rw_off_l, read_block_num_l, compressed_size_l);
                    r_info->stop = true;
                    r_info->bad_data = true;
                    finish_thread(r_info, 100, true, t_handler);
                }
                uint16_t records_count_l;
                uint64_t decomp_size_l = 0;
                int de_l = decompress_block(r_info->zbuff, read_l, read_block_num_l, bs, r_info, &decomp_size_l, 0, t_handler);

                if (de_l != 0 || decomp_size_l == 0) {
                    continue;
                }
                memcpy(&records_count_l, r_info->zbuff + sizeof(uint32_t) + sizeof(uint64_t) - sizeof(uint16_t), sizeof(uint16_t));
                decomp_size_l = align_up(decomp_size_l, FIX_BSIZE_FACTOR);

                if (r_info->rbuff_size < decomp_size_l) {
                    uint8_t *rbuff_tmp = realloc(r_info->rbuff, decomp_size_l + MAX_BSIZE_FACTOR);

                    if (rbuff_tmp == NULL) {
                        fprintf(stderr, "%s WARN [COMPACT_SERVICE=%015lu] Failed realloc rbuff to %lu, sleep 10s to avoid DDoS\n", curr_dt(dt), t_handler->cid,
                                (decomp_size_l + MAX_BSIZE_FACTOR));
                        sleep(10); // TODO Skip compact in future
                        return 0;
                    }
                    r_info->rbuff = rbuff_tmp;
                    r_info->rbuff_size = decomp_size_l + MAX_BSIZE_FACTOR;
                }
                memcpy(r_info->rbuff, r_info->bbuff, decomp_size_l);
                uint32_t compressed_size_r = (uint32_t)z_units_r * (uint32_t)local_bsize_factor;

                if (compressed_size_r == 0)
                    continue; // TODO Check is it ok?
                uint32_t read_r = read_file(r_info->r_idx_b[bs]->fd[b], r_info->zbuff, rw_off_r, compressed_size_r, r_info, t_handler->cid);

                if (read_r == 0) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Read zero bytes %s at rw_off_r=%lu, block=%u where compressed_size_r=%u\n", curr_dt(dt), t_handler->cid,
                            r_info->uuid, rw_off_r, read_block_num_r, compressed_size_r);
                    r_info->stop = true;
                    r_info->bad_data = true;
                    finish_thread(r_info, 101, true, t_handler);
                }
                uint16_t records_count_r;
                uint64_t decomp_size_r = 0;
                int de_r = decompress_block(r_info->zbuff, read_r, read_block_num_r, bs, r_info, &decomp_size_r, 0, t_handler);

                if (de_r != 0 || decomp_size_r == 0) {
                    continue;
                }
                memcpy(&records_count_r, r_info->zbuff + sizeof(uint32_t) + sizeof(uint64_t) - sizeof(uint16_t), sizeof(uint16_t));
                decomp_size_r = align_up(decomp_size_r, FIX_BSIZE_FACTOR);

                if (r_info->rbuff_size < decomp_size_l + decomp_size_r + MAX_BSIZE_FACTOR) {
                    uint64_t need = decomp_size_l + decomp_size_r + MAX_BSIZE_FACTOR;
                    uint8_t *rbuff_tmp = realloc(r_info->rbuff, need);

                    if (rbuff_tmp == NULL) {
                        fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Failed realloc rbuff to %lu\n", curr_dt(dt), t_handler->cid, (unsigned long)need);
                        r_info->stop = true;
                        r_info->bad_data = true;
                        finish_thread(r_info, 102, true, t_handler);
                    }
                    r_info->rbuff = rbuff_tmp;
                    r_info->rbuff_size = need;
                }
                memcpy(r_info->rbuff + decomp_size_l, r_info->bbuff, decomp_size_r);
                uint64_t decomp_total = decomp_size_l + decomp_size_r;
                uint32_t write_block, empty_block;
                uint64_t data_write_off, empty_write_off;

                if (read_block_num_l < read_block_num_r) {
                    write_block = read_block_num_l;
                    empty_block = read_block_num_r;
                    data_write_off = rw_off_l;
                    empty_write_off = rw_off_r;
                } else {
                    write_block = read_block_num_r;
                    empty_block = read_block_num_l;
                    data_write_off = rw_off_r;
                    empty_write_off = rw_off_l;
                }
                uint64_t recompressed_size = 0;
                int result_compress = compress_block(r_info->rbuff, decomp_total, r_info, &recompressed_size, INIT_BLOCK_FACTOR, records_count_r + records_count_l);

                if (result_compress < 0) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s ERROR [RS_IDX=%015lu] Failed compress block %s, decomp_total=%lu, read_block_num_l=%u, read_block_num_r=%u, code=%d\n", curr_dt(dt),
                            t_handler->cid, r_info->uuid, decomp_total, read_block_num_l, read_block_num_r, result_compress);
                    graceful_exit(-66);
                }
                uint16_t round_count = 0;
                uint8_t snappy_block_factor = INIT_BLOCK_FACTOR;
                uint16_t records_count = records_count_r + records_count_l;

                if (records_count >= 104 && records_count < 112)
                    snappy_block_factor = INIT_BLOCK_FACTOR - 3;

                if (records_count >= 112 && records_count < 120)
                    snappy_block_factor = INIT_BLOCK_FACTOR - 4;

                if (records_count >= 120 && records_count < 128)
                    snappy_block_factor = INIT_BLOCK_FACTOR - 5;

                if (records_count >= 128)
                    snappy_block_factor = INIT_BLOCK_FACTOR - 6;

                while (recompressed_size + MAX_BSIZE_FACTOR * snappy_block_factor >= local_block_size) {
                    rotate_block_messages(r_info, decomp_total);

                    if (log_level > 1)
                        fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Rotated %s read_blocks %u + %u, recompressed_size=%lu, snappy_block_factor=%u, round_count=%u\n",
                                curr_dt(dt), t_handler->cid, r_info->uuid, read_block_num_l, read_block_num_r, recompressed_size, snappy_block_factor, round_count);

                    if (++round_count > records_count) {
                        recompressed_size = 0;
                        break;
                    }
                    result_compress = compress_block(r_info->rbuff, decomp_total, r_info, &recompressed_size, snappy_block_factor, records_count_r + records_count_l);

                    if (result_compress < 0) {
                        char dt[DT_LOG_SIZE];
                        fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Failed recompress %s, decomp_total=%lu, read_block_num_l=%u, read_block_num_r=%u, code=%d\n",
                                curr_dt(dt), t_handler->cid, r_info->uuid, decomp_total, read_block_num_l, read_block_num_r, result_compress);
                        graceful_exit(-1);
                    }
                }

                if (recompressed_size == 0) {
                    uint32_t void_increase = VOIDS - 1U - (z_units_l + 1U);

                    if (log_level > 1) {
                        char dt[DT_LOG_SIZE];
                        fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] void_increase=%u, block_l=%u, block_r=%u, void_count_curr_l=%u, v=%u, v2=%u\n", curr_dt(dt),
                                t_handler->cid, void_increase, read_block_num_l, read_block_num_r, void_count_curr_l, v, v2);
                    }
                    continue;
                }
                r_info->r_idx_b[bs]->void_info[b][v2].void_arr[void_count_curr_r] = 0;
                r_info->r_idx_b[bs]->void_info[b][v2].void_count_curr--;
                r_info->r_idx_b[bs]->void_info[b][v2].void_count_tot--;

                if (r_info->r_idx_b[bs]->max_void_pos == v2 && r_info->r_idx_b[bs]->max_void_count != 0)
                    r_info->r_idx_b[bs]->max_void_count--;
                r_info->r_idx_b[bs]->void_info[b][v].void_arr[void_count_curr_l] = 0;
                r_info->r_idx_b[bs]->void_info[b][v].void_count_curr--;
                r_info->r_idx_b[bs]->void_info[b][v].void_count_tot--;

                if (r_info->r_idx_b[bs]->max_void_pos == v && r_info->r_idx_b[bs]->max_void_count != 0)
                    r_info->r_idx_b[bs]->max_void_count--;
                uint64_t r_real_offset = 0;
                decomp_total = align_up(decomp_total, FIX_BSIZE_FACTOR);

                do {
                    clear_extracted_key_fields(r_info->msg_get);

                    if (extract_void_key(r_info, r_info->rbuff + r_real_offset, t_handler) < 0) {
                        graceful_exit(-58);
                        return 0;
                    }

                    if (r_info->msg_get->msg_size == 0) {
                        fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Break merge void index_corrector msg_size=%u for %s, key_size=%u\n", curr_dt(dt), t_handler->cid,
                                r_info->msg_get->msg_size, r_info->r_idx_b[bs]->fname, r_info->msg_get->kv->key_size);
                        break; // TODO stop
                    }
                    oc_index_corrector(r_info, b, r_info->msg_get, write_block, -1, bs, t_handler);
                    handle_scans(r_info, b, bs, write_block, empty_block, r_info->msg_get, t_handler->cid);
                    r_real_offset += (calc_zip_record_size_fix(r_info->msg_get->msg_size) * FIX_BSIZE_FACTOR);
                } while (r_real_offset != decomp_total);
                clear_extracted_key_fields(r_info->msg_get);
                uint32_t new_zindex_units = (uint32_t)((recompressed_size + (uint64_t)local_bsize_factor - 1ULL) / (uint64_t)local_bsize_factor);

                if (read_block_num_l < read_block_num_r) {
                    r_info->r_idx_b[bs]->idx[b][read_block_num_l].z_index = (uint8_t)new_zindex_units;
                    r_info->r_idx_b[bs]->idx[b][read_block_num_r].z_index = 0;
                } else {
                    r_info->r_idx_b[bs]->idx[b][read_block_num_r].z_index = (uint8_t)new_zindex_units;
                    r_info->r_idx_b[bs]->idx[b][read_block_num_l].z_index = 0;
                }
                uint32_t void_pos_this = VOIDS - 1U - new_zindex_units;
                add_void(r_info, b, void_pos_this, write_block, bs);

                if (r_info->r_idx_b[bs]->void_info[b][VOIDS - 1].void_count_curr < MAX_VOID_ELEMENTS) {
                    r_info->r_idx_b[bs]->void_info[b][VOIDS - 1].void_arr[r_info->r_idx_b[bs]->void_info[b][VOIDS - 1].void_count_curr] = empty_block;
                    r_info->r_idx_b[bs]->void_info[b][VOIDS - 1].void_count_curr++;
                }

                if (r_info->r_idx_b[bs]->void_info[b][VOIDS - 1].void_arr[0] == 0 && r_info->r_idx_b[bs]->void_info[b][VOIDS - 1].void_count_curr > 0) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s DEBUG [RS_IDX=%015lu] b_offset=%u\n", curr_dt(dt), 0L, 0);
                }
                r_info->r_idx_b[bs]->void_info[b][VOIDS - 1].void_count_tot++;

                if (data_write_off == 0) {
                    fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 229c. Try to write data to meta section %s, data_write_off=%lu, b_offset=%u\n", curr_dt(dt),
                            t_handler->cid, r_info->r_idx_b[bs]->fname, data_write_off, r_info->r_idx_b[bs]->b_offset[b]);
                    r_info->stop = true;
                    r_info->bad_data = true;
                    finish_thread(r_info, 103, true, t_handler);
                }

                if (empty_write_off == 0) {
                    fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 230c. Try to write data to meta section %s, empty_write_off=%lu, b_offset=%u\n", curr_dt(dt),
                            t_handler->cid, r_info->r_idx_b[bs]->fname, empty_write_off, r_info->r_idx_b[bs]->b_offset[b]);
                    r_info->stop = true;
                    r_info->bad_data = true;
                    finish_thread(r_info, 104, true, t_handler);
                }

                if (log_level > 1) {
                    fprintf(stderr,
                            "%s DEBUG [COMPACT_SERVICE=%015lu] Data write %s[%d] data_write_off=%lu, "
                            "recompressed_size=%llu (units=%u), Lz=%u, Rz=%u, decomp_total=%llu\n",
                            curr_dt(dt), t_handler->cid, r_info->uuid, b, data_write_off, (unsigned long long)recompressed_size, new_zindex_units, z_units_l, z_units_r,
                            (unsigned long long)decomp_total);
                }
                recompressed_size = (uint32_t)((recompressed_size - 1) / MAX_BSIZE_FACTOR + 1) * MAX_BSIZE_FACTOR;
                replicas_writer(r_info, r_info->bbuff, b, data_write_off, (uint32_t)recompressed_size, false, bs, true, t_handler);
                memset(r_info->rbuff, 0, local_block_size);

                if (log_level > 1) {
                    fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Void %s[%d] empty_write_off=%lu (zeroing %u bytes)\n", curr_dt(dt), t_handler->cid, r_info->uuid, b,
                            empty_write_off, local_block_size);
                }
                replicas_writer(r_info, r_info->rbuff, b, empty_write_off, local_block_size, false, bs, true, t_handler);
            }
        }
    }
    move_compressed_void(r_info, b, bs, t_handler);
    return 0;
}

void move_compressed_void(struct region_info_t *r_info, uint8_t b, uint8_t bs, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    uint64_t block_l;
    uint64_t block_off_l;
    uint64_t block_m;
    uint64_t block_off_m;
    uint8_t r_filled_zip_size_m;
    uint64_t block_r;
    uint64_t block_off_r;
    uint8_t r_filled_zip_size_r;
    uint64_t r_real_offset;
    uint32_t v2 = VOIDS - 1;

    if (r_info->r_idx_b[bs]->void_info[b][v2].void_count_curr == 0)
        return;
    uint32_t void_count_curr_l = r_info->r_idx_b[bs]->void_info[b][v2].void_count_curr - 1;
    uint32_t local_block_size = r_info->r_idx_b[bs]->block_size;
    uint16_t local_bsize_factor = r_info->r_idx_b[bs]->bsize_factor;

    while (atomic_load(&is_shutting_down))
        sleep(10);
    block_m = r_info->r_idx_b[bs]->b_offset[b] - 1;
    block_off_m = (uint64_t)block_m * local_block_size;
    r_filled_zip_size_m = r_info->r_idx_b[bs]->idx[b][block_m].z_index;
    uint32_t void_pos_this = VOIDS - 1 - r_filled_zip_size_m;
    memset(r_info->rbuff, 0, local_block_size);

    if (r_filled_zip_size_m != 0) {
        uint32_t compressed_size_m = (uint32_t)r_filled_zip_size_m * (uint32_t)local_bsize_factor;
        uint32_t read_m = read_file(r_info->r_idx_b[bs]->fd[b], r_info->rbuff, block_off_m, compressed_size_m, r_info, t_handler->cid);

        if (read_m == 0) {
            fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 290. Read m failed at %s[%d], off=%lu, size=%u\n", curr_dt(dt), t_handler->cid,
                    r_info->r_idx_b[bs]->fname, b, block_off_m, compressed_size_m);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 105, true, t_handler);
        }
        uint64_t decomp_size_m = 0;

        if (decompress_block(r_info->rbuff, read_m, block_m, bs, r_info, &decomp_size_m, 0, t_handler) != 0) {
            fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 291. Decompress m failed at %s[%d]\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, b);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 106, true, t_handler);
        }
        block_l = r_info->r_idx_b[bs]->void_info[b][v2].void_arr[void_count_curr_l];
        block_off_l = (uint64_t)block_l * local_block_size;
        r_real_offset = 0;
        decomp_size_m = align_up(decomp_size_m, FIX_BSIZE_FACTOR);

        do {
            clear_extracted_key_fields(r_info->msg_get);
            extract_void_key(r_info, r_info->bbuff + r_real_offset, t_handler);

            if (r_info->msg_get->msg_size == 0) {
                fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Compressed break move middle void index_corrector msg_size=%u for %s, key_size=%u\n", curr_dt(dt),
                        t_handler->cid, r_info->msg_get->msg_size, r_info->r_idx_b[bs]->fname, r_info->msg_get->kv->key_size);
                break; // TODO stop
            }
            oc_index_corrector(r_info, b, r_info->msg_get, block_l, -1, bs, t_handler);
            handle_scans(r_info, b, bs, block_l, block_m, r_info->msg_get, t_handler->cid);
            r_real_offset += (uint64_t)calc_zip_record_size_fix(r_info->msg_get->msg_size) * (uint64_t)FIX_BSIZE_FACTOR;
        } while (r_real_offset != decomp_size_m);
        clear_extracted_key_fields(r_info->msg_get);

        if (block_off_l == 0) {
            fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 292. Try to write data to meta section %s, block_off_l = %lu, b_offset = %u\n", curr_dt(dt),
                    t_handler->cid, r_info->r_idx_b[bs]->fname, block_off_l, r_info->r_idx_b[bs]->b_offset[b]);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 107, true, t_handler);
        }

        if (log_level > 1)
            fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Compressed data write %s[%d] block_off_l=%lu, r_filled_zip_size_m=%d\n", curr_dt(dt), t_handler->cid,
                    r_info->r_idx_b[bs]->fname, b, block_off_l, r_filled_zip_size_m);
        replicas_writer(r_info, r_info->rbuff, b, block_off_l, local_block_size, false, bs, true, t_handler);
        r_info->r_idx_b[bs]->idx[b][block_l].z_index = r_filled_zip_size_m;
        r_info->r_idx_b[bs]->idx[b][block_m].z_index = 0;
        change_block_void(r_info, b, void_pos_this, block_m, block_l, bs);
        r_info->r_idx_b[bs]->void_info[b][v2].void_arr[void_count_curr_l] = 0;
        r_info->r_idx_b[bs]->void_info[b][v2].void_count_curr--;
        r_info->r_idx_b[bs]->void_info[b][v2].void_count_tot--;
    } else {
        remove_void(r_info, b, v2, block_m, bs);
    }
    block_r = r_info->r_idx_b[bs]->b_offset[b];
    block_off_r = (uint64_t)block_r * local_block_size;
    r_filled_zip_size_r = r_info->r_idx_b[bs]->idx[b][block_r].z_index;

    if (r_filled_zip_size_r != 0) {
        memset(r_info->rbuff, 0, local_block_size);
        uint32_t compressed_size_r = (uint32_t)r_filled_zip_size_r * (uint32_t)local_bsize_factor;
        uint32_t read_r = read_file(r_info->r_idx_b[bs]->fd[b], r_info->rbuff, block_off_r, compressed_size_r, r_info, t_handler->cid);

        if (read_r == 0) {
            fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 293. Read r failed at %s[%d], off=%lu, size=%u\n", curr_dt(dt), t_handler->cid,
                    r_info->r_idx_b[bs]->fname, b, block_off_r, compressed_size_r);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 108, true, t_handler);
        }
        uint64_t decomp_size_r = 0;

        if (decompress_block(r_info->rbuff, read_r, block_r, bs, r_info, &decomp_size_r, 0, t_handler) != 0) {
            fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 294. Decompress r failed\n", curr_dt(dt), t_handler->cid);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 109, true, t_handler);
        }
        r_real_offset = 0;
        decomp_size_r = align_up(decomp_size_r, FIX_BSIZE_FACTOR);

        do {
            clear_extracted_key_fields(r_info->msg_get);
            extract_void_key(r_info, r_info->bbuff + r_real_offset, t_handler);

            if (r_info->msg_get->msg_size == 0) {
                fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Compressed break move middle void index_corrector msg_size=%u for %s, key_size=%u\n", curr_dt(dt),
                        t_handler->cid, r_info->msg_get->msg_size, r_info->r_idx_b[bs]->fname, r_info->msg_get->kv->key_size);
                break; // TODO stop
            }
            oc_index_corrector(r_info, b, r_info->msg_get, block_m, -1, bs, t_handler);
            handle_scans(r_info, b, bs, block_m, block_r, r_info->msg_get, t_handler->cid);
            r_real_offset += (uint64_t)calc_zip_record_size_fix(r_info->msg_get->msg_size) * (uint64_t)FIX_BSIZE_FACTOR;
        } while (r_real_offset != decomp_size_r);
        clear_extracted_key_fields(r_info->msg_get);

        if (block_off_m == 0) {
            fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 295. Try to write data to meta section %s, block_off_m = %lu, b_offset = %u\n", curr_dt(dt),
                    t_handler->cid, r_info->r_idx_b[bs]->fname, block_off_m, r_info->r_idx_b[bs]->b_offset[b]);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 110, true, t_handler);
        }

        if (log_level > 1)
            fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Compressed data write %s[%d] block_off_m=%lu, r_filled_zip_size_r=%d\n", curr_dt(dt), t_handler->cid,
                    r_info->r_idx_b[bs]->fname, b, block_off_m, r_filled_zip_size_r);
        replicas_writer(r_info, r_info->rbuff, b, block_off_m, local_block_size, false, bs, true, t_handler);
    } else {
        if (block_off_m == 0) {
            fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 296. Try to write data to meta section %s, block_off_m = %lu, b_offset = %u\n", curr_dt(dt),
                    t_handler->cid, r_info->r_idx_b[bs]->fname, block_off_m, r_info->r_idx_b[bs]->b_offset[b]);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 111, true, t_handler);
        }

        if (log_level > 1)
            fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Compressed data truncate %s[%d] block_off_m=%lu, r_filled_zip_size_r=%d\n", curr_dt(dt), t_handler->cid,
                    r_info->r_idx_b[bs]->fname, b, block_off_m, r_filled_zip_size_r);
        replicas_writer(r_info, r_info->rbuff, b, block_off_m, local_block_size, true, bs, true, t_handler);
        r_info->r_idx_b[bs]->b_offset[b]--;
        return;
    }

    if (block_off_r == 0) {
        fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 297. Try to write data to meta section %s, block_off_r = %lu, b_offset = %u\n", curr_dt(dt), t_handler->cid,
                r_info->r_idx_b[bs]->fname, block_off_r, r_info->r_idx_b[bs]->b_offset[b]);
        r_info->stop = true;
        r_info->bad_data = true;
        finish_thread(r_info, 112, true, t_handler);
    }

    if (log_level > 1)
        fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Compressed truncate %s on block_off_r=%lu, r_filled_zip_size_r=%d\n", curr_dt(dt), t_handler->cid,
                r_info->r_idx_b[bs]->fname, block_off_r, r_filled_zip_size_r);
    replicas_writer(r_info, r_info->rbuff, b, block_off_r, local_block_size, true, bs, true, t_handler);
    r_info->r_idx_b[bs]->idx[b][block_m].z_index = r_filled_zip_size_r;
    r_info->r_idx_b[bs]->idx[b][block_r].z_index = 0;
    r_info->r_idx_b[bs]->b_offset[b]--;
}

int merge_voids(int v, struct region_info_t *r_info, uint8_t b, uint8_t bs, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    uint64_t write_block = 0;
    uint64_t empty_block = 0;
    uint64_t data_write_off = 0;
    uint64_t empty_write_off = 0;
    uint64_t void_pos_this = 0;
    uint32_t local_block_size = r_info->r_idx_b[bs]->block_size;
    uint16_t local_bsize_factor = r_info->r_idx_b[bs]->bsize_factor;

    for (int v2 = VOIDS - v; v2 < VOIDS - 1; v2++) {
        if (v2 > VOIDS / 2 && r_info->r_idx_b[bs]->max_void_count < r_info->r_idx_b[bs]->void_info[b][v2].void_count_tot) {
            r_info->r_idx_b[bs]->max_void_pos = v2;
            r_info->r_idx_b[bs]->max_void_count = r_info->r_idx_b[bs]->void_info[b][v2].void_count_tot;
        }

        if (v2 != v && v + v2 > VOIDS) {
            if (r_info->r_idx_b[bs]->void_info[b][v].void_count_curr > 0 && r_info->r_idx_b[bs]->void_info[b][v2].void_count_curr > 0) {
                int32_t void_count_curr_l = r_info->r_idx_b[bs]->void_info[b][v].void_count_curr - 1;
                uint32_t read_block_num_l = r_info->r_idx_b[bs]->void_info[b][v].void_arr[void_count_curr_l];
                uint64_t rw_off_l = (uint64_t)read_block_num_l * local_block_size;
                uint8_t r_filled_zip_size_l = r_info->r_idx_b[bs]->idx[b][read_block_num_l].z_index;
                uint32_t data_size = read_file(r_info->r_idx_b[bs]->fd[b], r_info->rbuff, rw_off_l, r_filled_zip_size_l * local_bsize_factor, r_info, t_handler->cid);
                int32_t void_count_curr_r = r_info->r_idx_b[bs]->void_info[b][v2].void_count_curr - 1;
                uint32_t read_block_num_r = r_info->r_idx_b[bs]->void_info[b][v2].void_arr[void_count_curr_r];
                uint8_t r_filled_zip_size_r = r_info->r_idx_b[bs]->idx[b][read_block_num_r].z_index;
                uint64_t rw_off_r = (uint64_t)read_block_num_r * local_block_size;
                read_file(r_info->r_idx_b[bs]->fd[b], r_info->rbuff + r_filled_zip_size_l * local_bsize_factor, rw_off_r, r_filled_zip_size_r * local_bsize_factor, r_info,
                          t_handler->cid);
                uint32_t new_rsize = (r_filled_zip_size_l + r_filled_zip_size_r) * local_bsize_factor;

                if (read_block_num_l < read_block_num_r) {
                    write_block = read_block_num_l;
                    empty_block = read_block_num_r;
                    data_write_off = rw_off_l;
                    empty_write_off = rw_off_r;
                    r_info->r_idx_b[bs]->idx[b][read_block_num_l].z_index = r_filled_zip_size_l + r_filled_zip_size_r;
                    r_info->r_idx_b[bs]->idx[b][read_block_num_r].z_index = 0;
                } else {
                    write_block = read_block_num_r;
                    empty_block = read_block_num_l;
                    data_write_off = rw_off_r;
                    empty_write_off = rw_off_l;
                    r_info->r_idx_b[bs]->idx[b][read_block_num_r].z_index = r_filled_zip_size_l + r_filled_zip_size_r;
                    r_info->r_idx_b[bs]->idx[b][read_block_num_l].z_index = 0;
                }
                uint32_t r_real_offset = 0;

                do {
                    clear_extracted_key_fields(r_info->msg_get);
                    extract_void_key(r_info, r_info->rbuff + r_real_offset, t_handler);

                    if (r_info->msg_get->msg_size == 0) {
                        fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 34. Can't read next key-value at block %lu/%lu (r_real_offset=%d) in %s[%d]\n", curr_dt(dt),
                                t_handler->cid, rw_off_l, rw_off_r, r_real_offset, r_info->r_idx_b[bs]->fname, b);
                        r_info->stop = true;
                        r_info->bad_data = true;
                        finish_thread(r_info, 113, true, t_handler);
                    }
                    oc_index_corrector(r_info, b, r_info->msg_get, write_block, -1, bs, t_handler);
                    handle_scans(r_info, b, bs, write_block, empty_block, r_info->msg_get, t_handler->cid);
                    r_real_offset += calc_zip_record_size(r_info, r_info->msg_get->msg_size, bs) * local_bsize_factor;
                } while (r_real_offset != new_rsize);
                r_info->r_idx_b[bs]->void_info[b][v].void_arr[void_count_curr_l] = 0;
                r_info->r_idx_b[bs]->void_info[b][v2].void_arr[void_count_curr_r] = 0;
                r_info->r_idx_b[bs]->void_info[b][v].void_count_curr--;
                r_info->r_idx_b[bs]->void_info[b][v2].void_count_curr--;
                r_info->r_idx_b[bs]->void_info[b][v].void_count_tot--;
                r_info->r_idx_b[bs]->void_info[b][v2].void_count_tot--;

                if ((r_info->r_idx_b[bs]->max_void_pos == v || r_info->r_idx_b[bs]->max_void_pos == v2) && r_info->r_idx_b[bs]->max_void_count != 0)
                    r_info->r_idx_b[bs]->max_void_count--;
                void_pos_this = VOIDS - 1 - (r_filled_zip_size_l + r_filled_zip_size_r);
                add_void(r_info, b, void_pos_this, write_block, bs);

                if (r_info->r_idx_b[bs]->void_info[b][VOIDS - 1].void_count_curr < MAX_VOID_ELEMENTS) {
                    r_info->r_idx_b[bs]->void_info[b][VOIDS - 1].void_arr[r_info->r_idx_b[bs]->void_info[b][VOIDS - 1].void_count_curr] = empty_block;
                    r_info->r_idx_b[bs]->void_info[b][VOIDS - 1].void_count_curr++;
                }

                if (r_info->r_idx_b[bs]->void_info[b][VOIDS - 1].void_arr[0] == 0 && r_info->r_idx_b[bs]->void_info[b][VOIDS - 1].void_count_curr > 0) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s DEBUG [RS_IDX=%015lu] b_offset=%u\n", curr_dt(dt), 0L, 0);
                }
                r_info->r_idx_b[bs]->void_info[b][VOIDS - 1].void_count_tot++;

                if (data_write_off == 0) {
                    fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 229. Try to write data to meta section %s, data_write_off = %lu, r_info->b_offset = %u\n",
                            curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, data_write_off, r_info->r_idx_b[bs]->b_offset[b]);
                    r_info->stop = true;
                    r_info->bad_data = true;
                    finish_thread(r_info, 114, true, t_handler);
                }

                if (empty_write_off == 0) {
                    fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 230. Try to write data to meta section %s, empty_write_off = %lu, r_info->b_offset = %u\n",
                            curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, empty_write_off, r_info->r_idx_b[bs]->b_offset[b]);
                    r_info->stop = true;
                    r_info->bad_data = true;
                    finish_thread(r_info, 115, true, t_handler);
                }

                if (log_level > 1)
                    fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Data write %s[%d] data_write_off=%lu, size=%d, r_filled_zip_size_l=%d, r_filled_zip_size_r=%d\n",
                            curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, b, data_write_off, new_rsize, r_filled_zip_size_l, r_filled_zip_size_r);
                replicas_writer(r_info, r_info->rbuff, b, data_write_off, new_rsize, false, bs, true, t_handler);
                memset(r_info->rbuff, 0, local_block_size);

                if (log_level > 1)
                    fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Void %s[%d] empty_write_off=%lu\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, b,
                            empty_write_off);
                replicas_writer(r_info, r_info->rbuff, b, empty_write_off, local_block_size, false, bs, true, t_handler);
            }
        }
    }
    move_void(r_info, b, bs, t_handler);
}

void move_void(struct region_info_t *r_info, uint8_t b, uint8_t bs, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    uint64_t block_l;
    uint64_t block_off_l;
    uint64_t block_m;
    uint64_t block_off_m;
    uint8_t r_filled_zip_size_m;
    uint64_t block_r;
    uint64_t block_off_r;
    uint8_t r_filled_zip_size_r;
    uint64_t r_real_offset;
    uint32_t v2 = VOIDS - 1;

    if (r_info->r_idx_b[bs]->void_info[b][v2].void_count_curr == 0)
        return;
    uint32_t void_count_curr_l = r_info->r_idx_b[bs]->void_info[b][v2].void_count_curr - 1;
    uint32_t local_block_size = r_info->r_idx_b[bs]->block_size;
    uint16_t local_bsize_factor = r_info->r_idx_b[bs]->bsize_factor;

    while (atomic_load(&is_shutting_down))
        sleep(10);
    block_m = r_info->r_idx_b[bs]->b_offset[b] - 1;
    block_off_m = (uint64_t)block_m * local_block_size;
    r_filled_zip_size_m = r_info->r_idx_b[bs]->idx[b][block_m].z_index;
    uint32_t void_pos_this = VOIDS - 1 - r_filled_zip_size_m;

    if (r_filled_zip_size_m != 0) {
        memset(r_info->rbuff, 0, local_block_size);
        read_file(r_info->r_idx_b[bs]->fd[b], r_info->rbuff, block_off_m, r_filled_zip_size_m * local_bsize_factor, r_info, t_handler->cid);
        block_l = r_info->r_idx_b[bs]->void_info[b][v2].void_arr[void_count_curr_l];
        block_off_l = (uint64_t)block_l * local_block_size;
        r_real_offset = 0;

        do {
            clear_extracted_key_fields(r_info->msg_get);
            extract_void_key(r_info, r_info->rbuff + r_real_offset, t_handler);

            if (r_info->msg_get->msg_size == 0) {
                fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 35. Cannot read next key-value at block %lu (r_real_offset=%lu) in %s[%d].\n", curr_dt(dt),
                        t_handler->cid, block_off_m, r_real_offset, r_info->r_idx_b[bs]->fname, b);
                r_info->stop = true;
                r_info->bad_data = true;
                finish_thread(r_info, 116, true, t_handler);
            }
            oc_index_corrector(r_info, b, r_info->msg_get, block_l, -1, bs, t_handler);
            handle_scans(r_info, b, bs, block_l, block_m, r_info->msg_get, t_handler->cid);
            r_real_offset += calc_zip_record_size(r_info, r_info->msg_get->msg_size, bs) * local_bsize_factor;
        } while (r_real_offset != r_filled_zip_size_m * local_bsize_factor);

        if (block_off_l == 0) {
            fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 231. Try to write data to meta section %s, block_off_l = %lu, r_info->b_offset = %u\n", curr_dt(dt),
                    t_handler->cid, r_info->r_idx_b[bs]->fname, block_off_l, r_info->r_idx_b[bs]->b_offset[b]);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 117, true, t_handler);
        }

        if (log_level > 1)
            fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Data write %s[%d] block_off_l=%lu, r_filled_zip_size_m=%d\n", curr_dt(dt), t_handler->cid,
                    r_info->r_idx_b[bs]->fname, b, block_off_l, r_filled_zip_size_m);
        replicas_writer(r_info, r_info->rbuff, b, block_off_l, local_block_size, false, bs, true, t_handler);
        r_info->r_idx_b[bs]->idx[b][block_l].z_index = r_filled_zip_size_m;
        r_info->r_idx_b[bs]->idx[b][block_m].z_index = 0;
        change_block_void(r_info, b, void_pos_this, block_m, block_l, bs);
        r_info->r_idx_b[bs]->void_info[b][v2].void_arr[void_count_curr_l] = 0;
        r_info->r_idx_b[bs]->void_info[b][v2].void_count_curr--;
        r_info->r_idx_b[bs]->void_info[b][v2].void_count_tot--;
    } else
        remove_void(r_info, b, v2, block_m, bs);
    block_r = r_info->r_idx_b[bs]->b_offset[b];
    block_off_r = (uint64_t)block_r * local_block_size;
    r_filled_zip_size_r = r_info->r_idx_b[bs]->idx[b][block_r].z_index;

    if (r_filled_zip_size_r != 0) {
        memset(r_info->rbuff, 0, local_block_size);
        read_file(r_info->r_idx_b[bs]->fd[b], r_info->rbuff, block_off_r, r_filled_zip_size_r * local_bsize_factor, r_info, t_handler->cid);
        r_real_offset = 0;

        do {
            clear_extracted_key_fields(r_info->msg_get);
            extract_void_key(r_info, r_info->rbuff + r_real_offset, t_handler);

            if (r_info->msg_get->msg_size == 0) {
                fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 36. Cannot read next key-value at block %lu (r_real_offset=%lu) in %s[%d]\n", curr_dt(dt),
                        t_handler->cid, block_off_r, r_real_offset, r_info->r_idx_b[bs]->fname, b);
                r_info->stop = true;
                r_info->bad_data = true;
                finish_thread(r_info, 118, true, t_handler);
            }
            oc_index_corrector(r_info, b, r_info->msg_get, block_m, -1, bs, t_handler);
            handle_scans(r_info, b, bs, block_m, block_r, r_info->msg_get, t_handler->cid);
            r_real_offset += calc_zip_record_size(r_info, r_info->msg_get->msg_size, bs) * local_bsize_factor;
        } while (r_real_offset != r_filled_zip_size_r * local_bsize_factor);
        clear_extracted_key_fields(r_info->msg_get);

        if (block_off_m == 0) {
            fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 232. Try to write data to meta section %s, block_off_m = %lu, r_info->b_offset = %u\n", curr_dt(dt),
                    t_handler->cid, r_info->r_idx_b[bs]->fname, block_off_m, r_info->r_idx_b[bs]->b_offset[b]);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 119, true, t_handler);
        }

        if (log_level > 1)
            fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Data write %s[%d] block_off_m=%lu, r_filled_zip_size_r=%d\n", curr_dt(dt), t_handler->cid,
                    r_info->r_idx_b[bs]->fname, b, block_off_m, r_filled_zip_size_r);
        replicas_writer(r_info, r_info->rbuff, b, block_off_m, local_block_size, false, bs, true, t_handler);
    } else {
        if (block_off_m == 0) {
            fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 233. Try to write data to meta section %s, block_off_m = %lu, r_info->b_offset = %u\n", curr_dt(dt),
                    t_handler->cid, r_info->r_idx_b[bs]->fname, block_off_m, r_info->r_idx_b[bs]->b_offset[b]);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 120, true, t_handler);
        }

        if (log_level > 1)
            fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Data truncate %s[%d] block_off_m=%lu, r_filled_zip_size_r=%d\n", curr_dt(dt), t_handler->cid,
                    r_info->r_idx_b[bs]->fname, b, block_off_m, r_filled_zip_size_r);
        replicas_writer(r_info, r_info->rbuff, b, block_off_m, local_block_size, true, bs, true, t_handler);
        r_info->r_idx_b[bs]->b_offset[b]--;
        return;
    }

    if (block_off_r == 0) {
        fprintf(stderr, "%s ERROR [COMPACT_SERVICE=%015lu] Error code 234. Try to write data to meta section %s, block_off_r = %lu, r_info->b_offset = %u\n", curr_dt(dt),
                t_handler->cid, r_info->r_idx_b[bs]->fname, block_off_r, r_info->r_idx_b[bs]->b_offset[b]);
        r_info->stop = true;
        r_info->bad_data = true;
        finish_thread(r_info, 121, true, t_handler);
    }

    if (log_level > 1)
        fprintf(stderr, "%s DEBUG [COMPACT_SERVICE=%015lu] Truncate %s on block_off_r=%lu, r_filled_zip_size_r=%d\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname,
                block_off_r, r_filled_zip_size_r);
    replicas_writer(r_info, r_info->rbuff, b, block_off_r, local_block_size, true, bs, true, t_handler);
    r_info->r_idx_b[bs]->idx[b][block_m].z_index = r_filled_zip_size_r;
    r_info->r_idx_b[bs]->idx[b][block_r].z_index = 0;
    r_info->r_idx_b[bs]->b_offset[b]--;
}

/*
_Noreturn void *pthread_void_service() {
    char dt[DT_LOG_SIZE];
    sleep(30);
    table_handler_t* t_handler = create_t_handler(0);
    uint64_t cid = t_handler->cid = get_new_cid();
    fprintf(stderr,"%s INFO [VOIDS_SERVICE=%013lu] Service thread started\n", curr_dt(dt), cid);
    start_ts = getCurrentTimeMs();
    uint32_t cleaner_count = 0;
    float total_void_size = 0;
    uint32_t total_empty_blocks = 0;
    uint32_t cycle = 0;
    table_info_t* t_info;
    uint32_t v2 = VOIDS - 1;
    int32_t void_count_curr_l;
    uint32_t compact_count = 0;
    float total_compacted_size = 0;
    for(;;) {
        cycle++;
        total_void_size = 0;
        total_empty_blocks = 0;
        pthread_mutex_lock(&tables_lock);
        for (int t = 0; t < table_count_curr; t++) {
            t_info = table_info[t];
            pthread_mutex_unlock(&tables_lock);
            get_t_lock(t_info, 14, false, cid);
            for (int r = 0; r < t_info->region_count_curr; r++) {
                region_info_t *r_info = t_info->region_info[r];
                if (r_info->is_leader) {
                    do_t_unlock(t_info, 11, false, cid);
                    get_r_lock(r_info, 101, false, false, t_handler);
                    if (r_info->is_freed) {
                        do_r_unlock(r_info, 102, false, t_handler);
                        get_t_lock(t_info, 21, false, cid);
                        break;
                    }
                    if (true) {
                        for (int b = 0; b < BUCKETS; b++) {
                            for (uint8_t bs = 0; bs < r_info->max_idx_b; bs++) {
                                total_empty_blocks += r_info->r_idx_b[bs]->void_info[b][VOIDS - 1].void_count_tot;
                                for (int v = 0; v < VOIDS - 1; v++) {
                                    total_void_size += (r_info->r_idx_b[bs]->void_info[b][v].void_count_curr * v * r_info->r_idx_b[bs]->bsize_factor);
                                    merge_voids(v, r_info, b, bs, t_handler);
                                    cleaner_count++;
                                }
                                if (r_info->r_idx_b[bs]->void_info[b][v2].void_count_curr > 0) {
                                    //void_count_curr_l = r_info->r_idx_b[bs]->void_info[b][v2].void_count_curr - 1;
                                    //if (r_info->r_idx_b[bs]->void_info[b][v2].void_arr[void_count_curr_l] < r_info->r_idx_b[bs]->b_offset[b] - 1) { // TODO Попробовать убрать
это условие move_void(r_info, b, bs, t_handler); total_compacted_size += r_info->r_idx_b[bs]->block_size; compact_count++;
                                    //}
                                }
                            }
                        }
                    }
                    do_r_unlock(r_info, 113, false, t_handler);
                    get_t_lock(t_info, 19, false, cid);
                    usleep(1000);
                }
            }
            do_t_unlock(t_info, 20, false, cid);
            pthread_mutex_lock(&tables_lock);
        }
        pthread_mutex_unlock(&tables_lock);
        usleep(10000);
        if (cycle == 10) {
            if (total_void_size != 0 && cleaner_count != 0) {
                total_void_size = total_void_size / 1024 / 1024;
                fprintf(stderr, "%s INFO [VOIDS_SERVICE=%013lu] cleaner_count=%d, total_empty_blocks=%d, total_void_size=%.2f (MB)\n",
                    curr_dt(dt), cid,cleaner_count, total_empty_blocks, total_void_size);
            }
            if (total_compacted_size != 0 && compact_count != 0) {
                total_compacted_size = total_compacted_size / 1024 / 1024;
                fprintf(stderr, "%s INFO [VOIDS_SERVICE=%013lu] compact_count=%d, compacted=%.2f (MB)\n",
                    curr_dt(dt), cid, compact_count, total_compacted_size);
            }
            cleaner_count = 0;
            compact_count = 0;
            cycle = 0;
        }
    }
}
*/
