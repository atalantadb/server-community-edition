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
#ifndef ADB_REGION_MEMORY_H
#define ADB_REGION_MEMORY_H
#include "../index/index_builder.h"
#include "../index/index_resizer.h"
static inline uint64_t region_auxiliary_memory_locked(const region_info_t *r) {
    uint64_t bytes = sizeof(*r);

    if (r->replica != NULL)
        bytes += sizeof(*r->replica);

    if (r->uuid != NULL)
        bytes += strlen(r->uuid) + 1U;

    if (r->r_fname != NULL)
        bytes += strlen(r->r_fname) + 1U;

    if (r->k_beg != NULL)
        bytes += REGION_BYTE_RANGE;

    if (r->k_end != NULL)
        bytes += REGION_BYTE_RANGE;

    if (r->r_idx_b != NULL) {
        bytes += (BLOCK_MAX_SIZE_M + 1U) * sizeof(*r->r_idx_b);

        if (r->r_idx_b[BS_LOB] != NULL)
            bytes += sizeof(*r->r_idx_b[BS_LOB]);
    }

    if (r->rbuff != NULL)
        bytes += r->rbuff_size;

    if (r->zbuff != NULL)
        bytes += r->zbuff_size;

    if (r->bbuff != NULL)
        bytes += r->bbuff_size;

    if (r->write_4K_buff != NULL)
        bytes += FIX_BLOCK_SIZE;

    if (r->append_compact_dirty_map != NULL)
        bytes += r->append_compact_dirty_map_size;

    if (r->block_comp_ins_buff != NULL)
        bytes += r->block_comp_ins_buff_size;

    if (r->block_comp_buff != NULL)
        bytes += r->block_comp_buff_size;

    if (r->scan_key != NULL)
        bytes += r->scan_key_size;

    if (r->bulk_buff1 != NULL)
        bytes += r->bulk_buff_size1;

    if (r->bulk_buff2 != NULL)
        bytes += r->bulk_buff_size2;

    if (r->resize_buff != NULL)
        bytes += 8U * sizeof(*r->resize_buff);

    if (r->resize_buff_size != NULL)
        bytes += 8U * sizeof(*r->resize_buff_size);

    if (r->lob_buff != NULL)
        bytes += FIX_BLOCK_SIZE;

    if (r->rsc_msg != NULL)
        bytes += RSC_MSG_BUFFER_SIZE;

    if (r->qfl_info != NULL)
        bytes += QFL_INFO_CAP;

    if (r->snappy_work_buff != NULL)
        bytes += 1U << SNAPPY_BLOCK_SIZE_POW2;

    if (r->lob_map != NULL && r->lob_count_total > 0)
        bytes += (uint64_t)r->lob_count_total * sizeof(*r->lob_map);

    if (r->z_stat != NULL) {
        bytes += (uint64_t)r->z_stat_size * sizeof(*r->z_stat);

        for (unsigned i = 0; i < r->z_stat_size; ++i)
            if (r->z_stat[i] != NULL)
                bytes += 256U;
    }

    if (r->qfl_dic != NULL) {
        bytes += 65536U * sizeof(*r->qfl_dic);
        uint64_t lengths = r->qfl_dic_lens > 0 ? (uint64_t)r->qfl_dic_lens * 2U : 0;

        if (r->qfl_dic_total_size > 0 && (uint64_t)r->qfl_dic_total_size >= lengths)
            bytes += (uint64_t)r->qfl_dic_total_size - lengths;
    }

    if (r->qfl_dic_size != NULL)
        bytes += 65536U * sizeof(*r->qfl_dic_size);
    const msg_cli_t *messages[] = {r->msg_get, r->msg_get_put, r->msg_get_rotate, r->msg_get_link, r->msg_get_put_link};

    for (unsigned i = 0; i < sizeof(messages) / sizeof(messages[0]); ++i) {
        const msg_cli_t *msg = messages[i];

        if (msg == NULL)
            continue;
        bytes += sizeof(*msg);

        if (msg->kv == NULL)
            continue;
        bytes += sizeof(*msg->kv);

        if (msg->kv->qv != NULL)
            bytes += sizeof(*msg->kv->qv);
    }

    if (r->msg_get_put != NULL && r->msg_get_put->kv != NULL) {
        const msg_kv_t *kv = r->msg_get_put->kv;

        if (kv->key != NULL)
            bytes += r->msg_get_put_allocated_key_size;

        if (kv->cf != NULL)
            bytes += r->msg_get_put_allocated_cf_size;

        if (kv->qv != NULL && kv->qv->qfl != NULL)
            bytes += r->msg_get_put_allocated_qfl_size;
    }

    if (r->scan_vars != NULL)
        bytes += sizeof(*r->scan_vars);

    if (r->scan_keys_list != NULL) {
        int count = r->scan_keys_list_count_tot;

        if (count > 0)
            bytes += (uint64_t)count * sizeof(*r->scan_keys_list);

        for (int i = 0; i < count; ++i) {
            const scan_keys_list_t *scan = r->scan_keys_list[i];

            if (scan == NULL)
                continue;
            bytes += sizeof(*scan);

            if (scan->keys != NULL)
                bytes += scan->keys_mem_usage_limit;

            if (scan->key_sizes != NULL)
                bytes += scan->max_key_count != 0 ? scan->max_key_count * 2U : 1U;
        }
    }
    return bytes;
}


static inline void reset_region_memory_snapshot(region_info_t *r) {
    atomic_store_explicit(&r->auxiliary_memory_bytes, 0, memory_order_relaxed);
    r->auxiliary_memory_sample_ms = 0;
}

static inline void refresh_region_memory_locked(region_info_t *r, uint64_t now_ms) {
    if (r->is_freed || r->stop || r->bad_data || r->has_parent_replica || r->idx_resizer != RESIZE_IDX_DONE)
        return;
    uint64_t previous = r->auxiliary_memory_sample_ms;

    if (now_ms != 0 && previous != 0 && (now_ms < previous || now_ms - previous < 1000U))
        return;
    atomic_store_explicit(&r->auxiliary_memory_bytes, region_auxiliary_memory_locked(r), memory_order_relaxed);
    r->auxiliary_memory_sample_ms = now_ms;
}
#endif
