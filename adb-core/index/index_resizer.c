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
#include "index_resizer.h"
#include "../crypto/xxhash.h"
static void *resize_service_entry(void *args) {
    pthread_idx_resize_service(args);
    return NULL;
}


bool resize_thread_runner(struct region_info_t *r_info, uint32_t b, uint8_t bs, table_handler_t *t_handler) {
    invalidate_record_gc(r_info);
    uint64_t cid = t_handler->cid;
    char dt[DT_LOG_SIZE];
    r_info->idx_resizer = RESIZE_IDX_PREPARING;
    pthread_t pthread;
    pthread_attr_t pthread_attr;
    bool attr_initialized = false;
    struct arg_struct *args = NULL;
    table_handler_t *resize_handler = NULL;

    if (pthread_attr_init(&pthread_attr) != 0) {
        fprintf(stderr, "%s WARN [IDX_RESIZER=%lu] Cannot create %s index resizer thread (pthread_attr_init)\n", curr_dt(dt), cid, r_info->r_fname);
        goto start_failed;
    }
    attr_initialized = true;

    if (pthread_attr_setdetachstate(&pthread_attr, PTHREAD_CREATE_DETACHED) != 0) {
        fprintf(stderr, "%s WARN [IDX_RESIZER=%lu] Cannot create %s index resizer thread (pthread_attr_setdetachstate)\n", curr_dt(dt), cid, r_info->r_fname);
        goto start_failed;
    }
    args = index_memory_malloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.resize_args, sizeof(*args));

    if (args == NULL) {
        fprintf(stderr, "%s WARN [IDX_RESIZER=%lu] Cannot allocate resize arguments for %s\n", curr_dt(dt), cid, r_info->r_fname);
        goto start_failed;
    }
    args->r_info = r_info;
    args->b = b;
    args->bs = bs;
    resize_handler = create_t_handler(0);

    if (resize_handler == NULL)
        goto start_failed;
    args->t_handler = resize_handler;
    args->t_handler->cid = get_new_cid();

    if (pthread_create(&pthread, &pthread_attr, resize_service_entry, args) != 0) {
        fprintf(stderr, "%s WARN [IDX_RESIZER=%lu] Cannot create %s index resizer thread\n", curr_dt(dt), cid, r_info->r_fname);
        goto start_failed;
    }
    pthread_attr_destroy(&pthread_attr);

    do {
        usleep(10000);
    } while (r_info->idx_resizer == RESIZE_IDX_PREPARING);
    fprintf(stderr, "%s DEBUG [IDX_RESIZER=%lu] GO r_info->idx_resizer = %d\n", curr_dt(dt), cid, r_info->idx_resizer);
    return r_info->idx_resizer >= RESIZE_IDX_PREPARED && !r_info->bad_data;
start_failed:
    if (attr_initialized)
        pthread_attr_destroy(&pthread_attr);

    if (resize_handler != NULL)
        free_t_handler(resize_handler);
    index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.resize_args, args);
    r_info->idx_resizer = RESIZE_IDX_DONE;
    return false;
}


void records_distributer(region_info_t *r_info, uint8_t b, uint8_t *volatile record_keeper, uint32_t volatile record_count_start, uint32_t volatile record_count_curr,
                         bool is_keys, uint8_t bs, table_handler_t *t_handler) {
    uint64_t cid = t_handler->cid;
    char dt[DT_LOG_SIZE];
    union i2b_u i2b_hsh;
    union i2b_u i2b_offset;

    for (int c = record_count_start * ((uint32_t)COLL_KEEPER_SIZE); c < record_count_curr * ((uint32_t)COLL_KEEPER_SIZE); c += ((uint32_t)COLL_KEEPER_SIZE)) {
        memcpy(i2b_hsh.byte, record_keeper + c, sizeof(i2b_hsh.byte));
        uint32_t k_idx_x2 = (i2b_hsh.integer_val & ((r_info->r_idx_b[bs]->idx_mask[b] + 1) * 2 - 1));
        r_info->r_idx_b[bs]->idx_r_help_x2->k_idx = k_idx_x2;
#ifdef COLL_LOB_TEST
        r_info->r_idx_b[bs]->idx_r_help_x2->c_idx = record_keeper[c + C_IDX_POS];
#else
        r_info->r_idx_b[bs]->idx_r_help_x2->c_idx =
            record_keeper[c + C_IDX_POS] + (record_keeper[c + C_IDX_POS + 1] << 8) + ((record_keeper[c + C_IDX_POS + 2] & 0x7Fu) << 16);
#endif
        r_info->r_idx_b[bs]->idx_r_help_x2->b = b;
        memcpy(i2b_offset.byte, record_keeper + c + ((uint32_t)DIGEST_SIZE_FULL), ((uint32_t)OFFSET_COLL_KEEPER_SIZE));
        i2b_offset.byte[OFFSET_COLL_KEEPER_SIZE] = 0;
        uint8_t result_chk = check_k_idx(r_info, is_keys, i2b_hsh.byte, record_keeper + c, i2b_offset.integer_val, bs, t_handler);

        if (result_chk == INS_IDX_CHECK_ERROR)
            return;

        if (result_chk == INS_IDX_UPDATE) {
            k_idx_x2 = r_info->r_idx_b[bs]->idx_r_help_x2->k_idx;
            r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].o_index = i2b_offset.integer_val;
        }

        if (result_chk == INS_IDX_OK) {
            if (record_keeper[c] != 0 || record_keeper[c + 1] != 0 || record_keeper[c + 2] != 0 || record_keeper[c + 3] != 0 || record_keeper[c + 4] != 0) {
                if (find_k_idx_x2(r_info, 0, bs, t_handler) == false) {
                    fprintf(
                        stderr,
                        "%s ERROR [IDX_RESIZER=%015lu] Error code 123. Cannot find the new place for r_info->r_idx_b[%d]->idx_r_help_x2->k_idx=%d after index resize %s[%d]\n",
                        curr_dt(dt), cid, bs, r_info->r_idx_b[bs]->idx_r_help_x2->k_idx, r_info->r_idx_b[bs]->fname, b);
                    r_info->stop = true;
                    r_info->bad_data = true;
                    finish_thread(r_info, 61, true, t_handler);
                }
                k_idx_x2 = r_info->r_idx_b[bs]->idx_r_help_x2->k_idx;
                r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].o_index = i2b_offset.integer_val;
                r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].c_index = r_info->r_idx_b[bs]->idx_r_help_x2->c_idx;

                if (r_info->r_idx_b[bs]->f_idx_size[b] < (1U << 16))
                    r_info->r_idx_b[bs]->k1_index_x2[b][k_idx_x2] = i2b_hsh.byte[1];

                if (r_info->r_idx_b[bs]->f_idx_size[b] < (1U << 24))
                    r_info->r_idx_b[bs]->k2_index_x2[b][k_idx_x2] = i2b_hsh.byte[2];
                r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].k3_index = i2b_hsh.byte[3];

                if (r_info->r_idx_b[bs]->f_idx_size[b] >= (1U << 16))
                    i2b_hsh.byte[1] = (k_idx_x2 & 0xFF00) >> 8;

                if (r_info->r_idx_b[bs]->f_idx_size[b] >= (1U << 24))
                    i2b_hsh.byte[2] = (k_idx_x2 & 0xFF0000) >> 16;

                if (!is_keys)
                    memset(record_keeper + c, 0, COLL_KEEPER_SIZE);
            }
        }

        if (result_chk == INS_COLL_OK) {
            int32_t cpos = -1;

            for (int c2 = 0; c2 < r_info->r_idx_b[bs]->coll_count_curr_x2[b] * ((uint32_t)COLL_KEEPER_SIZE); c2 += ((uint32_t)COLL_KEEPER_SIZE)) {
                if (memcmp(record_keeper + c, r_info->r_idx_b[bs]->coll_keeper_x2[b] + c2, ((uint32_t)DIGEST_SIZE_FULL)) == 0) {
                    cpos = c2;
                    break;
                }
            }

            if (cpos == -1) {
                if (r_info->r_idx_b[bs]->coll_count_curr_x2[b] == r_info->r_idx_b[bs]->coll_count_tot_x2[b])
                    if (!resize_coll_keeper_x2(r_info, b, bs, cid)) {
                        r_info->stop = true;
                        r_info->bad_data = true;
                        return;
                    }
                cpos = r_info->r_idx_b[bs]->coll_count_curr_x2[b] * ((uint32_t)COLL_KEEPER_SIZE);
                memcpy(r_info->r_idx_b[bs]->coll_keeper_x2[b] + cpos, record_keeper + c, DIGEST_SIZE_FULL);
                r_info->r_idx_b[bs]->coll_count_curr_x2[b]++;
            }
            memcpy(r_info->r_idx_b[bs]->coll_keeper_x2[b] + cpos + ((uint32_t)DIGEST_SIZE_FULL), i2b_offset.byte, OFFSET_COLL_KEEPER_SIZE);
            find_k_idx_x2(r_info, 0, bs, t_handler);
            k_idx_x2 = r_info->r_idx_b[bs]->idx_r_help_x2->k_idx;
            memcpy(r_info->r_idx_b[bs]->coll_keeper_x2[b] + cpos + ((uint32_t)DIGEST_SIZE_FULL + (uint32_t)OFFSET_COLL_KEEPER_SIZE), &k_idx_x2, COLL_IDX_SIZE);
        }
    }
}
#define ELEM_STEP_7_5M 7500000ull
#define ELEM_STEP_15M 15000000ull
#define ELEM_STEP_30M 30000000ull
static inline uint32_t calc_idx_resize_num_chunk_by_elems(uint64_t elem_count) {
    if (elem_count < ELEM_STEP_7_5M)
        return 1;

    if (elem_count < ELEM_STEP_15M)
        return 2;

    if (elem_count < ELEM_STEP_30M)
        return 4;
    uint64_t step = elem_count / ELEM_STEP_30M;
    uint32_t n = 8u << (uint32_t)(step - 1);

    if (n > idx_resize_max_num_chunk)
        n = idx_resize_max_num_chunk;
    return n;
}


void calc_idx_resize_chunks(idx_resize_chunks_t *plan, uint32_t f_idx_size, uint32_t idx_resize_num_chunk) {
    plan->f_idx_real_size = 0;
    plan->num_chunks = 0;
    plan->chunk_size = 0;

    if (f_idx_size <= IDX_OVERHEAD_MAX) {
        return;
    }
    plan->f_idx_real_size = f_idx_size - IDX_OVERHEAD_MAX;

    if (plan->f_idx_real_size <= PURE_IDX_MIN_SIZE || idx_resize_num_chunk <= 1) {
        plan->num_chunks = 0;
        plan->chunk_size = plan->f_idx_real_size;
        return;
    }
    uint32_t max_chunks_geom = plan->f_idx_real_size / PURE_IDX_MIN_SIZE;

    if (max_chunks_geom == 0) {
        plan->num_chunks = 0;
        plan->chunk_size = plan->f_idx_real_size;
        return;
    }
    uint32_t n = idx_resize_num_chunk;

    if (n > idx_resize_max_num_chunk)
        n = idx_resize_max_num_chunk;

    if (n > max_chunks_geom)
        n = max_chunks_geom;

    if (n <= 1) {
        plan->num_chunks = 0;
        plan->chunk_size = plan->f_idx_real_size;
        return;
    }
    while (n > 1 && (plan->f_idx_real_size % n) != 0)
        n >>= 1;

    if (n <= 1) {
        plan->num_chunks = 0;
        plan->chunk_size = plan->f_idx_real_size;
        return;
    }
    plan->num_chunks = n;
    plan->chunk_size = plan->f_idx_real_size / n;
}


typedef struct {
    region_info_t *r_info;
    uint8_t b;
    uint8_t bs;
    uint32_t begin;
    uint32_t end;
    uint32_t f_idx_size_old;
} zero_tail_args_t;
typedef struct {
    region_info_t *r_info;
    uint8_t b;
    uint8_t bs;
    uint32_t start;
    uint32_t end;
    int need_k1;
    int need_k2;
} idx_zero_task_t;
static void *idx_zero_worker(void *arg) {
    idx_zero_task_t *t = (idx_zero_task_t *)arg;
    region_info_t *r_info = t->r_info;
    uint8_t b = t->b;
    uint8_t bs = t->bs;
    uint32_t start = t->start;
    uint32_t end = t->end;

    if (start >= end)
        return NULL;
    size_t n = (size_t)(end - start);
    idx_t *idx_base = &r_info->r_idx_b[bs]->idx_x2[b][start];
    memset(idx_base, 0, n * sizeof(idx_t));

    if (t->need_k1) {
        uint8_t *k1_base = &r_info->r_idx_b[bs]->k1_index_x2[b][start];
        memset(k1_base, 0, n);
    }

    if (t->need_k2) {
        uint8_t *k2_base = &r_info->r_idx_b[bs]->k2_index_x2[b][start];
        memset(k2_base, 0, n);
    }
    return NULL;
}

void resize_index_zero_tail(region_info_t *r_info, uint8_t b, uint8_t bs, uint32_t f_idx_size_x2, const idx_resize_chunks_t *plan, uint64_t cid) {
    if (r_info->is_freed)
        return;
    uint32_t old_f = r_info->r_idx_b[bs]->f_idx_size[b];

    if (old_f >= f_idx_size_x2)
        return;
    uint32_t start = old_f;
    uint32_t end = f_idx_size_x2;
    uint32_t len = end - start;

    if (len == 0)
        return;
    int need_k1 = (r_info->r_idx_b[bs]->f_idx_size[b] < (1U << 16));
    int need_k2 = (r_info->r_idx_b[bs]->f_idx_size[b] < (1U << 24));
    uint32_t num_chunks = 0;

    if (plan != NULL)
        num_chunks = plan->num_chunks;

    if (num_chunks <= 1 || len < PURE_IDX_MIN_SIZE) {
        idx_t *idx_base = &r_info->r_idx_b[bs]->idx_x2[b][start];
        memset(idx_base, 0, (size_t)len * sizeof(idx_t));

        if (need_k1) {
            uint8_t *k1_base = &r_info->r_idx_b[bs]->k1_index_x2[b][start];
            memset(k1_base, 0, (size_t)len);
        }

        if (need_k2) {
            uint8_t *k2_base = &r_info->r_idx_b[bs]->k2_index_x2[b][start];
            memset(k2_base, 0, (size_t)len);
        }
        return;
    }

    if (num_chunks > idx_resize_max_num_chunk)
        num_chunks = idx_resize_max_num_chunk;

    while (num_chunks > 1 && (len / num_chunks) < PURE_IDX_MIN_SIZE)
        num_chunks >>= 1;

    if (num_chunks <= 1) {
        idx_t *idx_base = &r_info->r_idx_b[bs]->idx_x2[b][start];
        memset(idx_base, 0, (size_t)len * sizeof(idx_t));

        if (need_k1) {
            uint8_t *k1_base = &r_info->r_idx_b[bs]->k1_index_x2[b][start];
            memset(k1_base, 0, (size_t)len);
        }

        if (need_k2) {
            uint8_t *k2_base = &r_info->r_idx_b[bs]->k2_index_x2[b][start];
            memset(k2_base, 0, (size_t)len);
        }
        return;
    }
    pthread_t threads[idx_resize_max_num_chunk];
    idx_zero_task_t tasks[idx_resize_max_num_chunk];
    uint32_t base = start;
    uint32_t chunk_len = len / num_chunks;
    uint32_t rest = len % num_chunks;

    for (uint32_t t = 0; t < num_chunks; ++t) {
        uint32_t cur_len = chunk_len + (t < rest ? 1u : 0u);
        uint32_t s = base;
        uint32_t e = s + cur_len;
        base = e;
        tasks[t].r_info = r_info;
        tasks[t].b = b;
        tasks[t].bs = bs;
        tasks[t].start = s;
        tasks[t].end = e;
        tasks[t].need_k1 = need_k1;
        tasks[t].need_k2 = need_k2;
        int rc = pthread_create(&threads[t], NULL, idx_zero_worker, &tasks[t]);

        if (rc != 0) {
            r_info->stop = true;
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [IDX_RESIZER=%015lu] Pthread_create idx_zero_worker failed: rc=%d, region %s stopped\n", curr_dt(dt), cid, rc, r_info->uuid);
        }
    }

    for (uint32_t t = 0; t < num_chunks; ++t)
        pthread_join(threads[t], NULL);
}


void *resize_index_zero_tail_worker(void *arg) {
    zero_tail_args_t *a = (zero_tail_args_t *)arg;
    region_info_t *r_info = a->r_info;
    uint8_t b = a->b;
    uint8_t bs = a->bs;
    uint32_t f_idx_size_old = a->f_idx_size_old;
    idx_t *idx_x2 = r_info->r_idx_b[bs]->idx_x2[b];
    uint8_t *k1_index_x2 = r_info->r_idx_b[bs]->k1_index_x2[b];
    uint8_t *k2_index_x2 = r_info->r_idx_b[bs]->k2_index_x2[b];
    int use_k1 = (f_idx_size_old < (1U << 16));
    int use_k2 = (f_idx_size_old < (1U << 24));

    for (uint32_t i = a->begin; i < a->end; ++i) {
        idx_x2[i].o_index = 0;
        idx_x2[i].c_index = 0;
        idx_x2[i].s_index = 0;
        idx_x2[i].j_index = 0;
        idx_x2[i].z_index = 0;
        idx_x2[i].k3_index = 0;

        if (use_k1 && k1_index_x2)
            k1_index_x2[i] = 0;

        if (use_k2 && k2_index_x2)
            k2_index_x2[i] = 0;
    }
    return NULL;
}

bool resize_index_chunk_finalize(region_info_t *r_info, uint8_t b, uint32_t f_idx_size_x2, uint8_t bs, uint32_t f_idx_size, uint32_t f_idx_real_size, uint32_t num_chunks,
                                 uint8_t *idx_moved_flag, table_handler_t *t_handler, uint64_t cid) {
    char dt[DT_LOG_SIZE];

    if (r_info->is_freed == true || num_chunks == 0) {
        return true;
    }
    fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Fin begin\n", curr_dt(dt), cid);

    if (num_chunks == 0) {
        return true;
    }
    uint32_t chunk_size = f_idx_real_size / num_chunks;
    idx_t *idx_x3 = NULL;
    uint8_t *k1_index_x3 = NULL;
    uint8_t *k2_index_x3 = NULL;
    uint32_t shift_keeper_len = r_info->r_idx_b[bs]->count_max_idx_jump * r_info->r_idx_b[bs]->count_max_idx_shift + 100;
    size_t shift_keeper_bytes = 0;
    uint16_t *shift_keeper = index_memory_malloc(&r_info->index_memory_bytes, &shift_keeper_bytes, shift_keeper_len * sizeof(uint16_t));

    if (shift_keeper == NULL) {
        fprintf(stderr, "%s WARN [IDX_RESIZER=%015lu] Failed malloc shift_keeper in finalize\n", curr_dt(dt), cid);
        return false;
    }
    memset(shift_keeper, 0, shift_keeper_len * sizeof(uint16_t));

    if (r_info->resize_buff[bs] == NULL)
        r_info->resize_buff[bs] = index_memory_malloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.resize_buff, r_info->r_idx_b[bs]->block_size);

    if (r_info->resize_buff[bs] == NULL) {
        fprintf(stderr, "%s WARN [IDX_RESIZER=%015lu] Failed malloc resize_buff in finalize\n", curr_dt(dt), cid);
        index_memory_free(&r_info->index_memory_bytes, &shift_keeper_bytes, shift_keeper);
        return false;
    }
    r_info->resize_buff_size[bs] = r_info->r_idx_b[bs]->block_size;

    if (log_level > 0) {
        fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Finalize phase: f_idx_size=%u, f_idx_real_size=%u, num_chunks=%u, chunk_size=%u\n", curr_dt(dt), cid, f_idx_size,
                f_idx_real_size, num_chunks, chunk_size);
    }

    for (uint32_t chunk_index = 0; chunk_index < num_chunks; ++chunk_index) {
        uint32_t pure_start = chunk_index * chunk_size;
        uint32_t pure_end = pure_start + chunk_size;

        if (pure_start >= f_idx_real_size)
            break;

        if (pure_end > f_idx_real_size)
            pure_end = f_idx_real_size;

        if (chunk_index > 0) {
            uint32_t front_start = pure_start;
            uint32_t front_end = pure_start + IDX_OVERHEAD_MAX;

            if (front_end > pure_end)
                front_end = pure_end;

            if (front_start < front_end) {
                if (log_level > 1) {
                    fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Finalize chunk %u front [%u, %u)\n", curr_dt(dt), cid, chunk_index, front_start, front_end);
                }

                for (uint32_t i = front_start; i < front_end; ++i) {
                    move_index(r_info, b, i, 0, shift_keeper, idx_moved_flag, bs, idx_x3, k1_index_x3, k2_index_x3, f_idx_size, t_handler);
                }
            }
        }
        uint32_t tail_start;
        uint32_t tail_end = pure_end;

        if (pure_end > IDX_OVERHEAD_MAX)
            tail_start = pure_end - IDX_OVERHEAD_MAX;
        else
            tail_start = pure_start;

        if (tail_start < pure_start)
            tail_start = pure_start;

        if (tail_start < tail_end) {
            if (log_level > 1) {
                fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Finalize chunk %u tail [%u, %u)\n", curr_dt(dt), cid, chunk_index, tail_start, tail_end);
            }

            for (uint32_t i = tail_start; i < tail_end; ++i) {
                move_index(r_info, b, i, 0, shift_keeper, idx_moved_flag, bs, idx_x3, k1_index_x3, k2_index_x3, f_idx_size, t_handler);
            }
        }
    }
    {
        uint32_t overhead_start = f_idx_real_size;
        uint32_t overhead_end = f_idx_size;

        if (overhead_start < overhead_end) {
            if (log_level > 0) {
                fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Finalize overhead [%u, %u)\n", curr_dt(dt), cid, overhead_start, overhead_end);
            }
            fprintf(stderr, "Finalize overhead: real=%u size=%u chunkSz=%u numChunks=%u\n", f_idx_real_size, f_idx_size, chunk_size, num_chunks);

            for (uint32_t i = f_idx_real_size; i < f_idx_size_x2; ++i) {
                if (i >= f_idx_size_x2) {
                    fprintf(stderr, "OVERHEAD OOB: i=%u f_idx_real_size=%u f_idx_size_x2=%u\n", i, f_idx_real_size, f_idx_size_x2);
                    abort();
                }
            }

            for (uint32_t i = overhead_start; i < overhead_end; ++i) {
                move_index(r_info, b, i, 0, shift_keeper, idx_moved_flag, bs, idx_x3, k1_index_x3, k2_index_x3, f_idx_size, t_handler);
            }
        }
    }

    if (log_level > 0) {
        fprintf(stderr, "%s INFO [IDX_RESIZER=%015lu] Finalize phase done\n", curr_dt(dt), cid);
    }
    index_memory_free(&r_info->index_memory_bytes, &shift_keeper_bytes, shift_keeper);
    free(idx_x3);
    free(k1_index_x3);
    free(k2_index_x3);
    return !r_info->bad_data;
}

void *pthread_resize_index_chunk(void *arg) {
    resize_index_chunk_thread_arg_t *a = (resize_index_chunk_thread_arg_t *)arg;
    bool ok = resize_index_chunk(a->r_info, a->b, a->f_idx_size_x2, a->bs, a->f_idx_size, a->f_idx_real_size, a->num_chunks, a->chunk_index, a->pure_start, a->pure_end,
                                 a->idx_moved_flag, a->t_handler);
    a->result = ok ? 0 : 1;
    return NULL;
}


void pthread_idx_resize_service(void *arguments) {
    char dt[DT_LOG_SIZE];
    struct arg_struct *args = (struct arg_struct *)arguments;
    struct region_info_t *r_info = args->r_info;
    struct table_handler_t *t_handler = args->t_handler;
    uint32_t b = args->b;
    uint8_t bs = args->bs;
    index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.resize_args, args);
    t_handler->resizing++;
    uint64_t cid = t_handler->cid;
    bool final_lock_held = false;
    idx_resize_chunks_t *plan = NULL;
    size_t plan_bytes = 0;
    uint32_t f_idx_size = r_info->r_idx_b[bs]->f_idx_size[b];
    fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Start to resize %s index, bs=%d\n", curr_dt(dt), cid, r_info->r_fname, bs);
    uint32_t coll_count_curr_before_resize = r_info->r_idx_b[bs]->coll_count_curr[b];
    uint32_t f_idx_size_x2 = 0;
    f_idx_size_x2 = resize_index_prepare(r_info, b, bs, t_handler->cid); // TODO Malloc and free all at once

    if (f_idx_size_x2 == 0) {
        fprintf(stderr, "%s WARN [IDX_RESIZER=%015lu] Cannot resize %s index\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname);
        goto resize_prepare_failed;
    }
    plan = index_memory_malloc(&r_info->index_memory_bytes, &plan_bytes, sizeof(*plan));

    if (plan == NULL) {
        fprintf(stderr, "%s WARN [IDX_RESIZER=%015lu] Cannot allocate resize plan for %s\n", curr_dt(dt), cid, r_info->r_fname);
        goto resize_prepare_failed;
    }
    memset(plan, 0, sizeof(idx_resize_chunks_t));
    uint32_t desired_chunks = calc_idx_resize_num_chunk_by_elems(f_idx_size);
    calc_idx_resize_chunks(plan, f_idx_size, desired_chunks);
    resize_index_create_copy(r_info, b, bs, &r_info->r_idx_b[bs]->idx[b], &r_info->r_idx_b[bs]->idx_x2[b], &r_info->r_idx_b[bs]->k1_index[b],
                             &r_info->r_idx_b[bs]->k1_index_x2[b], &r_info->r_idx_b[bs]->k2_index[b], &r_info->r_idx_b[bs]->k2_index_x2[b], f_idx_size, plan, cid);
    r_info->idx_resizer = RESIZE_IDX_PREPARED;
    fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Index copy %s is created[%d]\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname, b);
    resize_index_zero_tail(r_info, b, bs, f_idx_size_x2, plan, cid);
    bool ok = true;

    if (plan->num_chunks == 0 || r_info->enable_block_compression != BLOCK_COMPRESSION_NONE) {
        if (plan->num_chunks > 0 && r_info->enable_block_compression != BLOCK_COMPRESSION_NONE) {
            fprintf(stderr, "%s INFO [IDX_RESIZER=%015lu] Chunked resize disabled because block compression is enabled\n", curr_dt(dt), cid);
        }

        if (resize_index(r_info, b, f_idx_size_x2, bs, f_idx_size, t_handler) == false) {
            ok = false;
        }
    } else {
        uint32_t num = plan->num_chunks;
        uint32_t f_idx_real_size = plan->f_idx_real_size;
        pthread_t *threads = NULL;
        resize_index_chunk_thread_arg_t *cargs = NULL;
        uint32_t started_threads = 0;
        size_t threads_bytes = 0, cargs_bytes = 0, moved_bytes = 0;
        fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Using PARALLEL chunked resize: f_idx_size=%u, f_idx_real_size=%u, num_chunks=%u, chunk_size=%u\n", curr_dt(dt), cid,
                f_idx_size, f_idx_real_size, num, plan->chunk_size);
        uint8_t *idx_moved_flag = index_memory_malloc(&r_info->index_memory_bytes, &moved_bytes, f_idx_size_x2 * sizeof(uint8_t));

        if (!idx_moved_flag) {
            fprintf(stderr, "%s WARN [IDX_RESIZER=%015lu] Failed malloc idx_moved_flag, fallback to single-thread resize\n", curr_dt(dt), cid);

            if (resize_index(r_info, b, f_idx_size_x2, bs, f_idx_size, t_handler) == false)
                ok = false;
            goto resize_done;
        }

        for (uint32_t i = 0; i < f_idx_size_x2; ++i) {
            idx_moved_flag[i] = (r_info->r_idx_b[bs]->idx_x2[b][i].o_index > 0) ? 1 : 0;
        }
        r_info->resize_buff[bs] = index_memory_malloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.resize_buff, r_info->r_idx_b[bs]->block_size);

        if (r_info->resize_buff[bs] == NULL) {
            fprintf(stderr, "%s WARN [IDX_RESIZER=%015lu] Failed malloc shared resize_buff, fallback to single-thread resize\n", curr_dt(dt), cid);
            index_memory_free(&r_info->index_memory_bytes, &moved_bytes, idx_moved_flag);
            idx_moved_flag = NULL;

            if (resize_index(r_info, b, f_idx_size_x2, bs, f_idx_size, t_handler) == false)
                ok = false;
            goto resize_done;
        }
        r_info->resize_buff_size[bs] = r_info->r_idx_b[bs]->block_size;
        threads = index_memory_malloc(&r_info->index_memory_bytes, &threads_bytes, num * sizeof(pthread_t));
        cargs = index_memory_malloc(&r_info->index_memory_bytes, &cargs_bytes, num * sizeof(resize_index_chunk_thread_arg_t));

        if (!threads || !cargs) {
            fprintf(stderr, "%s WARN [IDX_RESIZER=%015lu] Failed to alloc thread structures, fallback to single-thread resize\n", curr_dt(dt), cid);
            index_memory_free(&r_info->index_memory_bytes, &moved_bytes, idx_moved_flag);
            idx_moved_flag = NULL;
            index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.resize_buff, r_info->resize_buff[bs]);
            r_info->resize_buff[bs] = NULL;
            r_info->resize_buff_size[bs] = 0;

            if (resize_index(r_info, b, f_idx_size_x2, bs, f_idx_size, t_handler) == false)
                ok = false;
            goto resize_done;
        }

        for (uint32_t chunk_index = 0; chunk_index < num; ++chunk_index) {
            uint32_t pure_start = chunk_index * plan->chunk_size;
            uint32_t pure_end = pure_start + plan->chunk_size;

            if (pure_start >= f_idx_real_size)
                pure_start = f_idx_real_size;

            if (pure_end > f_idx_real_size)
                pure_end = f_idx_real_size;
            cargs[chunk_index].r_info = r_info;
            cargs[chunk_index].b = b;
            cargs[chunk_index].bs = bs;
            cargs[chunk_index].f_idx_size_x2 = f_idx_size_x2;
            cargs[chunk_index].f_idx_size = f_idx_size;
            cargs[chunk_index].f_idx_real_size = f_idx_real_size;
            cargs[chunk_index].num_chunks = num;
            cargs[chunk_index].chunk_index = chunk_index;
            cargs[chunk_index].pure_start = pure_start;
            cargs[chunk_index].pure_end = pure_end;
            cargs[chunk_index].idx_moved_flag = idx_moved_flag;
            cargs[chunk_index].t_handler = t_handler;
            cargs[chunk_index].result = 0;
            int rc = pthread_create(&threads[chunk_index], NULL, pthread_resize_index_chunk, &cargs[chunk_index]);

            if (rc != 0) {
                fprintf(stderr, "%s WARN [IDX_RESIZER=%015lu] Failed to create chunk thread %u, rc=%d\n", curr_dt(dt), cid, chunk_index, rc);
                cargs[chunk_index].result = 1;
                ok = false;
                break;
            }
            started_threads++;
        }

        for (uint32_t chunk_index = 0; chunk_index < started_threads; ++chunk_index) {
            (void)pthread_join(threads[chunk_index], NULL);

            if (cargs[chunk_index].result != 0)
                ok = false;
        }

        if (ok) {
            if (!resize_index_chunk_finalize(r_info, b, f_idx_size_x2, bs, f_idx_size, f_idx_real_size, num, idx_moved_flag, t_handler, cid)) {
                ok = false;
            }
        }
    resize_done:
        index_memory_free(&r_info->index_memory_bytes, &threads_bytes, threads);
        index_memory_free(&r_info->index_memory_bytes, &cargs_bytes, cargs);
        index_memory_free(&r_info->index_memory_bytes, &moved_bytes, idx_moved_flag);

        if (r_info->resize_buff[bs]) {
            index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.resize_buff, r_info->resize_buff[bs]);
            r_info->resize_buff[bs] = NULL;
            r_info->resize_buff_size[bs] = 0;
        }
    }
    index_memory_free(&r_info->index_memory_bytes, &plan_bytes, plan);

    if (!ok || r_info->bad_data)
        goto resize_failed;
    r_info->idx_resizer = RESIZE_IDX_RESIZED;
    pthread_mutex_lock(&r_info->rlock);
    r_info->r_lock_id = IDX_RESIZER_LOCK;
    r_info->curr_cid_lock = t_handler->cid;
    final_lock_held = true;

    if (r_info->is_freed) {
        fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Region has freed, finish\n", curr_dt(dt), cid);
        t_handler->resizing--;
        free_t_handler(t_handler);
        r_info->idx_resizer = RESIZE_IDX_DONE;
        r_info->r_lock_id = 0;
        r_info->curr_cid_lock = 0;
        pthread_mutex_unlock(&r_info->rlock);
        return;
    }
    fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Start to distribute %s key_keeper[%d] = %d\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname, b,
            r_info->r_idx_b[bs]->key_count_curr[b]);
    records_distributer(r_info, b, r_info->r_idx_b[bs]->key_keeper[b], 0, r_info->r_idx_b[bs]->key_count_curr[b], true, bs, t_handler);

    if (r_info->bad_data)
        goto resize_failed;
    r_info->r_idx_b[bs]->key_count_curr[b] = 0;
    fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Distribute %s key_keeper[%d] has done\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname, b);
    fprintf(stderr, "%s INFO [IDX_RESIZER=%015lu] Start to process %d collision_x2 %s[%d]\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->coll_count_curr_x2[b],
            r_info->r_idx_b[bs]->fname, b);
    records_distributer(r_info, b, r_info->r_idx_b[bs]->coll_keeper_x2[b], 0, r_info->r_idx_b[bs]->coll_count_curr_x2[b], false, bs, t_handler);

    if (r_info->bad_data)
        goto resize_failed;
    fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Distribute %s coll_keeper_x2[%d] has done\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname, b);
    r_info->r_idx_b[bs]->f_idx_size[b] = f_idx_size_x2;
    // write_idx_meta(r_info, b, bs, t_handler); TODO dont use while assign
    fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Start to distribute %s coll_keeper[%d] = %d\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname, b,
            r_info->r_idx_b[bs]->coll_count_curr[b]);
    records_distributer(r_info, b, r_info->r_idx_b[bs]->coll_keeper[b], coll_count_curr_before_resize, r_info->r_idx_b[bs]->coll_count_curr[b], false, bs, t_handler);

    if (r_info->bad_data)
        goto resize_failed;
    fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Distribute %s coll_keeper[%d] has done\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname, b);
    f_idx_size = (r_info->r_idx_b[bs]->f_idx_size[b] - IDX_OVERHEAD_MAX) / 2 + IDX_OVERHEAD_MAX;

    for (int o = 0; o < f_idx_size; o++)
        r_info->r_idx_b[bs]->idx_x2[b][o].z_index = r_info->r_idx_b[bs]->idx[b][o].z_index;
    index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.idx[b], r_info->r_idx_b[bs]->idx[b]);
    index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k1_index[b], r_info->r_idx_b[bs]->k1_index[b]);
    index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k2_index[b], r_info->r_idx_b[bs]->k2_index[b]);
    r_info->r_idx_b[bs]->idx[b] = r_info->r_idx_b[bs]->idx_x2[b];
    index_memory_transfer(&r_info->r_idx_b[bs]->memory.idx[b], &r_info->r_idx_b[bs]->memory.idx_x2[b]);
    r_info->r_idx_b[bs]->k1_index[b] = r_info->r_idx_b[bs]->k1_index_x2[b];
    index_memory_transfer(&r_info->r_idx_b[bs]->memory.k1_index[b], &r_info->r_idx_b[bs]->memory.k1_index_x2[b]);
    r_info->r_idx_b[bs]->k2_index[b] = r_info->r_idx_b[bs]->k2_index_x2[b];
    index_memory_transfer(&r_info->r_idx_b[bs]->memory.k2_index[b], &r_info->r_idx_b[bs]->memory.k2_index_x2[b]);
    index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.coll_keeper[b], r_info->r_idx_b[bs]->coll_keeper[b]);
    r_info->r_idx_b[bs]->coll_keeper[b] = r_info->r_idx_b[bs]->coll_keeper_x2[b];
    index_memory_transfer(&r_info->r_idx_b[bs]->memory.coll_keeper[b], &r_info->r_idx_b[bs]->memory.coll_keeper_x2[b]);
    r_info->r_idx_b[bs]->coll_count_curr[b] = r_info->r_idx_b[bs]->coll_count_curr_x2[b];
    r_info->r_idx_b[bs]->coll_count_tot[b] = r_info->r_idx_b[bs]->coll_count_tot_x2[b];
    fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Resize %s[%d] index has done (coll %d)\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname, b,
            r_info->r_idx_b[bs]->coll_count_curr[b]);
    r_info->r_idx_b[bs]->idx_mask[b] = f_idx_size_x2 - IDX_OVERHEAD_MAX - 1;
    t_handler->resizing--;
    free_t_handler(t_handler);
    r_info->idx_resizer = RESIZE_IDX_DONE;
    r_info->r_lock_id = 0;
    r_info->curr_cid_lock = 0;
    pthread_mutex_unlock(&r_info->rlock);
    return;
resize_failed:
    if (!final_lock_held) {
        pthread_mutex_lock(&r_info->rlock);
        r_info->r_lock_id = IDX_RESIZER_LOCK;
        r_info->curr_cid_lock = cid;
    }
    stop_region_after_key_error(r_info, INS_IDX_CHECK_ERROR, t_handler);
    r_info->r_idx_b[bs]->f_idx_size[b] = f_idx_size;
    resize_index_free_prepared(r_info, b, bs);
    r_info->idx_resizer = RESIZE_IDX_DONE;
    t_handler->resizing--;
    do_r_unlock(r_info, 128, true, t_handler);
    free_t_handler(t_handler);
    return;
resize_prepare_failed:
    index_memory_free(&r_info->index_memory_bytes, &plan_bytes, plan);
    resize_index_free_prepared(r_info, b, bs);
    t_handler->resizing--;
    free_t_handler(t_handler);
    r_info->idx_resizer = RESIZE_IDX_DONE;
}


void move_index(region_info_t *r_info, uint8_t b, uint32_t i, int16_t jump, uint16_t *shift_keeper, uint8_t *idx_moved_flag, uint8_t bs, idx_t *idx_x3, uint8_t *k1_index_x3,
                uint8_t *k2_index_x3, uint32_t f_idx_size, table_handler_t *t_handler) {
    union i2b_u i2b_full_hash;

    if (r_info->bad_data || r_info->is_freed == true || (idx_moved_flag[i] == 0 && jump == 0)) {
        return;
    }

    if (r_info->r_idx_b[bs]->idx_x2[b][i].o_index == 0)
        return;
    i2b_full_hash.integer_val = 0;

    if (f_idx_size < (1U << 16))
        i2b_full_hash.byte[1] = r_info->r_idx_b[bs]->k1_index_x2[b][i];

    if (f_idx_size < (1U << 24))
        i2b_full_hash.byte[2] = r_info->r_idx_b[bs]->k2_index_x2[b][i];
    i2b_full_hash.byte[3] = r_info->r_idx_b[bs]->idx_x2[b][i].k3_index;

    if (jump == 0) {
        i2b_full_hash.byte[0] = i & 0xFF;

        if (f_idx_size >= (1U << 16))
            i2b_full_hash.byte[1] = (i & 0xFF00) >> 8;

        if (f_idx_size >= (1U << 24))
            i2b_full_hash.byte[2] = (i & 0xFF0000) >> 16;
    } else {
        uint32_t shift_final = 0;
        uint16_t count = 0;

        for (int j = r_info->r_idx_b[bs]->idx_x2[b][i].j_index; j > 0; j--)
            if (jump >= count)
                shift_final += shift_keeper[jump - count++];
            else {
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s INFO [IDX_RESIZER=%015lu] Pos=%u idx_x3.c=%u, idx_x3.j=%u, idx_x3.o=%u, idx_x3.s=%u, idx_x3.z=%u, k3_x3=%u\n", curr_dt(dt), t_handler->cid,
                        i, r_info->r_idx_b[bs]->idx_x2[b][i].c_index, r_info->r_idx_b[bs]->idx_x2[b][i].j_index, r_info->r_idx_b[bs]->idx_x2[b][i].o_index,
                        r_info->r_idx_b[bs]->idx_x2[b][i].s_index, r_info->r_idx_b[bs]->idx_x2[b][i].z_index, r_info->r_idx_b[bs]->idx_x2[b][i].k3_index);
                fprintf(stderr, "%s ERROR [IDX_RESIZER=%015lu] Error code 132. Index pos=%d resize error. Found jump=%d < count=%d for file %s[%d]\n", curr_dt(dt),
                        t_handler->cid, i, jump, count, r_info->r_idx_b[bs]->fname, b);
                r_info->stop = true;
                r_info->bad_data = true;
                finish_thread(r_info, 63, false, t_handler);
            }
        i2b_full_hash.byte[0] = (i - shift_final) & 0xFF;

        if (f_idx_size >= (1U << 16))
            i2b_full_hash.byte[1] = ((i - shift_final) & 0xFF00) >> 8;

        if (f_idx_size >= (1U << 24))
            i2b_full_hash.byte[2] = ((i - shift_final) & 0xFF0000) >> 16;
        r_info->r_idx_b[bs]->idx_x2[b][i].j_index = 0;
    }
    uint32_t k_idx_x2 = i2b_full_hash.integer_val & ((r_info->r_idx_b[bs]->idx_mask[b] + 1) * 2 - 1);
    uint32_t target_c_idx = r_info->r_idx_b[bs]->idx_x2[b][i].c_index;

    if (!find_k_idx_x2_local(r_info, b, &k_idx_x2, target_c_idx, i, bs, t_handler)) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [IDX_RESIZER=%015lu] Error code 107. Cannot find the new place for k_idx=%u after index resize %s[%d]\n", curr_dt(dt), t_handler->cid,
                k_idx_x2, r_info->r_idx_b[bs]->fname, b);
        r_info->stop = true;
        r_info->bad_data = true;
        finish_thread(r_info, 64, false, t_handler);
    }

    if (i != k_idx_x2) {
        if (r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].o_index != 0) {
            pthread_mutex_lock(&r_info->idx_resize_coll_lock);

            if (log_level > 0) {
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Resize %s, k_idx_x2=%d, o_index=%u\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, k_idx_x2,
                        r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].o_index);
            }

            if (r_info->r_idx_b[bs]->coll_count_curr_x2[b] == r_info->r_idx_b[bs]->coll_count_tot_x2[b])
                if (!resize_coll_keeper_x2(r_info, b, bs, t_handler->cid)) {
                    r_info->stop = true;
                    r_info->bad_data = true;
                    pthread_mutex_unlock(&r_info->idx_resize_coll_lock);
                    return;
                }
            uint64_t rw_off = (uint64_t)r_info->r_idx_b[bs]->idx_x2[b][i].o_index * r_info->r_idx_b[bs]->block_size;
            uint16_t bsize_factor = r_info->r_idx_b[bs]->bsize_factor;
            uint32_t r_filled_zip_size = calc_zip_record_size(r_info, r_info->r_idx_b[bs]->block_size - bsize_factor, bs);
            bool read_failed = false;
            get_r_lock(r_info, RESIZE_MOVE_IDX_LOCK, true, false, t_handler);

            if (r_info->is_freed) {
                read_failed = true;
            } else if (r_info->enable_block_compression == BLOCK_COMPRESSION_NONE) {
                memset(r_info->resize_buff[bs], 0, r_info->r_idx_b[bs]->block_size);
                int read_result = read_file(r_info->r_idx_b[bs]->fd[b], r_info->resize_buff[bs], rw_off, r_filled_zip_size * bsize_factor, r_info, t_handler->cid);

                if (read_result <= 0) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s WARN [IDX_BUILDER=%015lu] Failed read_result=%d for %s\n", curr_dt(dt), t_handler->cid, read_result, r_info->uuid);
                    r_info->stop = true;
                    r_info->bad_data = true;
                    read_failed = true;
                }
            } else {
                int read_result = read_file(r_info->r_idx_b[bs]->fd[b], r_info->resize_buff[bs], rw_off, BLOCK_MAX_SIZE, r_info, t_handler->cid);
                uint32_t compressed_size = 0;
                uint64_t decompressed_size = 0;

                if (read_result < (int)sizeof(compressed_size)) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s WARN [IDX_BUILDER=%015lu] Failed read_result=%d for compressed block %s\n", curr_dt(dt), t_handler->cid, read_result, r_info->uuid);
                    r_info->stop = true;
                    r_info->bad_data = true;
                    read_failed = true;
                } else {
                    memcpy(&compressed_size, r_info->resize_buff[bs], sizeof(compressed_size));
                    uint32_t aligned_compressed_size = compressed_size <= BLOCK_MAX_SIZE ? (uint32_t)align_up(compressed_size, MAX_BSIZE_FACTOR) : 0;

                    if (compressed_size < sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t) || compressed_size > BLOCK_MAX_SIZE ||
                        aligned_compressed_size > (uint32_t)read_result) {
                        char dt[DT_LOG_SIZE];
                        fprintf(stderr, "%s WARN [IDX_BUILDER=%015lu] Wrong compressed block size=%u, aligned_size=%u, read_result=%d for %s\n", curr_dt(dt), t_handler->cid,
                                compressed_size, aligned_compressed_size, read_result, r_info->uuid);
                        r_info->stop = true;
                        r_info->bad_data = true;
                        read_failed = true;
                    } else {
                        uint64_t decompress_result = decompress_block(r_info->resize_buff[bs], (size_t)read_result, r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].o_index, bs,
                                                                      r_info, &decompressed_size, 0, t_handler);

                        if (decompress_result != 0) {
                            char dt[DT_LOG_SIZE];
                            fprintf(stderr, "%s WARN [IDX_BUILDER=%015lu] Failed call index, init decompress_snappy result=%lu\n", curr_dt(dt), t_handler->cid,
                                    decompress_result);
                            r_info->stop = true;
                            r_info->bad_data = true;
                            read_failed = true;
                        } else if (decompressed_size > UINT32_MAX - (FIX_BSIZE_FACTOR - 1)) {
                            char dt[DT_LOG_SIZE];
                            fprintf(stderr, "%s WARN [IDX_BUILDER=%015lu] Wrong decompressed_size=%lu for %s\n", curr_dt(dt), t_handler->cid, decompressed_size, r_info->uuid);
                            r_info->stop = true;
                            r_info->bad_data = true;
                            read_failed = true;
                        } else {
                            uint32_t aligned_decompressed_size = (uint32_t)align_up(decompressed_size, FIX_BSIZE_FACTOR);

                            if (aligned_decompressed_size > r_info->resize_buff_size[bs]) {
                                uint8_t *init_buff_tmp = index_memory_realloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.resize_buff, r_info->resize_buff[bs],
                                                                              aligned_decompressed_size);

                                if (init_buff_tmp == NULL) {
                                    char dt[DT_LOG_SIZE];
                                    fprintf(stderr, "%s WARN [IDX_BUILDER=%015lu] Failed realloc init_buff to %u\n", curr_dt(dt), t_handler->cid, aligned_decompressed_size);
                                    r_info->stop = true;
                                    r_info->bad_data = true;
                                    read_failed = true;
                                } else {
                                    r_info->resize_buff[bs] = init_buff_tmp;
                                    r_info->resize_buff_size[bs] = aligned_decompressed_size;
                                }
                            }

                            if (!read_failed) {
                                memcpy(r_info->resize_buff[bs], r_info->bbuff, aligned_decompressed_size);
                                bsize_factor = FIX_BSIZE_FACTOR;
                                r_filled_zip_size = (uint32_t)((decompressed_size + bsize_factor - 1) / bsize_factor);
                            }
                        }
                    }
                }
            }
            do_r_unlock(r_info, 128, true, t_handler);

            if (read_failed) {
                pthread_mutex_unlock(&r_info->idx_resize_coll_lock);
                return;
            }
            uint8_t digest[DIGEST_SIZE_FULL];
            uint32_t r_real_offset = 0;
            uint32_t cpos = -1;
            struct msg_qv_t qv = {0};
            msg_kv_t kv = {.qv = &qv};
            msg_cli_t message = {.kv = &kv};
            msg_cli_t *msg_get = &message;

            do {
                if (msg_get->kv->key_size > 0) {
                    free(msg_get->kv->key);
                    msg_get->kv->key = NULL;
                    msg_get->kv->key_size = 0;
                }

                if (msg_get->kv->cf_size > 0) {
                    free(msg_get->kv->cf);
                    msg_get->kv->cf = NULL;
                    msg_get->kv->cf_size = 0;
                }
                int extract_result = get_key_from_msg_bytes_checked(msg_get, r_info->resize_buff[bs] + r_real_offset, t_handler->cid);

                if (extract_result < 0) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s WARN [IDX_RESIZER=%015lu] Cannot extract resize key, code=%d\n", curr_dt(dt), t_handler->cid, extract_result);
                    clear_extracted_key_fields(msg_get);
                    r_info->stop = true;
                    r_info->bad_data = true;
                    pthread_mutex_unlock(&r_info->idx_resize_coll_lock);
                    return;
                }

                if (msg_get->msg_size == 0) {
                    for (int c = 0; c < r_info->r_idx_b[bs]->coll_count_curr[b] * ((uint32_t)COLL_KEEPER_SIZE); c += ((uint32_t)COLL_KEEPER_SIZE)) {
                        if (i2b_full_hash.byte[0] == r_info->r_idx_b[bs]->coll_keeper[b][c] && i2b_full_hash.byte[1] == r_info->r_idx_b[bs]->coll_keeper[b][c + 1] &&
                            i2b_full_hash.byte[2] == r_info->r_idx_b[bs]->coll_keeper[b][c + 2] && i2b_full_hash.byte[3] == r_info->r_idx_b[bs]->coll_keeper[b][c + 3]) {
                            cpos = c;
                            union i2b_u i2b;
                            i2b.integer_val = r_info->r_idx_b[bs]->idx_x2[b][i].o_index;
                            memcpy(r_info->r_idx_b[bs]->coll_keeper_x2[b] + cpos + ((uint32_t)DIGEST_SIZE_FULL), i2b.byte, ((uint32_t)OFFSET_COLL_KEEPER_SIZE));
                            break;
                        }
                    }
                }

                if (cpos != -1)
                    break;

                if (msg_get->msg_size == 0) {
                    // TODO Попробуй убрать
                    break;
                }
                calc_hash(msg_get->kv->key, msg_get->kv->key_size, digest);
                calc_hash_full(msg_get->kv->key, msg_get->kv->key_size, digest, DIGEST_SIZE_SMALL);
#ifdef COLL_LOB_TEST
                uint32_t c_idx = digest[C_IDX_POS];
#else
                uint32_t c_idx = digest[C_IDX_POS] + (digest[C_IDX_POS + 1] << 8) + ((digest[C_IDX_POS + 2] & 0x7Fu) << 16);
#endif
                if (c_idx == target_c_idx) {
                    // TODO !MAJOR! Тут проблема, возможно есть очень маленькая вероятность, что в одном блоке окажется две записи у которых совпадают все 4 байта хеша
                    if (memcmp(digest, i2b_full_hash.byte, 4) == 0) {
                        for (int c = 0; c < r_info->r_idx_b[bs]->coll_count_curr_x2[b] * ((uint32_t)COLL_KEEPER_SIZE); c += ((uint32_t)COLL_KEEPER_SIZE)) {
                            if (digest[0] == r_info->r_idx_b[bs]->coll_keeper_x2[b][c] && digest[1] == r_info->r_idx_b[bs]->coll_keeper_x2[b][c + 1]) {
                                if (memcmp(digest, r_info->r_idx_b[bs]->coll_keeper_x2[b] + c, ((uint32_t)DIGEST_SIZE_FULL)) == 0) {
                                    if (log_level > 0) {
                                        char dt[DT_LOG_SIZE];
                                        fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Resize %s, cpos=%d\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, cpos);
                                    }
                                    cpos = c;
                                    union i2b_u i2b;
                                    i2b.integer_val = r_info->r_idx_b[bs]->idx_x2[b][i].o_index;
                                    memcpy(r_info->r_idx_b[bs]->coll_keeper_x2[b] + cpos + ((uint32_t)DIGEST_SIZE_FULL), i2b.byte, ((uint32_t)OFFSET_COLL_KEEPER_SIZE));
                                    break;
                                }
                            }
                        }

                        if (cpos == -1) {
                            cpos = r_info->r_idx_b[bs]->coll_count_curr_x2[b] * ((uint32_t)COLL_KEEPER_SIZE);
                            memcpy(r_info->r_idx_b[bs]->coll_keeper_x2[b] + cpos, digest, ((uint32_t)DIGEST_SIZE_FULL));
                            r_info->r_idx_b[bs]->coll_count_curr_x2[b]++;
                        }
                        union i2b_u i2b;
                        i2b.integer_val = r_info->r_idx_b[bs]->idx_x2[b][i].o_index;
                        memcpy(r_info->r_idx_b[bs]->coll_keeper_x2[b] + cpos + ((uint32_t)DIGEST_SIZE_FULL), i2b.byte, ((uint32_t)OFFSET_COLL_KEEPER_SIZE));

                        if (log_level > 0) {
                            char dt[DT_LOG_SIZE];
                            fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Resize %s, cpos=%d, o_index=%d\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, cpos,
                                    i2b.integer_val);
                        }
                        break;
                    }
                }

                if (r_info->enable_block_compression)
                    r_real_offset += calc_zip_record_size_fix(msg_get->msg_size) * bsize_factor;
                else
                    r_real_offset += calc_zip_record_size(r_info, msg_get->msg_size, bs) * bsize_factor;
            } while (r_real_offset != r_filled_zip_size * bsize_factor);

            if (cpos == -1) {
                r_info->stop = true;
                char dt[DT_LOG_SIZE];
                fprintf(stderr,
                        "%s ERROR [IDX_RESIZER=%015lu] Collision not resolved: i=%u, k_idx_x2=%u, o_index=%u, full_hash=%02x%02x%02x%02x, "
                        "c_idx=%u, jump=%d, region %s has stopped\n",
                        curr_dt(dt), t_handler->cid, i, k_idx_x2, r_info->r_idx_b[bs]->idx_x2[b][i].o_index, i2b_full_hash.byte[0], i2b_full_hash.byte[1],
                        i2b_full_hash.byte[2], i2b_full_hash.byte[3], target_c_idx, jump, r_info->uuid);
                r_info->stop = true;
            }
            clear_extracted_key_fields(msg_get);
            pthread_mutex_unlock(&r_info->idx_resize_coll_lock);
        } else {
            r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].o_index = r_info->r_idx_b[bs]->idx_x2[b][i].o_index;
            r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].c_index = r_info->r_idx_b[bs]->idx_x2[b][i].c_index;

            if (f_idx_size < (1U << 16))
                r_info->r_idx_b[bs]->k1_index_x2[b][k_idx_x2] = i2b_full_hash.byte[K1_POS];

            if (f_idx_size < (1U << 24))
                r_info->r_idx_b[bs]->k2_index_x2[b][k_idx_x2] = i2b_full_hash.byte[K2_POS];
            r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].k3_index = i2b_full_hash.byte[K3_POS];
        }
        r_info->r_idx_b[bs]->idx_x2[b][i].o_index = 0;
        r_info->r_idx_b[bs]->idx_x2[b][i].c_index = 0;

        if (f_idx_size < (1U << 16))
            r_info->r_idx_b[bs]->k1_index_x2[b][i] = 0;

        if (f_idx_size < (1U << 24))
            r_info->r_idx_b[bs]->k2_index_x2[b][i] = 0;
        r_info->r_idx_b[bs]->idx_x2[b][i].k3_index = 0;
    }
    idx_moved_flag[i] = 0;
    uint16_t shift = r_info->r_idx_b[bs]->idx_x2[b][i].s_index;

    if (shift > 0) {
        r_info->r_idx_b[bs]->idx_x2[b][i].s_index = 0;
        jump++;
        shift_keeper[jump] = shift;
        move_index(r_info, b, i + shift, jump, shift_keeper, idx_moved_flag, bs, idx_x3, k1_index_x3, k2_index_x3, f_idx_size, t_handler);
    }

    if (jump >= IDX_OVERHEAD_MAX * 4) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [IDX_RESIZER=%015lu] Error code 109. Index resizer jump=%d\n", curr_dt(dt), t_handler->cid, jump);
        r_info->stop = true;
        r_info->bad_data = true;
        finish_thread(r_info, 65, false, t_handler);
    }

    if (jump >= 0)
        shift_keeper[jump] = 0;
}


void *pthread_idx_resize_coordinator(void *arg) {
    (void)arg;
    const uint64_t trigger_percent = 90;
    const uint64_t candidate_percent = 87;
    const uint8_t table_lock_id = 30;
    const uint8_t region_lock_id = IDX_RESIZER_LOCK;
    const struct timespec poll_interval = {.tv_sec = 1, .tv_nsec = 0};
    uint64_t cid = get_new_cid();
    table_handler_t *t_handler;

    while ((t_handler = create_t_handler(0)) == NULL)
        sleep(1);
    t_handler->cid = cid;

    while (atomic_load(&is_shutting_down) == 0) {
        nanosleep(&poll_interval, NULL);

        if (atomic_load(&is_shutting_down) != 0)
            break;
        pthread_mutex_lock(&tables_lock);

        for (int t = 0; t < table_count_curr; t++) {
            table_info_t *t_info = table_info[t];

            if (t_info == NULL || pthread_mutex_trylock(&t_info->tlock) != 0)
                continue;

            if (t_info->t_lock_id != 0) {
                pthread_mutex_unlock(&t_info->tlock);
                continue;
            }
            t_info->t_lock_id = table_lock_id;
            pthread_mutex_unlock(&tables_lock);
            int region_count = t_info->region_count_curr;
            uint8_t *candidate_bs = malloc((size_t)region_count);
            region_info_t *trigger_region = NULL;
            uint8_t trigger_bs = 0;

            if (candidate_bs == NULL && region_count != 0) {
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s WARN [IDX_RESIZE_COORDINATOR=%015lu] Cannot allocate candidates for table %s\n", curr_dt(dt), cid, t_info->fullname);
            } else {
                if (region_count != 0)
                    memset(candidate_bs, UINT8_MAX, (size_t)region_count);

                for (int r = 0; r < region_count; r++) {
                    region_info_t *r_info = t_info->region_info[r];

                    if (r_info == NULL || pthread_mutex_trylock(&r_info->rlock) != 0)
                        continue;
                    pthread_mutex_lock(&r_info->control_mutex);

                    if (r_info->is_freed || r_info->r_lock_id != 0 || r_info->waiting_count_lock != 0) {
                        pthread_mutex_unlock(&r_info->control_mutex);
                        pthread_mutex_unlock(&r_info->rlock);
                        continue;
                    }
                    r_info->r_lock_id = region_lock_id;
                    r_info->curr_cid_lock = cid;
                    pthread_mutex_unlock(&r_info->control_mutex);
                    uint64_t best_curr = 0;
                    uint64_t best_size = 0;
                    uint8_t best_bs = UINT8_MAX;

                    if (r_info->is_leader && !r_info->stop && !r_info->bad_data && (r_info->status == STATUS_ASSIGN_REGION || r_info->status == STATUS_START_REGION) &&
                        r_info->r_idx_b != NULL) {
                        for (uint8_t bs = 0; bs < r_info->max_idx_b; bs++) {
                            idx_region_info_t *idx_info = r_info->r_idx_b[bs];

                            if (idx_info == NULL || idx_info->idx[FBP] == NULL || idx_info->f_idx_size[FBP] == 0)
                                continue;
                            uint64_t curr = idx_info->f_idx_curr_size[FBP];
                            uint64_t size = idx_info->f_idx_size[FBP];

                            if (best_bs == UINT8_MAX || curr * best_size > best_curr * size) {
                                best_curr = curr;
                                best_size = size;
                                best_bs = bs;
                            }
                        }
                    }

                    if (best_bs != UINT8_MAX && best_curr * 100 >= best_size * candidate_percent) {
                        candidate_bs[r] = best_bs;

                        if (best_curr * 100 >= best_size * trigger_percent && trigger_region == NULL) {
                            trigger_region = r_info;
                            trigger_bs = best_bs;
                        }
                    }
                    do_r_unlock(r_info, 130, false, t_handler);
                }

                if (trigger_region != NULL) {
                    bool candidates_complete = true;

                    for (int pass = 0; pass < 2; pass++) {
                        for (int r = 0; r < region_count; r++) {
                            region_info_t *r_info = t_info->region_info[r];
                            bool is_trigger = r_info == trigger_region && candidate_bs[r] == trigger_bs;

                            if (candidate_bs[r] == UINT8_MAX || (pass == 0 && is_trigger) || (pass == 1 && !is_trigger))
                                continue;

                            if (pass == 1 && !candidates_complete)
                                break;

                            if (pthread_mutex_trylock(&r_info->rlock) != 0) {
                                candidates_complete = false;
                                continue;
                            }
                            pthread_mutex_lock(&r_info->control_mutex);

                            if (r_info->is_freed || r_info->r_lock_id != 0 || r_info->waiting_count_lock != 0) {
                                pthread_mutex_unlock(&r_info->control_mutex);
                                pthread_mutex_unlock(&r_info->rlock);
                                candidates_complete = false;
                                continue;
                            }
                            r_info->r_lock_id = region_lock_id;
                            r_info->curr_cid_lock = cid;
                            pthread_mutex_unlock(&r_info->control_mutex);
                            uint8_t bs = candidate_bs[r];
                            idx_region_info_t *idx_info = NULL;

                            if (r_info->is_leader && !r_info->is_freed && !r_info->stop && !r_info->bad_data &&
                                (r_info->status == STATUS_ASSIGN_REGION || r_info->status == STATUS_START_REGION) && r_info->r_idx_b != NULL && bs < r_info->max_idx_b)
                                idx_info = r_info->r_idx_b[bs];

                            if (idx_info != NULL && idx_info->idx[FBP] != NULL && idx_info->f_idx_size[FBP] != 0 && r_info->idx_resizer == RESIZE_IDX_DONE) {
                                uint64_t curr = idx_info->f_idx_curr_size[FBP];
                                uint64_t size = idx_info->f_idx_size[FBP];
                                uint64_t threshold = is_trigger ? trigger_percent : candidate_percent;

                                if (curr * 100 >= size * threshold) {
                                    char dt[DT_LOG_SIZE];
                                    fprintf(stderr, "%s INFO [IDX_RESIZE_COORDINATOR=%015lu] Resize table=%s region=%s index=%s, filled=%lu/%lu\n", curr_dt(dt), cid,
                                            t_info->fullname, r_info->uuid, idx_info->fname, curr, size);

                                    if (!resize_thread_runner(r_info, FBP, bs, t_handler)) {
                                        fprintf(stderr, "%s WARN [IDX_RESIZE_COORDINATOR=%015lu] Cannot start resize for %s[%d]\n", curr_dt(dt), cid, idx_info->fname, FBP);
                                        candidates_complete = false;
                                    }
                                }
                            }
                            do_r_unlock(r_info, 130, false, t_handler);
                        }
                    }
                }
            }
            free(candidate_bs);
            do_t_unlock(t_info, 32, false, cid);
            pthread_mutex_lock(&tables_lock);

            if (atomic_load(&is_shutting_down) != 0)
                break;
        }
        pthread_mutex_unlock(&tables_lock);
    }
    free_t_handler(t_handler);
    return NULL;
}

void print_idx(char *label, region_info_t *r_info, uint8_t b, uint8_t bs, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] s_index\n", curr_dt(dt), cid);
    int countI = 0;
    int countT = 0;

    for (int j = 0; j < r_info->r_idx_b[bs]->f_idx_size[b]; j++) {
        fprintf(stderr, "%s[%d] %03d:\t%03d:%03d\t%03d-%02d %03d,%03d\t\t\t", label, b, j, r_info->r_idx_b[bs]->idx[b][j].o_index, r_info->r_idx_b[bs]->idx[b][j].c_index,
                r_info->r_idx_b[bs]->idx[b][j].s_index, r_info->r_idx_b[bs]->idx[b][j].j_index, r_info->r_idx_b[bs]->k1_index[b][j], r_info->r_idx_b[bs]->k2_index[b][j]);
        countT++;

        if (r_info->r_idx_b[bs]->idx[b][j].o_index != 0)
            countI++;

        if (j == r_info->r_idx_b[bs]->f_idx_size[b] - IDX_OVERHEAD_MAX - 1)
            fprintf(stderr, "\n");
        fprintf(stderr, "\n");
    }
    fprintf(stderr, "BUCKET %d\n\n", b);
    fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] countT = %d, countI = %d\n", curr_dt(dt), cid, countT, countI);
}


void calc_skew_idx(region_info_t *r_info, uint8_t b, uint8_t bs, uint64_t cid) {
    int b1 = 0;
    int b2 = 0;
    int b3 = 0;
    int b4 = 0;

    for (int j = 0; j < r_info->r_idx_b[bs]->f_idx_size[b]; j++) {
        int x = (r_info->r_idx_b[bs]->f_idx_size[b] - IDX_OVERHEAD_MAX) / 4;

        if (r_info->r_idx_b[bs]->idx[b][j].o_index > 0) {
            if (j >= 0 && j < x)
                b1++;

            if (j >= x && j < x * 2)
                b2++;

            if (j >= x * 2 && j < x * 3)
                b3++;

            if (j >= x * 3 && j < x * 4)
                b4++;
        }
    }
    fprintf(stderr, "%s SKEW %03d\t%03d\t%03d\t%03d\n", r_info->r_idx_b[bs]->fname, b1, b2, b3, b4);
}

uint8_t check_k_idx(region_info_t *r_info, bool is_keys, uint8_t *hsh, uint8_t *volatile record_keeper, uint32_t new_b_offset, uint8_t bs, table_handler_t *t_handler) {
    uint8_t b = r_info->r_idx_b[bs]->idx_r_help_x2->b;
    uint32_t k_idx_x2 = r_info->r_idx_b[bs]->idx_r_help_x2->k_idx;
    uint32_t c_idx_x2 = r_info->r_idx_b[bs]->idx_r_help_x2->c_idx;

    if (r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].c_index != 0) {
        uint32_t j_count = 0;

        while (r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].c_index != c_idx_x2 ||
               (r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].c_index == c_idx_x2 && r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].j_index != j_count)) {
            if (r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].s_index == 0)
                return INS_IDX_OK;
            else {
                k_idx_x2 = k_idx_x2 + r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].s_index;
                j_count++;
            }
        }
    } else
        return INS_IDX_OK;
    // TODO Разобраться, является ли например несовпадение К3 достаточным признаком коллизии
    if (is_keys == true && r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].c_index == c_idx_x2) {
        if (r_info->r_idx_b[bs]->f_idx_size[b] < (1U << 24)) {
            if (r_info->r_idx_b[bs]->k2_index_x2[b][k_idx_x2] != hsh[K2_POS])
                return INS_COLL_OK;
        }

        if (r_info->r_idx_b[bs]->f_idx_size[b] < (1U << 16)) {
            if (r_info->r_idx_b[bs]->k1_index_x2[b][k_idx_x2] != hsh[K1_POS])
                return INS_COLL_OK;
        }

        if (r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].o_index != new_b_offset) {
            char dt[DT_LOG_SIZE];

            if (log_level > 0)
                fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Pre-collision state for hash %x %x %x %x offset_new=%d, offset_x2=%d (k_idx_x2=%d) in %s[%d]\n", curr_dt(dt),
                        t_handler->cid, hsh[0], hsh[1], hsh[2], hsh[3], new_b_offset, r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].o_index, k_idx_x2,
                        r_info->r_idx_b[bs]->dir_name, b);
            uint32_t buff_size = r_info->r_idx_b[bs]->block_size;
            uint16_t bsize_factor = r_info->r_idx_b[bs]->bsize_factor;
            size_t buff_bytes = 0;
            uint8_t *buff = index_memory_malloc(&r_info->index_memory_bytes, &buff_bytes, buff_size);

            if (buff == NULL) {
                stop_region_after_key_error(r_info, KEY_EXTRACT_NOMEM, t_handler);
                return INS_IDX_CHECK_ERROR;
            }
            memset(buff, 0, buff_size);
            uint64_t r_off = (uint64_t)r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].o_index * r_info->r_idx_b[bs]->block_size;
            uint32_t r_filled_zip_size = calc_zip_record_size(r_info, r_info->r_idx_b[bs]->block_size - bsize_factor, bs);

            if (r_info->enable_block_compression == BLOCK_COMPRESSION_NONE)
                read_file(r_info->r_idx_b[bs]->fd[b], buff, r_off, r_filled_zip_size * bsize_factor, r_info, t_handler->cid);
            else {
                uint32_t bytes_read = read_file(r_info->r_idx_b[bs]->fd[b], buff, r_off, BLOCK_MAX_SIZE, r_info, t_handler->cid);
                uint64_t decompressed_size = 0;

                if (r_info->enable_block_compression) {
                    if (bytes_read > 0) {
                        const size_t input_size = bytes_read;

                        if (input_size >= sizeof(bytes_read))
                            memcpy(&bytes_read, buff, sizeof(bytes_read));

                        if (bytes_read > 0) {
                            int decompress_result =
                                decompress_block(buff, input_size, r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].o_index, bs, r_info, &decompressed_size, 0, t_handler);

                            if (decompress_result == 0) {
                                if (decompressed_size > buff_size) {
                                    uint8_t *init_buff_tmp = index_memory_realloc(&r_info->index_memory_bytes, &buff_bytes, buff, decompressed_size);

                                    if (init_buff_tmp == NULL) {
                                        char dt[DT_LOG_SIZE];
                                        fprintf(stderr, "%s WARN [IDX_BUILDER=%015lu] Failed realloc init_buff to %lu\n", curr_dt(dt), t_handler->cid, decompressed_size);
                                        r_info->stop = true;
                                        r_info->bad_data = true;
                                        index_memory_free(&r_info->index_memory_bytes, &buff_bytes, buff);
                                        return INS_IDX_CHECK_ERROR;
                                    } else {
                                        buff = init_buff_tmp;
                                        buff_size = decompressed_size;
                                    }
                                }
                                memcpy(buff, r_info->bbuff, decompressed_size);
                                bsize_factor = FIX_BSIZE_FACTOR;
                                r_filled_zip_size = ((decompressed_size + bsize_factor - 1) / bsize_factor);
                            } else {
                                // TODO data_fix
                                fprintf(stderr, "%s WARN [IDX_BUILDER=%015lu] Failed call index, init decompress_snappy result=%u\n", curr_dt(dt), t_handler->cid,
                                        decompress_result);
                                r_info->stop = true;
                                r_info->bad_data = true;
                                index_memory_free(&r_info->index_memory_bytes, &buff_bytes, buff);
                                return INS_IDX_CHECK_ERROR;
                            }
                        }
                    }
                }
            }
            struct msg_qv_t qv = {0};
            msg_kv_t kv = {.qv = &qv};
            msg_cli_t message = {.kv = &kv};
            msg_cli_t *msg_get = &message;
            uint32_t r_zip_offset = 0;
            uint32_t r_real_offset = 0;
            uint32_t zip_old_val_size = 0;
            uint8_t digest[DIGEST_SIZE_FULL];

            do {
                clear_extracted_key_fields(msg_get);
                int extract_result = get_key_from_msg_bytes_checked(msg_get, buff + r_real_offset, t_handler->cid);

                if (extract_result < 0) {
                    clear_extracted_key_fields(msg_get);
                    index_memory_free(&r_info->index_memory_bytes, &buff_bytes, buff);
                    stop_region_after_key_error(r_info, extract_result, t_handler);
                    return INS_IDX_CHECK_ERROR;
                }

                if (msg_get->msg_size == 0)
                    break;

                if (r_info->enable_block_compression)
                    zip_old_val_size = calc_zip_record_size_fix(msg_get->msg_size);
                else
                    zip_old_val_size = calc_zip_record_size(r_info, msg_get->msg_size, bs);
                calc_hash(msg_get->kv->key, msg_get->kv->key_size, digest);
                calc_hash_full(msg_get->kv->key, msg_get->kv->key_size, digest, DIGEST_SIZE_SMALL);

                if (memcmp(digest, record_keeper, DIGEST_SIZE_FULL) == 0) {
                    if (log_level > 0)
                        fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Record %x %x %x %x has found at offset=%d in %s[%d], update offset to %u\n", curr_dt(dt),
                                t_handler->cid, hsh[0], hsh[1], hsh[2], hsh[3], r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].o_index, r_info->r_idx_b[bs]->dir_name, b,
                                new_b_offset);
                    r_info->r_idx_b[bs]->idx_r_help_x2->k_idx = k_idx_x2;
                    clear_extracted_key_fields(msg_get);
                    index_memory_free(&r_info->index_memory_bytes, &buff_bytes, buff);
                    return INS_IDX_UPDATE;
                }
                uint32_t digest_c_idx;
                uint32_t record_keeper_c_idx;
#ifdef COLL_LOB_TEST
                digest_c_idx = digest[C_IDX_POS];
                record_keeper_c_idx = record_keeper[C_IDX_POS];
#else
                digest_c_idx = digest[C_IDX_POS] + (digest[C_IDX_POS + 1] << 8) + ((digest[C_IDX_POS + 2] & 0x7Fu) << 16);
                record_keeper_c_idx = record_keeper[C_IDX_POS] + (record_keeper[C_IDX_POS + 1] << 8) + ((record_keeper[C_IDX_POS + 2] & 0x7Fu) << 16);
                ;
#endif
                if (digest_c_idx == record_keeper_c_idx) {
                    union i2b_u i2b_hsh;
                    i2b_hsh.byte[0] = digest[0];
                    i2b_hsh.byte[1] = digest[1];
                    i2b_hsh.byte[2] = digest[2];
                    i2b_hsh.byte[3] = digest[3];
                    uint32_t k_idx_chk = (i2b_hsh.integer_val & ((r_info->r_idx_b[bs]->idx_mask[b] + 1) * 2 - 1));
                    k_idx_chk = calc_k_idx_x2(r_info, b, k_idx_chk, digest_c_idx, bs, t_handler->cid);

                    if (k_idx_chk == k_idx_x2) {
                        if (log_level > 0)
                            fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Born the new collision %x %x %x %x because not found the record at offset=%d in %s[%d]\n",
                                    curr_dt(dt), t_handler->cid, hsh[0], hsh[1], hsh[2], hsh[3], r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].o_index,
                                    r_info->r_idx_b[bs]->dir_name, b);
                        clear_extracted_key_fields(msg_get);
                        index_memory_free(&r_info->index_memory_bytes, &buff_bytes, buff);
                        return INS_COLL_OK;
                    }
                }
                r_zip_offset += zip_old_val_size;
                r_real_offset = r_zip_offset * bsize_factor;
            } while (r_real_offset != r_filled_zip_size * bsize_factor);

            if (log_level > 0)
                fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Hash not found %x %x %x %x at offset=%d in %s[%d]\n", curr_dt(dt), t_handler->cid, hsh[0], hsh[1], hsh[2],
                        hsh[3], r_info->r_idx_b[bs]->idx_x2[b][k_idx_x2].o_index, r_info->r_idx_b[bs]->dir_name, b);
            clear_extracted_key_fields(msg_get);
            index_memory_free(&r_info->index_memory_bytes, &buff_bytes, buff);
        }
        return INS_IDX_OK;
    }
    return INS_COLL_OK;
}

void resize_index_free_prepared(region_info_t *r_info, uint8_t b, uint8_t bs) {
    if (r_info->r_idx_b[bs]->idx_x2[b]) {
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.idx_x2[b], r_info->r_idx_b[bs]->idx_x2[b]);
        r_info->r_idx_b[bs]->idx_x2[b] = NULL;
    }

    if (r_info->r_idx_b[bs]->k1_index_x2[b]) {
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k1_index_x2[b], r_info->r_idx_b[bs]->k1_index_x2[b]);
        r_info->r_idx_b[bs]->k1_index_x2[b] = NULL;
    }

    if (r_info->r_idx_b[bs]->k2_index_x2[b]) {
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k2_index_x2[b], r_info->r_idx_b[bs]->k2_index_x2[b]);
        r_info->r_idx_b[bs]->k2_index_x2[b] = NULL;
    }

    if (r_info->r_idx_b[bs]->coll_keeper_x2[b]) {
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.coll_keeper_x2[b], r_info->r_idx_b[bs]->coll_keeper_x2[b]);
        r_info->r_idx_b[bs]->coll_keeper_x2[b] = NULL;
    }
}

uint32_t resize_index_prepare(region_info_t *r_info, uint8_t b, uint8_t bs, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    r_info->r_idx_b[bs]->idx_x2[b] = NULL;
    r_info->r_idx_b[bs]->k1_index_x2[b] = NULL;
    r_info->r_idx_b[bs]->k2_index_x2[b] = NULL;
    r_info->r_idx_b[bs]->coll_keeper_x2[b] = NULL;
    uint32_t f_idx_size_x2 = (r_info->r_idx_b[bs]->f_idx_size[b] - IDX_OVERHEAD_MAX) * 2 + IDX_OVERHEAD_MAX;
    fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Start to resize index bs=%d for %s[%d] from %lu to %u (filled size %lu)\n", curr_dt(dt), cid, bs,
            r_info->r_idx_b[bs]->fname, b, r_info->r_idx_b[bs]->f_idx_size[b], f_idx_size_x2, r_info->r_idx_b[bs]->f_idx_curr_size[b]);

    for (int i = 10; i >= 0; i--) {
        r_info->r_idx_b[bs]->idx_x2[b] =
            (struct idx_t *)index_memory_malloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.idx_x2[b], f_idx_size_x2 * sizeof(struct idx_t));

        if (r_info->r_idx_b[bs]->idx_x2[b] != NULL)
            break;

        if (r_info->r_idx_b[bs]->idx_x2[b] == NULL && i == 0) {
            fprintf(stderr, "%s ERROR [IDX_RESIZER=%015lu] Malloc r_info->r_idx_b[bs]->idx_x2[%d] to %ld is failed. Cannot put new value to %s\n", curr_dt(dt), cid, b,
                    r_info->r_idx_b[bs]->f_idx_size[b], r_info->r_idx_b[bs]->fname);
            resize_index_free_prepared(r_info, b, bs);
            return 0;
        }
    }

    if (r_info->r_idx_b[bs]->f_idx_size[b] < (1U << 16)) {
        for (int i = 10; i >= 0; i--) {
            r_info->r_idx_b[bs]->k1_index_x2[b] =
                index_memory_malloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k1_index_x2[b], f_idx_size_x2 * sizeof(uint8_t));

            if (r_info->r_idx_b[bs]->k1_index_x2[b] != NULL)
                break;

            if (r_info->r_idx_b[bs]->k1_index_x2[b] == NULL && i == 0) {
                fprintf(stderr, "%s ERROR [IDX_RESIZER=%015lu] Realloc r_info->r_idx_b[bs]->k1_index[%d] to %ld is failed. Cannot put new value to %s\n", curr_dt(dt), cid, b,
                        r_info->r_idx_b[bs]->f_idx_size[b], r_info->r_idx_b[bs]->fname);
                resize_index_free_prepared(r_info, b, bs);
                return 0;
            }
        }
    } else {
        r_info->r_idx_b[bs]->k1_index_x2[b] = NULL;
    }

    if (r_info->r_idx_b[bs]->f_idx_size[b] < (1U << 24)) {
        for (int i = 10; i >= 0; i--) {
            r_info->r_idx_b[bs]->k2_index_x2[b] =
                index_memory_malloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k2_index_x2[b], f_idx_size_x2 * sizeof(uint8_t));

            if (r_info->r_idx_b[bs]->k2_index_x2[b] != NULL)
                break;

            if (r_info->r_idx_b[bs]->k2_index_x2[b] == NULL && i == 0) {
                fprintf(stderr, "%s ERROR [IDX_RESIZER=%015lu] Realloc r_info->r_idx_b[bs]->k2_index[%d] to %ld is failed. Cannot put new value to %s\n", curr_dt(dt), cid, b,
                        r_info->r_idx_b[bs]->f_idx_size[b], r_info->r_idx_b[bs]->fname);
                resize_index_free_prepared(r_info, b, bs);
                return 0;
            }
        }
    } else {
        r_info->r_idx_b[bs]->k2_index_x2[b] = NULL;
    }
    r_info->r_idx_b[bs]->coll_count_curr_x2[b] = r_info->r_idx_b[bs]->coll_count_curr[b];
    r_info->r_idx_b[bs]->coll_count_tot_x2[b] = r_info->r_idx_b[bs]->coll_count_tot[b];
    uint32_t coll_keeper_size_x2 = (uint32_t)r_info->r_idx_b[bs]->coll_count_tot_x2[b] * ((uint32_t)COLL_KEEPER_SIZE * sizeof(uint8_t));
    r_info->r_idx_b[bs]->coll_keeper_x2[b] = (uint8_t *)index_memory_malloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.coll_keeper_x2[b], coll_keeper_size_x2);

    if (r_info->r_idx_b[bs]->coll_keeper_x2[b] == NULL) {
        fprintf(stderr, "%s INFO [IDX_RESIZER=%015lu] Malloc %d bytes r_info->r_idx_b[bs]->coll_keeper_x2[%d] == NULL\n", curr_dt(dt), cid, coll_keeper_size_x2, b);
        resize_index_free_prepared(r_info, b, bs);
        return 0;
    }
    memcpy(r_info->r_idx_b[bs]->coll_keeper_x2[b], r_info->r_idx_b[bs]->coll_keeper[b], coll_keeper_size_x2);
    return f_idx_size_x2;
}

typedef struct {
    region_info_t *r_info;
    uint8_t b;
    uint8_t bs;
    idx_t *idx;
    idx_t *idx_x2;
    uint8_t *k1_index;
    uint8_t *k1_index_x2;
    uint8_t *k2_index;
    uint8_t *k2_index_x2;
    uint32_t begin;
    uint32_t end;
    int use_k1;
    int use_k2;
} resize_copy_args_t;
static void *resize_index_copy_worker(void *arg) {
    resize_copy_args_t *a = (resize_copy_args_t *)arg;
    region_info_t *r_info = a->r_info;
    idx_t *idx = a->idx;
    idx_t *idx_x2 = a->idx_x2;
    uint8_t *k1_index = a->k1_index;
    uint8_t *k1_index_x2 = a->k1_index_x2;
    uint8_t *k2_index = a->k2_index;
    uint8_t *k2_index_x2 = a->k2_index_x2;
    uint32_t begin = a->begin;
    uint32_t end = a->end;
    uint32_t count = end - begin;

    if (count == 0)
        return NULL;
    memcpy(&idx_x2[begin], &idx[begin], count * sizeof(idx_t));

    if (a->use_k1 && k1_index && k1_index_x2) {
        memcpy(&k1_index_x2[begin], &k1_index[begin], count);
    }

    if (a->use_k2 && k2_index && k2_index_x2) {
        memcpy(&k2_index_x2[begin], &k2_index[begin], count);
    }
    return NULL;
}

void resize_index_create_copy(region_info_t *r_info, uint8_t b, uint8_t bs, idx_t **idx, idx_t **idx_x2, uint8_t **k1_index, uint8_t **k1_index_x2, uint8_t **k2_index,
                              uint8_t **k2_index_x2, uint32_t f_idx_size, const idx_resize_chunks_t *plan, uint64_t cid) {
    uint32_t len = f_idx_size;

    if (len == 0)
        return;
    int use_k1 = (r_info->r_idx_b[bs]->f_idx_size[b] < (1U << 16));
    int use_k2 = (r_info->r_idx_b[bs]->f_idx_size[b] < (1U << 24));
    uint32_t n_threads = 1;

    if (plan && plan->num_chunks > 0) {
        n_threads = plan->num_chunks;

        if (n_threads > idx_resize_max_num_chunk)
            n_threads = idx_resize_max_num_chunk;

        if (n_threads > len)
            n_threads = len;

        if (n_threads == 0)
            n_threads = 1;
    }

    if (len < ELEM_STEP_7_5M || n_threads <= 1) {
        {
            char dt[DT_LOG_SIZE];

            for (uint32_t i = 0; i < len; ++i) {
                if ((*idx)[i].o_index == 0 && (*idx)[i].c_index != 0) {
                    fprintf(stderr, "%s TRACE [RS_IDX=%015lu] Copy %s k_idx=%u, o_index=%u, c_index=%u\n", curr_dt(dt), 0L, r_info->uuid, i, (*idx)[i].o_index,
                            (*idx)[i].c_index);
                }
            }
        }
        memcpy(*idx_x2, *idx, len * sizeof(idx_t));

        if (use_k1)
            memcpy(*k1_index_x2, *k1_index, len);

        if (use_k2)
            memcpy(*k2_index_x2, *k2_index, len);
        return;
    }
    pthread_t threads[idx_resize_max_num_chunk];
    resize_copy_args_t args[idx_resize_max_num_chunk];
    uint32_t base = len / n_threads;
    uint32_t rem = len % n_threads;
    uint32_t pos = 0;

    for (uint32_t t = 0; t < n_threads; ++t) {
        uint32_t chunk_len = base + (t < rem ? 1u : 0u);
        args[t].r_info = r_info;
        args[t].b = b;
        args[t].bs = bs;
        args[t].idx = *idx;
        args[t].idx_x2 = *idx_x2;
        args[t].k1_index = use_k1 ? *k1_index : NULL;
        args[t].k1_index_x2 = use_k1 ? *k1_index_x2 : NULL;
        args[t].k2_index = use_k2 ? *k2_index : NULL;
        args[t].k2_index_x2 = use_k2 ? *k2_index_x2 : NULL;
        args[t].begin = pos;
        args[t].end = pos + chunk_len;
        args[t].use_k1 = use_k1;
        args[t].use_k2 = use_k2;
        int rc = pthread_create(&threads[t], NULL, resize_index_copy_worker, &args[t]);

        if (rc != 0) {
            r_info->stop = true;
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [IDX_RESIZER=%015lu] pthread_create resize_index_copy_worker failed: rc=%d, region %s stopped\n", curr_dt(dt), cid, rc, r_info->uuid);
        }
        pos += chunk_len;
    }

    for (uint32_t t = 0; t < n_threads; ++t) {
        pthread_join(threads[t], NULL);
    }
}


bool resize_index_chunk(region_info_t *r_info, uint8_t b, uint32_t f_idx_size_x2, uint8_t bs, uint32_t f_idx_size, uint32_t f_idx_real_size, uint32_t num_chunks,
                        uint32_t chunk_index, uint32_t pure_start, uint32_t pure_end, uint8_t *idx_moved_flag, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    idx_t *idx_x3 = NULL;
    uint8_t *k1_index_x3 = NULL;
    uint8_t *k2_index_x3 = NULL;

    if (pure_start >= pure_end)
        return true;

    if (pure_start >= f_idx_real_size)
        return true;

    if (pure_end > f_idx_real_size)
        pure_end = f_idx_real_size;
    uint32_t begin_main = pure_start;
    uint32_t end_main = pure_end;

    if (end_main > IDX_OVERHEAD_MAX)
        end_main -= IDX_OVERHEAD_MAX;
    else
        end_main = begin_main;

    if (begin_main >= end_main) {
        if (log_level > 1) {
            fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Chunk %u has empty main range [%u, %u)\n", curr_dt(dt), t_handler->cid, chunk_index, begin_main, end_main);
        }
        return true;
    }
    uint32_t shift_keeper_len = r_info->r_idx_b[bs]->count_max_idx_jump * r_info->r_idx_b[bs]->count_max_idx_shift + 100;
    uint16_t shift_keeper[shift_keeper_len];
    memset(shift_keeper, 0, shift_keeper_len * sizeof(uint16_t));

    if (log_level > 0) {
        fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Chunk %u main range [%u, %u) (pure [%u, %u), f_idx_real_size=%u)\n", curr_dt(dt), t_handler->cid, chunk_index,
                begin_main, end_main, pure_start, pure_end, f_idx_real_size);
    }

    for (uint32_t i = begin_main; i < end_main; ++i) {
        if (chunk_index > 0 && i < pure_start + IDX_OVERHEAD_MAX) {
            if (r_info->r_idx_b[bs]->idx_x2[b][i].j_index != 0)
                continue;
        }
        move_index(r_info, b, i, 0, shift_keeper, idx_moved_flag, bs, idx_x3, k1_index_x3, k2_index_x3, f_idx_size, t_handler);
    }

    if (log_level > 0) {
        fprintf(stderr, "%s INFO [IDX_RESIZER=%015lu] Chunk %u main range [%u, %u) processed\n", curr_dt(dt), t_handler->cid, chunk_index, begin_main, end_main);
    }
    free(idx_x3);
    free(k1_index_x3);
    free(k2_index_x3);
    return !r_info->bad_data;
}

bool resize_index(region_info_t *r_info, uint8_t b, uint32_t f_idx_size_x2, uint8_t bs, uint32_t f_idx_size, table_handler_t *t_handler) {
    // TODO Malloc and free all at once
    char dt[DT_LOG_SIZE];
    idx_t *idx_x3 = NULL;
    uint8_t *k1_index_x3 = NULL;
    uint8_t *k2_index_x3 = NULL;
    uint16_t shift_keeper[r_info->r_idx_b[bs]->count_max_idx_jump * r_info->r_idx_b[bs]->count_max_idx_shift + 100];
    memset(shift_keeper, 0, r_info->r_idx_b[bs]->count_max_idx_jump * r_info->r_idx_b[bs]->count_max_idx_shift + 100);
    size_t moved_bytes = 0;
    uint8_t *idx_moved_flag = index_memory_malloc(&r_info->index_memory_bytes, &moved_bytes, f_idx_size_x2 * sizeof(uint8_t));

    if (idx_moved_flag == NULL) {
        resize_index_free_prepared(r_info, b, bs);
        return false;
    }

    for (int i = 0; i < f_idx_size_x2; i++)
        if (r_info->r_idx_b[bs]->idx_x2[b][i].o_index > 0)
            idx_moved_flag[i] = 1;
        else
            idx_moved_flag[i] = 0;
    r_info->resize_buff[bs] = index_memory_malloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.resize_buff, r_info->r_idx_b[bs]->block_size); // TODO Check

    if (r_info->resize_buff[bs] == NULL) {
        index_memory_free(&r_info->index_memory_bytes, &moved_bytes, idx_moved_flag);
        return false;
    }
    r_info->resize_buff_size[bs] = r_info->r_idx_b[bs]->block_size;

    for (uint32_t i = 0; i < r_info->r_idx_b[bs]->f_idx_size[b]; i++)
        move_index(r_info, b, i, 0, shift_keeper, idx_moved_flag, bs, idx_x3, k1_index_x3, k2_index_x3, f_idx_size, t_handler);
    fprintf(stderr, "%s INFO [IDX_RESIZER=%015lu] Move index %s[%d] done\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, b);
    index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.resize_buff, r_info->resize_buff[bs]);
    r_info->resize_buff[bs] = NULL;
    r_info->resize_buff_size[bs] = 0;
    free(idx_x3);
    free(k1_index_x3);
    free(k2_index_x3);
    index_memory_free(&r_info->index_memory_bytes, &moved_bytes, idx_moved_flag);
    return !r_info->bad_data;
}


bool write_idx_meta(region_info_t *r_info, uint8_t b, uint8_t bs, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    uint32_t block_size = r_info->r_idx_b[bs]->block_size;
    uint8_t meta_buff[block_size];
    memset(meta_buff, 0, block_size);
    uint32_t meta_crc32;
    read_file(r_info->r_idx_b[bs]->fd[b], meta_buff, 0, block_size - sizeof(meta_crc32), r_info, t_handler->cid);
    uint32_t f_idx_size;
    f_idx_size = r_info->r_idx_b[bs]->f_idx_size[b];
    memcpy(meta_buff, &f_idx_size, sizeof(f_idx_size));
    uint16_t idx_overhead = IDX_OVERHEAD_MAX;
    memcpy(meta_buff + sizeof(f_idx_size), &idx_overhead, sizeof(idx_overhead));
    meta_crc32 = crc_32(meta_buff, block_size - sizeof(uint32_t));
    memcpy(meta_buff + block_size - sizeof(uint32_t), &meta_crc32, sizeof(uint32_t));
    replicas_writer(r_info, meta_buff, b, META_OFFSET, block_size, false, bs, true, t_handler);
    fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Index for %s[%d] has resized\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, b);
    fprintf(stderr, "%s DEBUG [IDX_RESIZER=%015lu] Region %s[%d] idx size %lu\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, b,
            r_info->r_idx_b[bs]->f_idx_curr_size[b]);
    return true;
}
