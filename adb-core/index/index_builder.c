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
#include "index_builder.h"
#include "../utils/region_memory.h"
#include "f_index.h"
#include "../crypto/xxhash.h"
#include "index_resizer.h"
#include "../handle/block_handler.h"
#include "../handle/replica_handler.h"
#include "../handle/storage.h"
static void finish_bulk_replication_before_resize(region_info_t *r_info, table_handler_t *t_handler) {
    if (r_info->bulk_mode == BUIK_RELICA_OFF)
        return;
    uint8_t msg_type = r_info->bulk_buff1[MSG_TYPE_OFFSET];
    uint8_t bulk_header_size = msg_type == MSG_BULK_PUT_NIO ? 13 : 9;

    if (msg_type != MSG_BULK_PUT && msg_type != MSG_BULK_PUT_NIO) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Cannot finish replica bulk before resize: wrong msg_type=%u for %s\n", curr_dt(dt), t_handler->cid, msg_type,
                r_info->r_fname);
        r_info->stop = true;
        r_info->bad_data = true;
        finish_thread(r_info, 70, true, t_handler);
    }

    if (r_info->bulk_buff_offset1 > bulk_header_size || r_info->bulk_buff_offset2 > bulk_header_size) {
        memcpy(r_info->bulk_buff1, &r_info->bulk_buff_offset1, sizeof(r_info->bulk_buff_offset1));
        r_info->bulk_buff1[MSG_TYPE_OFFSET] = msg_type;
        memcpy(r_info->bulk_buff2, &r_info->bulk_buff_offset2, sizeof(r_info->bulk_buff_offset2));
        r_info->bulk_buff2[MSG_TYPE_OFFSET] = msg_type;
        bulk_replicas_writer(r_info, t_handler);
    }
    r_info->bulk_mode = BUIK_RELICA_OFF;
    r_info->bulk_buff_offset1 = 0;
    r_info->bulk_buff_offset2 = 0;
}


uint32_t find_new_place(struct region_info_t *r_info, uint8_t b, uint32_t k_idx, uint32_t curr_k_idx, uint8_t bs, uint64_t cid) {
    for (uint32_t i = 1; i < IDX_OVERHEAD_MAX; i++) {
        if (r_info->r_idx_b[bs]->idx[b][k_idx + i].c_index == 0 || (k_idx + i) == curr_k_idx) {
            if (r_info->r_idx_b[bs]->count_max_idx_shift < i) {
                r_info->r_idx_b[bs]->count_max_idx_shift = i;
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] For %s[%d] count_max_idx_shift=%d\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname, b,
                        r_info->r_idx_b[bs]->count_max_idx_shift);
            }
            return k_idx + i;
        }
    }
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Cannot insert the new k_idx=%d to index %s[%d]. Need to resize it.\n", curr_dt(dt), cid, k_idx, r_info->r_idx_b[bs]->fname,
            b);
    return k_idx;
}

uint32_t find_new_place_x2(struct region_info_t *r_info, uint8_t b, uint32_t k_idx, uint32_t curr_k_idx, uint8_t bs, uint64_t cid) {
    // TODO 2022.01.07 - (k_idx + i) == curr_k_idx) сомневаюсь что это вообще срабатывает, проверить!
    for (uint32_t i = 1; i < IDX_OVERHEAD_MAX; i++) {
        if (r_info->r_idx_b[bs]->idx_x2[b][k_idx + i].c_index == 0 || (k_idx + i) == curr_k_idx) {
            if (r_info->r_idx_b[bs]->count_max_idx_shift_x2 < i) {
                r_info->r_idx_b[bs]->count_max_idx_shift_x2 = i;
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] For %s[%d] count_max_idx_shift_x2=%d\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname, b,
                        r_info->r_idx_b[bs]->count_max_idx_shift_x2);
            }
            return k_idx + i;
        }
    }
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Cannot insert the new k_idx_x2=%d to index %s[%d]. Need to resize it.\n", curr_dt(dt), cid, k_idx,
            r_info->r_idx_b[bs]->fname, b);
    return k_idx;
}


bool coll_handler(region_info_t *r_info, msg_cli_t *msg_cli, uint8_t b, uint8_t bs, uint64_t cid) {
    uint32_t read_off = 0;
    union i2b;
    uint8_t digest[DIGEST_SIZE_FULL];
    calc_hash(msg_cli->kv->key, msg_cli->kv->key_size, digest);
    calc_hash_full(msg_cli->kv->key, msg_cli->kv->key_size, digest, DIGEST_SIZE_SMALL);

    if (r_info->r_idx_b[bs]->coll_count_curr[b] == r_info->r_idx_b[bs]->coll_count_tot[b])
        if (!resize_coll_keeper(r_info, b, bs, cid)) {
            return false;
        }
    uint32_t cpos = r_info->r_idx_b[bs]->coll_count_curr[b] * ((uint32_t)COLL_KEEPER_SIZE);
    memcpy(r_info->r_idx_b[bs]->coll_keeper[b] + cpos, digest, ((uint32_t)DIGEST_SIZE_FULL));
    return true;
}


void build_hash_params(msg_cli_t *msg, region_info_t *r_info, uint8_t bs) {
    if (!r_info->has_digest_calc) {
        calc_hash(msg->kv->key, msg->kv->key_size, r_info->digest);
        r_info->has_digest_calc = true;
    }
    union i2b_u i2b;
    i2b.byte[K0_POS] = r_info->digest[K0_POS];
    i2b.byte[K1_POS] = r_info->digest[K1_POS];
    i2b.byte[K2_POS] = r_info->digest[K2_POS];
    i2b.byte[K3_POS] = r_info->digest[K3_POS];
    uint8_t b = r_info->r_idx_b[bs]->idx_r_help->b = r_info->digest[BUCKET_POS] % BUCKETS;
    r_info->r_idx_b[bs]->idx_r_help->k_idx = r_info->r_idx_b[bs]->idx_r_help->k_idx_orig = i2b.integer_val & r_info->r_idx_b[bs]->idx_mask[b];
#ifdef COLL_LOB_TEST
    r_info->r_idx_b[bs]->idx_r_help->c_idx = r_info->digest[C_IDX_POS];
#else
    r_info->r_idx_b[bs]->idx_r_help->c_idx = r_info->digest[C_IDX_POS] + (r_info->digest[C_IDX_POS + 1] << 8) + ((r_info->digest[C_IDX_POS + 2] & 0x7Fu) << 16);
#endif
    if (r_info->r_idx_b[bs]->f_idx_size[b] < (1U << 16))
        r_info->r_idx_b[bs]->idx_r_help->k1 = r_info->digest[K1_POS];
    else
        r_info->r_idx_b[bs]->idx_r_help->k1 = 0;

    if (r_info->r_idx_b[bs]->f_idx_size[b] < (1U << 24))
        r_info->r_idx_b[bs]->idx_r_help->k2 = r_info->digest[K2_POS];
    else
        r_info->r_idx_b[bs]->idx_r_help->k2 = 0;
    r_info->r_idx_b[bs]->idx_r_help->k3 = r_info->digest[K3_POS];
}

void run_index_resize(region_info_t *r_info, table_handler_t *t_handler, bool save2idx, uint8_t b, uint8_t bs) {
    finish_bulk_replication_before_resize(r_info, t_handler);

    if (r_info->idx_resizer == RESIZE_IDX_DONE && save2idx) {
        if (resize_thread_runner(r_info, b, bs, t_handler) != true) {
            r_info->stop = true;
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Cannot resize %s index\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->dir_name);
            finish_thread(r_info, 45, true, t_handler);
        }

        do {
            char dt[DT_LOG_SIZE];
            r_info->r_lock_id = 0;
            r_info->curr_cid_lock = 0;
            pthread_mutex_unlock(&r_info->rlock);
            fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] P1 Index %s[%d] is resizing, sleep 0.1s\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname,
                    r_info->r_idx_b[bs]->idx_r_help->b);
            usleep(100000);
            pthread_mutex_lock(&r_info->rlock);
            r_info->r_lock_id = IDX_BUILDER_LOCK;
            r_info->curr_cid_lock = t_handler->cid;
        } while (r_info->idx_resizer != RESIZE_IDX_DONE);
    } else {
        char dt[DT_LOG_SIZE];

        do {
            r_info->r_lock_id = 0;
            r_info->curr_cid_lock = 0;
            pthread_mutex_unlock(&r_info->rlock);
            fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] P2 Index %s[%d] is resizing, sleep 0.1s\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname,
                    r_info->r_idx_b[bs]->idx_r_help->b);
            usleep(100000);
            pthread_mutex_lock(&r_info->rlock);
            r_info->r_lock_id = IDX_BUILDER_WAITING_LOCK;
            r_info->curr_cid_lock = t_handler->cid;
        } while (r_info->idx_resizer != RESIZE_IDX_DONE);
    }
    r_info->has_digest_calc = false;
}


bool find_k_idx(region_info_t *r_info, bool save2idx, uint32_t curr_k_idx, uint8_t bs, table_handler_t *t_handler) {
    uint8_t b = r_info->r_idx_b[bs]->idx_r_help->b;
    uint32_t k_idx = r_info->r_idx_b[bs]->idx_r_help->k_idx;
    uint32_t c_idx = r_info->r_idx_b[bs]->idx_r_help->c_idx;

    if (k_idx >= r_info->r_idx_b[bs]->f_idx_size[b]) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Error code 100. Index %s[%d] size=%lu is not actual, k_idx=%u, k_idx_orig=%u, bs=%d\n", curr_dt(dt), t_handler->cid,
                r_info->r_idx_b[bs]->fname, b, r_info->r_idx_b[bs]->f_idx_size[b], k_idx, r_info->r_idx_b[bs]->idx_r_help->k_idx_orig, bs);
        r_info->stop = true;
        r_info->bad_data = true;
        finish_thread(r_info, 46, true, t_handler);
    }

    if (r_info->r_idx_b[bs]->idx[b] == NULL) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Error code 110. Idx %s[%d] size=%lu is NULL, k_idx=%u, k_idx_orig=%u, bs=%d\n", curr_dt(dt), t_handler->cid,
                r_info->r_idx_b[bs]->fname, b, r_info->r_idx_b[bs]->f_idx_size[b], k_idx, r_info->r_idx_b[bs]->idx_r_help->k_idx_orig, bs);
        r_info->stop = true;
        r_info->bad_data = true;
        finish_thread(r_info, 47, true, t_handler);
    }

    if (r_info->r_idx_b[bs]->idx[b][k_idx].c_index != 0) {
        uint16_t j_count = 0;

        while (r_info->r_idx_b[bs]->idx[b][k_idx].c_index != c_idx ||
               (r_info->r_idx_b[bs]->idx[b][k_idx].c_index == c_idx && r_info->r_idx_b[bs]->idx[b][k_idx].j_index != j_count)) {
            if (r_info->r_idx_b[bs]->idx[b][k_idx].s_index == 0) {
                if (!save2idx)
                    return false;
                uint32_t new_k_idx = find_new_place(r_info, b, k_idx, curr_k_idx, bs, t_handler->cid);

                if (new_k_idx == k_idx) {
                    run_index_resize(r_info, t_handler, save2idx, b, bs);
                    return false;
                }
                r_info->r_idx_b[bs]->idx[b][k_idx].s_index = (new_k_idx - k_idx);
                k_idx = new_k_idx;

                if (j_count > (1u << J_INDEX_SIZE)) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Error code 111. Cannot put the new key, j_count=%u. TODO Don't fail here.\n", curr_dt(dt), t_handler->cid,
                            j_count);
                    r_info->stop = true;
                    r_info->bad_data = true;
                    finish_thread(r_info, 48, true, t_handler);
                }
                r_info->r_idx_b[bs]->idx[b][k_idx].j_index = ++j_count;

                if (r_info->r_idx_b[bs]->count_max_idx_jump < j_count) {
                    r_info->r_idx_b[bs]->count_max_idx_jump = j_count;
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] For %s[%d] count_max_idx_jump=%d\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, b,
                            r_info->r_idx_b[bs]->count_max_idx_jump);
                }
                break;
            } else {
                k_idx = k_idx + r_info->r_idx_b[bs]->idx[b][k_idx].s_index;
                j_count++;
            }
        }
    }
    r_info->r_idx_b[bs]->idx_r_help->k_idx = k_idx;

    if (save2idx)
        r_info->r_idx_b[bs]->idx[b][k_idx].c_index = c_idx;
    return true;
}

bool find_k_idx_x2_local(region_info_t *r_info, uint8_t b, uint32_t *k_idx_io, uint32_t c_idx, uint32_t curr_k_idx, uint8_t bs, table_handler_t *t_handler) {
    uint32_t k_idx = *k_idx_io;

    if (r_info->r_idx_b[bs]->idx_x2[b][k_idx].c_index != 0) {
        uint16_t j_count = 0;

        while (r_info->r_idx_b[bs]->idx_x2[b][k_idx].c_index != c_idx ||
               (r_info->r_idx_b[bs]->idx_x2[b][k_idx].c_index == c_idx && r_info->r_idx_b[bs]->idx_x2[b][k_idx].j_index != j_count)) {
            if (r_info->r_idx_b[bs]->idx_x2[b][k_idx].s_index == 0) {
                uint32_t new_k_idx = find_new_place_x2(r_info, b, k_idx, curr_k_idx, bs, t_handler->cid);

                if (new_k_idx == k_idx) {
                    *k_idx_io = k_idx;
                    return true;
                }
                r_info->r_idx_b[bs]->idx_x2[b][k_idx].s_index = (new_k_idx - k_idx);
                k_idx = new_k_idx;

                if (j_count > (1u << J_INDEX_SIZE)) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Error code 112. Cannot put the new key. j_count=%u, TODO Don't fail here.\n", curr_dt(dt), t_handler->cid,
                            j_count);
                    r_info->stop = true;
                    r_info->bad_data = true;
                    finish_thread(r_info, 49, true, t_handler);
                } else {
                    r_info->r_idx_b[bs]->idx_x2[b][k_idx].j_index = ++j_count;
                }

                if (r_info->r_idx_b[bs]->count_max_idx_jump_x2 < j_count) {
                    r_info->r_idx_b[bs]->count_max_idx_jump_x2 = j_count;
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] For %s[%d] count_max_idx_jump_x2=%d\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, b,
                            r_info->r_idx_b[bs]->count_max_idx_jump_x2);
                }
                break;
            } else {
                k_idx = k_idx + r_info->r_idx_b[bs]->idx_x2[b][k_idx].s_index;
                j_count++;
            }
        }
    }
    r_info->r_idx_b[bs]->idx_x2[b][k_idx].c_index = c_idx;
    *k_idx_io = k_idx;
    return true;
}

bool find_k_idx_x2(region_info_t *r_info, uint32_t curr_k_idx, uint8_t bs, table_handler_t *t_handler) {
    uint8_t b = r_info->r_idx_b[bs]->idx_r_help_x2->b;
    uint32_t k_idx = r_info->r_idx_b[bs]->idx_r_help_x2->k_idx;
    uint32_t c_idx = r_info->r_idx_b[bs]->idx_r_help_x2->c_idx;

    if (r_info->r_idx_b[bs]->idx_x2[b][k_idx].c_index != 0) {
        uint16_t j_count = 0;

        while (r_info->r_idx_b[bs]->idx_x2[b][k_idx].c_index != c_idx ||
               (r_info->r_idx_b[bs]->idx_x2[b][k_idx].c_index == c_idx && r_info->r_idx_b[bs]->idx_x2[b][k_idx].j_index != j_count)) {
            if (r_info->r_idx_b[bs]->idx_x2[b][k_idx].s_index == 0) {
                uint32_t new_k_idx = find_new_place_x2(r_info, b, k_idx, curr_k_idx, bs, t_handler->cid);

                if (new_k_idx == k_idx) {
                    return true;
                }
                r_info->r_idx_b[bs]->idx_x2[b][k_idx].s_index = (new_k_idx - k_idx);
                k_idx = new_k_idx;

                if (j_count > (1u << J_INDEX_SIZE)) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Error code 113. Cannot put the new key, j_count=%u. TODO Don't fail here.\n", curr_dt(dt), t_handler->cid,
                            j_count);
                    r_info->stop = true;
                    r_info->bad_data = true;
                    finish_thread(r_info, 50, true, t_handler);
                } else
                    r_info->r_idx_b[bs]->idx_x2[b][k_idx].j_index = ++j_count;

                if (r_info->r_idx_b[bs]->count_max_idx_jump_x2 < j_count) {
                    r_info->r_idx_b[bs]->count_max_idx_jump_x2 = j_count;
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] For %s[%d] count_max_idx_jump_x2=%d\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname, b,
                            r_info->r_idx_b[bs]->count_max_idx_jump_x2);
                }
                break;
            } else {
                k_idx = k_idx + r_info->r_idx_b[bs]->idx_x2[b][k_idx].s_index;
                j_count++;
            }
        }
    }
    r_info->r_idx_b[bs]->idx_r_help_x2->k_idx = k_idx;
    r_info->r_idx_b[bs]->idx_x2[b][k_idx].c_index = c_idx;
    return true;
}


uint32_t calc_k_idx_x2(region_info_t *r_info, uint8_t b, uint32_t k_idx, uint32_t c_idx, uint8_t bs, uint64_t cid) {
    if (r_info->r_idx_b[bs]->idx_x2[b][k_idx].c_index != 0) {
        uint16_t j_count = 0;

        while (r_info->r_idx_b[bs]->idx_x2[b][k_idx].c_index != c_idx ||
               (r_info->r_idx_b[bs]->idx_x2[b][k_idx].c_index == c_idx && r_info->r_idx_b[bs]->idx_x2[b][k_idx].j_index != j_count)) {
            if (r_info->r_idx_b[bs]->idx_x2[b][k_idx].s_index == 0) {
                return find_new_place_x2(r_info, b, k_idx, 0, bs, cid);
            } else {
                k_idx = k_idx + r_info->r_idx_b[bs]->idx_x2[b][k_idx].s_index;
                j_count++;
            }
        }
    }
    return k_idx;
}


/*
int build_idx_vars(region_info_t* r_info, msg_cli_t *msg_any, uint8_t *bs, uint64_t cid) {
    int8_t bs_orig = 0; // TODO Это медленно!
    for (int i = 0; i < r_info->max_idx_b + 1; i++) {
        build_hash_params(msg_any, r_info, *bs);
        if (msg_any->type == MSG_PUT) {
            // Если вставка, то мы точно знаем размер блока
            if (find_k_idx(r_info, cid, true, 0, *bs) == false) {
                return -1;
            } else
                return 0;
        } else {
            find_k_idx(r_info, cid, false, 0, *bs);
            if (r_info->r_idx_b[*bs]->idx[FBP][r_info->r_idx_b[*bs]->idx_r_help->k_idx].o_index != 0)
                return 0;
            else {
                //*bs = r_info->bs_map_r[i];
                *bs = r_info->bs_map[r_info->r_idx_b[*bs]->pos];
            }
        }
    }
    return -1;
}
*/
bool build_idx_vars_old(region_info_t *r_info, msg_cli_t *msg_get, uint8_t bs, table_handler_t *t_handler) {
    build_hash_params(msg_get, r_info, bs);
    return find_k_idx(r_info, msg_get->type == MSG_PUT, 0, bs, t_handler);
}

int build_idx_vars(region_info_t *r_info, msg_cli_t *msg_any, uint8_t *bs, table_handler_t *t_handler) {
    build_hash_params(msg_any, r_info, *bs);

    if (msg_any->type == MSG_PUT) {
        if (find_k_idx(r_info, true, 0, *bs, t_handler) == false) {
            return -1;
        } else
            return 0;
    } else {
        find_k_idx(r_info, false, 0, *bs, t_handler);

        if (r_info->r_idx_b[*bs]->idx[FBP][r_info->r_idx_b[*bs]->idx_r_help->k_idx].o_index != 0)
            return 0;
        else {
            *bs = r_info->bs_map[r_info->r_idx_b[*bs]->pos];
        }
    }
}

void extract_max_ts(region_info_t *r_info, msg_cli_t *msg_cli, uint8_t *buff, msg_info_t *msg_info, table_handler_t *t_handler) {
    uint64_t cid = t_handler->cid;
    uint32_t curr = KEY_SIZE_RECORD_OFFSET;
    uint32_t key_size = buff_to_field(buff, &curr, sizeof(msg_cli->kv->key_size));
    uint32_t cf_size = buff_to_field(buff, &curr, sizeof(msg_cli->kv->cf_size));

    if ((msg_cli->zip_info & 0x01) == 1 && r_info->enable_record_compression == 1) {
        uint32_t qv_offset = KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET + key_size + cf_size;
        uint64_t dst_capacity = r_info->zbuff_size - qv_offset;
        msg_cli->msg_size = decompress_record(r_info, buff + qv_offset, msg_cli->msg_size - qv_offset, r_info->zbuff + qv_offset, dst_capacity, cid);

        if (msg_cli->msg_size == 0) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Error code 114. Result from LZ4_decompress_safe indicates a failure trying to decompress the data %s\n",
                    curr_dt(dt), cid, r_info->r_fname);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 51, true, t_handler);
        }
        memcpy(r_info->zbuff, buff, qv_offset);
        msg_cli->msg_size += qv_offset;
    } else
        memcpy(r_info->zbuff, buff, msg_cli->msg_size);
    msg_info->qv_size = buff_to_field(r_info->zbuff, &curr, sizeof(msg_cli->kv->qv_size));
    curr += key_size;
    curr += cf_size;
    uint64_t max_ts = 0;
    uint64_t ts = 0;
    uint32_t qfl_size;
    uint32_t val_size;
    uint16_t qfl_count = 0;

    do {
        ts = buff_to_field(r_info->zbuff, &curr, sizeof(msg_cli->kv->qv->ts));

        if (ts > max_ts)
            max_ts = ts;
        qfl_size = buff_to_field(r_info->zbuff, &curr, sizeof(msg_cli->kv->qv->qfl_size));
        val_size = buff_to_field(r_info->zbuff, &curr, sizeof(msg_cli->kv->qv->val_size));
        curr += qfl_size;
        curr += val_size;
        qfl_count++;
    } while (msg_cli->msg_size != curr);
    msg_info->max_ts = max_ts;
    msg_info->qfl_count = qfl_count;
}


bool add_double_key(struct region_info_t *r_info, struct msg_cli_t *msg_get, uint8_t bs_del, uint32_t block_del, table_handler_t *t_handler) {
    uint64_t cid = t_handler->cid;
    char dt[DT_LOG_SIZE];
    keys_double_t *kd = r_info->k_dbl[bs_del];
    uint16_t key_sz = (uint16_t)msg_get->kv->key_size;
    uint64_t need_bytes = kd->keys_offset + key_sz;

    if (need_bytes > kd->keys_mem_limit) {
        kd->keys_mem_limit += (1024 * 1024) * 10;
        fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Realloc keys in k_dbl[%d]: new limit = %lu\n", curr_dt(dt), cid, bs_del, kd->keys_mem_limit);
        char *tmp_keys = realloc(kd->keys, kd->keys_mem_limit);

        if (!tmp_keys) {
            fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Error code 115. Cannot realloc kd->keys to %lu bytes\n", curr_dt(dt), cid, kd->keys_mem_limit);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 52, true, t_handler);
        }
        kd->keys = tmp_keys;
    }

    if (kd->k_count + 1 > kd->max_key_count) {
        kd->max_key_count += 1024;
        fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Realloc key_sizes/block_del in k_dbl[%d]: new max_key_count = %lu\n", curr_dt(dt), cid, bs_del, kd->max_key_count);
        char *tmp_key_sizes = realloc(kd->key_sizes, kd->max_key_count * 2);

        if (!tmp_key_sizes) {
            fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Cannot realloc kd->key_sizes to %lu for %s\n", curr_dt(dt), cid, kd->max_key_count * 2UL, r_info->uuid);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 53, true, t_handler);
        }
        kd->key_sizes = tmp_key_sizes;
        uint32_t *tmp_block_del = realloc(kd->block_del, kd->max_key_count * sizeof(uint32_t));

        if (!tmp_block_del) {
            fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Cannot realloc tmp_block_del to %lu for %s\n", curr_dt(dt), cid, kd->max_key_count * sizeof(uint32_t),
                    r_info->uuid);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 54, true, t_handler);
        }
        kd->block_del = tmp_block_del;
    }
    memcpy(kd->keys + kd->keys_offset, msg_get->kv->key, key_sz);
    memcpy(kd->key_sizes + kd->k_count * 2, &key_sz, 2);
    kd->block_del[kd->k_count] = block_del;
    kd->keys_offset += key_sz;
    kd->k_count++;
    fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] k_dbl[%d]: stored double key of size=%u in block=%u, k_count=%lu\n", curr_dt(dt), cid, bs_del, key_sz, block_del,
            kd->k_count);
}

void add_key_to_idx(region_info_t *r_info, uint8_t *buff, uint8_t bs, uint32_t f, uint8_t zip_val_size, msg_cli_t *msg_get, table_handler_t *t_handler) {
    uint32_t k_idx = r_info->r_idx_b[bs]->idx_r_help->k_idx;
    uint8_t k3 = r_info->r_idx_b[bs]->idx_r_help->k3;
    uint8_t k2 = r_info->r_idx_b[bs]->idx_r_help->k2;
    uint8_t k1 = r_info->r_idx_b[bs]->idx_r_help->k1;
    uint8_t b = r_info->r_idx_b[bs]->idx_r_help->b;
    r_info->r_idx_b[bs]->f_idx_curr_size[b]++;

    if (r_info->r_idx_b[bs]->idx[b][k_idx].o_index == 0) {
        r_info->r_idx_b[bs]->idx[b][k_idx].o_index = f;
        r_info->r_idx_b[bs]->idx[b][k_idx].k3_index = k3;

        if (r_info->r_idx_b[bs]->f_idx_size[b] < pow(2, 24))
            r_info->r_idx_b[bs]->k2_index[b][k_idx] = k2;

        if (r_info->r_idx_b[bs]->f_idx_size[b] < pow(2, 16))
            r_info->r_idx_b[bs]->k1_index[b][k_idx] = k1;
        r_info->r_idx_b[bs]->idx[b][f].z_index += zip_val_size;
    } else {
        if (!coll_handler(r_info, msg_get, b, bs, t_handler->cid)) {
            stop_region_after_key_error(r_info, KEY_EXTRACT_NOMEM, t_handler);
            finish_thread(r_info, 80, true, t_handler);
        }
        union i2b_u i2b;
        i2b.integer_val = f;
        uint32_t cpos = r_info->r_idx_b[bs]->coll_count_curr[b] * ((uint32_t)COLL_KEEPER_SIZE);
        memcpy(r_info->r_idx_b[bs]->coll_keeper[b] + cpos + ((uint32_t)DIGEST_SIZE_FULL), i2b.byte, ((uint32_t)OFFSET_COLL_KEEPER_SIZE));
        i2b.integer_val = r_info->r_idx_b[bs]->idx_r_help->k_idx;
        memcpy(r_info->r_idx_b[bs]->coll_keeper[b] + cpos + ((uint32_t)DIGEST_SIZE_FULL + OFFSET_COLL_KEEPER_SIZE), i2b.byte, ((uint8_t)COLL_IDX_SIZE));
        r_info->r_idx_b[bs]->coll_count_curr[b]++;
        r_info->r_idx_b[bs]->idx[b][f].z_index += zip_val_size;
#ifdef COLL_LOB_TEST
        char dt[DT_LOG_SIZE];
        uint8_t hash[MAX_HASH_SIZE];
        snprintf(hash, MAX_HASH_SIZE, "%x_%x_%x_%x c_idx=%d", r_info->digest[0], r_info->digest[1], r_info->digest[2], r_info->digest[3], r_info->digest[C_IDX_POS]);
        uint8_t *key_hex = extract_key(buff);
        fprintf(stderr, "%s DEBUG [RS_IDX=%015lu] INSc %s, key=%s, hash=%s, k_idx=%d, block=%u, zip_val_size=%u\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname,
                key_hex, hash, k_idx, f, zip_val_size);
        free(key_hex);
#endif
    }
}


bool check_meta_quality(const uint8_t *meta_buff, uint32_t meta_size, uint32_t block_size, uint64_t cid) {
    char dt[DT_LOG_SIZE];

    if (meta_size == 0)
        return true;

    if (meta_size < block_size && meta_crc_enable == true) {
        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Meta too small %d (block_size=%d)\n", curr_dt(dt), cid, meta_size, block_size);
        return false;
    }
    uint32_t expected_crc;
    memcpy(&expected_crc, meta_buff + meta_size - sizeof(expected_crc), sizeof(uint32_t));

    if (expected_crc == 0) {
        bool zero_meta = true;

        for (int i = 0; i < block_size; i++) {
            if (meta_buff[i] != 0) {
                zero_meta = false;
                break;
            }
        }

        if (zero_meta)
            return true;
    }
    uint32_t actual_crc = 0;
    actual_crc = crc_32(meta_buff, meta_size - sizeof(uint32_t));

    if (actual_crc != expected_crc && meta_crc_enable == true) {
        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015" PRIu64 "] Invalid CRC32. Expected 0x%08X, got 0x%08X\n", curr_dt(dt), cid, expected_crc, actual_crc);
        return false;
    } else
        fprintf(stderr, "%s INFO [IDX_BUILDER=%015" PRIu64 "] Meta CRC32=0x%08X\n", curr_dt(dt), cid, actual_crc);
    const uint8_t *data_ptr = meta_buff + USED_META;
    uint32_t data_len = meta_size - USED_META - sizeof(expected_crc);
    uint32_t offset = 0;

    while (offset + DICT_SIZE <= data_len) {
        uint16_t size = (uint16_t)(data_ptr[offset] | (data_ptr[offset + 1] << 8));
        offset += DICT_SIZE;

        if (size == 0) {
            for (uint32_t i = offset; i < data_len; i++) {
                if (data_ptr[i] != 0) {
                    fprintf(stderr, "%s ERROR [IDX_BUILDER=%015" PRIu64 "] Non-zero byte (0x%02X) found after zero size at offset %u\n", curr_dt(dt), cid, data_ptr[i],
                            (USED_META + i));
                    return false;
                }
            }
            fprintf(stderr, "%s INFO [IDX_BUILDER=%015" PRIu64 "] Meta is OK, offset=%u, size=%u\n", curr_dt(dt), cid, offset, meta_size);
            return true;
        } else {
            if (offset + size > data_len) {
                fprintf(stderr, "%s ERROR [IDX_BUILDER=%015" PRIu64 "] Not enough data to read size=%u at offset %u\n", curr_dt(dt), cid, size,
                        (USED_META + offset - DICT_SIZE));
                return false;
            }
            offset += size;
        }
    }

    if (offset < data_len) {
        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015" PRIu64 "] Truncated size field at offset %u\n", curr_dt(dt), cid, (8 + offset));
        return false;
    }
    fprintf(stderr, "%s INFO [IDX_BUILDER=%015" PRIu64 "] Meta is OK, data_len=%u, size=%u\n", curr_dt(dt), cid, offset, data_len);
    return true;
}


msg_file_info_t *get_file_info(region_info_t *r_info, uint8_t bs, table_handler_t *t_handler) {
    uint64_t cid = t_handler->cid;
    char dt[DT_LOG_SIZE];
    msg_file_info_t *file_info = NULL;

    if (r_info->r_need_data_chk) {
        if (r_info->files_list == NULL) {
            fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Error code 117. Cannot find files_list in %s\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 54, true, t_handler);
        }

        for (int i = 0; i < r_info->files_list->files_count; i++) {
            char *basename = strrchr(r_info->r_idx_b[bs]->fname, '/');

            if (basename)
                basename++;
            else
                basename = r_info->r_idx_b[bs]->fname;

            if (strcmp(r_info->files_list->fi[i].file_name, basename) == 0) {
                file_info = &r_info->files_list->fi[i];
                break;
            }
        }

        if (file_info == NULL) {
            fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Error code 118. Cannot find file_info in %s where files_count=%d\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname,
                    r_info->files_list->files_count);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 55, true, t_handler);
        }
    }
    return file_info;
}

void free_index_scanner_buff(msg_cli_t *msg_get, msg_info_t *msg_info1, msg_info_t *msg_info2, uint8_t *tmp_buff, uint8_t *init_buff, uint8_t *zbuff, uint8_t *crc_buff,
                             uint8_t *chunk_buffer) {
    if (msg_get != NULL && msg_get->kv != NULL) {
        clear_extracted_key_fields(msg_get);
        free(msg_get->kv->qv);
        free(msg_get->kv);
    }
    free(msg_get);
    free(msg_info1);
    free(msg_info2);
    free(tmp_buff);
    free(init_buff);
    free(zbuff);
    free(crc_buff);
    free(chunk_buffer);
}


static bool has_identical_serialized_record(region_info_t *r_info, const uint8_t *record, uint32_t record_size, uint8_t bs, uint8_t z_index) {
    uint64_t used_size = (uint64_t)z_index * r_info->r_idx_b[bs]->bsize_factor;

    if (record_size < sizeof(uint32_t) || used_size == 0 || used_size > r_info->r_idx_b[bs]->block_size)
        return false;
    uint64_t offset = 0;

    while (offset + sizeof(uint32_t) <= used_size) {
        uint32_t old_record_size = 0;
        memcpy(&old_record_size, r_info->rbuff + offset, sizeof(old_record_size));

        if (old_record_size < sizeof(uint32_t) || old_record_size > used_size - offset)
            return false;
        uint64_t old_record_allocated_size = (uint64_t)calc_zip_record_size(r_info, old_record_size, bs) * r_info->r_idx_b[bs]->bsize_factor;

        if (old_record_allocated_size < old_record_size || old_record_allocated_size > used_size - offset)
            return false;

        if (old_record_size == record_size && memcmp(r_info->rbuff + offset, record, record_size) == 0)
            return true;
        offset += old_record_allocated_size;
    }
    return false;
}


typedef struct {
    uint8_t *bytes;
    disk_record_header_t header;
    uint32_t block;
    uint8_t bs;
} restore_record_t;
static int read_restore_record_at(region_info_t *r, msg_cli_t *key, uint8_t bs, uint32_t block, restore_record_t *out, table_handler_t *th, const uint8_t *prefix,
                                  size_t prefix_size) {
    idx_region_info_t *idx = r->r_idx_b[bs];

    if (block >= idx->f_idx_size[FBP])
        return -1;
    size_t used = prefix != NULL ? prefix_size : (size_t)idx->idx[FBP][block].z_index * idx->bsize_factor;

    if (used == 0)
        return 0;

    if (prefix == NULL && used > idx->block_size)
        return -1;
    uint8_t *buf = malloc(prefix != NULL ? used : idx->block_size);

    if (buf == NULL)
        return -1;
    int result = -1;

    if (prefix != NULL)
        memcpy(buf, prefix, used);
    else if (read_file(idx->fd[FBP], buf, (uint64_t)block * idx->block_size, (uint32_t)used, r, th->cid) != (int)used)
        goto done;
    uint32_t factor = r->enable_block_compression ? FIX_BSIZE_FACTOR : idx->bsize_factor;

    if (r->enable_block_compression && prefix == NULL) {
        uint64_t size = 0;

        if (decompress_block(buf, used, block, bs, r, &size, 0, th) != 0 || size > SIZE_MAX)
            goto done;
        uint8_t *tmp = realloc(buf, size != 0 ? (size_t)size : 1);

        if (tmp == NULL)
            goto done;
        buf = tmp;
        memcpy(buf, r->bbuff, size);
        used = size;
        factor = FIX_BSIZE_FACTOR;
    }
    result = 0;

    for (size_t off = 0; off < used;) {
        disk_record_header_t h;

        if (!disk_record_decode_header(buf + off, used - off, &h)) {
            result = -1;
            break;
        }
        size_t aligned = align_up(h.msg_size, factor);

        if (aligned > used - off) {
            result = -1;
            break;
        }

        if (h.key_size == key->kv->key_size && memcmp(buf + off + DISK_RECORD_HEADER_SIZE, key->kv->key, h.key_size) == 0) {
            if (result == 1) {
                result = -1;
                break;
            }
            out->bytes = malloc(h.msg_size);

            if (out->bytes == NULL) {
                result = -1;
                break;
            }
            memcpy(out->bytes, buf + off, h.msg_size);
            out->header = h;
            out->bs = bs;
            out->block = block;
            result = 1;
        }
        off += aligned;
    }
done:
    free(buf);

    if (result != 1) {
        free(out->bytes);
        out->bytes = NULL;
    }
    return result;
}

static int read_restore_record(region_info_t *r, msg_cli_t *key, uint8_t bs, restore_record_t *out, table_handler_t *th) {
    uint32_t block;

    if (!get_current_record_block(r, key, bs, &block, th))
        return 0;
    return read_restore_record_at(r, key, bs, block, out, th, NULL, 0);
}

static bool collect_restore_candidate(region_info_t *r, msg_cli_t *key, uint8_t bs, uint32_t block, restore_record_t **items, size_t *count, table_handler_t *th,
                                      const restore_record_t *current, uint32_t current_offset) {
    if (block == 0)
        return true;

    for (size_t i = 0; i < *count; ++i)
        if ((*items)[i].bs == bs && (*items)[i].block == block)
            return true;
    restore_record_t candidate = {0};
    bool same_block = bs == current->bs && block == current->block;
    int found = read_restore_record_at(r, key, bs, block, &candidate, th, same_block ? current->bytes - current_offset : NULL, current_offset);

    if (found <= 0)
        return found == 0;

    if (*count >= SIZE_MAX / sizeof(**items) - 1) {
        free(candidate.bytes);
        return false;
    }
    restore_record_t *tmp = realloc(*items, (*count + 1) * sizeof(**items));

    if (tmp == NULL) {
        free(candidate.bytes);
        return false;
    }
    *items = tmp;
    (*items)[(*count)++] = candidate;
    return true;
}


static uint8_t *restore_record_payload(region_info_t *r, const restore_record_t *rec, uint32_t *size, table_handler_t *th) {
    const disk_record_header_t *h = &rec->header;
    const uint8_t *src = rec->bytes + h->qv_offset;
    uint64_t length = h->msg_size - h->qv_offset;
    uint8_t *payload = NULL;

    if (rec->bytes[13] != 0) {
        msg_srv_t *srv = th->msg_srv;
        uint32_t input_size = h->msg_size;

        if (realloc_resp_buff(srv, input_size, th->cid) < 0)
            return NULL;
        { memcpy(srv->resp_buff, rec->bytes, input_size); }
        if (decompress_lzma(srv, h->qv_offset, input_size, r, &length, th->cid) != 0 || length > UINT32_MAX)
            return NULL;
        src = r->bbuff;
    } else if ((rec->bytes[14] & 1U) != 0) {
        if (h->qv_size > INT_MAX || length > INT_MAX)
            return NULL;
        payload = malloc(h->qv_size != 0 ? h->qv_size : 1);

        if (payload == NULL)
            return NULL;
        int decoded = LZ4_decompress_safe((const char *)src, (char *)payload, (int)length, (int)h->qv_size);

        if (decoded < 0) {
            free(payload);
            return NULL;
        }
        length = (uint32_t)decoded;
    }

    if (length > UINT32_MAX) {
        free(payload);
        return NULL;
    }

    if (payload == NULL) {
        payload = malloc(length != 0 ? (size_t)length : 1);

        if (payload == NULL)
            return NULL;

        if (length != 0)
            memcpy(payload, src, length);
    }
    *size = (uint32_t)length;
    return payload;
}


static bool equal_restore_qv(region_info_t *r, const uint8_t *a, uint32_t as, bool adict, const uint8_t *b, uint32_t bs, bool bdict) {
    uint32_t ai = 0, bi = 0;

    while (ai < as && bi < bs) {
        if (as - ai < 16 || bs - bi < 16 || memcmp(a + ai, b + bi, 8) != 0)
            return false;
        uint32_t aq = (uint32_t)record_header_load_le(a + ai + 8, 4);
        uint32_t bq = (uint32_t)record_header_load_le(b + bi + 8, 4);
        uint32_t av = (uint32_t)record_header_load_le(a + ai + 12, 4);
        uint32_t bv = (uint32_t)record_header_load_le(b + bi + 12, 4);
        ai += 16;
        bi += 16;

        if (aq > as - ai || av > as - ai - aq || bq > bs - bi || bv > bs - bi - bq)
            return false;
        const uint8_t *ak = a + ai, *bk = b + bi;
        uint32_t an = aq, bn = bq;

        if (adict) {
            if (aq != DICT_SIZE)
                return false;
            unsigned id = (unsigned)record_header_load_le(ak, DICT_SIZE);

            if (r->qfl_dic_lens <= 0 || id >= (unsigned)r->qfl_dic_lens)
                return false;
            ak = (uint8_t *)r->qfl_dic[id];
            an = r->qfl_dic_size[id];
        }

        if (bdict) {
            if (bq != DICT_SIZE)
                return false;
            unsigned id = (unsigned)record_header_load_le(bk, DICT_SIZE);

            if (r->qfl_dic_lens <= 0 || id >= (unsigned)r->qfl_dic_lens)
                return false;
            bk = (uint8_t *)r->qfl_dic[id];
            bn = r->qfl_dic_size[id];
        }

        if (an != bn || av != bv || memcmp(ak, bk, an) != 0 || memcmp(a + ai + aq, b + bi + bq, av) != 0)
            return false;
        ai += aq + av;
        bi += bq + bv;
    }
    return ai == as && bi == bs;
}

static bool equal_restore_records(region_info_t *r, const restore_record_t *a, const restore_record_t *b, table_handler_t *th) {
    if (a->header.kind != b->header.kind || a->header.cf_size != b->header.cf_size || a->header.key_size != b->header.key_size ||
        memcmp(a->bytes + DISK_RECORD_HEADER_SIZE, b->bytes + DISK_RECORD_HEADER_SIZE, a->header.key_size + a->header.cf_size) != 0)
        return false;

    if (a->header.kind == DISK_RECORD_TOMBSTONE)
        return true;
    uint32_t as = 0, bs = 0;
    uint8_t *ap = restore_record_payload(r, a, &as, th);

    if (ap == NULL)
        return false;
    uint8_t *bp = restore_record_payload(r, b, &bs, th);
    bool adict = a->bytes[13] == 0 && (a->bytes[14] & 2U) == 0;
    bool bdict = b->bytes[13] == 0 && (b->bytes[14] & 2U) == 0;
    bool equal = false;

    if (ap != NULL && bp != NULL) {
        if (adict == bdict && as == bs && memcmp(ap, bp, as) == 0)
            equal = true;
        else {
            bool dictionary_ready = r->qfl_dic_lens != 0;

            if (!dictionary_ready && (adict || bdict))
                dictionary_ready = load_dict(r, th);

            if (!(adict || bdict) || dictionary_ready)
                equal = equal_restore_qv(r, ap, as, adict && r->qfl_dic_lens != 0, bp, bs, bdict && r->qfl_dic_lens != 0);
        }
    }
    free(ap);
    free(bp);
    return equal;
}

bool obsolete_marker_has_winner(region_info_t *r, uint8_t *record, size_t available, table_handler_t *th) {
    disk_record_header_t h;

    if (!disk_record_decode_header(record, available, &h))
        return false;

    if (h.kind != DISK_RECORD_TOMBSTONE)
        return true;
    msg_kv_t kv = {.key = record + DISK_RECORD_HEADER_SIZE, .key_size = h.key_size};
    msg_cli_t key = {.kv = &kv};
    bool proven = false;

    if (r->max_idx_b < 0 || r->max_idx_b > BLOCK_MAX_SIZE_M)
        return false;

    for (uint8_t bs = 0; bs < (unsigned)r->max_idx_b; ++bs) {
        if (r->r_idx_b[bs] == NULL || r->r_idx_b[bs]->f_idx_curr_size[FBP] == 0)
            continue;
        restore_record_t old = {0};
        int found = read_restore_record(r, &key, bs, &old, th);

        if (found < 0)
            return false;

        if (found > 0) {
            proven =
                old.header.version > h.version || (old.header.version == h.version && old.header.kind == DISK_RECORD_TOMBSTONE && old.header.cf_size == h.cf_size &&
                                                   memcmp(old.bytes + DISK_RECORD_HEADER_SIZE + h.key_size, record + DISK_RECORD_HEADER_SIZE + h.key_size, h.cf_size) == 0);
            free(old.bytes);

            if (proven)
                break;
        }
    }
    r->has_digest_calc = false;
    return proven;
}

static bool reserve_restore_duplicate(region_info_t *r, uint8_t bs, uint32_t key_size) {
    if (r->k_dbl == NULL || r->k_dbl[bs] == NULL || key_size > UINT16_MAX)
        return false;
    keys_double_t *kd = r->k_dbl[bs];

    if (kd->keys_offset > SIZE_MAX - key_size || kd->k_count >= SIZE_MAX / sizeof(uint32_t) - 1)
        return false;
    size_t needed = kd->keys_offset + key_size;

    if (needed > kd->keys_mem_limit) {
        char *tmp = realloc(kd->keys, needed);

        if (tmp == NULL)
            return false;
        kd->keys = tmp;
        kd->keys_mem_limit = needed;
    }

    if (kd->k_count >= kd->max_key_count) {
        size_t count = kd->k_count + 1;
        char *sizes = realloc(kd->key_sizes, count * sizeof(uint16_t));

        if (sizes == NULL)
            return false;
        kd->key_sizes = sizes;
        uint32_t *blocks = realloc(kd->block_del, count * sizeof(uint32_t));

        if (blocks == NULL)
            return false;
        kd->block_del = blocks;
        kd->max_key_count = count;
    }
    return true;
}


static int restore_versioned_record(region_info_t *r, uint8_t *bytes, size_t available, uint8_t bs, uint32_t block, uint32_t current_offset, uint8_t zip_size, msg_cli_t *key,
                                    table_handler_t *th) {
    restore_record_t *candidates = NULL;
    size_t count = 0;
    restore_record_t current = {.bytes = bytes, .bs = bs, .block = block};

    if (!disk_record_decode_header(bytes, available, &current.header))
        return -1;

    if (current.header.kind == DISK_RECORD_TOMBSTONE && (r->r_idx_b[bs]->fd[FBP] != r->data_000_fd || r->enable_block_compression != BLOCK_COMPRESSION_NONE))
        return -1;

    if (!record_version_observe(&r->record_versions, current.header.version))
        return -1;

    if (r->record_versions.confirmed == 0)
        return 0;
    int result = -1;
    bool has_version = current.header.version != 0;
    restore_record_t *winner = &current;

    if (r->max_idx_b < 0)
        return -1;
    unsigned limit = (unsigned)r->max_idx_b > bs ? (unsigned)r->max_idx_b : (unsigned)bs + 1U;

    if (limit > BLOCK_MAX_SIZE_M)
        return -1;

    for (uint8_t ibs = 0; ibs < limit; ++ibs) {
        if (r->r_idx_b[ibs] == NULL || r->r_idx_b[ibs]->idx[FBP] == NULL || r->r_idx_b[ibs]->f_idx_curr_size[FBP] == 0)
            continue;
        idx_region_info_t *idx = r->r_idx_b[ibs];
        r->has_digest_calc = false;
        build_hash_params(key, r, ibs);

        if (find_k_idx(r, false, 0, ibs, th)) {
            uint32_t pos = idx->idx_r_help->k_idx;
            uint8_t bucket = idx->idx_r_help->b;

            if (pos < idx->f_idx_size[bucket] && idx->idx[bucket][pos].c_index == idx->idx_r_help->c_idx &&
                !collect_restore_candidate(r, key, ibs, idx->idx[bucket][pos].o_index, &candidates, &count, th, &current, current_offset))
                goto done;
        }
        uint8_t digest[DIGEST_SIZE_FULL];
        calc_hash(key->kv->key, key->kv->key_size, (char *)digest);
        calc_hash_full(key->kv->key, key->kv->key_size, (char *)digest, DIGEST_SIZE_SMALL);
        uint8_t bucket = digest[BUCKET_POS] % BUCKETS;

        for (unsigned c = 0; c < (unsigned)idx->coll_count_curr[bucket]; ++c) {
            uint8_t *entry = idx->coll_keeper[bucket] + (size_t)c * COLL_KEEPER_SIZE;

            if (memcmp(entry, digest, DIGEST_SIZE_FULL) != 0)
                continue;
            uint32_t address = 0;
            memcpy(&address, entry + DIGEST_SIZE_FULL, OFFSET_COLL_KEEPER_SIZE);

            if (!collect_restore_candidate(r, key, ibs, address, &candidates, &count, th, &current, current_offset))
                goto done;
        }
    }

    for (size_t i = 0; i < count; ++i) {
        restore_record_t *old = &candidates[i];
        has_version |= old->header.version != 0;

        if (old->header.version > winner->header.version)
            winner = old;
        else if (old->header.version != 0 && old->header.version == winner->header.version) {
            if (!equal_restore_records(r, old, winner, th))
                goto done;

            if (winner == &current)
                winner = old;
        }
    }

    if (!has_version) {
        result = 0;
        goto done;
    }

    for (size_t i = 0; i < count; ++i) {
        restore_record_t *old = &candidates[i];
        uint8_t ibs = old->bs;

        if (old->bs == bs && old->block == block)
            goto done;

        if (old == winner)
            continue;

        if (r->r_idx_b[ibs]->fd[FBP] == r->data_000_fd) {
            if (!prepare_append_compaction_dirty_map(r, old->block, th->cid))
                goto done;
            mark_append_compaction_segment(r, old->block, align_up(old->header.msg_size, r->r_idx_b[ibs]->bsize_factor), ibs);
        } else {
            if (!reserve_restore_duplicate(r, ibs, key->kv->key_size))
                goto done;
            add_double_key(r, key, ibs, old->block, th);
        }
        r->has_digest_calc = false;
        build_hash_params(key, r, ibs);

        if (!find_k_idx(r, false, 0, ibs, th))
            goto done;
        del_from_index(r, old->block, key, th, (uint32_t)-1, ibs);

        if (old->header.kind == DISK_RECORD_TOMBSTONE) {
            if (r->pending_delete == 0)
                goto done;
            r->pending_delete--;
        }
    }

    if (winner == &current) {
        key->type = MSG_PUT;
        r->has_digest_calc = false;

        if (!build_idx_vars_old(r, key, bs, th)) {
            if (r->stop || r->bad_data || r->is_freed || r->idx_resizer != RESIZE_IDX_DONE)
                goto done;
            r->has_digest_calc = false;

            if (!build_idx_vars_old(r, key, bs, th))
                goto done;
        }
        idx_region_info_t *idx = r->r_idx_b[bs];
        uint8_t bucket = idx->idx_r_help->b;

        if (idx->idx[bucket][idx->idx_r_help->k_idx].o_index != 0 && idx->coll_count_curr[bucket] == idx->coll_count_tot[bucket] &&
            !resize_coll_keeper(r, bucket, bs, th->cid))
            goto done;
        add_key_to_idx(r, bytes, bs, block, zip_size, key, th);

        if (current.header.kind == DISK_RECORD_TOMBSTONE)
            r->pending_delete++;
    } else {
        r->r_idx_b[bs]->idx[FBP][block].z_index += zip_size;

        if (r->r_idx_b[bs]->fd[FBP] == r->data_000_fd) {
            if (!prepare_append_compaction_dirty_map(r, block, th->cid))
                goto done;
            mark_append_compaction_segment(r, block, (uint64_t)zip_size * r->r_idx_b[bs]->bsize_factor, bs);
        } else {
            if (!reserve_restore_duplicate(r, bs, key->kv->key_size))
                goto done;
            add_double_key(r, key, bs, block, th);
        }
    }
    result = 1;
done:
    for (size_t i = 0; i < count; ++i)
        free(candidates[i].bytes);
    free(candidates);
    r->has_digest_calc = false;
    return result;
}


bool scan_index(region_info_t *r, uint8_t b, uint64_t file_size, uint8_t bs, table_handler_t *h) {
    char dt[DT_LOG_SIZE];
    uint64_t cid = h->cid;
    uint32_t zip_val_size_del = 0;
    uint32_t zip_val_size = -1;
    uint32_t k_idx = -1;
    r->r_idx_b[bs]->b_offset[b] = 0;
    r->r_idx_b[bs]->f_idx_curr_size[b] = 0;

    if (r->r_idx_b[bs]->fd[b] == r->data_000_fd)
        r->pending_delete = 0;
    struct rusage r_usage;
    struct sysinfo info;
    getrusage(RUSAGE_SELF, &r_usage);
    double usage_mem_before = (double)r_usage.ru_maxrss;
    msg_cli_t *msg = calloc(1, sizeof(*msg));
    msg_info_t *msg_info1 = NULL, *msg_info2 = NULL;
    char *tmp = NULL, *zbuff = NULL;
    uint8_t *buf = NULL, *crc_buff = NULL, *chunk_buffer = NULL;

    if (!msg)
        goto scanner_resource_failure;
    msg->kv = calloc(1, sizeof(*msg->kv));

    if (!msg->kv)
        goto scanner_resource_failure;
    msg->kv->qv = calloc(1, sizeof(*msg->kv->qv));

    if (!msg->kv->qv)
        goto scanner_resource_failure;
    uint32_t final_block = file_size == 0 ? 0 : (uint32_t)((file_size - 1U) / r->r_idx_b[bs]->block_size);
    fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Start to building index for %s[%d], file_size=%lu, block_size=%d, final_block=%u, lock=%d\n", curr_dt(dt), cid,
            r->r_idx_b[bs]->fname, b, file_size, r->r_idx_b[bs]->block_size, final_block, r->r_lock_id);
    uint64_t duplicates = 0;
    uint8_t bs_dbl;
    struct msg_srv_t *reply = h->msg_srv;
    msg_info1 = malloc(sizeof(struct msg_info_t));

    if (!msg_info1)
        goto scanner_resource_failure;
    msg_info2 = malloc(sizeof(struct msg_info_t));

    if (!msg_info2)
        goto scanner_resource_failure;
    memset(reply->resp_buff, 0, reply->curr_max_msg_size);
    reply->msg_size = 0;
    tmp = malloc(BLOCK_MAX_SIZE * ZIP_OVERHEAD_M);

    if (!tmp)
        goto scanner_resource_failure;
    uint32_t cap = BLOCK_MAX_SIZE * ZIP_OVERHEAD_M;
    buf = malloc(cap);

    if (!buf)
        goto scanner_resource_failure;
    int16_t curr_pos = 0;
    r->r_idx_b[bs]->b_offset[b] = final_block + 1;
    zbuff = malloc(BLOCK_MAX_SIZE * ZIP_OVERHEAD_M * 10);

    if (!zbuff)
        goto scanner_resource_failure;
    // TODO Кажется тут будет утечка?
    msg_file_info_t *file_info = NULL;

    if (data_check_enabled == 1)
        file_info = get_file_info(r, bs, h);
    uint16_t bsize_factor = r->r_idx_b[bs]->bsize_factor;
    uint32_t block_size = r->r_idx_b[bs]->block_size;
    chunk_buffer = malloc(CHUNK_SIZE);

    if (!chunk_buffer)
        goto scanner_resource_failure;
    uint64_t accumulated = 0;
    uint64_t current_chunk = 0;
    uint32_t rec_crc = 0;
    crc_buff = malloc(sizeof(rec_crc));

    if (!crc_buff)
        goto scanner_resource_failure;
    uint64_t count_recalculated_keys = 0;
    uint32_t wal_chk_result = 0;
    uint32_t size;
    bool data_state = true;
    bool logical_eof_found = false;
    uint32_t logical_final_block = final_block;

    for (uint32_t f = 0; f <= final_block; f++) {
        memset(buf, 0, block_size);
        uint64_t rw_off = (uint64_t)f * block_size;
        int32_t bytes_read = read_file(r->r_idx_b[bs]->fd[b], buf, rw_off, block_size, r, cid);

        if (data_check_enabled == 1 && r->r_need_data_chk) {
            memcpy(chunk_buffer + accumulated, buf, bytes_read);
            accumulated += bytes_read;

            if (accumulated == CHUNK_SIZE || (f == final_block && accumulated > 0)) {
                if (current_chunk >= file_info->num_blocks) {
                    fprintf(stderr, "%s ERROR [IDX_BUILDER=%015" PRIu64 "] Check current_chunk=%lu > file_info->num_blocks=%lu for region %s\n", curr_dt(dt), cid,
                            current_chunk, file_info->num_blocks, r->r_idx_b[bs]->fname);
                    r->stop = true;
                    r->bad_data = true;
                    free_index_scanner_buff(msg, msg_info1, msg_info2, tmp, buf, zbuff, crc_buff, chunk_buffer);
                    return false;
                }
                XXH128_hash_t hash = XXH3_128bits(chunk_buffer, accumulated);
                msg_hsh_info_t *hsh_info = file_info->msg_hsh_info[current_chunk];
                hsh_info->position = current_chunk;
                hsh_info->data_state = data_state;
                memcpy(hsh_info->xxh128, &hash, XXH128_SIZE);
                current_chunk++;

                if (accumulated > CHUNK_SIZE) {
                }
                accumulated = 0;
                data_state = true;

                if (log_level > 0) {
                    char xxh_str[XXH128_SIZE * 2 + 1] = {0};

                    for (int k = 0; k < XXH128_SIZE; k++)
                        snprintf(&xxh_str[k * 2], 3, "%02x", hsh_info->xxh128[k]);
                    xxh_str[XXH128_SIZE * 2] = '\0';
                    fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015" PRIu64 "] Calc chunk %lu XXH %s for region %s, data_state=%d\n", curr_dt(dt), cid, hsh_info->position,
                            xxh_str, r->r_idx_b[bs]->fname, hsh_info->data_state);
                }
            }
        }

        if (f == 0) {
            continue;
        }

        if (bytes_read >= (int32_t)sizeof(size)) {
            memcpy(&size, buf, sizeof(size));

            if (size == 0) {
                for (uint32_t i = sizeof(size); i < (uint32_t)bytes_read; ++i) {
                    if (buf[i] != 0) {
                        size = i;
                        fprintf(stderr, "%s WARN [IDX_BUILDER=%015" PRIu64 "] Found non-zero block=%u[%u] in %s\n", curr_dt(dt), cid, f, i, r->r_idx_b[bs]->fname);
                    }
                }
                continue;
            }
        } else if (bytes_read == 0 && f == final_block) {
            break;
        }
        uint64_t decompressed_size = 0;

        if (r->enable_block_compression) {
            int decompress_result = decompress_block(buf, bytes_read > 0 ? (size_t)bytes_read : 0, f, bs, r, &decompressed_size, 0, h);

            if (decompress_result == 0) {
                memcpy(&bytes_read, buf, sizeof(bytes_read));

                if (decompressed_size > cap) {
                    uint8_t *init_buff_tmp = realloc(buf, decompressed_size);

                    if (!init_buff_tmp) {
                        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Failed realloc init_buff to %lu\n", curr_dt(dt), h->cid, decompressed_size);
                        goto scanner_resource_failure;
                    } else {
                        buf = init_buff_tmp;
                        cap = decompressed_size;
                    }
                }
                memcpy(buf, r->bbuff, decompressed_size);
            } else {
                // TODO data_fix
                fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Failed init decompress_snappy result=%u\n", curr_dt(dt), h->cid, decompress_result);
                r->stop = true;
                r->bad_data = true;
                free_index_scanner_buff(msg, msg_info1, msg_info2, tmp, buf, zbuff, crc_buff, chunk_buffer);
                return false;
            }
        }
        uint32_t r_real_offset = 0;
        uint32_t r_zip_offset = 0;

        do {
            if (r->enable_block_compression) {
                r_real_offset = r_zip_offset * FIX_BSIZE_FACTOR;

                if (r_real_offset >= decompressed_size) {
                    r->r_idx_b[bs]->idx[b][f].z_index = calc_zip_record_size(r, bytes_read, bs);
                    break;
                }
            } else {
                if (rw_off + r_zip_offset * bsize_factor >= file_size)
                    break;
                r_real_offset = r_zip_offset * bsize_factor;
            }

            if (r->enable_block_compression == BLOCK_COMPRESSION_NONE && r->r_idx_b[bs]->fd[b] == r->data_000_fd && r->r_idx_b[bs]->block_size == FIX_BLOCK_SIZE &&
                is_append_logical_eof_marker(buf + r_real_offset, bytes_read > (int32_t)r_real_offset ? (size_t)bytes_read - r_real_offset : 0U)) {
                r->r_idx_b[bs]->idx[b][f].z_index = r_zip_offset;
                logical_eof_found = true;
                logical_final_block = f;

                if (log_level > 2)
                    fprintf(stderr,
                            "%s TRACE [APPEND_EOF=%015lu] stage=index_found uuid=%s file=%s "
                            "block=%u block_offset=%u absolute_offset=%lu z_index=%u bytes_read=%d\n",
                            curr_dt(dt), h->cid, r->uuid, r->r_idx_b[bs]->fname, f, r_real_offset, rw_off + r_real_offset, r_zip_offset, bytes_read);
                break;
            }

            if (msg->kv->key_size > 0) {
                free(msg->kv->key);
                msg->kv->key_size = 0;
            }

            if (msg->kv->cf_size > 0) {
                free(msg->kv->cf);
                msg->kv->cf_size = 0;
            }
            msg->kv->key = NULL;
            msg->kv->cf = NULL;
            curr_pos = get_key_from_msg_bytes_safety_checked(msg, buf + r_real_offset);

            if (curr_pos == KEY_EXTRACT_NOMEM || curr_pos == KEY_EXTRACT_INVALID_STATE) {
                stop_region_after_key_error(r, curr_pos, h);
                goto scanner_extraction_failure;
            }

            if (curr_pos < 0) {
                if (data_check_enabled) {
                    data_state = false;
                    set_region_recovery_info(r, 1000000L, h);
                    fprintf(stderr, "%s WARN [IDX_BUILDER=%015lu] Bad block %d in %s[%d] set %s for repair, r_zip_offset=%d, zip_val_size=%d, bs=%d, code=%d\n", curr_dt(dt),
                            cid, f, r->r_idx_b[bs]->fname, b, recovery_info_fn, r_zip_offset, zip_val_size, bs, curr_pos);
                } else
                    fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Bad block %d in %s[%d], r_zip_offset=%d, zip_val_size=%d, bs=%d, code=%d\n", curr_dt(dt), cid, f,
                            r->r_idx_b[bs]->fname, b, r_zip_offset, zip_val_size, bs, curr_pos);
                break;
            }
            uint32_t rec_size = msg->msg_size;

            if (msg->msg_size == 0) {
                for (int i = r_real_offset; i < block_size; i++) {
                    if (buf[i] != 0) {
                        if (i < r_real_offset + KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET)
                            fprintf(stderr, "%s WARN [IDX_BUILDER=%015lu] Bad end of block %u in %s, r_real_offset=%u, r_zip=%d, zip_val=%d, init_buff[%d]=%x\n", curr_dt(dt),
                                    cid, f, r->r_idx_b[bs]->fname, r_real_offset, r_zip_offset, zip_val_size, i, buf[i]);
                        r->stop = true;
                        r->bad_data = true;
                        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Data corruption detected in %s offset=%lu, cannot load index data, region stopped\n", curr_dt(dt),
                                h->cid, r->r_idx_b[bs]->fname, rw_off + r_real_offset);
                        free_index_scanner_buff(msg, msg_info1, msg_info2, tmp, buf, zbuff, crc_buff, chunk_buffer);
                        return false;
                    }
                }
                break;
            }

            if (r->enable_block_compression)
                zip_val_size = calc_zip_record_size_fix(msg->msg_size);
            else
                zip_val_size = calc_zip_record_size(r, rec_size, bs);

            if (r_zip_offset + zip_val_size > r->r_idx_b[bs]->block_size_zip && r->enable_block_compression == 0) {
                fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Iterate over block %d while building index for %s[%d], r_zip_offset=%d, zip_val_size=%d, bs=%d, rec_size=%d\n",
                        curr_dt(dt), cid, f, r->r_idx_b[bs]->fname, b, r_zip_offset, zip_val_size, bs, rec_size);
                r->stop = true;
                r->bad_data = true;
                free_index_scanner_buff(msg, msg_info1, msg_info2, tmp, buf, zbuff, crc_buff, chunk_buffer);
                return false;
            }

            if (r->enable_block_compression == BLOCK_COMPRESSION_NONE) {
                for (int i = r_real_offset + msg->msg_size; i < r_real_offset + zip_val_size * bsize_factor; i++) {
                    if (buf[i] != 0) {
                        fprintf(stderr, "%s WARN [IDX_BUILDER=%015lu] Bad end of record %u in %s, r_real_offset=%u, r_zip=%d, zip_val=%d, init_buff[%d]=%x\n", curr_dt(dt),
                                cid, f, r->r_idx_b[bs]->fname, r_real_offset, r_zip_offset, zip_val_size, i, buf[i]);
                        r->stop = true;
                        r->bad_data = true;
                        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Data corruption detected in %s offset=%lu, cannot load index data, region stopped\n", curr_dt(dt),
                                h->cid, r->r_idx_b[bs]->fname, rw_off + r_real_offset);
                        free_index_scanner_buff(msg, msg_info1, msg_info2, tmp, buf, zbuff, crc_buff, chunk_buffer);
                        return false;
                    }
                }
            }
            size_t version_available = r->enable_block_compression ? (size_t)decompressed_size : (bytes_read > 0 ? (size_t)bytes_read : 0);

            if (r_real_offset > version_available)
                goto scanner_version_failure;
            bool is_lob = false;
            uint32_t chunk_mark = 0;
            uint32_t data_offset = 0;

            if (msg->lob_info == 1) {
                disk_record_header_t header;

                if (!disk_record_decode_header(buf + r_real_offset, version_available - r_real_offset, &header))
                    goto scanner_version_failure;
                data_offset = header.qv_offset;
                restore_record_t inline_rec = {.bytes = buf + r_real_offset, .header = header, .bs = bs, .block = f};
                uint32_t inline_size = 0;
                uint8_t *payload = restore_record_payload(r, &inline_rec, &inline_size, h);

                if (!payload)
                    goto scanner_version_failure;
                free(payload);
            }
            int version_result = restore_versioned_record(r, buf + r_real_offset, version_available - r_real_offset, bs, f, r_real_offset, zip_val_size, msg, h);

            if (version_result < 0)
                goto scanner_version_failure;

            if (version_result > 0) {
                r_zip_offset += zip_val_size;
                continue;
            }
            r->has_digest_calc = false;

            if (build_idx_vars(r, msg, &bs, h) == -1) {
                while (r->idx_resizer != RESIZE_IDX_DONE)
                    usleep(100000);

                if (build_idx_vars(r, msg, &bs, h) == -1) {
                    fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Error code 104. Resize during building index for %s[%d], bs=%d\n", curr_dt(dt), cid, r->r_idx_b[bs]->fname,
                            b, bs);
                    r->stop = true;
                    r->bad_data = true;
                    finish_thread(r, 60, true, h);
                }
            }
            k_idx = r->r_idx_b[bs]->idx_r_help->k_idx;

            if (log_level > 3) {
                uint8_t *key_hex = extract_key(buf + r_real_offset);
                fprintf(stderr, "%s TRACE [IDX_BUILDER=%015lu] Ready add key=%s to index %s, bs=%d, k_idx=%d\n", curr_dt(dt), cid, key_hex, r->r_idx_b[bs]->fname, bs, k_idx);
                free(key_hex);
            }
            b = r->r_idx_b[bs]->idx_r_help->b;
            r_zip_offset += zip_val_size;
            bool double_key = false;
            uint32_t msg_size2_bkp = msg->msg_size;
            uint64_t insert_bbuff_size = 0;
            bool did_insert = false;
            uint32_t block_num_dbl = 0;
            uint32_t tail_size = 0;
            uint64_t result_decompress_lzma = 0;

            if (check_double_keys == 1) {
                msg->type = MSG_GET;
                uint8_t zip_info_orig = msg->zip_info;
                uint32_t msg_size_orig = msg->msg_size;

                for (int ibs = bs; ibs >= 0; ibs--) {
                    if (r->r_idx_b[ibs]->f_idx_curr_size[FBP] == 0)
                        continue;

                    if (build_idx_vars_old(r, msg, ibs, h) == false && msg->type == MSG_PUT) {
                        while (r->idx_resizer != RESIZE_IDX_DONE)
                            usleep(100000);

                        if (build_idx_vars_old(r, msg, ibs, h) == false) {
                            fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Error code 127. Failed insert to index for %s, bs=%d\n", curr_dt(dt), cid, r->r_idx_b[ibs]->fname,
                                    ibs);
                            r->stop = true;
                            finish_thread(r, 130, true, h);
                        }
                    }
                    uint32_t old_k_idx = r->r_idx_b[ibs]->idx_r_help->k_idx;

                    if (r->r_idx_b[bs]->idx[b][k_idx].c_index == r->r_idx_b[ibs]->idx[b][old_k_idx].c_index) {
                        if (r->r_idx_b[ibs]->idx[b][old_k_idx].o_index != 0) {
                            msg->msg_size = msg_size_orig;

                            if (msg->lob_info == 1) {
                                {
                                    size_t output_buffer_size;
                                    memcpy(&output_buffer_size, buf + r_real_offset + data_offset + 5, sizeof(size_t));

                                    if (msg->kv->qv_size != output_buffer_size) {
                                        fprintf(stderr, "%s WARN [RS_IDX=%015lu] Double lob candidate found in %s, size=%zu, qv_size=%u\n", curr_dt(dt), h->cid,
                                                r->r_idx_b[bs]->fname, output_buffer_size, msg->kv->qv_size);
                                    } else {
                                        if (reply->curr_max_msg_size < output_buffer_size) {
                                            if (realloc_resp_buff(reply, output_buffer_size, h->cid) < 0) {
                                                fprintf(stderr, "%s WARN [RS_IDX=%015lu] Failed to reallocate memory to %zu bytes for %s\n", curr_dt(dt), h->cid,
                                                        output_buffer_size, r->r_idx_b[bs]->fname);
                                                r->stop = true;
                                                r->bad_data = true;
                                                uint8_t *key_hex = extract_key(buf + r_real_offset);
                                                fprintf(stderr, "%s WARN [RS_IDX=%015lu] Error code 220. Cannot process %s, key=%s, r_real_offset=%d, block_num=%d\n",
                                                        curr_dt(dt), h->cid, key_hex, r->r_idx_b[bs]->fname, r_real_offset, f);
                                                free(key_hex);
                                                data_state = false;
                                                continue;
                                            }
                                        }
                                    }
                                    memcpy(reply->resp_buff, buf + r_real_offset, msg->msg_size);
                                }
                                uint64_t out_size = 0;
                                result_decompress_lzma = decompress_lzma(reply, data_offset, msg->msg_size, r, &out_size, cid);

                                if (result_decompress_lzma != 0) {
                                    msg->msg_size = msg_size2_bkp;
                                    fprintf(stderr, "%s WARN [RS_IDX=%015lu] Double lob candidate found in %s, result_decompress_lzma=%lu, set msg_size=%u\n", curr_dt(dt),
                                            h->cid, r->r_idx_b[bs]->fname, result_decompress_lzma, msg->msg_size);
                                } else {
                                    did_insert = true;
                                    msg->msg_size = out_size + data_offset;

                                    if (decompressed_size == 0)
                                        decompressed_size = BLOCK_MAX_SIZE;
                                    tail_size = decompressed_size - (r_real_offset + data_offset);
                                    insert_bbuff_size = r->bbuff_size;

                                    if (cap < r_real_offset + data_offset + insert_bbuff_size + tail_size) {
                                        uint64_t required = (uint64_t)r_real_offset + data_offset + insert_bbuff_size + tail_size;

                                        if (required > UINT32_MAX || required > SIZE_MAX)
                                            goto scanner_resource_failure;
                                        uint8_t *init_buff_tmp = realloc(buf, (size_t)required);

                                        if (!init_buff_tmp)
                                            goto scanner_resource_failure;
                                        buf = init_buff_tmp;
                                        cap = (uint32_t)required;
                                    }

                                    if (r->zbuff_size < msg->msg_size) {
                                        uint64_t required = (uint64_t)msg->msg_size + FIX_BSIZE_FACTOR;

                                        if (required > SIZE_MAX)
                                            goto scanner_resource_failure;
                                        uint8_t *zbuff_tmp = realloc(r->zbuff, (size_t)required);

                                        if (!zbuff_tmp)
                                            goto scanner_resource_failure;
                                        r->zbuff = zbuff_tmp;
                                        r->zbuff_size = required;
                                    }
                                    memmove(buf + r_real_offset + data_offset + insert_bbuff_size, buf + r_real_offset + data_offset, tail_size);
                                    memcpy(buf + r_real_offset + data_offset, r->bbuff, r->bbuff_size);
                                }
                            }
                            msg->zip_info = zip_info_orig;
                            block_num_dbl = r->r_idx_b[ibs]->idx[b][old_k_idx].o_index;
                            double_key = true;

                            if (result_decompress_lzma == 0)
                                extract_max_ts(r, msg, buf + r_real_offset, msg_info2, h);
                            else {
                                msg_info2->max_ts = 0;
                                msg_info2->qfl_count = 0;
                                msg_info2->qv_size = 0;
                            }

                            if (log_level > 1)
                                fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Candidate block_dbl=%u for %s, result_decompress_lzma=%lu\n", curr_dt(dt), cid, block_num_dbl,
                                        r->r_idx_b[ibs]->fname, result_decompress_lzma);
                            msg->type = MSG_GET_DOUBLE;
                            msg->zip_info = 0;
                            msg->msg_size = KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET + msg->kv->key_size + msg->kv->cf_size;
                            memcpy(tmp, buf + r_real_offset, msg->msg_size);
                            int result = get_put(r, block_num_dbl, msg, tmp, h, -1, ibs);

                            if (result == GET_PUT_EXTRACT_ERROR)
                                goto scanner_extraction_failure;

                            if (result == UPDATE_COLLISION) {
                                msg->type = MSG_CHK_DOUBLE;
                                int c = get_upd_coll(r, msg, tmp, h, ibs, true, k_idx);

                                if (c == GET_PUT_EXTRACT_ERROR)
                                    goto scanner_extraction_failure;

                                if (c != GET_DOUBLE_NOT_FOUND_IN_COLL) {
                                    block_num_dbl = 0;
                                    memcpy(&block_num_dbl, r->r_idx_b[ibs]->coll_keeper[b] + c + ((uint32_t)DIGEST_SIZE_FULL), ((uint32_t)OFFSET_COLL_KEEPER_SIZE));
                                } else
                                    double_key = false;
                            }

                            if (double_key) {
                                bs_dbl = ibs;
                                break;
                            }
                        }
                    }
                }
                msg->type = MSG_PUT;
            }

            if (!double_key) {
                add_key_to_idx(r, buf + r_real_offset, bs, f, zip_val_size, msg, h);
            } else {
                union i2b_u i2b;
                duplicates++;

                if (log_level > 1) {
                    char key[msg->kv->key_size * 3 + 1];
                    char *ptr = key;

                    for (int i = 0; i < msg->kv->key_size; i++)
                        ptr += sprintf(ptr, "%x_", msg->kv->key[i]);

                    if (ptr != key)
                        *(ptr - 1) = '\0';
                    fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Found double key=%s in %s (block=%u, block_dbl=%u, bs=%d, bs_dbl=%d, z1=%u, z2=%u)\n", curr_dt(dt), cid, key,
                            r->r_idx_b[bs]->fname, f, block_num_dbl, bs, bs_dbl, r->r_idx_b[bs]->idx[b][f].z_index, r->r_idx_b[bs_dbl]->idx[b][block_num_dbl].z_index);
                }
                msg->type = MSG_GET_DOUBLE;
                msg->zip_info = 0;
                msg->msg_size = KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET + msg->kv->key_size + msg->kv->cf_size;
                memcpy(tmp, buf + r_real_offset, msg->msg_size);
                uint8_t z_bkp = r->r_idx_b[bs_dbl]->idx[b][block_num_dbl].z_index;
                int result = get_put(r, block_num_dbl, msg, tmp, h, -1, bs_dbl);

                if (result == GET_PUT_EXTRACT_ERROR)
                    goto scanner_extraction_failure;

                if (result == UPDATE_COLLISION) {
                    result = get_upd_coll(r, msg, tmp, h, bs_dbl, true, k_idx); // TODO Check result

                    if (result == GET_PUT_EXTRACT_ERROR)
                        goto scanner_extraction_failure;

                    if (result != UPDATE_OK) {
                        r->stop = true;
                        r->bad_data = true;
                        free_index_scanner_buff(msg, msg_info1, msg_info2, tmp, buf, zbuff, crc_buff, chunk_buffer);
                        return false;
                    }
                }
                bool identical_earlier_record = false;
                uint8_t bs_del = 0;
                uint32_t block_del = 0;
                result_decompress_lzma = 0;
                uint64_t out_size = 0;

                if (bs == bs_dbl && block_num_dbl < f && r->enable_block_compression == BLOCK_COMPRESSION_NONE && r->r_idx_b[bs]->fd[b] == r->data_000_fd &&
                    r->r_idx_b[bs_dbl]->fd[b] == r->data_000_fd && r->r_idx_b[bs]->block_size == FIX_BLOCK_SIZE && r->r_idx_b[bs_dbl]->block_size == FIX_BLOCK_SIZE &&
                    buf[r_real_offset + offsetof(msg_cli_t, lob_info)] == 0)
                    identical_earlier_record = has_identical_serialized_record(r, buf + r_real_offset, msg_size2_bkp, bs_dbl, z_bkp);

                if (r->msg_get_put->lob_info == 1) {
                    data_offset = KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET + msg->kv->key_size + msg->kv->cf_size;
                    result_decompress_lzma = decompress_lzma(reply, data_offset, reply->msg_size, r, &out_size, cid);

                    if (result_decompress_lzma == 0) {
                        reply->msg_size = out_size + data_offset;

                        if (reply->curr_max_msg_size < reply->msg_size + FIX_BSIZE_FACTOR) {
                            if (realloc_resp_buff(reply, reply->msg_size, h->cid) < 0) {
                                r->stop = true;
                                r->bad_data = true;
                                free_index_scanner_buff(msg, msg_info1, msg_info2, tmp, buf, zbuff, crc_buff, chunk_buffer);
                                return false;
                            }
                        }
                        memcpy(reply->resp_buff + data_offset, r->bbuff, out_size);

                        if (r->zbuff_size < (uint64_t)reply->msg_size + FIX_BSIZE_FACTOR) {
                            uint64_t required = (uint64_t)reply->msg_size + FIX_BSIZE_FACTOR;

                            if (required > SIZE_MAX)
                                goto scanner_resource_failure;
                            uint8_t *zbuff_tmp = realloc(r->zbuff, (size_t)required);

                            if (!zbuff_tmp)
                                goto scanner_resource_failure;
                            r->zbuff = zbuff_tmp;
                            r->zbuff_size = required;
                        }
                    }
                }

                if (result_decompress_lzma != 0) {
                    fprintf(stderr, "%s WARN [IDX_BUILDER=%015lu] Result %lu for decompress_lzma for block_num_dbl=%u\n", curr_dt(dt), cid, result_decompress_lzma,
                            block_num_dbl);
                    block_del = block_num_dbl;
                    bs_del = bs_dbl;
                } else {
                    uint8_t new_version1 = reply->resp_buff[NEW_VERSION_RECORD_OFFSET];
                    uint8_t new_version2 = buf[r_real_offset + NEW_VERSION_RECORD_OFFSET];
                    msg->msg_size = reply->msg_size;
                    extract_max_ts(r, msg, reply->resp_buff, msg_info1, h);
                    const char *duplicate_reason = NULL;

                    if (r->r_idx_b[bs_dbl]->block_size > r->r_idx_b[bs]->block_size) {
                        block_del = f;
                        bs_del = bs;
                        duplicate_reason = "larger_block1";
                    } else if (r->r_idx_b[bs]->block_size > r->r_idx_b[bs_dbl]->block_size) {
                        block_del = block_num_dbl;
                        bs_del = bs_dbl;
                        duplicate_reason = "larger_block2";
                    } else if (msg_info1->max_ts > msg_info2->max_ts) {
                        block_del = f;
                        bs_del = bs;
                        duplicate_reason = "max_ts1";
                    } else if (msg_info2->max_ts > msg_info1->max_ts) {
                        block_del = block_num_dbl;
                        bs_del = bs_dbl;
                        duplicate_reason = "max_ts2";
                    } else if (new_version1 > new_version2) {
                        block_del = f;
                        bs_del = bs;
                        duplicate_reason = "new_version1";
                    } else if (new_version2 > new_version1) {
                        block_del = block_num_dbl;
                        bs_del = bs_dbl;
                        duplicate_reason = "new_version2";
                    } else if (msg_info1->qfl_count > msg_info2->qfl_count) {
                        block_del = f;
                        bs_del = bs;
                        duplicate_reason = "qfl_count1";
                    } else if (msg_info2->qfl_count > msg_info1->qfl_count) {
                        block_del = block_num_dbl;
                        bs_del = bs_dbl;
                        duplicate_reason = "qfl_count2";
                    } else if (msg_info1->qv_size > msg_info2->qv_size) {
                        block_del = f;
                        bs_del = bs;
                        duplicate_reason = "qv_size1";
                    } else if (msg_info2->qv_size > msg_info1->qv_size) {
                        block_del = block_num_dbl;
                        bs_del = bs_dbl;
                        duplicate_reason = "qv_size2";
                    } else if (identical_earlier_record) {
                        block_del = f;
                        bs_del = bs;
                        duplicate_reason = "identical_earlier_record";
                    } else if (block_num_dbl > f) {
                        block_del = f;
                        bs_del = bs;
                        duplicate_reason = "physical_order1";
                    } else {
                        block_del = block_num_dbl;
                        bs_del = bs_dbl;
                        duplicate_reason = "physical_order2";
                    }

                    if (log_level > 1) {
                        fprintf(stderr,
                                "%s WARN [IDX_BUILDER=%015lu] Duplicate decision for %s: "
                                "block1=%u, bs1=%u, version1=%u, ts1=%lu, "
                                "qfl_count1=%u, qv_size1=%u; "
                                "block2=%u, bs2=%u, version2=%u, ts2=%lu, "
                                "qfl_count2=%u, qv_size2=%u; "
                                "block_del=%u, bs_del=%u, reason=%s\n",
                                curr_dt(dt), cid, r->r_idx_b[bs]->fname, block_num_dbl, bs_dbl, new_version1, msg_info1->max_ts, msg_info1->qfl_count, msg_info1->qv_size, f,
                                bs, new_version2, msg_info2->max_ts, msg_info2->qfl_count, msg_info2->qv_size, block_del, bs_del, duplicate_reason);
                    }

                    if (bs_del == bs && block_del == f && (bs != bs_dbl || f != block_num_dbl))
                        msg->msg_size = msg_size2_bkp;
                    else
                        msg->msg_size = reply->msg_size_zip;
                    zip_val_size_del = calc_zip_record_size(r, msg->msg_size, bs_del);

                    if (log_level > 1) {
                        fprintf(stderr,
                                "%s INFO [IDX_BUILDER=%015lu] State before sync, f=%d, block_del=%d, o_index=%d, z_index[f]=%d, z_index[dbl]=%d, z_index[block_del]=%d, "
                                "zip_val_size=%d, zip_val_size_del=%d\n",
                                curr_dt(dt), cid, f, block_del, block_num_dbl, r->r_idx_b[bs]->idx[b][f].z_index, r->r_idx_b[bs_dbl]->idx[b][block_num_dbl].z_index,
                                r->r_idx_b[bs_del]->idx[b][block_del].z_index, zip_val_size, zip_val_size_del);
                    }
                    z_bkp = r->r_idx_b[bs_del]->idx[b][block_del].z_index;
                    r->r_idx_b[bs_del]->idx[b][block_del].z_index = 255;
                    result = get_put(r, block_del, msg, buf + r_real_offset, h, -1, bs_del);
                    r->r_idx_b[bs_del]->idx[b][block_del].z_index = z_bkp;

                    if (result == GET_PUT_EXTRACT_ERROR)
                        goto scanner_extraction_failure;

                    if (result == UPDATE_COLLISION) {
                        fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Get double collision, k_idx=%d\n", curr_dt(dt), cid, k_idx);
                        msg->type = MSG_GET_DOUBLE;
                        int c = get_upd_coll(r, msg, tmp, h, bs_del, true, k_idx);

                        if (c == GET_PUT_EXTRACT_ERROR)
                            goto scanner_extraction_failure;
                        memcpy(i2b.byte, r->r_idx_b[bs_del]->coll_keeper[b] + c + ((uint32_t)DIGEST_SIZE_FULL), ((uint32_t)OFFSET_COLL_KEEPER_SIZE));
                        block_del = i2b.integer_val;
                        result = UPDATE_OK;
                    }
                }

                if (identical_earlier_record || bs != bs_dbl || f != block_del || msg_info1->max_ts != msg_info2->max_ts || msg_info1->qfl_count != msg_info2->qfl_count) {
                    if (log_level > 1) {
                        fprintf(stderr,
                                "%s INFO [IDX_BUILDER=%015lu] State after sync, f=%d, block_del=%d, o_index=%d, z_index[f]=%d, z_index[dbl]=%d, z_index[block_del]=%d, "
                                "zip_val_size=%d\n",
                                curr_dt(dt), cid, f, block_del, block_num_dbl, r->r_idx_b[bs]->idx[b][f].z_index, r->r_idx_b[bs_dbl]->idx[b][block_num_dbl].z_index,
                                r->r_idx_b[bs_del]->idx[b][block_del].z_index, zip_val_size);
                    }

                    if (result != UPDATE_OK) {
                        r->stop = true;
                        r->bad_data = true;
                        free_index_scanner_buff(msg, msg_info1, msg_info2, tmp, buf, zbuff, crc_buff, chunk_buffer);
                        return false;
                    }
                    bool defer_duplicate_to_compaction = APPEND_COMPACTION_ENABLED && block_del != 0 && zip_val_size_del != 0 && msg->lob_info == 0 &&
                                                         r->r_idx_b[bs_del]->fd[b] == r->data_000_fd && r->r_idx_b[bs_del]->block_size == FIX_BLOCK_SIZE &&
                                                         r->enable_block_compression == BLOCK_COMPRESSION_NONE && prepare_append_compaction_dirty_map(r, block_del, cid);

                    if (defer_duplicate_to_compaction) {
                        mark_append_compaction_segment(r, block_del, (uint64_t)zip_val_size_del * r->r_idx_b[bs_del]->bsize_factor, bs_del);
                    } else {
                        add_double_key(r, msg, bs_del, block_del, h);
                    }

                    if (bs == bs_del)
                        if (f > block_del) {
                            oc_index_corrector(r, b, msg, f, k_idx, bs, h);
                        }

                    if (bs != bs_del) {
                        del_from_index(r, block_del, msg, h, -1, bs_del);
                        add_key_to_idx(r, buf + r_real_offset, bs, f, zip_val_size, msg, h);
                    } else
                        r->r_idx_b[bs]->idx[b][f].z_index += zip_val_size;
                } else {
                    add_key_to_idx(r, buf + r_real_offset, bs, f, zip_val_size, msg, h);
                    fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] False double in block=%d, skip\n", curr_dt(dt), cid, f);
                }
            }

            if (msg->lob_info == 1 && did_insert) {
                memmove(buf + r_real_offset + data_offset, buf + r_real_offset + data_offset + insert_bbuff_size, tail_size);
                memset(buf + r_real_offset + data_offset + tail_size, 0, insert_bbuff_size);

                if (log_level > 2)
                    fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Data moved in %s offset=%lu, data_offset=%u, tail_size=%u\n", curr_dt(dt), h->cid, r->r_idx_b[bs]->fname,
                            rw_off + r_real_offset, data_offset, tail_size);
            }
        } while (true);
        uint32_t void_pos = VOIDS - 1 - r->r_idx_b[bs]->idx[b][f].z_index; // TODO Надо ли делать -1?

        if (!logical_eof_found && f + 1 < final_block) {
            add_void(r, b, void_pos, f, bs);
        }

        if (r->r_idx_b[bs]->fd[b] == r->data_000_fd && r->r_idx_b[bs]->block_size == FIX_BLOCK_SIZE) {
            uint64_t occupied_bytes = (uint64_t)r->r_idx_b[bs]->idx[b][f].z_index * r->r_idx_b[bs]->bsize_factor;

            if (UINT64_MAX - r->append_compact_occupied_bytes < occupied_bytes)
                r->append_compact_occupied_bytes = UINT64_MAX;
            else
                r->append_compact_occupied_bytes += occupied_bytes;
        }

        if (logical_eof_found)
            break;
    }
    free_index_scanner_buff(msg, msg_info1, msg_info2, tmp, buf, zbuff, crc_buff, chunk_buffer);
    r->r_idx_b[bs]->b_offset[b] = file_size == 0 ? 0 : logical_final_block;

    if (b == FBP)
        (void)mark_append_compaction_free_space(r, 1, r->r_idx_b[bs]->b_offset[b], bs, cid);

    if (logical_eof_found)
        atomic_store(&r->append_compact_truncate_pending, true);
    fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Index built for %s[%d] with %ld elements from %ld (%f), %d collisions, b_offset=%u, fsize=%lu\n", curr_dt(dt), cid,
            r->r_idx_b[bs]->fname, b, r->r_idx_b[bs]->f_idx_curr_size[b], r->r_idx_b[bs]->f_idx_size[b],
            (double)((double)r->r_idx_b[bs]->f_idx_curr_size[b] / (double)r->r_idx_b[bs]->f_idx_size[b]), r->r_idx_b[bs]->coll_count_curr[b], r->r_idx_b[bs]->b_offset[b],
            file_size);
    fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Double keys count=%lu, count_recalculated_keys=%lu, for index %s[%d], bs=%d\n", curr_dt(dt), cid, duplicates,
            count_recalculated_keys, r->r_idx_b[bs]->fname, b, bs);
    getrusage(RUSAGE_SELF, &r_usage);
    sysinfo(&info);
    double usage_mem = (double)r_usage.ru_maxrss;
    double total_mem = (double)info.totalram / 1024;
    double share_mem_usage = usage_mem / total_mem * 100;
    fprintf(stderr, "%s  INFO [IDX_BUILDER=%015lu] Region memory usage %s = %d kb (usage_mem=%.0lf, total_mem=%.0lf, share_mem_usage=%.2lf)\n", curr_dt(dt), cid,
            r->r_idx_b[bs]->fname, (int32_t)(usage_mem - usage_mem_before), usage_mem, total_mem, share_mem_usage);
    return true;
scanner_version_failure:
    fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Cannot restore record state in %s; region stopped\n", curr_dt(dt), cid, r->r_idx_b[bs]->fname);
    stop_region_after_key_error(r, KEY_EXTRACT_INVALID_STATE, h);
    goto scanner_extraction_failure;
scanner_resource_failure:
    stop_region_after_key_error(r, KEY_EXTRACT_NOMEM, h);
scanner_extraction_failure:
    free_index_scanner_buff(msg, msg_info1, msg_info2, tmp, buf, zbuff, crc_buff, chunk_buffer);
    return false;
}

static size_t normalize_record_target_count(size_t record_target_count) {
    if (record_target_count > 0 && record_target_count <= ((size_t)1 << 16) + IDX_OVERHEAD_MAX)
        record_target_count = ((size_t)1 << 16) + IDX_OVERHEAD_MAX;

    if (record_target_count > ((size_t)1 << 16) + IDX_OVERHEAD_MAX && record_target_count <= ((size_t)1 << 17) + IDX_OVERHEAD_MAX)
        record_target_count = ((size_t)1 << 17) + IDX_OVERHEAD_MAX;

    if (record_target_count > ((size_t)1 << 17) + IDX_OVERHEAD_MAX && record_target_count <= ((size_t)1 << 18) + IDX_OVERHEAD_MAX)
        record_target_count = ((size_t)1 << 18) + IDX_OVERHEAD_MAX;

    if (record_target_count > ((size_t)1 << 18) + IDX_OVERHEAD_MAX && record_target_count <= ((size_t)1 << 19) + IDX_OVERHEAD_MAX)
        record_target_count = ((size_t)1 << 19) + IDX_OVERHEAD_MAX;

    if (record_target_count > ((size_t)1 << 19) + IDX_OVERHEAD_MAX && record_target_count <= ((size_t)1 << 20) + IDX_OVERHEAD_MAX)
        record_target_count = ((size_t)1 << 20) + IDX_OVERHEAD_MAX;

    if (record_target_count > ((size_t)1 << 20) + IDX_OVERHEAD_MAX && record_target_count <= ((size_t)1 << 21) + IDX_OVERHEAD_MAX)
        record_target_count = ((size_t)1 << 21) + IDX_OVERHEAD_MAX;

    if (record_target_count > ((size_t)1 << 21) + IDX_OVERHEAD_MAX && record_target_count <= ((size_t)1 << 22) + IDX_OVERHEAD_MAX)
        record_target_count = ((size_t)1 << 22) + IDX_OVERHEAD_MAX;

    if (record_target_count > ((size_t)1 << 22) + IDX_OVERHEAD_MAX && record_target_count <= ((size_t)1 << 23) + IDX_OVERHEAD_MAX)
        record_target_count = ((size_t)1 << 23) + IDX_OVERHEAD_MAX;

    if (record_target_count > ((size_t)1 << 23) + IDX_OVERHEAD_MAX && record_target_count <= ((size_t)1 << 24) + IDX_OVERHEAD_MAX)
        record_target_count = ((size_t)1 << 24) + IDX_OVERHEAD_MAX;

    if (record_target_count > ((size_t)1 << 24) + IDX_OVERHEAD_MAX && record_target_count <= ((size_t)1 << 25) + IDX_OVERHEAD_MAX)
        record_target_count = ((size_t)1 << 25) + IDX_OVERHEAD_MAX;

    if (record_target_count > ((size_t)1 << 25) + IDX_OVERHEAD_MAX && record_target_count <= ((size_t)1 << 26) + IDX_OVERHEAD_MAX)
        record_target_count = ((size_t)1 << 26) + IDX_OVERHEAD_MAX;

    if (record_target_count > ((size_t)1 << 26) + IDX_OVERHEAD_MAX)
        record_target_count = ((size_t)1 << 27) + IDX_OVERHEAD_MAX;
    return record_target_count;
}

bool idx_alloc(struct region_info_t *r_info, uint8_t b, bool is_create, uint8_t bs, uint64_t file_size, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    uint64_t cid = t_handler->cid;

    if (is_create) {
        if (rebuild_index == 0) {
            // TODO Избавиться от этого
            union i2b_u i2b_f_idx_size;
            read_file(r_info->r_idx_b[bs]->fd[b], i2b_f_idx_size.byte, 0, sizeof(i2b_f_idx_size.byte), r_info, cid);

            if (i2b_f_idx_size.integer_val != 0) {
                r_info->r_idx_b[bs]->f_idx_size[b] = i2b_f_idx_size.integer_val;
                fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Load index size=%ld\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->f_idx_size[b]);
            }
            // TODO Not used now
            union s2b_u s2b_idx_overhead;
            read_file(r_info->r_idx_b[bs]->fd[b], s2b_idx_overhead.byte, sizeof(i2b_f_idx_size.byte), sizeof(s2b_idx_overhead.byte), r_info, cid);

            if (s2b_idx_overhead.short_val != 0)
                fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Index overhead=%d\n", curr_dt(dt), cid, s2b_idx_overhead.short_val);
        } else {
            struct msg_qv_t qv = {0};
            msg_kv_t kv = {.qv = &qv};
            msg_cli_t message = {.kv = &kv};
            msg_cli_t *msg_get = &message;
            fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Start to calculate index size for %s[%d]\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname, b);
            uint32_t zip_val_size = -1;
            double key_count = 0;
            double key_size_count = 0;
            uint32_t final_block = file_size / r_info->r_idx_b[bs]->block_size;
            uint16_t bsize_factor = r_info->r_idx_b[bs]->bsize_factor;
            uint32_t init_buff_size = FIX_MAX_MSG_SIZE * 16 + MAX_BSIZE_FACTOR;
            uint8_t *init_buff = malloc(FIX_MAX_MSG_SIZE * 16 + MAX_BSIZE_FACTOR);

            if (init_buff == NULL) {
                stop_region_after_key_error(r_info, KEY_EXTRACT_NOMEM, t_handler);
                return false;
            }
            bool logical_eof_found = false;

            for (uint32_t f = 1; f <= final_block; f++) {
                uint64_t rw_off = (uint64_t)f * r_info->r_idx_b[bs]->block_size;
                int32_t bytes_read = read_file(r_info->r_idx_b[bs]->fd[b], init_buff, rw_off, r_info->r_idx_b[bs]->block_size, r_info, cid);
                uint64_t decompressed_size = 0;

                if (r_info->enable_block_compression) {
                    if (bytes_read > 0) {
                        const size_t input_size = (size_t)bytes_read;

                        if (input_size >= sizeof(bytes_read))
                            memcpy(&bytes_read, init_buff, sizeof(bytes_read));

                        if (bytes_read != 0) {
                            int decompress_result = decompress_block(init_buff, input_size, f, bs, r_info, &decompressed_size, 0, t_handler);

                            if (decompress_result == 0) {
                                if (decompressed_size > init_buff_size) {
                                    uint8_t *init_buff_tmp = realloc(init_buff, decompressed_size);

                                    if (init_buff_tmp == NULL) {
                                        fprintf(stderr, "%s WARN [IDX_BUILDER=%015lu] Failed realloc init_buff to %lu\n", curr_dt(dt), cid, decompressed_size);
                                        clear_extracted_key_fields(msg_get);
                                        free(init_buff);
                                        stop_region_after_key_error(r_info, KEY_EXTRACT_NOMEM, t_handler);
                                        return false;
                                    } else {
                                        init_buff = init_buff_tmp;
                                        init_buff_size = decompressed_size;
                                    }
                                }
                                memcpy(init_buff, r_info->bbuff, decompressed_size);
                            } else {
                                // TODO data_fix
                                fprintf(stderr, "%s WARN [IDX_BUILDER=%015lu] Failed call index, init decompress_snappy result=%u\n", curr_dt(dt), cid, decompress_result);
                                clear_extracted_key_fields(msg_get);
                                free(init_buff);
                                r_info->stop = true;
                                r_info->bad_data = true;
                                return false;
                            }
                        }
                    }
                }
                uint32_t r_zip_offset = 0;
                uint32_t r_real_offset = 0;

                do {
                    if (r_info->enable_block_compression) {
                        r_real_offset = r_zip_offset * FIX_BSIZE_FACTOR;

                        if (r_real_offset >= decompressed_size) {
                            break;
                        }
                    } else {
                        if (rw_off + r_zip_offset * bsize_factor >= file_size)
                            break;
                        r_real_offset = r_zip_offset * bsize_factor;
                    }

                    if (r_info->enable_block_compression == BLOCK_COMPRESSION_NONE && r_info->r_idx_b[bs]->fd[b] == r_info->data_000_fd &&
                        r_info->r_idx_b[bs]->block_size == FIX_BLOCK_SIZE &&
                        is_append_logical_eof_marker(init_buff + r_real_offset, bytes_read > (int32_t)r_real_offset ? (size_t)bytes_read - r_real_offset : 0U)) {
                        logical_eof_found = true;
                        break;
                    }

                    if (msg_get->kv->key_size > 0) {
                        free(msg_get->kv->key);
                        msg_get->kv->key_size = 0;
                    }

                    if (msg_get->kv->cf_size > 0) {
                        free(msg_get->kv->cf);
                        msg_get->kv->cf_size = 0;
                    }
                    kv.key = NULL;
                    kv.cf = NULL;
                    int extract_result = get_key_from_msg_bytes_checked(msg_get, init_buff + r_real_offset, cid);

                    if (extract_result < 0) {
                        clear_extracted_key_fields(msg_get);
                        free(init_buff);
                        stop_region_after_key_error(r_info, extract_result, t_handler);
                        return false;
                    }
                    uint32_t rec_size = msg_get->msg_size;

                    if (msg_get->msg_size == 0) {
                        break;
                    }

                    if (msg_get->kv->key_size == 0) {
                        f = final_block + 1;
                        break;
                    }
                    key_count++;
                    key_size_count += msg_get->msg_size;

                    if (key_count >= rebuild_index) {
                        f = final_block + 1;
                        break;
                    }

                    if (r_info->enable_block_compression)
                        zip_val_size = calc_zip_record_size_fix(msg_get->msg_size);
                    else
                        zip_val_size = calc_zip_record_size(r_info, rec_size, bs);

                    if (r_info->enable_block_compression == BLOCK_COMPRESSION_NONE) {
                        if (r_zip_offset + zip_val_size > r_info->r_idx_b[bs]->block_size_zip) {
                            fprintf(stderr,
                                    "%s ERROR [IDX_BUILDER=%015lu] Error code 108. Iterate over block %d while building index for %s[%d], r_zip_offset=%d, zip_val_size=%d, "
                                    "bs=%d, rec_size=%d\n",
                                    curr_dt(dt), cid, f, r_info->r_idx_b[bs]->fname, b, r_zip_offset, zip_val_size, bs, rec_size);
                            r_info->stop = true;
                            r_info->bad_data = true;
                            finish_thread(r_info, 108, true, t_handler);
                        }
                    }
                    r_zip_offset += zip_val_size;
                } while (true);

                if (logical_eof_found)
                    break;
            }
            clear_extracted_key_fields(msg_get);
            free(init_buff);
            fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Count %d keys, total size %d\n", curr_dt(dt), cid, (int)key_count, (int)key_size_count);

            if (key_size_count > 0) {
                double avg_msg_size = key_size_count / key_count;
                uint64_t total_keys_calc = file_size / avg_msg_size;
                fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] total_keys_calc=%lu keys, avg_msg_size=%lf\n", curr_dt(dt), cid, total_keys_calc, avg_msg_size);
                r_info->r_idx_b[bs]->f_idx_size[b] = normalize_record_target_count(total_keys_calc);
                fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Region %s f_idx_size=%lu\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname, r_info->r_idx_b[bs]->f_idx_size[b]);
            }
        }

        if (r_info->record_target_count > r_info->r_idx_b[bs]->f_idx_size[b])
            r_info->r_idx_b[bs]->f_idx_size[b] = normalize_record_target_count(r_info->record_target_count);
    }

    if (r_info->record_target_count > r_info->r_idx_b[bs]->f_idx_size[b] && r_info->is_leader && r_info->r_idx_b[bs]->bsize_factor == FIX_BSIZE_FACTOR)
        r_info->r_idx_b[bs]->f_idx_size[b] = normalize_record_target_count(r_info->record_target_count);
    r_info->r_idx_b[bs]->idx_mask[b] = r_info->r_idx_b[bs]->f_idx_size[b] - IDX_OVERHEAD_MAX - 1;
    fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] mask size = %u\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->idx_mask[b]);
    r_info->r_idx_b[bs]->idx[b] =
        (struct idx_t *)index_memory_malloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.idx[b], r_info->r_idx_b[bs]->f_idx_size[b] * sizeof(struct idx_t));

    if (r_info->r_idx_b[bs]->idx[b] == NULL) {
        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Malloc r_info->r_idx_b[bs]->idx[%d] == NULL\n", curr_dt(dt), cid, b);
        return false;
    }
    memset(r_info->r_idx_b[bs]->idx[b], 0, r_info->r_idx_b[bs]->f_idx_size[b] * sizeof(struct idx_t));

    if (r_info->r_idx_b[bs]->f_idx_size[b] < pow(2, 16)) {
        r_info->r_idx_b[bs]->k1_index[b] =
            index_memory_malloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k1_index[b], r_info->r_idx_b[bs]->f_idx_size[b] * sizeof(uint8_t));

        if (r_info->r_idx_b[bs]->k1_index[b] == NULL) {
            fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Malloc r_info->r_idx_b[bs]->k1_index[%d] == NULL\n", curr_dt(dt), cid, b);
            index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.idx[b], r_info->r_idx_b[bs]->idx[b]);
            r_info->r_idx_b[bs]->idx[b] = NULL;
            return false;
        }
        memset(r_info->r_idx_b[bs]->k1_index[b], 0, r_info->r_idx_b[bs]->f_idx_size[b] * sizeof(uint8_t));
    } else
        r_info->r_idx_b[bs]->k1_index[b] = NULL;

    if (r_info->r_idx_b[bs]->f_idx_size[b] < pow(2, 24)) {
        r_info->r_idx_b[bs]->k2_index[b] =
            index_memory_malloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k2_index[b], r_info->r_idx_b[bs]->f_idx_size[b] * sizeof(uint8_t));

        if (r_info->r_idx_b[bs]->k2_index[b] == NULL) {
            fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Malloc r_info->r_idx_b[bs]->k2_index[%d] == NULL\n", curr_dt(dt), cid, b);
            index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.idx[b], r_info->r_idx_b[bs]->idx[b]);
            r_info->r_idx_b[bs]->idx[b] = NULL;
            index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k1_index[b], r_info->r_idx_b[bs]->k1_index[b]);
            r_info->r_idx_b[bs]->k1_index[b] = NULL;
            return false;
        }
        memset(r_info->r_idx_b[bs]->k2_index[b], 0, r_info->r_idx_b[bs]->f_idx_size[b] * sizeof(uint8_t));
    } else
        r_info->r_idx_b[bs]->k2_index[b] = NULL;
    r_info->r_idx_b[bs]->coll_count_tot[b] = INIT_COLL_KEEPER;
    r_info->r_idx_b[bs]->coll_keeper[b] = (uint8_t *)index_memory_malloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.coll_keeper[b],
                                                                         ((uint32_t)INIT_COLL_KEEPER) * ((uint32_t)COLL_KEEPER_SIZE) * sizeof(uint8_t));

    if (r_info->r_idx_b[bs]->coll_keeper[b] == NULL) {
        fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Malloc r_info->r_idx_b[bs]->coll_keeper[%d] == NULL\n", curr_dt(dt), cid, b);
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.idx[b], r_info->r_idx_b[bs]->idx[b]);
        r_info->r_idx_b[bs]->idx[b] = NULL;
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k1_index[b], r_info->r_idx_b[bs]->k1_index[b]);
        r_info->r_idx_b[bs]->k1_index[b] = NULL;
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k2_index[b], r_info->r_idx_b[bs]->k2_index[b]);
        r_info->r_idx_b[bs]->k2_index[b] = NULL;
        return false;
    }
    memset(r_info->r_idx_b[bs]->coll_keeper[b], 1, ((uint32_t)COLL_KEEPER_SIZE));
    r_info->r_idx_b[bs]->coll_count_curr[b] = 0;
    r_info->r_idx_b[bs]->key_count_tot[b] = INIT_COLL_KEEPER;
    r_info->r_idx_b[bs]->key_keeper[b] = (uint8_t *)index_memory_malloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.key_keeper[b],
                                                                        ((uint32_t)INIT_COLL_KEEPER) * ((uint32_t)COLL_KEEPER_SIZE) * sizeof(uint8_t));

    if (r_info->r_idx_b[bs]->key_keeper[b] == NULL) {
        fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Malloc r_info->r_idx_b[bs]->key_keeper[%d] == NULL\n", curr_dt(dt), cid, b);
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.idx[b], r_info->r_idx_b[bs]->idx[b]);
        r_info->r_idx_b[bs]->idx[b] = NULL;
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k1_index[b], r_info->r_idx_b[bs]->k1_index[b]);
        r_info->r_idx_b[bs]->k1_index[b] = NULL;
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k2_index[b], r_info->r_idx_b[bs]->k2_index[b]);
        r_info->r_idx_b[bs]->k2_index[b] = NULL;
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.coll_keeper[b], r_info->r_idx_b[bs]->coll_keeper[b]);
        r_info->r_idx_b[bs]->coll_keeper[b] = NULL;
        return false;
    }
    memset(r_info->r_idx_b[bs]->key_keeper[b], 1, ((uint32_t)COLL_KEEPER_SIZE));
    r_info->r_idx_b[bs]->key_count_curr[b] = 0;

    for (int v = 0; v < VOIDS; v++) {
        r_info->r_idx_b[bs]->void_info[b][v].void_count_tot = 0;
        r_info->r_idx_b[bs]->void_info[b][v].void_count_curr = 0;

        for (int i = 0; i < MAX_VOID_ELEMENTS; i++)
            r_info->r_idx_b[bs]->void_info[b][v].void_arr[i] = 0;
    }
    return true;
}

static void free_initial_index_arrays(region_info_t *r_info, idx_region_info_t *idx) {
    for (int b = 0; b < BUCKETS; b++) {
        if (idx->idx != NULL && idx->idx_x2 != NULL && idx->idx[b] == idx->idx_x2[b])
            idx->idx_x2[b] = NULL;

        if (idx->k1_index != NULL && idx->k1_index_x2 != NULL && idx->k1_index[b] == idx->k1_index_x2[b])
            idx->k1_index_x2[b] = NULL;

        if (idx->k2_index != NULL && idx->k2_index_x2 != NULL && idx->k2_index[b] == idx->k2_index_x2[b])
            idx->k2_index_x2[b] = NULL;

        if (idx->coll_keeper != NULL && idx->coll_keeper_x2 != NULL && idx->coll_keeper[b] == idx->coll_keeper_x2[b])
            idx->coll_keeper_x2[b] = NULL;
    }

    if (idx->idx != NULL) {
        for (int b = 0; b < BUCKETS; b++)
            index_memory_free(&r_info->index_memory_bytes, &idx->memory.idx[b], idx->idx[b]);
        index_memory_free(&r_info->index_memory_bytes, &idx->memory.idx_ptrs, idx->idx);
        idx->idx = NULL;
    }

    if (idx->idx_x2 != NULL) {
        for (int b = 0; b < BUCKETS; b++)
            index_memory_free(&r_info->index_memory_bytes, &idx->memory.idx_x2[b], idx->idx_x2[b]);
        index_memory_free(&r_info->index_memory_bytes, &idx->memory.idx_x2_ptrs, idx->idx_x2);
        idx->idx_x2 = NULL;
    }

    if (idx->k1_index != NULL) {
        for (int b = 0; b < BUCKETS; b++)
            index_memory_free(&r_info->index_memory_bytes, &idx->memory.k1_index[b], idx->k1_index[b]);
        index_memory_free(&r_info->index_memory_bytes, &idx->memory.k1_index_ptrs, idx->k1_index);
        idx->k1_index = NULL;
    }

    if (idx->k1_index_x2 != NULL) {
        for (int b = 0; b < BUCKETS; b++)
            index_memory_free(&r_info->index_memory_bytes, &idx->memory.k1_index_x2[b], idx->k1_index_x2[b]);
        index_memory_free(&r_info->index_memory_bytes, &idx->memory.k1_index_x2_ptrs, idx->k1_index_x2);
        idx->k1_index_x2 = NULL;
    }

    if (idx->k2_index != NULL) {
        for (int b = 0; b < BUCKETS; b++)
            index_memory_free(&r_info->index_memory_bytes, &idx->memory.k2_index[b], idx->k2_index[b]);
        index_memory_free(&r_info->index_memory_bytes, &idx->memory.k2_index_ptrs, idx->k2_index);
        idx->k2_index = NULL;
    }

    if (idx->k2_index_x2 != NULL) {
        for (int b = 0; b < BUCKETS; b++)
            index_memory_free(&r_info->index_memory_bytes, &idx->memory.k2_index_x2[b], idx->k2_index_x2[b]);
        index_memory_free(&r_info->index_memory_bytes, &idx->memory.k2_index_x2_ptrs, idx->k2_index_x2);
        idx->k2_index_x2 = NULL;
    }

    if (idx->coll_keeper != NULL) {
        for (int b = 0; b < BUCKETS; b++)
            index_memory_free(&r_info->index_memory_bytes, &idx->memory.coll_keeper[b], idx->coll_keeper[b]);
        index_memory_free(&r_info->index_memory_bytes, &idx->memory.coll_keeper_ptrs, idx->coll_keeper);
        idx->coll_keeper = NULL;
    }

    if (idx->coll_keeper_x2 != NULL) {
        for (int b = 0; b < BUCKETS; b++)
            index_memory_free(&r_info->index_memory_bytes, &idx->memory.coll_keeper_x2[b], idx->coll_keeper_x2[b]);
        index_memory_free(&r_info->index_memory_bytes, &idx->memory.coll_keeper_x2_ptrs, idx->coll_keeper_x2);
        idx->coll_keeper_x2 = NULL;
    }

    if (idx->key_keeper != NULL) {
        for (int b = 0; b < BUCKETS; b++)
            index_memory_free(&r_info->index_memory_bytes, &idx->memory.key_keeper[b], idx->key_keeper[b]);
        index_memory_free(&r_info->index_memory_bytes, &idx->memory.key_keeper_ptrs, idx->key_keeper);
        idx->key_keeper = NULL;
    }
    index_memory_free(&r_info->index_memory_bytes, &idx->memory.idx_r_help, idx->idx_r_help);
    index_memory_free(&r_info->index_memory_bytes, &idx->memory.idx_r_help_x2, idx->idx_r_help_x2);
    idx->idx_r_help = idx->idx_r_help_x2 = NULL;
}

uint32_t prepare_idx_pointers(struct region_info_t *r_info, uint8_t bs, uint64_t cid) {
    idx_region_info_t *idx = r_info->r_idx_b[bs];
    idx->idx = index_memory_calloc(&r_info->index_memory_bytes, &idx->memory.idx_ptrs, BUCKETS, sizeof(*idx->idx));

    if (idx->idx == NULL)
        goto failed;
    idx->idx_x2 = index_memory_calloc(&r_info->index_memory_bytes, &idx->memory.idx_x2_ptrs, BUCKETS, sizeof(*idx->idx_x2));

    if (idx->idx_x2 == NULL)
        goto failed;
    idx->k1_index = index_memory_calloc(&r_info->index_memory_bytes, &idx->memory.k1_index_ptrs, BUCKETS, sizeof(*idx->k1_index));

    if (idx->k1_index == NULL)
        goto failed;
    idx->k1_index_x2 = index_memory_calloc(&r_info->index_memory_bytes, &idx->memory.k1_index_x2_ptrs, BUCKETS, sizeof(*idx->k1_index_x2));

    if (idx->k1_index_x2 == NULL)
        goto failed;
    idx->k2_index = index_memory_calloc(&r_info->index_memory_bytes, &idx->memory.k2_index_ptrs, BUCKETS, sizeof(*idx->k2_index));

    if (idx->k2_index == NULL)
        goto failed;
    idx->k2_index_x2 = index_memory_calloc(&r_info->index_memory_bytes, &idx->memory.k2_index_x2_ptrs, BUCKETS, sizeof(*idx->k2_index_x2));

    if (idx->k2_index_x2 == NULL)
        goto failed;
    idx->coll_keeper = index_memory_calloc(&r_info->index_memory_bytes, &idx->memory.coll_keeper_ptrs, INIT_COLL_KEEPER * BUCKETS, sizeof(*idx->coll_keeper));

    if (idx->coll_keeper == NULL)
        goto failed;
    idx->coll_keeper_x2 = index_memory_calloc(&r_info->index_memory_bytes, &idx->memory.coll_keeper_x2_ptrs, INIT_COLL_KEEPER * BUCKETS, sizeof(*idx->coll_keeper_x2));

    if (idx->coll_keeper_x2 == NULL)
        goto failed;
    idx->key_keeper = index_memory_calloc(&r_info->index_memory_bytes, &idx->memory.key_keeper_ptrs, INIT_COLL_KEEPER * BUCKETS, sizeof(*idx->key_keeper));

    if (idx->key_keeper == NULL)
        goto failed;
    idx->idx_r_help = index_memory_calloc(&r_info->index_memory_bytes, &idx->memory.idx_r_help, 1, sizeof(*idx->idx_r_help));

    if (idx->idx_r_help == NULL)
        goto failed;
    idx->idx_r_help_x2 = index_memory_calloc(&r_info->index_memory_bytes, &idx->memory.idx_r_help_x2, 1, sizeof(*idx->idx_r_help_x2));

    if (idx->idx_r_help_x2 == NULL)
        goto failed;
    return 0;
failed:
    free_initial_index_arrays(r_info, idx);
    fprintf(stderr, "ERROR [IDX_BUILDER=%015lu] Cannot allocate index pointers for %s\n", cid, r_info->uuid);
    return (uint32_t)-1;
}


uint32_t build_index(struct region_info_t *r_info, uint8_t bs, table_handler_t *t_handler) {
    bool allocated_z_stat = false;
    uint64_t cid = t_handler->cid;
    r_info->max_file_bsize = max_file_size;
    char dt[DT_LOG_SIZE];

    if (log_level > 0)
        fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Max data file size = %u\n", curr_dt(dt), cid, r_info->max_file_bsize * r_info->r_idx_b[bs]->block_size * BUCKETS);
    fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Initial index coefficient = %u\n", curr_dt(dt), cid, initial_idx_coeff);

    if (prepare_idx_pointers(r_info, bs, cid) == -1)
        goto failed;
    r_info->hash_idx = true;
    r_info->log_level = 0;
    r_info->r_idx_b[bs]->count_max_idx_shift = 0;
    r_info->r_idx_b[bs]->count_max_idx_jump = 0;

    if (r_info->is_freed)
        goto failed;
    r_info->avg_zip_rate = 1;

    if (r_info->enable_block_compression > BLOCK_COMPRESSION_NONE && r_info->z_stat == NULL) {
        uint8_t **z_stat = malloc(sizeof(*z_stat));

        if (z_stat == NULL)
            goto failed;
        z_stat[0] = malloc(256);

        if (z_stat[0] == NULL) {
            free(z_stat);
            goto failed;
        }
        memset(z_stat[0], 0xFF, 256);
        r_info->z_stat = z_stat;
        r_info->z_stat_size = 1;
        allocated_z_stat = true;
    }
    struct stat st = {0};

    if (stat(r_info->r_idx_b[bs]->dir_name, &st) == -1) {
        fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Creating dir %s\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->dir_name);
        mkdir(r_info->r_idx_b[bs]->dir_name, 0700);
    }

    for (int b = 0; b < BUCKETS; b++) {
        r_info->r_idx_b[bs]->f_idx_size[b] = r_info->max_file_bsize * initial_idx_coeff;
        fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Initial %s[%d] index size = %lu\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->dir_name, b,
                r_info->r_idx_b[bs]->f_idx_size[b]);
        r_info->r_idx_b[bs]->f_idx_size[b] += IDX_OVERHEAD_MAX;
        fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Result %s[%d] index size = %lu, IDX_OVERHEAD_MAX = %d\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->dir_name, b,
                r_info->r_idx_b[bs]->f_idx_size[b], IDX_OVERHEAD_MAX);

        if (r_info->r_idx_b[bs]->f_idx_size[b] < 256) {
            fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Error code 109. Max_file_size=%d * initial_idx_coeff=%d cannot be less then 256\n", curr_dt(dt), cid,
                    r_info->max_file_bsize, initial_idx_coeff);
            r_info->stop = true;
            r_info->bad_data = true;
            finish_thread(r_info, 109, true, t_handler);
        }
        char fname[MAX_FN_SIZE];
        snprintf(fname, MAX_FN_SIZE, "%s/%03d.data", r_info->r_idx_b[bs]->dir_name, r_info->r_idx_b[bs]->pos - 1);

        if (r_info->r_idx_b[bs]->fname == NULL) {
            r_info->r_idx_b[bs]->fname = malloc(strlen(fname) + 1);

            if (r_info->r_idx_b[bs]->fname == NULL)
                goto failed;
            strcpy(r_info->r_idx_b[bs]->fname, fname);
        }
        fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Created filename %s, block_size=%d, bs=%d\n", curr_dt(dt), cid, fname, r_info->r_idx_b[bs]->block_size, bs);
        uint64_t file_size = get_file_size(fname);
        fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Region %s[%d] file size = %lu\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname, b, file_size);

        if (file_size > r_info->r_idx_b[bs]->block_size) {
            r_info->r_idx_b[bs]->fd[b] = open_file(fname, cid);
            fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Open region %s r_info->r_idx_b[%d]->fd[%d] = %d\n", curr_dt(dt), cid, r_info->uuid, bs, b,
                    r_info->r_idx_b[bs]->fd[b]);

            if (r_info->r_idx_b[bs]->pos == 1)
                bind_data_000_fd(r_info, r_info->r_idx_b[bs]->fd[b], cid);

            if (r_info->replica == NULL)
                goto failed;

            if (r_info->is_leader) {
                if (rf == 1 || r_info->replica->replica_num > 0) {
                    if (idx_alloc(r_info, b, true, bs, file_size, t_handler) == false)
                        goto failed;

                    if (!scan_index(r_info, b, file_size, bs, t_handler))
                        goto failed;
                } else if (idx_alloc(r_info, b, false, bs, file_size, t_handler) == false)
                    goto failed;
            }
        } else {
            if (access(fname, F_OK) != 0)
                fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Creating file %s\n", curr_dt(dt), cid, fname);
            else
                fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Openig file %s\n", curr_dt(dt), cid, fname);
            r_info->r_idx_b[bs]->fd[b] = open_file(fname, cid);
            fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Open region %s r_info->r_idx_b[%d]->fd[%d] = %d\n", curr_dt(dt), cid, r_info->uuid, bs, b,
                    r_info->r_idx_b[bs]->fd[b]);

            if (r_info->r_idx_b[bs]->pos == 1)
                bind_data_000_fd(r_info, r_info->r_idx_b[bs]->fd[b], cid);

            if (r_info->is_leader) {
                if (idx_alloc(r_info, b, false, bs, file_size, t_handler) == false)
                    goto failed;

                if (r_info->r_need_data_chk > 0 && file_size > r_info->r_idx_b[bs]->block_size && !scan_index(r_info, b, file_size, bs, t_handler))
                    goto failed;
            }
            r_info->r_idx_b[bs]->f_idx_curr_size[b] = 0;
            r_info->r_idx_b[bs]->b_offset[b] = 1;
        }

        if (file_size > 0 && r_info->r_need_data_chk == 0 && data_check_enabled == 1) {
            char *meta_buff = malloc(r_info->r_idx_b[bs]->block_size);

            if (meta_buff == NULL)
                goto failed;
            uint64_t bytes_read = read_file(r_info->r_idx_b[bs]->fd[b], meta_buff, 0, r_info->r_idx_b[bs]->block_size, r_info, cid);

            if (check_meta_quality(meta_buff, bytes_read, r_info->r_idx_b[bs]->block_size, cid) == false) {
                set_region_recovery_info(r_info, 1000001L, t_handler);
                r_info->r_need_data_chk = 1;
            }
            free(meta_buff);
        }
        struct stat info;

        if (fstat(r_info->r_idx_b[bs]->fd[b], &info) != 0)
            perror("fstat() error");
        else
            fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] FStat %s fd=%d => inode=%lu\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->dir_name, r_info->r_idx_b[bs]->fd[b],
                    info.st_ino);
    }
    return 0;
failed:
    r_info->stop = true;
    r_info->bad_data = true;
    set_region_recovery_info(r_info, 1, t_handler);

    for (int b = 0; b < BUCKETS; b++) {
        int fd = r_info->r_idx_b[bs]->fd[b];

        if (fd < 0)
            continue;

        if (r_info->write_4K_fd == fd && r_info->write_4K_buff_used != 0)
            flush_write_4K_buffer(r_info, cid);
        unbind_data_000_fd(r_info, fd, cid);
        close(fd);
        r_info->r_idx_b[bs]->fd[b] = -1;
    }
    free_initial_index_arrays(r_info, r_info->r_idx_b[bs]);
    free(r_info->r_idx_b[bs]->fname);
    r_info->r_idx_b[bs]->fname = NULL;

    if (allocated_z_stat) {
        for (unsigned i = 0; i < r_info->z_stat_size; i++)
            free(r_info->z_stat[i]);
        free(r_info->z_stat);
        r_info->z_stat = NULL;
        r_info->z_stat_size = 0;
    }
    fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Initial index build failed for %s, bs=%u\n", curr_dt(dt), cid, r_info->uuid, bs);
    return (uint32_t)-1;
}


bool resize_coll_keeper(struct region_info_t *r_info, uint32_t b, uint8_t bs, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Resize %s->coll_keeper[%d] to %d\n", curr_dt(dt), cid, r_info->uuid, b, r_info->r_idx_b[bs]->coll_count_tot[b] * 2);
    uint8_t *coll_keeper_tmp = (uint8_t *)index_memory_realloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.coll_keeper[b], r_info->r_idx_b[bs]->coll_keeper[b],
                                                               r_info->r_idx_b[bs]->coll_count_tot[b] * ((uint32_t)COLL_KEEPER_SIZE) * 2 * sizeof(uint8_t));

    if (coll_keeper_tmp == NULL) {
        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Cannot allocate new memory for collision's keeper %lu for %s\n", curr_dt(dt), cid,
                r_info->r_idx_b[bs]->coll_count_tot[b] * ((uint32_t)COLL_KEEPER_SIZE) * 2 * sizeof(uint8_t), r_info->uuid);
        return false;
    }
    memset(coll_keeper_tmp + r_info->r_idx_b[bs]->coll_count_tot[b] * ((uint32_t)COLL_KEEPER_SIZE), 0, r_info->r_idx_b[bs]->coll_count_tot[b] * ((uint32_t)COLL_KEEPER_SIZE));
    r_info->r_idx_b[bs]->coll_keeper[b] = coll_keeper_tmp;
    r_info->r_idx_b[bs]->coll_count_tot[b] *= 2;
    fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Resize %s->coll_keeper[%d] done\n", curr_dt(dt), cid, r_info->uuid, b);
    return true;
}

bool resize_coll_keeper_x2(struct region_info_t *r_info, uint32_t b, uint8_t bs, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Resize %s->coll_keeper_x2[%d] to %d\n", curr_dt(dt), cid, r_info->uuid, b, r_info->r_idx_b[bs]->coll_count_tot_x2[b] * 2);
    uint8_t *coll_keeper_tmp =
        (uint8_t *)index_memory_realloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.coll_keeper_x2[b], r_info->r_idx_b[bs]->coll_keeper_x2[b],
                                        r_info->r_idx_b[bs]->coll_count_tot_x2[b] * ((uint32_t)COLL_KEEPER_SIZE) * 2 * sizeof(uint8_t));

    if (coll_keeper_tmp == NULL) {
        // TODO Repeat?
        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Cannot allocate new memory for collision's keeper %lu for %s\n", curr_dt(dt), cid,
                r_info->r_idx_b[bs]->coll_count_tot_x2[b] * ((uint32_t)COLL_KEEPER_SIZE) * 2 * sizeof(uint8_t), r_info->uuid);
        return false;
    }
    memset(coll_keeper_tmp + r_info->r_idx_b[bs]->coll_count_tot_x2[b] * ((uint32_t)COLL_KEEPER_SIZE), 0,
           r_info->r_idx_b[bs]->coll_count_tot_x2[b] * ((uint32_t)COLL_KEEPER_SIZE));
    r_info->r_idx_b[bs]->coll_keeper_x2[b] = coll_keeper_tmp;
    r_info->r_idx_b[bs]->coll_count_tot_x2[b] *= 2;
    fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Resize %s->coll_keeper[%d] done\n", curr_dt(dt), cid, r_info->uuid, b);
    return true;
}

bool resize_key_keeper(struct region_info_t *r_info, uint32_t b, uint8_t bs, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Resize %s->key_keeper[%d] to %d\n", curr_dt(dt), cid, r_info->uuid, b, r_info->r_idx_b[bs]->key_count_tot[b] * 2);
    uint8_t *key_keeper_tmp = (uint8_t *)index_memory_realloc(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.key_keeper[b], r_info->r_idx_b[bs]->key_keeper[b],
                                                              r_info->r_idx_b[bs]->key_count_tot[b] * ((uint32_t)COLL_KEEPER_SIZE) * 2 * sizeof(uint8_t));

    if (key_keeper_tmp == NULL) {
        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Cannot allocate new memory for key's keeper\n", curr_dt(dt), cid);
        return false;
    }
    memset(key_keeper_tmp + r_info->r_idx_b[bs]->key_count_tot[b] * ((uint32_t)COLL_KEEPER_SIZE), 0, r_info->r_idx_b[bs]->key_count_tot[b] * ((uint32_t)COLL_KEEPER_SIZE));
    r_info->r_idx_b[bs]->key_keeper[b] = key_keeper_tmp;
    r_info->r_idx_b[bs]->key_count_tot[b] *= 2;
    fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Resize %s->key_keeper[%d] done\n", curr_dt(dt), cid, r_info->uuid, b);
    return true;
}


void free_index(struct region_info_t *r_info) {
    for (int bs = 0; bs < r_info->max_idx_b; bs++) {
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.idx_ptrs, r_info->r_idx_b[bs]->idx);
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.idx_x2_ptrs, r_info->r_idx_b[bs]->idx_x2);
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k1_index_ptrs, r_info->r_idx_b[bs]->k1_index);
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k1_index_x2_ptrs, r_info->r_idx_b[bs]->k1_index_x2);
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k2_index_ptrs, r_info->r_idx_b[bs]->k2_index);
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k2_index_x2_ptrs, r_info->r_idx_b[bs]->k2_index_x2);
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.coll_keeper_ptrs, r_info->r_idx_b[bs]->coll_keeper);
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.coll_keeper_x2_ptrs, r_info->r_idx_b[bs]->coll_keeper_x2);
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.key_keeper_ptrs, r_info->r_idx_b[bs]->key_keeper);
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.idx_r_help, r_info->r_idx_b[bs]->idx_r_help);
        index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.idx_r_help_x2, r_info->r_idx_b[bs]->idx_r_help_x2);
        index_memory_free(&r_info->index_memory_bytes, &r_info->index_object_bytes[bs], r_info->r_idx_b[bs]);
    }
    free(r_info->r_idx_b[BS_LOB]);
    free(r_info->r_idx_b);
}

void free_region(struct region_info_t *r_info, uint64_t cid) { // TODO Не все чистим, проверить что не юзается больше и зачищать
    reset_region_memory_snapshot(r_info);
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Region %s slot going to be free. Idx count = %d\n", curr_dt(dt), cid, r_info->uuid, r_info->max_idx_b);

    while (r_info->dont_free_now)
        usleep(10000);
    r_info->data_000_fd = -1;
    {
        if (r_info->replica->replica1_sock_fd > 0)
            close_socket(r_info, 1, r_info->replica->replica1_sock_fd);

        if (r_info->replica->replica2_sock_fd > 0)
            close_socket(r_info, 2, r_info->replica->replica2_sock_fd);
    }

    if (r_info->replica->replica1_port > 0)
        free(r_info->replica->replica1);

    if (r_info->replica->replica2_port > 0)
        free(r_info->replica->replica2);

    if (r_info->replica->request_params_size > 0)
        free(r_info->replica->request_params);
    r_info->replica->r_info1 = NULL; // TODO Странно что если этого не делать то раньше все работало пока не начал освобождать t_handler, WTF?
    r_info->replica->r_info2 = NULL;
    r_info->replica->replica1_sock_waiting_conn = 0;
    r_info->replica->replica2_sock_waiting_conn = 0;
    // free(r_info->replica); // TODO Будет утекать память, пофиксить
    for (int bs = 0; bs < r_info->max_idx_b; bs++) {
        if (r_info->r_idx_b[bs]->idx == NULL)
            break;

        if (r_info->is_leader) {
            for (int b = 0; b < BUCKETS; b++) {
                index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.idx[b], r_info->r_idx_b[bs]->idx[b]);
                r_info->r_idx_b[bs]->idx[b] = NULL;
                index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.coll_keeper[b], r_info->r_idx_b[bs]->coll_keeper[b]);
                r_info->r_idx_b[bs]->coll_keeper[b] = NULL;
                index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.key_keeper[b], r_info->r_idx_b[bs]->key_keeper[b]);
                index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k1_index[b], r_info->r_idx_b[bs]->k1_index[b]);
                r_info->r_idx_b[bs]->k1_index[b] = NULL;
                index_memory_free(&r_info->index_memory_bytes, &r_info->r_idx_b[bs]->memory.k2_index[b], r_info->r_idx_b[bs]->k2_index[b]);
                r_info->r_idx_b[bs]->k2_index[b] = NULL;
            }
        }
        free(r_info->r_idx_b[bs]->dir_name);
        free(r_info->r_idx_b[bs]->fname);
        fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Region %s idx[%d] freed\n", curr_dt(dt), cid, r_info->uuid, bs);
    }
    free_index(r_info);
    free(r_info->lob_map);
    r_info->bad_data = false;
    r_info->already_finishing = false;
    r_info->max_lob_id = 0;
    r_info->is_write = 0;
    r_info->is_replicate = 0;
    record_version_state_init(&r_info->record_versions);
    reset_record_gc(r_info);
    r_info->pending_delete = 0;

    for (int i = 0; i < r_info->qfl_dic_lens; i++)
        free(r_info->qfl_dic[i]);
    r_info->qfl_dic_lens = 0;
    free(r_info->qfl_dic);
    free(r_info->qfl_dic_size);
    r_info->qfl_dic = NULL;
    r_info->qfl_dic_size = NULL;
    free(r_info->k_beg);
    free(r_info->k_end);
    free(r_info->rbuff);
    r_info->rbuff = NULL;
    free(r_info->write_4K_buff);
    r_info->write_4K_buff = NULL;
    r_info->write_4K_buff_used = 0;
    r_info->write_4K_file_offset = 0;
    r_info->write_4K_fd = -1;
    r_info->write_4K_cache_ok = false;
    r_info->write_4K_cache_block_offset = 0;
    free(r_info->append_compact_dirty_map);
    r_info->append_compact_dirty_map = NULL;
    r_info->append_compact_dirty_map_size = 0;
    r_info->append_compact_dirty_segments = 0;
    r_info->append_compact_garbage_bytes = 0;
    r_info->append_compact_occupied_bytes = 0;
    r_info->append_compact_cursor_segment = 0;
    atomic_store(&r_info->append_compaction_requested, false);
    atomic_store(&r_info->append_compaction_requested_since_ms, 0);
    atomic_store(&r_info->append_compact_truncate_pending, false);

    if (r_info->is_leader) {
        free(r_info->zbuff);
        r_info->zbuff = NULL;
        free(r_info->bbuff);
        r_info->bbuff = NULL;
        free(r_info->lob_buff);
        r_info->lob_buff = NULL;

        if (r_info->enable_block_compression > BLOCK_COMPRESSION_NONE) {
            free(r_info->snappy_work_buff);
            r_info->snappy_work_buff = NULL;
            free(r_info->block_comp_ins_buff);
            r_info->block_comp_ins_buff = NULL;
            free(r_info->block_comp_buff);
            r_info->block_comp_buff = NULL;

            for (int i = 0; i < r_info->z_stat_size; i++) {
                free(r_info->z_stat[i]);
                r_info->z_stat[i] = NULL;
            }
            free(r_info->z_stat);
            r_info->z_stat = NULL;
        }
        free(r_info->rsc_msg);
    }
    free(r_info->msg_get->kv->qv);
    free(r_info->msg_get->kv);
    free(r_info->msg_get);
    free(r_info->msg_get_put->kv->qv->qfl);
    free(r_info->msg_get_put->kv->cf);
    free(r_info->msg_get_put->kv->key);
    free(r_info->msg_get_put->kv->qv);
    free(r_info->msg_get_put->kv);
    free(r_info->msg_get_put);
    free(r_info->msg_get_rotate->kv->qv);
    free(r_info->msg_get_rotate->kv);
    free(r_info->msg_get_rotate);

    for (int i = 0; i < r_info->scan_keys_list_count_tot; i++) {
        free(r_info->scan_keys_list[i]->keys);
        free(r_info->scan_keys_list[i]->key_sizes);
        free(r_info->scan_keys_list[i]);
    }
    free(r_info->scan_keys_list);
    free(r_info->scan_vars);
    // TODO Fix leak
    if (r_info->waiting_count_lock == 0 && r_info->waiting_count_reconnect == 0) {
        free(r_info->uuid);
        r_info->uuid = NULL;
    }

    for (int bs = 0; bs < BLOCK_MAX_SIZE_M; bs++)
        r_info->bs_map[bs] = 0;
    r_info->max_idx_b = 0;
    r_info->freed = MAGIC_NUM;
    fprintf(stderr, "%s INFO [IDX_BUILDER=%015lu] Region slot freed\n", curr_dt(dt), cid);
}
