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
#include "block_handler.h"
static bool resize_scan_buff(char **buff, uint64_t *capacity, uint64_t required, uint64_t cid, char *name, char *uuid) {
    if (*capacity >= required)
        return true;
    uint64_t new_capacity = *capacity == 0 ? 1 : *capacity;

    while (new_capacity < required) {
        if (new_capacity > UINT64_MAX / 2) {
            new_capacity = required;
            break;
        }
        new_capacity *= 2;
    }

    if (new_capacity > SIZE_MAX)
        return false;
    char *buff_tmp = realloc(*buff, new_capacity);

    if (buff_tmp == NULL) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [SCAN_HANDLER=%015lu] Cannot realloc %s to %lu for %s\n", curr_dt(dt), cid, name, new_capacity, uuid);
        return false;
    }
    *buff = buff_tmp;
    *capacity = new_capacity;
    return true;
}


static bool prepare_scan_extract_buff(region_info_t *r_info, char *src_buff, uint32_t msg_size, uint32_t key_size, uint32_t cf_size, uint32_t qv_size, uint8_t bs, uint64_t cid) {
    if (r_info->scan_vars->unzip_msg_size > UINT64_MAX - r_info->scan_vars->unzip_msg_size_sum)
        return false;
    uint64_t data_offset = r_info->scan_vars->unzip_msg_size_sum + r_info->scan_vars->unzip_msg_size;

    if (data_offset > r_info->zbuff_size)
        return false;
    uint64_t dst_capacity = r_info->zbuff_size - data_offset > UINT32_MAX ? UINT32_MAX : (uint32_t)(r_info->zbuff_size - data_offset);
    uint32_t required_size;

    if (calc_extract_qfls_size(r_info, src_buff, msg_size, key_size, cf_size, qv_size, dst_capacity, &required_size, bs, cid) != 0)
        return false;

    if (required_size <= dst_capacity)
        return true;

    if (data_offset > UINT64_MAX - required_size)
        return false;
    r_info->zbuff -= r_info->scan_vars->unzip_msg_size_sum;
    bool result = resize_scan_buff((char **)&r_info->zbuff, &r_info->zbuff_size, data_offset + required_size, cid, "zbuff", r_info->uuid);
    r_info->zbuff += r_info->scan_vars->unzip_msg_size_sum;
    return result;
}

static bool resize_scan_key_sizes(scan_keys_list_t *scan_control, uint64_t required, uint64_t cid, char *uuid) {
    if (scan_control->max_key_count >= required)
        return true;
    uint64_t new_capacity = scan_control->max_key_count == 0 ? 1 : scan_control->max_key_count;

    while (new_capacity < required) {
        if (new_capacity > SIZE_MAX / 4) {
            new_capacity = required;
            break;
        }
        new_capacity *= 2;
    }

    if (new_capacity > SIZE_MAX / 2)
        return false;
    char *key_sizes_tmp = realloc(scan_control->key_sizes, new_capacity * 2);

    if (key_sizes_tmp == NULL) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [SCAN_HANDLER=%015lu] Cannot realloc scan key_sizes to %lu for %s\n", curr_dt(dt), cid, new_capacity * 2, uuid);
        return false;
    }
    scan_control->key_sizes = key_sizes_tmp;
    scan_control->max_key_count = new_capacity;
    return true;
}

static void reset_scan_control(scan_keys_list_t *scan_control) {
    scan_control->cid = 0;
    scan_control->bs = 0;
    scan_control->scan_block_shift = 0;
    scan_control->scan_block_num = 0;
    scan_control->scan_block_num_rewind = 0;
    scan_control->last_client_scan_block_num = 1;
    scan_control->last_client_scan_block_count = 0;
    scan_control->k_count = 0;
    scan_control->k_shift_global = 0;
    scan_control->curr_offset = 0;
    scan_control->last_scan_ts = 0;
}

static scan_keys_list_t *get_current_scan_control(msg_srv_t *msg_srv, uint64_t cid) {
    if (msg_srv->p_scan_keys_list == NULL || msg_srv->p_scan_keys_list->cid != cid)
        return NULL;
    return msg_srv->p_scan_keys_list;
}


static void free_scan_msg(msg_cli_t *msg_get) {
    if (msg_get == NULL)
        return;

    if (msg_get->kv != NULL) {
        if (msg_get->kv->qv != NULL) {
            free(msg_get->kv->qv->qfl);
            free(msg_get->kv->qv);
        }
        free(msg_get->kv->cf);
        free(msg_get->kv->key);
        free(msg_get->kv);
    }
    free(msg_get);
}


void compact_scan_control(struct region_info_t *r_info, int sc, uint64_t cid) {
    char dt[DT_LOG_SIZE];

    if (log_level > 0) {
        fprintf(stderr, "%s DEBUG [COMPACT_SERVCE=%015lu] Scan position ready to change sc=%d, scan_keys_list_count_curr=%d\n", curr_dt(dt), cid, sc,
                r_info->scan_keys_list_count_curr);
    }

    if (r_info->scan_keys_list_count_curr <= 0 || sc < 0 || sc >= r_info->scan_keys_list_count_curr)
        return;
    uint32_t last = r_info->scan_keys_list_count_curr - 1;
    scan_keys_list_t *scan_control = r_info->scan_keys_list[sc];

    if (sc != last) {
        r_info->scan_keys_list[sc] = r_info->scan_keys_list[last];
        r_info->scan_keys_list[last] = scan_control;
    }
    reset_scan_control(r_info->scan_keys_list[last]);
    r_info->scan_keys_list_count_curr--;
}

void add_scan_keeper(struct region_info_t *r_info, uint8_t bs, msg_cli_t *msg_cli, int i, uint64_t cid) {
    union s2b_u key_size;
    key_size.short_val = (uint16_t)msg_cli->kv->key_size;

    if (r_info->scan_keys_list[i]->k_shift_global > UINT64_MAX - key_size.short_val || r_info->scan_keys_list[i]->k_count == UINT64_MAX) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [SCAN_HANDLER=%015lu] Moved keys size overflow sc=%d, %s\n", curr_dt(dt), cid, i, r_info->uuid);
        r_info->stop = true;
        r_info->bad_data = true;
        return;
    }
    uint64_t required_keys_size = r_info->scan_keys_list[i]->k_shift_global + key_size.short_val;
    uint64_t required_key_count = r_info->scan_keys_list[i]->k_count + 1;

    if (required_key_count > SIZE_MAX / 2 ||
        resize_scan_buff(&r_info->scan_keys_list[i]->keys, &r_info->scan_keys_list[i]->keys_mem_usage_limit, required_keys_size, cid, "scan keys", r_info->uuid) == false ||
        resize_scan_key_sizes(r_info->scan_keys_list[i], required_key_count, cid, r_info->uuid) == false) {
        r_info->stop = true;
        r_info->bad_data = true;
        return;
    }
    memcpy(r_info->scan_keys_list[i]->keys + r_info->scan_keys_list[i]->k_shift_global, msg_cli->kv->key, msg_cli->kv->key_size);
    r_info->scan_keys_list[i]->k_shift_global += key_size.short_val;
    memcpy(r_info->scan_keys_list[i]->key_sizes + r_info->scan_keys_list[i]->k_count * 2, key_size.byte, 2);
    r_info->scan_keys_list[i]->k_count++;
}

void handle_scans(struct region_info_t *r_info, uint8_t b, uint8_t bs, uint32_t l_offset, uint32_t r_offset, msg_cli_t *msg_cli, uint64_t cid) {
    char dt[DT_LOG_SIZE];

    for (int sc = 0; sc < r_info->scan_keys_list_count_curr; sc++) {
        uint64_t curr_ts = getCurrentTimeMs();

        if (curr_ts - r_info->scan_keys_list[sc]->last_scan_ts > scan_wait_client_msec) {
            fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Last scan ts %lu sc=%d, cid=%lu\n", curr_dt(dt), cid, curr_ts - r_info->scan_keys_list[sc]->last_scan_ts, sc,
                    r_info->scan_keys_list[sc]->cid);

            if (sc + 1 < r_info->scan_keys_list_count_curr) {
                if (r_info->scan_keys_list[r_info->scan_keys_list_count_curr - 1]->cid != 0) {
                    compact_scan_control(r_info, sc, cid);
                    sc--;
                    continue;
                }
            } else {
                fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Set scan sc=%d, cid=%lu to ZERO\n", curr_dt(dt), cid, sc, r_info->scan_keys_list[sc]->cid);
                reset_scan_control(r_info->scan_keys_list[sc]);
                r_info->scan_keys_list_count_curr--;
                continue;
            }
        }

        if (log_level > 1)
            fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] VOID curr_offset=%d, l_offset=%d, r_offset=%d\n", curr_dt(dt), cid, r_info->scan_keys_list[sc]->curr_offset, l_offset,
                    r_offset);

        if (r_info->scan_keys_list[sc]->cid != 0) {
            if (r_info->scan_keys_list[sc]->curr_offset >= l_offset && r_info->scan_keys_list[sc]->curr_offset < r_offset ||
                r_info->scan_keys_list[sc]->last_client_scan_block_num >= l_offset && r_info->scan_keys_list[sc]->last_client_scan_block_num < r_offset) {
                add_scan_keeper(r_info, bs, msg_cli, sc, cid);
            }
        } else {
            if (sc + 1 < r_info->scan_keys_list_count_curr) {
                if (r_info->scan_keys_list[r_info->scan_keys_list_count_curr - 1]->cid != 0) {
                    compact_scan_control(r_info, sc, cid);
                    sc--;
                }
            }

            for (int j = r_info->scan_keys_list_count_tot - 1; j >= 0; j--) {
                r_info->scan_keys_list_count_curr = j;

                if (r_info->scan_keys_list[j]->cid != 0) {
                    r_info->scan_keys_list_count_curr++;
                    break;
                } else {
                    r_info->scan_keys_list[j]->k_count = 0;
                    r_info->scan_keys_list[j]->k_shift_global = 0; // TODO Remove it
                }
            }
            fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Total scan r_info->scan_keys_list_count_tot=%d, r_info->scan_keys_list_count_curr=%d\n", curr_dt(dt), cid,
                    r_info->scan_keys_list_count_tot, r_info->scan_keys_list_count_curr);
            fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] New total scan r_info->scan_keys_list_count_tot=%d, r_info->scan_keys_list_count_curr=%d\n", curr_dt(dt), cid,
                    r_info->scan_keys_list_count_tot, r_info->scan_keys_list_count_curr);
        }
    }
}

uint8_t get_min_size_bs(region_info_t *r_info, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    uint8_t min_pos = 16;
    uint8_t min_bs = 0;

    for (int i = 0; i < r_info->max_idx_b; i++) {
        if (r_info->r_idx_b[i]->pos < min_pos) {
            min_pos = r_info->r_idx_b[i]->pos;
            min_bs = i;
        }
    }

    if (log_level > 1)
        fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Selected min_bs = %d\n", curr_dt(dt), cid, min_bs);
    return min_bs;
}

uint8_t get_next_size_bs(region_info_t *r_info, uint8_t bs, table_handler_t *t_handler) {
    uint8_t this_pos = r_info->r_idx_b[bs]->pos;
    bool is_next_pos = false;

    for (int i = 0; i < r_info->max_idx_b; i++) {
        for (int j = 0; j < r_info->max_idx_b; j++) {
            if (r_info->r_idx_b[j]->pos == this_pos * 2)
                return get_bs_map_pos(r_info, r_info->r_idx_b[j]->pos - 1, false, t_handler);

            if (r_info->r_idx_b[j]->pos > this_pos * 2)
                is_next_pos = true;
        }

        if (is_next_pos)
            this_pos *= 2;
        else
            break;
    }
    return 0;
}


int32_t get_scan_num(struct region_info_t *r_info, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    int32_t sc = -1;
    struct msg_srv_t *msg_srv = t_handler->msg_srv;

    if (t_handler->msg_srv->scan_keys_list_pos != -1 && r_info->scan_keys_list_count_curr > 0) {
        if (log_level > 1)
            fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Check position scan_keys_list_pos=%d, scan_keys_list_count_tot=%d, %s\n", curr_dt(dt), t_handler->cid,
                    t_handler->msg_srv->scan_keys_list_pos, r_info->scan_keys_list_count_tot, r_info->uuid);

        if (t_handler->msg_srv->scan_keys_list_pos >= r_info->scan_keys_list_count_curr)
            t_handler->msg_srv->scan_keys_list_pos = 0;

        if (r_info->scan_keys_list[t_handler->msg_srv->scan_keys_list_pos]->cid != t_handler->cid) {
            if (log_level > 1)
                fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Changed position cid=%lu, %s\n", curr_dt(dt), t_handler->cid,
                        r_info->scan_keys_list[t_handler->msg_srv->scan_keys_list_pos]->cid, r_info->uuid);

            for (int i = 0; i < r_info->scan_keys_list_count_curr; i++) {
                if (r_info->scan_keys_list[i]->cid == t_handler->cid) {
                    t_handler->msg_srv->scan_keys_list_pos = i;
                    sc = i;
                    msg_srv->p_scan_keys_list = r_info->scan_keys_list[sc];

                    if (log_level > 1)
                        fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Scan found new position sc=%d, %s\n", curr_dt(dt), t_handler->cid, sc, r_info->uuid);
                    return sc;
                }
            }

            if (log_level > 1)
                fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Scan not found position scan_keys_list_pos=%d, %s\n", curr_dt(dt), t_handler->cid,
                        t_handler->msg_srv->scan_keys_list_pos, r_info->uuid);
            goto NEW_SCAN;
        } else {
            sc = t_handler->msg_srv->scan_keys_list_pos;
            msg_srv->p_scan_keys_list = r_info->scan_keys_list[sc];

            if (log_level > 1)
                fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Scan found position sc=%d, %s\n", curr_dt(dt), t_handler->cid, sc, r_info->uuid);
        }
    } else {
    NEW_SCAN:
        for (int i = 0; i < r_info->scan_keys_list_count_curr; i++) {
            if (r_info->scan_keys_list[i]->cid == 0) {
                sc = i;
                break;
            }
        }

        if (sc == -1) {
            if (r_info->scan_keys_list_count_curr >= r_info->scan_keys_list_count_tot) {
                if (r_info->scan_keys_list_count_tot <= 0 || r_info->scan_keys_list_count_tot > INT_MAX / 2) {
                    fprintf(stderr, "%s ERROR [SCAN_HANDLER=%015lu] Cannot grow scan_keys_list_count_tot=%d, %s\n", curr_dt(dt), t_handler->cid, r_info->scan_keys_list_count_tot,
                            r_info->uuid);
                    return -1;
                }
                int old_count_tot = r_info->scan_keys_list_count_tot;
                int new_count_tot = old_count_tot * 2;
                scan_keys_list_t **scan_keys_list_tmp = realloc(r_info->scan_keys_list, (size_t)new_count_tot * sizeof(struct scan_keys_list_t *));

                if (scan_keys_list_tmp == NULL) {
                    fprintf(stderr, "%s ERROR [SCAN_HANDLER=%015lu] Cannot realloc scan_keys_list to %d, %s\n", curr_dt(dt), t_handler->cid, new_count_tot, r_info->uuid);
                    return -1;
                }
                r_info->scan_keys_list = scan_keys_list_tmp;

                for (int i = old_count_tot; i < new_count_tot; i++) {
                    r_info->scan_keys_list[i] = calloc(1, sizeof(struct scan_keys_list_t));

                    if (r_info->scan_keys_list[i] != NULL) {
                        r_info->scan_keys_list[i]->keys = malloc(1);
                        r_info->scan_keys_list[i]->key_sizes = malloc(1);
                    }

                    if (r_info->scan_keys_list[i] == NULL || r_info->scan_keys_list[i]->keys == NULL || r_info->scan_keys_list[i]->key_sizes == NULL) {
                        for (int j = old_count_tot; j <= i; j++) {
                            if (r_info->scan_keys_list[j] != NULL) {
                                free(r_info->scan_keys_list[j]->keys);
                                free(r_info->scan_keys_list[j]->key_sizes);
                                free(r_info->scan_keys_list[j]);
                                r_info->scan_keys_list[j] = NULL;
                            }
                        }
                        fprintf(stderr, "%s ERROR [SCAN_HANDLER=%015lu] Cannot allocate scan control %d, %s\n", curr_dt(dt), t_handler->cid, i, r_info->uuid);
                        return -1;
                    }
                    r_info->scan_keys_list[i]->keys_mem_usage_limit = 1;
                    r_info->scan_keys_list[i]->max_key_count = 0;
                }
                r_info->scan_keys_list_count_tot = new_count_tot;
            }
            sc = r_info->scan_keys_list_count_curr++;
        }
        t_handler->msg_srv->scan_keys_list_pos = sc;

        if (sc == r_info->scan_keys_list_count_tot) {
            fprintf(stderr, "%s ERROR [SCAN_HANDLER=%015lu] Reached scan_keys_list_count_tot=%d, %s\n", curr_dt(dt), t_handler->cid, sc, r_info->uuid);
            return -1;
        }

        if (log_level > 1) {
            fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Scan create new sc=%d, %s\n", curr_dt(dt), t_handler->cid, sc, r_info->uuid);
        }
        r_info->scan_keys_list[sc]->cid = t_handler->cid;
        msg_srv->p_scan_keys_list = r_info->scan_keys_list[sc];
        r_info->scan_keys_list[sc]->bs = get_min_size_bs(r_info, t_handler->cid);
        r_info->scan_keys_list[sc]->scan_block_shift = 0;
        r_info->scan_keys_list[sc]->scan_block_num = 0;
        r_info->scan_keys_list[sc]->scan_block_num_rewind = 0;
        r_info->scan_keys_list[sc]->last_client_scan_block_num = 1;
        r_info->scan_keys_list[sc]->last_client_scan_block_count = 0;
        r_info->scan_keys_list[sc]->k_count = 0;
        r_info->scan_keys_list[sc]->k_shift_global = 0;
        r_info->scan_keys_list[sc]->curr_offset = 0;
        r_info->scan_keys_list[sc]->last_scan_ts = getCurrentTimeMs();

        if (resize_scan_buff(&r_info->scan_keys_list[sc]->keys, &r_info->scan_keys_list[sc]->keys_mem_usage_limit, FIX_BLOCK_SIZE, t_handler->cid, "scan keys", r_info->uuid) ==
                false ||
            resize_scan_key_sizes(r_info->scan_keys_list[sc], 10, t_handler->cid, r_info->uuid) == false) {
            reset_scan_control(r_info->scan_keys_list[sc]);
            r_info->scan_keys_list_count_curr--;
            msg_srv->p_scan_keys_list = NULL;
            return -1;
        }
        memset(r_info->scan_keys_list[sc]->keys, 0, r_info->scan_keys_list[sc]->keys_mem_usage_limit);
        memset(r_info->scan_keys_list[sc]->key_sizes, 0, r_info->scan_keys_list[sc]->max_key_count * 2);
    }
    return sc;
}


void finish_scan_none(struct region_info_t *r_info, uint8_t *buff, uint8_t off_block_num, struct msg_srv_t *msg_srv) {
    memset(msg_srv->resp_buff, 0, MSG_SIZE_SCAN_NONE_RESP);
    msg_srv->msg_size = MSG_SIZE_SCAN_NONE_RESP;
    msg_srv->resp_buff[0] = MSG_SIZE_SCAN_NONE_RESP;
    memcpy(msg_srv->resp_buff + INT_SIZE, buff + off_block_num, SCAN_DATA_BLOCK_TRY_NUM_SIZE);
    msg_srv->resp_buff[MSG_SIZE_SCAN_DATA_POS] = SCAN_NONE_RESP;
    msg_srv->resp_buff[SCAN_RES_POS] = MSG_NIL;
}

bool handle_moved_keys(struct region_info_t *r_info, uint32_t sc, uint8_t bs, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    struct msg_srv_t *msg_srv = t_handler->msg_srv;

    if (r_info->scan_keys_list[sc]->k_count != 0) {
        if (log_level > 1)
            fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Scan %d found %lu moved keys\n", curr_dt(dt), t_handler->cid, sc, r_info->scan_keys_list[sc]->k_count);
        memset(r_info->bbuff, 0, FIX_BLOCK_SIZE);
        r_info->bbuff[MSG_TYPE_OFFSET] = MSG_GET;
        union s2b_u key_size;
        uint64_t k_shift_local = 0;
        msg_cli_t *msg_get = calloc(1, sizeof(*msg_get));

        if (msg_get != NULL)
            msg_get->kv = calloc(1, sizeof(*msg_get->kv));

        if (msg_get != NULL && msg_get->kv != NULL)
            msg_get->kv->qv = calloc(1, sizeof(*msg_get->kv->qv));

        if (msg_get != NULL && msg_get->kv != NULL && msg_get->kv->qv != NULL)
            msg_get->kv->key = malloc(FIX_BLOCK_SIZE);

        if (msg_get == NULL || msg_get->kv == NULL || msg_get->kv->qv == NULL || msg_get->kv->key == NULL) {
            fprintf(stderr, "%s WARN [SCAN_HANDLER=%015lu] Failed allocate msg_get for moved keys in %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
            free_scan_msg(msg_get);
            finish_thread(r_info, 150, true, t_handler);
            return true;
        }

        if (r_info->scan_keys_list[sc]->k_count > r_info->scan_keys_list[sc]->max_key_count ||
            r_info->scan_keys_list[sc]->k_shift_global > r_info->scan_keys_list[sc]->keys_mem_usage_limit) {
            fprintf(stderr, "%s ERROR [SCAN_HANDLER=%015lu] Wrong moved keys state sc=%u, k_count=%lu, max_key_count=%lu, k_shift_global=%lu, keys_mem_usage_limit=%lu, %s\n",
                    curr_dt(dt), t_handler->cid, sc, r_info->scan_keys_list[sc]->k_count, r_info->scan_keys_list[sc]->max_key_count, r_info->scan_keys_list[sc]->k_shift_global,
                    r_info->scan_keys_list[sc]->keys_mem_usage_limit, r_info->uuid);
            free_scan_msg(msg_get);
            finish_thread(r_info, 153, true, t_handler);
            return true;
        }

        for (uint64_t i = 0; i < r_info->scan_keys_list[sc]->k_count; i++) {
            memcpy(key_size.byte, r_info->scan_keys_list[sc]->key_sizes + i * 2, 2);

            if (key_size.short_val == 0)
                continue; // TODO Break

            if (k_shift_local > r_info->scan_keys_list[sc]->k_shift_global || key_size.short_val > r_info->scan_keys_list[sc]->k_shift_global - k_shift_local ||
                key_size.short_val > FIX_BLOCK_SIZE - KEY_SIZE_RECORD_OFFSET - KEY_RECORD_OFFSET) {
                fprintf(stderr, "%s ERROR [SCAN_HANDLER=%015lu] Wrong moved key size sc=%u, i=%lu, key_size=%u, k_count=%lu, k_shift_local=%lu, k_shift_global=%lu, %s\n",
                        curr_dt(dt), t_handler->cid, sc, i, key_size.short_val, r_info->scan_keys_list[sc]->k_count, k_shift_local, r_info->scan_keys_list[sc]->k_shift_global,
                        r_info->uuid);
                free_scan_msg(msg_get);
                finish_thread(r_info, 154, true, t_handler);
                return true;
            }
            memcpy(r_info->bbuff + KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET, r_info->scan_keys_list[sc]->keys + k_shift_local, key_size.short_val);
            memset(r_info->scan_keys_list[sc]->keys + k_shift_local, 0, key_size.short_val);
            memset(r_info->scan_keys_list[sc]->key_sizes + i * 2, 0, 2);
            memset(msg_srv->resp_buff, 0, msg_srv->curr_max_msg_size);
            k_shift_local += key_size.short_val;
            memcpy(r_info->bbuff + KEY_SIZE_RECORD_OFFSET, key_size.byte, 2);
            key_size.short_val += (KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET);
            memcpy(r_info->bbuff, key_size.byte, 2);
            msg_get->type = r_info->bbuff[MSG_TYPE_OFFSET] = MSG_GET;

            if (bytes_to_msg(msg_get, r_info->bbuff, key_size.short_val, t_handler) == 0)
                finish_thread(r_info, 33, true, t_handler);
            r_info->has_digest_calc = false;

            for (uint8_t bs_scan = 0; bs_scan < r_info->max_idx_b; bs_scan++) {
                build_hash_params(msg_get, r_info, bs_scan);

                if (find_k_idx(r_info, false, 0, bs_scan, t_handler) == true) {
                    if (r_info->r_idx_b[bs_scan]->idx[FBP][r_info->r_idx_b[bs_scan]->idx_r_help->k_idx].o_index != 0) {
                        if (log_level > 2) {
                            uint8_t b = r_info->r_idx_b[bs]->idx_r_help->b;
                            uint32_t k_idx = r_info->r_idx_b[bs]->idx_r_help->k_idx;
                            uint8_t *key_hex = extract_key(r_info->bbuff);
                            fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Moved key=%s, block=%u, msg_size=%u\n", curr_dt(dt), t_handler->cid, key_hex,
                                    r_info->r_idx_b[bs]->idx[b][k_idx].o_index, msg_get->msg_size);
                            free(key_hex);
                        }
                        uint32_t result = index_put(r_info, msg_get, r_info->bbuff, t_handler, bs_scan, false);

                        if (result != GET_OK) {
                        } else {
                            if (msg_srv->msg_size >= DISK_RECORD_HEADER_SIZE && record_header_load_le((uint8_t *)msg_srv->resp_buff + QV_SIZE_RECORD_OFFSET, 4) == 0) {
                                msg_srv->msg_size = 0;
                                break;
                            }

                            if (msg_srv->msg_size % r_info->r_idx_b[bs]->bsize_factor == 0)
                                r_info->scan_vars->record_size_round_up_unzip = ceil(msg_srv->msg_size / r_info->r_idx_b[bs]->bsize_factor + 0) * r_info->r_idx_b[bs]->bsize_factor;
                            else
                                r_info->scan_vars->record_size_round_up_unzip = ceil(msg_srv->msg_size / r_info->r_idx_b[bs]->bsize_factor + 1) * r_info->r_idx_b[bs]->bsize_factor;
                            r_info->scan_vars->last_record_size_unzip = msg_srv->msg_size;
                            msg_srv->msg_size = r_info->scan_vars->record_size_round_up_unzip;
                            memcpy(r_info->zbuff, msg_srv->resp_buff, msg_srv->msg_size);
                            r_info->zbuff += msg_srv->msg_size;
                            r_info->scan_vars->unzip_msg_size_sum += msg_srv->msg_size;
                            break;
                        }
                    }
                }
            }

            if (r_info->scan_vars->unzip_msg_size_sum + msg_srv->msg_size * 10 > BLOCK_MAX_SIZE) {
                uint64_t processed_key_count = i + 1;
                uint64_t remaining_key_count = r_info->scan_keys_list[sc]->k_count - processed_key_count;
                uint64_t remaining_keys_size = r_info->scan_keys_list[sc]->k_shift_global - k_shift_local;
                memmove(r_info->scan_keys_list[sc]->keys, r_info->scan_keys_list[sc]->keys + k_shift_local, remaining_keys_size);
                memmove(r_info->scan_keys_list[sc]->key_sizes, r_info->scan_keys_list[sc]->key_sizes + processed_key_count * 2, remaining_key_count * 2);
                memset(r_info->scan_keys_list[sc]->keys + remaining_keys_size, 0, k_shift_local);
                memset(r_info->scan_keys_list[sc]->key_sizes + remaining_key_count * 2, 0, processed_key_count * 2);
                r_info->scan_keys_list[sc]->k_count = remaining_key_count;
                r_info->scan_keys_list[sc]->k_shift_global = remaining_keys_size;
                r_info->scan_keys_list[sc]->scan_block_shift++;
                free_scan_msg(msg_get);
                return true;
            }
        }
        r_info->scan_keys_list[sc]->k_count = 0;
        r_info->scan_keys_list[sc]->k_shift_global = 0;
        free_scan_msg(msg_get);
    }
    return false;
}

void prepare_scan_data(struct region_info_t *r_info, uint8_t *buff, struct msg_srv_t *msg_srv, uint8_t bs, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    r_info->zbuff -= r_info->scan_vars->unzip_msg_size_sum;
    uint8_t off_block_num = offsetof(struct msg_cli_scan_t, block_num);
    bool is_buff_size_correct = true;
    r_info->scan_vars->unzip_msg_size_sum -= (r_info->scan_vars->record_size_round_up_unzip - r_info->scan_vars->last_record_size_unzip);

    if (r_info->scan_vars->unzip_msg_size_sum + r_info->scan_vars->tech_offset > msg_srv->curr_max_msg_size) {
        // TODO Сделать это вызовой единой функции, а не как сейчас в куче мест натыкано
        if (r_info->scan_vars->unzip_msg_size_sum + r_info->scan_vars->tech_offset > r_info->zbuff_size) {
            fprintf(stderr, "%s ERROR [SCAN_HANDLER=%015lu] Data size %s too big, size=%u, tech_offset=%d\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname,
                    r_info->scan_vars->unzip_msg_size_sum, r_info->scan_vars->tech_offset);
            is_buff_size_correct = false;
        } else {
            if (realloc_resp_buff(t_handler->msg_srv, r_info->scan_vars->unzip_msg_size_sum * 2, t_handler->cid) < 0)
                finish_thread(r_info, 34, true, t_handler);
        }
    }

    if (is_buff_size_correct)
        memcpy(msg_srv->resp_buff + r_info->scan_vars->tech_offset, r_info->zbuff, r_info->scan_vars->unzip_msg_size_sum);
    else {
        finish_scan_none(r_info, buff, off_block_num, msg_srv);
        return;
    }
    msg_srv->msg_size = r_info->scan_vars->unzip_msg_size_sum;
    union i2b_u i2b_block_size;
    msg_srv->msg_size += r_info->scan_vars->tech_offset;
    i2b_block_size.integer_val = msg_srv->msg_size;
    memcpy(msg_srv->resp_buff, i2b_block_size.byte, INT_SIZE);
    memcpy(msg_srv->resp_buff + INT_SIZE, buff + off_block_num, SCAN_DATA_BLOCK_TRY_NUM_SIZE);
    i2b_block_size.integer_val = r_info->r_idx_b[bs]->pos - 1;
    memcpy(msg_srv->resp_buff + INT_SIZE + SCAN_DATA_BLOCK_TRY_NUM_SIZE, i2b_block_size.byte, INT_SIZE);

    if (log_level > 1)
        fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Send scan result size=%d\n", curr_dt(dt), t_handler->cid, msg_srv->msg_size);
}


int check_scan_bs_end(struct region_info_t *r_info, uint32_t sc, uint64_t read_block, uint8_t *buff, struct msg_srv_t *msg_srv, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    uint8_t b = FBP; // TODO Для нескольких бакетов
    uint8_t off_block_num = offsetof(struct msg_cli_scan_t, block_num);
    int8_t bs = r_info->scan_keys_list[sc]->bs;

    if (read_block >= r_info->r_idx_b[bs]->b_offset[b] && msg_srv->msg_size == 0) {
        if (bs < r_info->max_idx_b && bs != 0) {
            if (log_level > 1)
                fprintf(stderr,
                        "%s DEBUG [SCAN_HANDLER=%015lu] Switch to next bs=%d. Current block=%d, read_block=%lu, requested=%d, scan_block_num_rewind=%d, scan_block_num=%d, "
                        "scan_block_shift=%d\n",
                        curr_dt(dt), t_handler->cid, bs, r_info->r_idx_b[bs]->b_offset[b], read_block, r_info->scan_keys_list[sc]->client_scan_block_num,
                        r_info->scan_keys_list[sc]->scan_block_num_rewind, r_info->scan_keys_list[sc]->scan_block_num, r_info->scan_keys_list[sc]->scan_block_shift);

            if (r_info->scan_vars->unzip_msg_size_sum > 0) {
                prepare_scan_data(r_info, buff, msg_srv, bs, t_handler);
                return 0;
            }
            r_info->scan_keys_list[sc]->scan_block_num = 0;
            r_info->scan_keys_list[sc]->last_client_scan_block_count = 0;
            bs = get_next_size_bs(r_info, r_info->scan_keys_list[sc]->bs, t_handler);
            r_info->scan_keys_list[sc]->bs = bs;
            r_info->scan_keys_list[sc]->scan_block_num_rewind += (r_info->scan_keys_list[sc]->client_scan_block_num - 1);
            r_info->scan_keys_list[sc]->client_scan_block_num = 1;
            r_info->scan_keys_list[sc]->scan_block_shift = 0;

            if (log_level > 1)
                fprintf(stderr,
                        "%s DEBUG [SCAN_HANDLER=%015lu] Switched to next bs=%d. Current block=%d, read_block=%lu, requested=%d, scan_block_num_rewind=%d, scan_block_num=%d, "
                        "scan_block_shift=%d\n",
                        curr_dt(dt), t_handler->cid, bs, r_info->r_idx_b[bs]->b_offset[b], read_block, r_info->scan_keys_list[sc]->client_scan_block_num,
                        r_info->scan_keys_list[sc]->scan_block_num_rewind, r_info->scan_keys_list[sc]->scan_block_num, r_info->scan_keys_list[sc]->scan_block_shift);
            return 1;
        }

        if (r_info->scan_vars->unzip_msg_size_sum > 0) {
            prepare_scan_data(r_info, buff, msg_srv, bs, t_handler);
            return 0;
        }

        if (log_level > 1)
            fprintf(stderr,
                    "%s DEBUG [SCAN_HANDLER=%015lu] Nothing to scan. Current block=%d, read_block=%lu, requested=%d, scan_block_num=%d, scan_block_num_rewind=%d, bs=%d, "
                    "unzip_msg_size_sum=%d\n",
                    curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->b_offset[b], read_block, r_info->scan_keys_list[sc]->client_scan_block_num,
                    r_info->scan_keys_list[sc]->scan_block_num, r_info->scan_keys_list[sc]->scan_block_num_rewind, bs, r_info->scan_vars->unzip_msg_size_sum);
        r_info->scan_keys_list[sc]->cid = 0;
        r_info->zbuff -= r_info->scan_vars->unzip_msg_size_sum;
        finish_scan_none(r_info, buff, off_block_num, msg_srv);
        return 0;
    }
    return 2;
}

static inline int hbase_bytes_compare(const uint8_t *a, size_t alen, const uint8_t *b, size_t blen) {
    if (a == b)
        return 0;

    if (!a)
        return -1;

    if (!b)
        return 1;
    size_t min = (alen < blen) ? alen : blen;

    if (min) {
        int c = memcmp(a, b, min);

        if (c != 0)
            return (c < 0) ? -1 : 1;
    }

    if (alen == blen)
        return 0;
    return (alen < blen) ? -1 : 1;
}


static inline bool hbase_scan_range_valid(const msg_cli_scan_hbase_t *req, bool stop_exclusive) {
    const bool hasStart = (req->start_key_len > 0);
    const bool hasStop = (req->end_key_len > 0);

    if ((hasStart && req->start_key == NULL) || (hasStop && req->end_key == NULL)) {
        return false;
    }

    if (req->start_key_len > FIX_BLOCK_SIZE || req->end_key_len > FIX_BLOCK_SIZE) {
        return false;
    }

    if (hasStart && hasStop) {
        int cmp = hbase_bytes_compare(req->start_key, req->start_key_len, req->end_key, req->end_key_len);

        if (stop_exclusive)
            return (cmp < 0);
        else
            return (cmp <= 0);
    }
    return true;
}

static inline bool hbase_key_in_range(const uint8_t *key, size_t key_len, const msg_cli_scan_hbase_t *req, bool stop_exclusive) {
    if (key == NULL)
        return false;

    if (key_len > FIX_BLOCK_SIZE)
        return false;
    const bool hasStart = (req->start_key_len > 0);
    const bool hasStop = (req->end_key_len > 0);

    if ((hasStart && req->start_key == NULL) || (hasStop && req->end_key == NULL)) {
        return false;
    }

    if (hasStart) {
        int c = hbase_bytes_compare(key, key_len, req->start_key, req->start_key_len);

        if (c < 0)
            return false;
    }

    if (hasStop) {
        int c = hbase_bytes_compare(key, key_len, req->end_key, req->end_key_len);

        if (stop_exclusive) {
            if (c >= 0)
                return false;
        } else {
            if (c > 0)
                return false;
        }
    }
    return true;
}


void apply_scan_filter(const msg_cli_scan_hbase_t *scan_msg, const uint8_t *key, size_t key_size, bool *out_in_range) {
    if (!hbase_scan_range_valid(scan_msg, region_border_exclusive)) {
        *out_in_range = false;
        return;
    }

    if (scan_msg->start_key_len == 0 && scan_msg->end_key_len == 0) {
        *out_in_range = true;
        return;
    }
    *out_in_range = hbase_key_in_range(key, key_size, scan_msg, region_border_exclusive);
}


uint16_t u16_le(const uint8_t *p) {
    return (uint16_t)p[0] | (uint16_t)(p[1] << 8);
}


uint32_t u32_le(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}


bool parse_msg_cli_scan(const uint8_t *buf, uint32_t sz, msg_cli_scan_hbase_t *out) {
    if (!buf || !out)
        return false;
    size_t off = 0;

    if (sz < 4 + 1 + 4 + 4 + 2 + 2 + 4)
        return false;
    out->msg_size = u32_le(buf + off);
    off += 4;
    out->type = buf[off];
    off += 1;
    out->region_pos = u32_le(buf + off);
    off += 4;
    out->block_num = u32_le(buf + off);
    off += 4;
    out->try_num = u16_le(buf + off);
    off += 2;
    out->reserve = u16_le(buf + off);
    off += 2;
    out->start_key_len = u32_le(buf + off);
    off += 4;

    if (out->start_key_len > FIX_BLOCK_SIZE)
        return false;

    if (sz < off + (size_t)out->start_key_len + 4)
        return false;
    out->start_key = (out->start_key_len == 0) ? NULL : (buf + off);
    off += (size_t)out->start_key_len;
    out->end_key_len = u32_le(buf + off);
    off += 4;

    if (out->end_key_len > FIX_BLOCK_SIZE)
        return false;

    if (sz < off + (size_t)out->end_key_len)
        return false;
    out->end_key = (out->end_key_len == 0) ? NULL : (buf + off);
    off += (size_t)out->end_key_len;

    if (out->msg_size != 0 && out->msg_size != off)
        return false;
    return true;
}

void prepare_scan_data_simple(region_info_t *r_info, uint8_t *buff, msg_srv_t *msg_srv, uint8_t bs, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    r_info->zbuff -= r_info->scan_vars->unzip_msg_size_sum;
    uint8_t off_block_num = offsetof(struct msg_cli_scan_t, block_num);
    bool is_buff_size_correct = true;
    r_info->scan_vars->unzip_msg_size_sum -= (r_info->scan_vars->record_size_round_up_unzip - r_info->scan_vars->last_record_size_unzip);

    if (r_info->scan_vars->unzip_msg_size_sum + r_info->scan_vars->tech_offset > msg_srv->curr_max_msg_size) {
        // TODO: вынести в общую функцию
        if (r_info->scan_vars->unzip_msg_size_sum + r_info->scan_vars->tech_offset > r_info->zbuff_size) {
            fprintf(stderr, "%s ERROR [SCAN_HANDLER=%015lu] Data size %s too big, size=%u, tech_offset=%d\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname,
                    r_info->scan_vars->unzip_msg_size_sum, r_info->scan_vars->tech_offset);
            is_buff_size_correct = false;
        } else {
            if (realloc_resp_buff(msg_srv, r_info->scan_vars->unzip_msg_size_sum * 2, t_handler->cid) < 0)
                finish_thread(r_info, 36, true, t_handler);
        }
    }

    if (is_buff_size_correct) {
        memcpy(msg_srv->resp_buff + r_info->scan_vars->tech_offset, r_info->zbuff, r_info->scan_vars->unzip_msg_size_sum);
    } else {
        finish_scan_none(r_info, buff, off_block_num, msg_srv);
        return;
    }
    msg_srv->msg_size = r_info->scan_vars->unzip_msg_size_sum;
    msg_srv->msg_size += r_info->scan_vars->tech_offset;
    union i2b_u i2b_val;
    i2b_val.integer_val = msg_srv->msg_size;
    memcpy(msg_srv->resp_buff, i2b_val.byte, INT_SIZE);
    memcpy(msg_srv->resp_buff + INT_SIZE, buff + off_block_num, INT_SIZE);
    memcpy(msg_srv->resp_buff + INT_SIZE + INT_SIZE, buff + off_block_num + INT_SIZE, SHORT_SIZE);
    msg_srv->resp_buff[INT_SIZE + INT_SIZE + SHORT_SIZE + 0] = 0;
    msg_srv->resp_buff[INT_SIZE + INT_SIZE + SHORT_SIZE + 1] = 0;
    i2b_val.integer_val = r_info->r_idx_b[bs]->pos - 1;
    memcpy(msg_srv->resp_buff + INT_SIZE + SCAN_DATA_BLOCK_TRY_NUM_SIZE, i2b_val.byte, INT_SIZE);
    uint32_t real_block_num = 0;
    scan_keys_list_t *scan_control = get_current_scan_control(msg_srv, t_handler->cid);

    if (scan_control == NULL) {
        fprintf(stderr, "%s ERROR [SCAN_HANDLER=%015lu] Lost current scan control for %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
        finish_thread(r_info, 155, true, t_handler);
        return;
    }
    real_block_num = (uint32_t)scan_control->curr_offset;
    i2b_val.integer_val = real_block_num;
    memcpy(msg_srv->resp_buff + INT_SIZE + SCAN_DATA_BLOCK_TRY_NUM_SIZE + INT_SIZE, &real_block_num, INT_SIZE);

    if (log_level > 1)
        fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Send scan result size=%d, bs=%d, real_block=%u\n", curr_dt(dt), t_handler->cid, msg_srv->msg_size, bs, real_block_num);
}


int check_scan_bs_end_simple(region_info_t *r_info, uint32_t sc, uint64_t *read_block, uint8_t *buff, msg_srv_t *msg_srv, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    uint8_t b = FBP; // TODO Для нескольких бакетов
    uint8_t off_block_num = offsetof(struct msg_cli_scan_t, block_num);
    int8_t bs = r_info->scan_keys_list[sc]->bs;

    if (*read_block >= r_info->r_idx_b[bs]->b_offset[b] && msg_srv->msg_size == 0) {
        if (bs < r_info->max_idx_b && bs != 0) {
            if (log_level > 1)
                fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Switch to next bs=%d. Current block=%d, *read_block=%lu, requested=%u, scan_block_num=%d, scan_block_shift=%d\n",
                        curr_dt(dt), t_handler->cid, bs, r_info->r_idx_b[bs]->b_offset[b], *read_block, r_info->scan_keys_list[sc]->client_scan_block_num,
                        r_info->scan_keys_list[sc]->scan_block_num, r_info->scan_keys_list[sc]->scan_block_shift);

            if (r_info->scan_vars->unzip_msg_size_sum > 0) {
                prepare_scan_data_simple(r_info, buff, msg_srv, bs, t_handler);
                return 0;
            }
            r_info->scan_keys_list[sc]->curr_offset = *read_block = 0;
            r_info->scan_keys_list[sc]->client_scan_block_num = 1;
            r_info->scan_keys_list[sc]->scan_block_num = 0;
            bs = get_next_size_bs(r_info, r_info->scan_keys_list[sc]->bs, t_handler);
            r_info->scan_keys_list[sc]->bs = bs;
            r_info->scan_keys_list[sc]->scan_block_shift = 0;

            if (log_level > 1)
                fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Switched to next bs=%d. Current block=%d, *read_block=%lu, requested=%u, scan_block_num=%d, scan_block_shift=%d\n",
                        curr_dt(dt), t_handler->cid, bs, r_info->r_idx_b[bs]->b_offset[b], *read_block, r_info->scan_keys_list[sc]->client_scan_block_num,
                        r_info->scan_keys_list[sc]->scan_block_num, r_info->scan_keys_list[sc]->scan_block_shift);
            return 1;
        }

        if (r_info->scan_vars->unzip_msg_size_sum > 0) {
            prepare_scan_data_simple(r_info, buff, msg_srv, bs, t_handler);
            return 0;
        }

        if (log_level > 1)
            fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Nothing to scan. Current block=%d, read_block=%lu, requested=%u, scan_block_num=%d, bs=%d, unzip_msg_size_sum=%d\n",
                    curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->b_offset[b], *read_block, r_info->scan_keys_list[sc]->client_scan_block_num,
                    r_info->scan_keys_list[sc]->scan_block_num, bs, r_info->scan_vars->unzip_msg_size_sum);
        r_info->scan_keys_list[sc]->cid = 0;
        r_info->zbuff -= r_info->scan_vars->unzip_msg_size_sum;
        finish_scan_none(r_info, buff, off_block_num, msg_srv);
        return 0;
    }
    return 2;
}

static bool scan_record_is_current(region_info_t *r_info, uint8_t b, uint8_t bs, uint32_t block, uint8_t *key, uint32_t key_size, table_handler_t *t_handler) {
    if (r_info->enable_block_compression != BLOCK_COMPRESSION_NONE || r_info->r_idx_b[bs]->block_size != FIX_BLOCK_SIZE || r_info->r_idx_b[bs]->fd[b] != r_info->data_000_fd)
        return true;
    msg_kv_t kv = {.key = key, .key_size = key_size};
    msg_cli_t record = {.kv = &kv};
    region_idx_helper_t saved_helper = *r_info->r_idx_b[bs]->idx_r_help;
    uint8_t saved_digest[DIGEST_SIZE_FULL];
    memcpy(saved_digest, r_info->digest, sizeof(saved_digest));
    bool saved_has_digest = r_info->has_digest_calc;
    uint32_t current_block = 0;
    bool is_current = get_current_record_block(r_info, &record, bs, &current_block, t_handler) && current_block == block;
    *r_info->r_idx_b[bs]->idx_r_help = saved_helper;
    memcpy(r_info->digest, saved_digest, sizeof(saved_digest));
    r_info->has_digest_calc = saved_has_digest;
    return is_current;
}

int scan_rows(struct region_info_t *r, uint8_t *buf, uint32_t size, table_handler_t *h) {
    char dt[DT_LOG_SIZE];
    h->all_req_count++;

    if (size < MSG_CLI_HBASE_SCAN_FILTER_SIZE)
        return -1;
    uint64_t read_block = 0;
    r->scan_vars->unzip_msg_size_sum = 0;
    r->scan_vars->unzip_msg_size = 0;
    r->scan_vars->record_size_round_up = 0;
    r->scan_vars->record_size_round_up_unzip = 0;
    r->scan_vars->last_record_size_unzip = 0;
    r->scan_vars->tech_offset = INT_SIZE + SCAN_DATA_BLOCK_TRY_NUM_SIZE + INT_SIZE + INT_SIZE;
    r->scan_vars->curr_record_offset = 0;
    r->scan_vars->last_record_size = 0;
    struct msg_srv_t *reply = h->msg_srv;
    struct msg_cli_t *msg = h->msg_cli;

    if (log_level > 1)
        fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Got scan request.\n", curr_dt(dt), h->cid);
    uint8_t b = FBP; // TODO Для нескольких бакетов
    uint8_t bs = 0;
    union i2b_u i2b_block_num;
    uint8_t off_block_num = offsetof(struct msg_cli_scan_t, block_num);
    memcpy(i2b_block_num.byte, buf + off_block_num, INT_SIZE);

    if (log_level > 1)
        fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Requested %d block, filename=%s, max_bs=%d, bs=%d\n", curr_dt(dt), h->cid, i2b_block_num.integer_val, r->uuid, r->max_idx_b,
                bs);
    uint32_t sc = get_scan_num(r, h);

    if (sc == -1) {
        fprintf(stderr, "%s ERROR [SCAN_HANDLER=%015lu] Scan on write wrong sc=%d\n", curr_dt(dt), h->cid, sc);
        return -1;
    }
    uint8_t bs_pos = offsetof(struct msg_cli_scan_hbase_t, reserve);

    if (buf[bs_pos] != 0xFF && new_scan_result == 2) {
        uint8_t pos = buf[bs_pos];

        if (pos > BLOCK_ARRAY_LAST_POS)
            return -1;
        bs = r->bs_map[BLOCK_ARRAY_LAST_POS - pos];

        if (bs >= r->max_idx_b || !r->r_idx_b[bs] || r->r_idx_b[bs]->pos != pos + 1U)
            return -1;
        r->scan_keys_list[sc]->bs = bs;
    } else
        bs = r->scan_keys_list[sc]->bs;
    r->scan_keys_list[sc]->client_scan_block_num = i2b_block_num.integer_val;
    reply->allow_multiple_block_scan = true;

    if (log_level > 1)
        fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] BS map %u block (REAL), filename=%s, bs=%d, bs_map[%d]=%d, block_size=%d, (client=%u + scan=%u)\n", curr_dt(dt), h->cid,
                r->scan_keys_list[sc]->client_scan_block_num, r->r_idx_b[bs]->fname, bs, bs, r->bs_map[bs], r->r_idx_b[bs]->block_size,
                r->scan_keys_list[sc]->client_scan_block_num, r->scan_keys_list[sc]->scan_block_num);
    r->scan_keys_list[sc]->last_scan_ts = getCurrentTimeMs();

    if (handle_moved_keys(r, sc, bs, h)) {
        prepare_scan_data_simple(r, buf, reply, bs, h);
        return 0;
    }

    for (;;) {
        do {
            read_block = (uint64_t)r->scan_keys_list[sc]->client_scan_block_num + r->scan_keys_list[sc]->scan_block_num;

            if (read_block >= r->r_idx_b[bs]->f_idx_size[b]) {
                fprintf(stderr, "%s ERROR [SCAN_HANDLER=%015lu] Wrong read block %lu (%u + %u) bs=%d, b_offset=%u\n", curr_dt(dt), h->cid, read_block,
                        r->scan_keys_list[sc]->client_scan_block_num, r->scan_keys_list[sc]->scan_block_num, bs, r->r_idx_b[bs]->b_offset[b]);
                goto scan_error;
            }
            reply->msg_size = r->r_idx_b[bs]->idx[b][read_block].z_index * r->r_idx_b[bs]->bsize_factor;

            if (reply->msg_size == 0 && read_block < r->r_idx_b[bs]->b_offset[b]) {
                r->scan_keys_list[sc]->client_scan_block_num++;
            }
        } while (reply->msg_size == 0 && read_block < r->r_idx_b[bs]->b_offset[b]);
        int rc = check_scan_bs_end_simple(r, sc, &read_block, buf, reply, h);
        bs = r->scan_keys_list[sc]->bs;

        if (rc == 0)
            return 0;

        if (rc == 1)
            continue;
        uint32_t read_size = read_file(r->r_idx_b[bs]->fd[b], r->bbuff + r->scan_vars->tech_offset, read_block * r->r_idx_b[bs]->block_size, reply->msg_size, r, h->cid);

        if (log_level > 1)
            fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Read %d bytes from %s, block=%lu (%d + %d)\n", curr_dt(dt), h->cid, read_size, r->r_idx_b[bs]->fname, read_block,
                    r->scan_keys_list[sc]->scan_block_num, r->scan_keys_list[sc]->client_scan_block_num);
        uint16_t bsize_factor = r->r_idx_b[bs]->bsize_factor;

        if (read_size > 0) {
            if (r->enable_block_compression > BLOCK_COMPRESSION_NONE) {
                memcpy(r->rbuff, r->bbuff + r->scan_vars->tech_offset, read_size);
                uint64_t decompressed_size = 0;
                uint64_t decompress_result = decompress_block(r->rbuff, read_size, read_block, bs, r, &decompressed_size, r->scan_vars->tech_offset, h);

                if (decompress_result != 0) {
                    fprintf(stderr, "%s WARN [SCAN_HANDLER=%015lu] Failed decompress %s, block=%lu, region stopped\n", curr_dt(dt), h->cid, r->r_idx_b[bs]->fname, read_block);
                    r->bad_data = true;
                    r->stop = true;
                    goto scan_error;
                }

                if (decompress_result == 0) {
                    uint64_t new_zbuff_size = r->scan_vars->unzip_msg_size_sum + decompressed_size + MAX_BSIZE_FACTOR;

                    if (r->zbuff_size < new_zbuff_size) {
                        uint8_t *zbuff_tmp = realloc(r->zbuff - r->scan_vars->unzip_msg_size_sum, new_zbuff_size);

                        if (!zbuff_tmp) {
                            fprintf(stderr, "%s WARN [SCAN_HANDLER=%015lu] Failed realloc rbuff to %lu\n", curr_dt(dt), h->cid, new_zbuff_size);
                            goto scan_error;
                        }
                        r->zbuff = zbuff_tmp + r->scan_vars->unzip_msg_size_sum;
                        r->zbuff_size = new_zbuff_size;
                    }

                    if (log_level > 2)
                        fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Decompressed block=%lu %s, decompressed_size=%lu, read_size=%u, z_index=%u\n", curr_dt(dt), h->cid,
                                read_block, r->uuid, decompressed_size, read_size, r->r_idx_b[bs]->idx[b][read_block].z_index);
                }
                reply->msg_size = align_up(decompressed_size, MAX_BSIZE_FACTOR);
                memset(r->bbuff + r->scan_vars->tech_offset + decompressed_size, 0, reply->msg_size - decompressed_size);
                bsize_factor = FIX_BSIZE_FACTOR;
            }
        }
        r->scan_keys_list[sc]->curr_offset = read_block;
        union i2b_u i2b_record_size_unzip;
        union i2b_u i2b_record_size;
        r->scan_vars->curr_record_offset = 0;
        r->scan_vars->last_record_size = 0;
        r->scan_vars->unzip_msg_size = 0;
        uint32_t keys = 0;
        uint32_t qv_size = 0;
        uint32_t cf_size = 0;
        uint32_t key_size = 0;
        uint32_t msg_size = 0;
        uint32_t curr = 0;
        uint8_t off_k_end = offsetof(struct msg_cli_scan_t, end_key);
        uint8_t off_k_beg = offsetof(struct msg_cli_scan_t, start_key);
        bool up_border = false;
        bool down_border = false;
        bool key_in_range = true;

        do {
            uint32_t off = r->scan_vars->tech_offset + r->scan_vars->curr_record_offset;
            memcpy(i2b_record_size.byte, r->bbuff + off, INT_SIZE);

            if (i2b_record_size.integer_val == 0)
                break;
            disk_record_header_t disk_header;

            if (r->scan_vars->curr_record_offset > reply->msg_size ||
                !disk_record_decode_header(r->bbuff + off, reply->msg_size - r->scan_vars->curr_record_offset, &disk_header) ||
                (disk_header.kind == DISK_RECORD_TOMBSTONE && r->r_idx_b[bs]->fd[b] != r->data_000_fd)) {
                stop_region_after_key_error(r, KEY_EXTRACT_INVALID_STATE, h);
                goto scan_error;
            }
            curr = 0;
            msg_size = buff_to_field(r->bbuff + off, &curr, sizeof(msg->kv->key_size));
            curr = KEY_SIZE_RECORD_OFFSET;
            key_size = buff_to_field(r->bbuff + off, &curr, sizeof(msg->kv->key_size));
            cf_size = buff_to_field(r->bbuff + off, &curr, sizeof(msg->kv->cf_size));
            uint32_t lob_chk_offset = off + offsetof(msg_cli_t, lob_info);

            if (r->bbuff[lob_chk_offset] == 1) {
                qv_size = 0;
                curr += sizeof(msg->kv->qv_size);
            } else {
                qv_size = buff_to_field(r->bbuff + off, &curr, sizeof(msg->kv->qv_size));

                if (key_size > BLOCK_MAX_SIZE || cf_size > BLOCK_MAX_SIZE || qv_size > BLOCK_MAX_SIZE) {
                    uint8_t *key_hex = extract_key(r->bbuff + off);
                    fprintf(stderr, "%s WARN [SCAN_HANDLER=%015lu] Error code 300. Cannot process %s, key=%s, r_real_offset=%u, block_num=%lu, type=%d\n", curr_dt(dt), h->cid,
                            r->r_idx_b[bs]->fname, key_hex, r->scan_vars->tech_offset, read_block, msg->type);
                    free(key_hex);
                    data_fix(r, bs, read_block, r->scan_vars->tech_offset, h);
                }
            }

            if (key_size > r->scan_key_size) {
                uint8_t *scan_key_tmp = realloc(r->scan_key, key_size);

                if (!scan_key_tmp) {
                    fprintf(stderr, "%s WARN [SCAN_HANDLER=%015lu] Failed realloc to %u scan_key for %s\n", curr_dt(dt), h->cid, key_size, r->uuid);
                    goto scan_error;
                }
                r->scan_key_size = key_size;
                r->scan_key = scan_key_tmp;
            }
            memcpy(r->scan_key, r->bbuff + off + curr, key_size);
            key_in_range = true;

            if (hbase_scan_filter) {
                if (size > MSG_CLI_HBASE_SCAN_FILTER_SIZE) {
                    msg_cli_scan_hbase_t req;
                    bool ok = parse_msg_cli_scan(buf, size, &req);

                    if (!ok) {
                        fprintf(stderr, "%s WARN [SCAN_HANDLER=%015lu] Failed parse_msg_cli_scan %u scan_key for %s\n", curr_dt(dt), h->cid, size, r->uuid);
                        goto scan_error;
                    } else
                        apply_scan_filter(&req, r->scan_key, key_size, &key_in_range);
                }
            } else {
                if (size == MSG_CLI_SCAN_FILTER_SIZE) {
                    down_border = false;
                    up_border = false;

                    for (int i = 0; i < REGION_BYTE_RANGE; i++) {
                        if (i == key_size)
                            break;

                        if (r->scan_key[i] > buf[off_k_beg + i] && r->scan_key[i] < buf[off_k_end + i])
                            break;

                        if (r->scan_key[i] == buf[off_k_beg + i] && !down_border && r->scan_key[i] == buf[off_k_end + i] && !up_border)
                            continue;

                        if ((r->scan_key[i] < buf[off_k_beg + i] && !down_border) || (r->scan_key[i] > buf[off_k_end + i] && !up_border)) {
                            key_in_range = false;
                            break;
                        }

                        if (r->scan_key[i] > buf[off_k_beg + i])
                            down_border = true;

                        if (r->scan_key[i] < buf[off_k_end + i])
                            up_border = true;

                        if (i == REGION_BYTE_RANGE - 1 && r->scan_key[i] == buf[off_k_end + i] && !up_border && region_border_exclusive == true)
                            key_in_range = false;
                    }
                }
            }

            if (disk_header.kind != DISK_RECORD_TOMBSTONE && key_in_range && (r->bbuff[lob_chk_offset] == 1 && allow_scan_lob == 1 || r->bbuff[lob_chk_offset] == 0) &&
                scan_record_is_current(r, b, bs, (uint32_t)read_block, r->scan_key, key_size, h)) {
                keys++;
                r->req_scn_counter++;
                r->req_scn_counter_sum++;

                if (r->enable_record_compression == 1 && r->bbuff[lob_chk_offset] != 1) {
                    uint32_t qv_offset = KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET + key_size + cf_size;

                    if ((r->bbuff[off + ZIP_INFO_RECORD_OFFSET] & 0x01) == 1) {
                        memcpy(r->zbuff + r->scan_vars->unzip_msg_size, r->bbuff + off, qv_offset);
                        uint64_t dst_capacity = r->zbuff_size - (r->scan_vars->unzip_msg_size_sum + r->scan_vars->unzip_msg_size + qv_offset);
                        msg_size =
                            decompress_record(r, r->bbuff + off + qv_offset, msg_size - qv_offset, r->zbuff + qv_offset + r->scan_vars->unzip_msg_size, dst_capacity, h->cid);

                        if (msg_size == 0) {
                            uint8_t *key_hex = extract_key(r->zbuff + r->scan_vars->unzip_msg_size);
                            fprintf(stderr, "%s WARN [SCAN_HANDLER=%015lu] Error code 268. Cannot process %s, key=%s, r_real_offset=%d, block_num=%lu, type=%d\n", curr_dt(dt),
                                    h->cid, r->r_idx_b[bs]->fname, key_hex, r->scan_vars->tech_offset, read_block, msg->type);
                            free(key_hex);
                            r->bad_data = true;
                            r->stop = true;
                            goto scan_error;
                        }
                        msg_size += qv_offset;
                    } else {
                        memcpy(r->zbuff + r->scan_vars->unzip_msg_size, r->bbuff + off, msg_size);
                    }

                    if (r->enable_dict == 1) {
                        if (prepare_extract_region_buff(r, r->zbuff + r->scan_vars->unzip_msg_size, msg_size, key_size, cf_size, qv_size, bs, &r->rbuff, &r->rbuff_size, h->cid) !=
                            0)
                            goto scan_error;
                        msg_size = extract_qfls(r, r->zbuff + r->scan_vars->unzip_msg_size, r->rbuff, r->rbuff_size, msg_size, key_size, cf_size, qv_size, bs, false, h->cid);

                        if (msg_size == 0) {
                            uint8_t *key_hex = extract_key(r->zbuff + r->scan_vars->unzip_msg_size);
                            fprintf(stderr, "%s WARN [SCAN_HANDLER=%015lu] Error code 267. Cannot process %s, key=%s, r_real_offset=%d, block_num=%lu, type=%d\n", curr_dt(dt),
                                    h->cid, r->r_idx_b[bs]->fname, key_hex, r->scan_vars->tech_offset, read_block, msg->type);
                            free(key_hex);
                            r->bad_data = true;
                            r->stop = true;
                            goto scan_error;
                        }
                        uint64_t new_zbuff_size = r->scan_vars->unzip_msg_size_sum + r->scan_vars->unzip_msg_size + msg_size + MAX_BSIZE_FACTOR;

                        if (r->zbuff_size < new_zbuff_size) {
                            uint8_t *zbuff_tmp = realloc(r->zbuff - r->scan_vars->unzip_msg_size_sum, new_zbuff_size);

                            if (!zbuff_tmp) {
                                fprintf(stderr, "%s WARN [SCAN_HANDLER=%015lu] Failed realloc to %lu zbuff for %s, sleep 10s\n", curr_dt(dt), h->cid, new_zbuff_size, r->uuid);
                                sleep(10);
                                goto scan_error;
                            } else {
                                r->zbuff = zbuff_tmp + r->scan_vars->unzip_msg_size_sum;
                                r->zbuff_size = new_zbuff_size;
                            }
                        }
                        memcpy(r->zbuff + r->scan_vars->unzip_msg_size, r->rbuff, msg_size);
                    }
                    i2b_record_size_unzip.integer_val = msg_size;
                    memcpy(r->zbuff + r->scan_vars->unzip_msg_size, i2b_record_size_unzip.byte, INT_SIZE);
                } else {
                    if (r->enable_dict == 1 && r->bbuff[lob_chk_offset] != 1) {
                        if (prepare_scan_extract_buff(r, r->bbuff + off, msg_size, key_size, cf_size, qv_size, bs, h->cid) == false)
                            goto scan_error;
                        msg_size =
                            extract_qfls(r, r->bbuff + off, r->zbuff + r->scan_vars->unzip_msg_size,
                                         r->zbuff_size - r->scan_vars->unzip_msg_size_sum - r->scan_vars->unzip_msg_size, msg_size, key_size, cf_size, qv_size, bs, false, h->cid);

                        if (msg_size == 0) {
                            uint8_t *key_hex = extract_key(r->bbuff + off);
                            fprintf(stderr, "%s WARN [SCAN_HANDLER=%015lu] Error code 269. Cannot process %s, key=%s, r_real_offset=%d, block_num=%lu, type=%d\n", curr_dt(dt),
                                    h->cid, r->r_idx_b[bs]->fname, key_hex, r->scan_vars->tech_offset, read_block, msg->type);
                            free(key_hex);
                            r->bad_data = true;
                            r->stop = true;
                            goto scan_error;
                        }
                    } else {
                        r->bbuff[off + MSG_TYPE_OFFSET] = MSG_RES;

                        if (r->bbuff[lob_chk_offset] == 1) {
                            bool is_lob = false;
                            uint64_t new_zbuff_size = r->scan_vars->unzip_msg_size_sum + r->scan_vars->unzip_msg_size + msg_size + MAX_BSIZE_FACTOR;

                            if (r->zbuff_size < new_zbuff_size) {
                                uint8_t *zbuff_tmp = realloc(r->zbuff - r->scan_vars->unzip_msg_size_sum, new_zbuff_size);

                                if (!zbuff_tmp) {
                                    fprintf(stderr, "%s WARN [SCAN_HANDLER=%015lu] Failed realloc to %lu zbuff for %s, sleep 10s\n", curr_dt(dt), h->cid, new_zbuff_size, r->uuid);
                                    sleep(10);
                                    goto scan_error;
                                } else {
                                    r->zbuff = zbuff_tmp + r->scan_vars->unzip_msg_size_sum;
                                    r->zbuff_size = new_zbuff_size;
                                }
                            }

                            if (is_lob)
                                memcpy(r->zbuff + r->scan_vars->unzip_msg_size, reply->resp_buff, msg_size);
                            else
                                memcpy(r->zbuff + r->scan_vars->unzip_msg_size, r->bbuff + off, msg_size);
                        } else {
                            memcpy(r->zbuff + r->scan_vars->unzip_msg_size, r->bbuff + off, msg_size);
                        }
                    }
                }

                if (msg_size % r->r_idx_b[bs]->bsize_factor == 0)
                    r->scan_vars->record_size_round_up_unzip = ceil(msg_size / r->r_idx_b[bs]->bsize_factor + 0) * r->r_idx_b[bs]->bsize_factor;
                else
                    r->scan_vars->record_size_round_up_unzip = ceil(msg_size / r->r_idx_b[bs]->bsize_factor + 1) * r->r_idx_b[bs]->bsize_factor;
                r->scan_vars->last_record_size_unzip = msg_size;
                r->scan_vars->unzip_msg_size += r->scan_vars->record_size_round_up_unzip;

                if (r->scan_vars->unzip_msg_size_sum + r->scan_vars->unzip_msg_size * 2 > r->zbuff_size) {
                    uint64_t zbuff_size_new = r->scan_vars->unzip_msg_size_sum + r->scan_vars->unzip_msg_size * 2;

                    if (log_level > 1)
                        fprintf(stderr, "%s INFO [SCAN_HANDLER=%015lu] Try realloc r_buff %s to %lu (bs=%d)\n", curr_dt(dt), h->cid, r->r_idx_b[bs]->fname, zbuff_size_new, bs);
                    char *rbuff_tmp = realloc(r->rbuff, zbuff_size_new);

                    if (!rbuff_tmp) {
                        fprintf(stderr, "%s WARN [SCAN_HANDLER=%015lu] Failed realloc to %lu rbuff for %s, sleep 10s\n", curr_dt(dt), h->cid, zbuff_size_new, r->uuid);
                        sleep(10);
                        goto scan_error;
                    } else
                        r->rbuff = rbuff_tmp;
                    r->rbuff_size = zbuff_size_new;
                    char *zbuff_tmp = realloc(r->zbuff - r->scan_vars->unzip_msg_size_sum, zbuff_size_new);

                    if (!zbuff_tmp) {
                        fprintf(stderr, "%s WARN [SCAN_HANDLER=%015lu] Failed realloc to %lu zbuff for %s, sleep 10s\n", curr_dt(dt), h->cid, zbuff_size_new, r->uuid);
                        sleep(10);
                        goto scan_error;
                    } else
                        r->zbuff = zbuff_tmp + r->scan_vars->unzip_msg_size_sum;
                    r->zbuff_size = zbuff_size_new;
                }
            }
            r->scan_vars->last_record_size = i2b_record_size.integer_val;

            if (i2b_record_size.integer_val % bsize_factor == 0)
                r->scan_vars->record_size_round_up = ceil(i2b_record_size.integer_val / bsize_factor + 0) * bsize_factor;
            else
                r->scan_vars->record_size_round_up = ceil(i2b_record_size.integer_val / bsize_factor + 1) * bsize_factor;
            r->scan_vars->curr_record_offset += r->scan_vars->record_size_round_up;
            r->req_counter++;
        } while (r->scan_vars->curr_record_offset != reply->msg_size);

        if (log_level > 1)
            fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Scan %u keys, done %d bytes from %s, block=%lu (%u + %u)\n", curr_dt(dt), h->cid, keys, reply->msg_size,
                    r->r_idx_b[bs]->fname, read_block, r->scan_keys_list[sc]->scan_block_num, r->scan_keys_list[sc]->client_scan_block_num);

        if (r->r_idx_b[bs]->block_size <= FIX_BLOCK_SIZE * 4) {
            if (r->scan_vars->unzip_msg_size_sum + r->scan_vars->unzip_msg_size * 10 < BLOCK_MAX_SIZE && reply->allow_multiple_block_scan == true) {
                if (read_block + 1 < r->r_idx_b[bs]->b_offset[b] || (r->scan_vars->unzip_msg_size_sum == 0 && read_block + 1 == r->r_idx_b[bs]->b_offset[b])) {
                    r->scan_keys_list[sc]->client_scan_block_num++;
                    r->zbuff += r->scan_vars->unzip_msg_size;
                    r->scan_vars->unzip_msg_size_sum += r->scan_vars->unzip_msg_size;
                    continue;
                }
            }

            if (keys == 0 && rc == 2) {
                r->scan_keys_list[sc]->client_scan_block_num++;

                if (log_level > 1)
                    fprintf(stderr, "%s DEBUG [SCAN_HANDLER=%015lu] Scan increment %u\n", curr_dt(dt), h->cid, r->scan_keys_list[sc]->scan_block_num);
                continue;
            }
        }
        r->zbuff += r->scan_vars->unzip_msg_size;
        r->scan_vars->unzip_msg_size_sum += r->scan_vars->unzip_msg_size;
        prepare_scan_data_simple(r, buf, reply, bs, h);
        return 0;
    }
scan_error:
    r->zbuff -= r->scan_vars->unzip_msg_size_sum;
    r->scan_vars->unzip_msg_size_sum = 0;
    r->scan_vars->unzip_msg_size = 0;
    return -1;
}
