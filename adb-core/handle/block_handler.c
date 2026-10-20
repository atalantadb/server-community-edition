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
#include "../crypto/checksum.h"
#include "../crypto/xxhash.h"
void error_handler(msg_cli_t *msg_cli, msg_srv_t *msg_srv, struct region_info_t *r_info, uint8_t type, uint64_t cid) {
    memset(msg_srv->resp_buff, 0, MSG_SIZE_NEW_REGION_ORDER);
    msg_srv->resp_buff[MSG_SIZE_OFFSET] = MSG_SIZE_NEW_REGION_ORDER;
    msg_srv->resp_buff[MSG_TYPE_OFFSET] = type;
    msg_srv->msg_size = MSG_SIZE_NEW_REGION_ORDER;
}


void error_replica_handler(msg_srv_t *msg_srv, struct region_info_t *r_info, uint64_t cid) {
    memset(msg_srv->resp_buff, 0, MSG_SIZE_NEW_REGION_ORDER);
    msg_srv->resp_buff[MSG_SIZE_OFFSET] = MSG_SIZE_NEW_REGION_ORDER;
    msg_srv->resp_buff[MSG_TYPE_OFFSET] = INVALID_REGION_MAP;
    msg_srv->msg_size = MSG_SIZE_NEW_REGION_ORDER;
}


int8_t get_bs_map_pos(region_info_t *r_info, uint8_t bs, bool create, table_handler_t *t_handler) {
    if (r_info->enable_block_compression)
        return 0;
    bs = -bs + BLOCK_ARRAY_LAST_POS;

    if (bs != 0 && r_info->bs_map[bs] == 0 && create) {
        uint32_t block_size = -4096 * (bs) + BLOCK_MAX_SIZE;

        if (block_size == 3 * 4096) {
            block_size = 4 * 4096;
            bs = 12;

            if (r_info->bs_map[bs] != 0)
                return r_info->bs_map[bs];
        }

        if (block_size >= 5 * 4096 && block_size < 8 * 4096) {
            block_size = 8 * 4096;
            bs = 8;

            if (r_info->bs_map[bs] != 0)
                return r_info->bs_map[bs];
        }

        if (block_size >= 9 * 4096 && block_size < 16 * 4096) {
            block_size = 16 * 4096;
            bs = 0;
            return r_info->bs_map[bs];
        }

        if (idx_builder(r_info, r_info->max_idx_b, block_size, t_handler) != 0) {
            sleep(60);
            finish_thread(r_info, 1, true, t_handler);
        }
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s INFO [BLOCK_HANDLER=%015lu] Prepare create %s idx (bs=%d, block_size=%d, max_idx_b=%d)\n", curr_dt(dt), t_handler->cid, r_info->r_fname, bs, block_size,
                r_info->max_idx_b);
        r_info->bs_map[bs] = r_info->max_idx_b - 1;
    }
    return r_info->bs_map[bs];
}


void swapbytes(uint8_t *p) {
    uint8_t tmp;
    size_t x = 7;

    for (size_t i = 0; i < 4; i++) {
        tmp = p[i];
        p[i] = p[x];
        p[x] = tmp;
        x--;
    }
}

uint32_t build_serialized_files_list_msg(struct region_info_t *r_info, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    msg_files_list_t *files_list = get_files_list(r_info, true, t_handler);

    if (files_list != NULL) {
        print_files_list(files_list, r_info->uuid, t_handler->cid);
        uint32_t serialized_size = 0;
        uint8_t *serialized_data = serialize_files_list(files_list, &serialized_size, t_handler->cid);

        if (t_handler->msg_srv->curr_max_msg_size < serialized_size + MIN_REC_SIZE) {
            if (log_level > 1)
                fprintf(stderr, "%s DEBUG [BLOCK_HANDLER=%015lu] Realloc %s client curr_max_msg_size=%u up to %u\n", curr_dt(dt), t_handler->cid, r_info->uuid,
                        t_handler->msg_srv->curr_max_msg_size, serialized_size + MIN_REC_SIZE);

            if (realloc_resp_buff(t_handler->msg_srv, serialized_size + MIN_REC_SIZE, t_handler->cid) < 0) {
                finish_thread(r_info, 3, true, t_handler);
            }
        }

        if (serialized_data != NULL) {
            memcpy(t_handler->msg_srv->resp_buff, serialized_data, serialized_size);
            free(serialized_data);
            free_files_list(files_list);
            fprintf(stderr, "%s INFO [BLOCK_HANDLER=%015lu] Ready to send data check %u bytes replica info %s, r_need_data_chk=%lu\n", curr_dt(dt), t_handler->cid, serialized_size,
                    r_info->uuid, r_info->r_need_data_chk);
            return serialized_size;
        } else {
            fprintf(stderr, "%s WARN [BLOCK_HANDLER=%015lu] Error send data check, replica info %s is NULL, r_need_data_chk=%lu\n", curr_dt(dt), t_handler->cid, r_info->uuid,
                    r_info->r_need_data_chk);
        }
    }
    return 0;
}


static int make_block(region_info_t *r, uint8_t *buf, uint32_t size, table_handler_t *h) {
    char dt[DT_LOG_SIZE];
    msg_srv_t *reply = h->msg_srv;
    int rc = 0;
    uint8_t type = buf[MSG_TYPE_OFFSET];

    if (type == MSG_PUT_AND_GET)
        type = buf[MSG_TYPE_OFFSET] = MSG_PUT;

    if (stop_write && (type == MSG_DIR_WRITE || type == MSG_DIR_WRITE_AND_COMPACT || type == MSG_PUT || type == MSG_DATA_CHK || type == MSG_DATA_FIX)) {
        if (buf[offsetof(msg_cli_t, lob_info)] != 2) {
            fprintf(stderr, "%s INFO [BLOCK_HANDLER] Any writing has stopped.\n", curr_dt(dt));
            sleep(60);
            error_handler(NULL, reply, r, INVALID_REGION_MAP, h->cid);
            return -2;
        }
    }

    if (r->is_freed) {
        fprintf(stderr, "%s WARN [BLOCK_HANDLER=%015lu] Region [%d] has been freed, sending INVALID_REGION_MAP\n", curr_dt(dt), h->cid, h->r_pos);
        error_handler(NULL, reply, r, INVALID_REGION_MAP, h->cid);
        return -3;
    }

    if (type == MSG_DIR_WRITE || type == MSG_DIR_WRITE_TUNC || type == MSG_DIR_WRITE_AND_COMPACT || type == MSG_DATA_FIX || type == MSG_DATA_GET) {
        uint32_t msg_rsc_region_pos;
        memcpy(&msg_rsc_region_pos, &buf[offsetof(struct msg_rsc_t, region_pos)], sizeof(msg_rsc_region_pos));
        uint32_t req_count_chk;
        memcpy(&req_count_chk, buf + offsetof(struct msg_rsc_t, req_count_chk), sizeof(req_count_chk));

        if (memcmp(buf + offsetof(struct msg_rsc_t, uuid), r->uuid, UUID_SIZE - 1) != 0) {
            buf[offsetof(struct msg_rsc_t, uuid) + UUID_SIZE] = 0;
            fprintf(stderr, "%s WARN [BLOCK_HANDLER=%015lu] Wrong request region UUID %s vs %s\n", curr_dt(dt), h->cid, r->uuid, buf + offsetof(struct msg_rsc_t, uuid));
            error_replica_handler(reply, r, h->cid);
            return -4;
        }
        uint64_t msg_rsc_offset;
        memcpy(&msg_rsc_offset, &buf[MSG_RSC_OFF_OFFSET], sizeof(msg_rsc_offset));
        bool drop_recovery_after_write = false;

        if (h->all_req_count == 0) {
            if (r->r_need_data_chk > 0) {
                if (type != MSG_DATA_FIX && type != MSG_DATA_GET && type != MSG_DIR_WRITE_AND_COMPACT) {
                    uint32_t serialized_size = build_serialized_files_list_msg(r, h);
                    memcpy(reply->resp_buff, &serialized_size, INT_SIZE);
                    reply->msg_type = h->msg_srv->resp_buff[MSG_TYPE_OFFSET] = MSG_DATA_CHK_RESULT;
                    memcpy(h->msg_srv->resp_buff + MSG_REPLICA_COUNTER_OFFSET, &req_count_chk, MSG_REPLICA_COUNTER_SIZE);
                    h->msg_srv->msg_size = serialized_size;
                    r->req_counter++;
                    r->req_rep_counter++;
                    h->all_req_count++;
                    return 0;
                }
            }
        } else if (atomic_load(&r->r_need_data_chk) > 0 && type == MSG_DIR_WRITE) {
            drop_recovery_after_write = true;
        }
        uint8_t bs = BS_LOB;

        if (type != MSG_DIR_WRITE_AND_COMPACT) {
            uint8_t wire_bs = buf[MSG_RSC_BS_OFFSET];

            if (wire_bs == 0) {
#if !ENTERPRISE
                error_replica_handler(reply, r, h->cid);
                return -6;
#endif
                bs = BS_LOB;
            } else {
                if (wire_bs > BLOCK_ARRAY_LAST_POS + 1) {
                    fprintf(stderr, "%s WARN [BLOCK_HANDLER=%015lu] Wrong replica block position=%u for %s\n", curr_dt(dt), h->cid, wire_bs, r->uuid);
                    error_replica_handler(reply, r, h->cid);
                    return -6;
                }
                int8_t mapped_bs = get_bs_map_pos(r, wire_bs - 1, true, h);

                if (mapped_bs < 0 || mapped_bs >= r->max_idx_b) {
                    fprintf(stderr, "%s WARN [BLOCK_HANDLER=%015lu] Cannot map replica block position=%u for %s, mapped_bs=%d, max_idx_b=%u\n", curr_dt(dt), h->cid, wire_bs,
                            r->uuid, mapped_bs, r->max_idx_b);
                    error_replica_handler(reply, r, h->cid);
                    return -6;
                }
                bs = (uint8_t)mapped_bs;
            }

            if (!r->r_idx_b[bs]) {
                fprintf(stderr, "%s WARN [BLOCK_HANDLER=%015lu] Replica block position=%u resolved to uninitialized bs=%u for %s\n", curr_dt(dt), h->cid, wire_bs, bs, r->uuid);
                error_replica_handler(reply, r, h->cid);
                return -6;
            }
        }

        if (r->status != STATUS_ASSIGN_REGION) {
            if (type == MSG_DIR_WRITE_AND_COMPACT && set_region_recovery_info(r, 1L, h))
                atomic_store(&r->r_need_data_chk, 1L);
            fprintf(stderr, "%s WARN [BLOCK_HANDLER=%015lu] Got request to not assigned replica region[%d]\n", curr_dt(dt), h->cid, msg_rsc_region_pos);
            error_replica_handler(reply, r, h->cid);
            return -5;
        }

        if (type == MSG_DIR_WRITE || type == MSG_DIR_WRITE_TUNC || type == MSG_DIR_WRITE_AND_COMPACT || type == MSG_DATA_FIX)
            h->leader_commit = false;

        if (type == MSG_DATA_FIX)
            fprintf(stderr, "%s INFO [BLOCK_HANDLER=%015lu] Replica data chunk fix %s, bs=%u, from %lu to %lu offset\n", curr_dt(dt), h->cid, r->uuid, bs, msg_rsc_offset,
                    msg_rsc_offset + (size - MSG_RSC_BUFF_OFFSET));

        if (type == MSG_DIR_WRITE_AND_COMPACT) {
            if (size < MSG_RSC_BUFF_OFFSET || !apply_write_compact_transaction(r, buf + MSG_RSC_BUFF_OFFSET, size - MSG_RSC_BUFF_OFFSET, h)) {
                error_replica_handler(reply, r, h->cid);
                return -6;
            }
        } else if (type == MSG_DIR_WRITE || type == MSG_DATA_FIX) {
            write_file(r->r_idx_b[bs]->fd[0], msg_rsc_offset, buf + MSG_RSC_BUFF_OFFSET, size - MSG_RSC_BUFF_OFFSET, r->r_idx_b[bs]->bsize_factor, r, h->cid);

            if (drop_recovery_after_write) {
                flush_write_4K_buffer(r, h->cid);
                drop_region_recovery_info(r, h->cid);

                if (atomic_load(&r->r_need_data_chk) != 0) {
                    fprintf(stderr, "%s ERROR [BLOCK_HANDLER=%015lu] Cannot remove recovery marker after replica write for %s\n", curr_dt(dt), h->cid, r->uuid);
                    error_replica_handler(reply, r, h->cid);
                    return -6;
                }
            }
        }

        if (type == MSG_DIR_WRITE_TUNC && truncate_file(r, r->r_idx_b[bs]->fd[0], msg_rsc_offset, h->cid) != 0) {
            fprintf(stderr, "%s ERROR [BLOCK_HANDLER=%015lu] Replica truncate failed for %s, bs=%u, size=%lu, errno=%d\n", curr_dt(dt), h->cid, r->uuid, bs, msg_rsc_offset, errno);
            r->stop = true;
            r->bad_data = true;
            set_region_recovery_info(r, 1L, h);
            error_replica_handler(reply, r, h->cid);
            return -6;
        }
        uint32_t data_size;

        if (type == MSG_DATA_GET) {
            uint32_t chunk_size;
            memcpy(&chunk_size, buf + MSG_RSC_BUFF_OFFSET, sizeof(chunk_size));

            if (chunk_size > BLOCK_MAX_SIZE) {
                error_replica_handler(reply, r, h->cid);
                return -6;
            }

            if (realloc_resp_buff(reply, chunk_size + MSG_SIZE_RESERVE_RS_ANSWER, h->cid) < 0)
                finish_thread(r, 3, true, h);
            fprintf(stderr, "%s INFO [BLOCK_HANDLER=%015lu] Replica data chunk get %s, from %lu to %lu offset\n", curr_dt(dt), h->cid, r->uuid, msg_rsc_offset,
                    msg_rsc_offset + chunk_size);
            data_size = read_file(r->r_idx_b[bs]->fd[0], reply->resp_buff + MSG_SIZE_RESERVE_RS_ANSWER, msg_rsc_offset, chunk_size, r, h->cid);

            if (data_size != chunk_size)
                fprintf(stderr, "%s INFO [BLOCK_HANDLER=%015lu] Replica read data chunk %s is not complite, read %d instead of %d\n", curr_dt(dt), h->cid, r->uuid, data_size,
                        chunk_size);
            data_size += MSG_SIZE_RESERVE_RS_ANSWER;
            memcpy(reply->resp_buff, &data_size, sizeof(data_size));
            reply->msg_type = reply->resp_buff[MSG_TYPE_OFFSET] = MSG_DATA_GET_RESULT;
            memcpy(h->msg_srv->resp_buff + MSG_REPLICA_COUNTER_OFFSET, &req_count_chk, MSG_REPLICA_COUNTER_SIZE);
            reply->msg_size = data_size;
            return 0;
        }
        data_size = MSG_SIZE_RESERVE_RS_ANSWER;
        memcpy(h->msg_srv->resp_buff, &data_size, sizeof(data_size));
        reply->msg_type = h->msg_srv->resp_buff[MSG_TYPE_OFFSET] = MSG_RESP_OK;
        memcpy(h->msg_srv->resp_buff + MSG_REPLICA_COUNTER_OFFSET, &req_count_chk, MSG_REPLICA_COUNTER_SIZE);
        h->msg_srv->msg_size = MSG_SIZE_RESERVE_RS_ANSWER;
        r->req_counter++;
        r->req_rep_counter++;
        h->all_req_count++;
        return 0;
    }

    if (type == MSG_DATA_CHK) {
        uint32_t off_uuid = offsetof(struct msg_chk_t, uuid);
        uint32_t off_req_count_chk = offsetof(struct msg_chk_t, req_count_chk);
        uint32_t off_region_pos = offsetof(struct msg_chk_t, region_pos);
        union i2b_u i2b;
        memcpy(i2b.byte, &buf[off_region_pos], sizeof(INT_SIZE));
        uint32_t msg_rsc_region_pos = i2b.integer_val;
        uint32_t req_count_chk;
        memcpy(&req_count_chk, buf + off_req_count_chk, sizeof(INT_SIZE));
        fprintf(stderr, "%s INFO [BLOCK_HANDLER=%015lu] Receive data check request for %s\n", curr_dt(dt), h->cid, r->uuid);
        table_info_t *request_t_info = NULL;

        if (h->t_info && h->t_info_size > 1 && h->t_info_curr < h->t_info_size - 1)
            request_t_info = h->t_info[h->t_info_curr];

        if (r->status != STATUS_ASSIGN_REGION || !request_t_info || h->cli_tbl_version != request_t_info->srv_tbl_version) {
            fprintf(stderr, "%s WARN [BLOCK_HANDLER=%015lu] Got data check request to not assigned replica region[%d]\n", curr_dt(dt), h->cid, msg_rsc_region_pos);
            error_replica_handler(reply, r, h->cid);
            return -6;
        }

        if (memcmp(buf + off_uuid, r->uuid, UUID_SIZE - 1) != 0) {
            buf[off_uuid + UUID_SIZE] = 0;
            fprintf(stderr, "%s WARN [BLOCK_HANDLER=%015lu] Wrong data check request region UUID %s vs %s\n", curr_dt(dt), h->cid, r->uuid, buf + off_uuid);
            error_replica_handler(reply, r, h->cid);
            return -7;
        }
        uint32_t serialized_size = build_serialized_files_list_msg(r, h);
        memcpy(reply->resp_buff, &serialized_size, INT_SIZE);
        reply->msg_type = h->msg_srv->resp_buff[MSG_TYPE_OFFSET] = MSG_DATA_CHK_RESULT;
        memcpy(h->msg_srv->resp_buff + MSG_REPLICA_COUNTER_OFFSET, &req_count_chk, MSG_REPLICA_COUNTER_SIZE);
        h->msg_srv->msg_size = serialized_size;
        return 0;
    }

    if (r->uuid[UUID_SIZE - 1] != '0') {
        fprintf(stderr, "%s WARN [BLOCK_HANDLER=%015lu] Requested wrong reserve replica %s, msg_type=%u. Sending INVALID_REGION_MAP\n", curr_dt(dt), h->cid, r->uuid, type);
        error_handler(NULL, reply, r, INVALID_REGION_MAP, h->cid);
        return -8;
    }
#if !ENTERPRISE
    if ((type == MSG_PUT || type == MSG_DEL) && size >= DISK_RECORD_HEADER_SIZE && buf[offsetof(msg_cli_t, lob_info)] >= 2) {
        error_handler(NULL, reply, r, RECORD_TOO_LARGE_ERROR, h->cid);
        reply->msg_type = 0;
        return RECORD_TOO_LARGE_RESULT;
    }
#endif
    struct msg_qv_t link_qv = {0};
    msg_kv_t link_kv = {0};
    msg_cli_t link_msg = {0};
    msg_cli_t *msg = h->msg_cli;
    {
        msg->msg_size = 0;

        if (type != MSG_SCN && type != MSG_CNT)
            if (bytes_to_msg(msg, buf, size, h) == 0)
                finish_thread(r, 4, true, h);
    }
#if !ENTERPRISE
    if (type == MSG_PUT && (msg->msg_size >= LARGE_RECORD_LIMIT || msg->lob_info >= 2))
        goto record_too_large;

    if (type == MSG_PUT) {
        msg->lob_info = 0;
        buf[offsetof(msg_cli_t, lob_info)] = 0;
    }
#endif
    if (type != MSG_PUT && type != MSG_GET && type != MSG_DEL && type != MSG_SCN && type != MSG_CNT) {
        fprintf(stderr, "%s WARN [BLOCK_HANDLER=%015lu] Got wrong request type %d. Sending INVALID_REGION_MAP\n", curr_dt(dt), h->cid, type);
        error_handler(msg, reply, r, INVALID_REGION_MAP, h->cid);
        return -9;
    }

    if (r->status != STATUS_ASSIGN_REGION) {
        fprintf(stderr, "%s WARN [BLOCK_HANDLER=%015lu] Got request to not assigned region[%d]. Sending INVALID_REGION_MAP\n", curr_dt(dt), h->cid, msg->region_pos);
        error_handler(msg, reply, r, INVALID_REGION_MAP, h->cid);
        return -10;
    }

    if (type == MSG_CNT) {
        reply->msg_size = sizeof(msg_cli_count_t);
        memcpy(reply->resp_buff, &reply->msg_size, sizeof(reply->msg_size));
        reply->resp_buff[MSG_TYPE_OFFSET] = MSG_CNT;
        uint32_t off_region_pos = offsetof(struct msg_cli_count_t, region_pos);
        memcpy(reply->resp_buff + off_region_pos, buf + off_region_pos, sizeof(off_region_pos));
        uint64_t l2b = 0;

        for (int bs = 0; bs < r->max_idx_b; bs++)
            l2b += r->r_idx_b[bs]->f_idx_curr_size[0];

        if (r->pending_delete > l2b) {
            stop_region_after_key_error(r, KEY_EXTRACT_INVALID_STATE, h);
            return -10;
        }
        l2b -= r->pending_delete;
        memcpy(reply->resp_buff + offsetof(struct msg_cli_count_t, count), &l2b, LONG_SIZE);
        r->has_digest_calc = false;
        h->all_req_count++;
        return 0;
    }

    if (type != MSG_SCN && type != MSG_CNT) {
        bool up_border = false;
        bool down_border = false;
        uint8_t *key = buf + KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET;

        for (int i = 0; i < REGION_BYTE_RANGE; i++) {
            if (i == msg->kv->key_size)
                break;

            if (key[i] > r->k_beg[i] && key[i] < r->k_end[i])
                break;

            if (key[i] == r->k_beg[i] && !down_border && key[i] == r->k_end[i] && !up_border)
                continue;

            if ((key[i] < r->k_beg[i] && !down_border) || (key[i] > r->k_end[i] && !up_border)) {
                fprintf(stderr, "%s WARN [BLOCK_HANDLER=%015lu] Got wrong request with region_pos[%d] to %s (key[%d]=%x, k_beg[%d]=%x, k_end[%d]=%x)\n", curr_dt(dt), h->cid,
                        msg->region_pos, r->uuid, i, key[i], i, r->k_beg[i], i, r->k_end[i]);
                uint8_t *key_hex = extract_key(buf);
                fprintf(stderr, "%s DEBUG [BLOCK_HANDLER=%015lu] Got wrong request KEY = %s. Sending INVALID_REGION_MAP\n", curr_dt(dt), h->cid, key_hex);
                free(key_hex);
                error_handler(msg, reply, r, INVALID_REGION_MAP, h->cid);
                return -11;
            }

            if (key[i] > r->k_beg[i])
                down_border = true;

            if (key[i] < r->k_end[i])
                up_border = true;

            if (i == REGION_BYTE_RANGE - 1 && key[i] == r->k_end[i] && !up_border && region_border_exclusive == true) {
                fprintf(stderr, "%s WARN [BLOCK_HANDLER=%015lu] Got wrong request with region_pos[%d] to %s (key[%d]=%x, k_beg[%d]=%x, k_end[%d]=%x)\n", curr_dt(dt), h->cid,
                        msg->region_pos, r->uuid, i, key[i], i, r->k_beg[i], i, r->k_end[i]);
                uint8_t *key_hex = extract_key(buf);
                fprintf(stderr, "%s DEBUG [BLOCK_HANDLER=%015lu] Got wrong request KEY = %s (hit the border). Sending INVALID_REGION_MAP\n", curr_dt(dt), h->cid, key_hex);
                free(key_hex);
                error_handler(msg, reply, r, INVALID_REGION_MAP, h->cid);
                return -12;
            }
        }
    }

    if (r->enable_dict == 1 && (type == MSG_PUT || type == MSG_DEL) && msg->lob_info == 0)
        buf[ZIP_INFO_RECORD_OFFSET] = 0x02;

    if (type == MSG_SCN) {
        if (new_scan_result == 0) {
            error_handler(msg, reply, r, INVALID_REGION_MAP, h->cid);
            return -19;
        } else {
            if (scan_rows(r, buf, size, h) >= 0)
                return 0;
        }
        error_handler(msg, reply, r, INVALID_REGION_MAP, h->cid);
        return -13;
    } else {
        r->has_digest_calc = false;

        if (type == MSG_GET)
            buf[MSG_TYPE_OFFSET] = MSG_RES;
        // TODO Отправлять сигнал ожидания клиенту
        if (r->idx_resizer != RESIZE_IDX_DONE && msg->type == MSG_DEL) {
            error_handler(msg, reply, r, INVALID_REGION_MAP, h->cid);
            return -14;
        }
        int result;
        uint8_t bs;

        if (h->record_mutation && msg->lob_info != 2 && (type == MSG_PUT || type == MSG_DEL)) {
            if (!record_version_reserve(&r->record_versions, &h->record_mutation->version) || !disk_record_version_write(buf, size, h->record_mutation->version)) {
                stop_region_after_key_error(r, KEY_EXTRACT_INVALID_STATE, h);
                return -16;
            }
        }
        r->has_digest_calc = false;

        if (r->hash_idx) {
            bool is_idx_found = false;

            if (msg->type == MSG_PUT || msg->type == MSG_DEL) {
                if (msg->type == MSG_PUT) {
                    r->req_put_counter_sum++;
                } else {
                    r->req_del_counter++;
                    r->req_del_counter_sum++;
                }

                for (bs = 0; bs < r->max_idx_b; bs++) {
                    if (r->r_idx_b[bs]->f_idx_curr_size[FBP] == 0)
                        continue;
                    build_hash_params(msg, r, bs);

                    if (find_k_idx(r, false, 0, bs, h) == true) {
                        if (r->r_idx_b[bs]->idx[FBP][r->r_idx_b[bs]->idx_r_help->k_idx].c_index == r->r_idx_b[bs]->idx_r_help->c_idx) {
                            if (r->r_idx_b[bs]->idx[FBP][r->r_idx_b[bs]->idx_r_help->k_idx].o_index != 0) {
                                if (log_level > 0 && h->mutation_req_count == 0 || log_level > 2 || r->log_level > 0) {
                                    uint8_t *key_hex = extract_key(buf);
                                    uint32_t b_offset = r->r_idx_b[bs]->idx[FBP][r->r_idx_b[bs]->idx_r_help->k_idx].o_index;

                                    if (b_offset == 0)
                                        b_offset = r->r_idx_b[bs]->b_offset[FBP];

                                    if (log_level > 2 || r->log_level > 1)
                                        print_full_request_infomation(r, " Ready handle put-del ", key_hex, msg, bs, h);
                                    free(key_hex);
                                }

                                if (r->r_idx_b[bs]->b_offset[FBP] + 1024 >= O_IDX_MAX_SIZE) {
                                    uint64_t mb_val = r->r_idx_b[bs]->b_offset[FBP] / 1024 / 1024 * r->r_idx_b[bs]->block_size;
                                    char grouped[64];
                                    fprintf(stderr, "%s WARN [BLOCK_HANDLER=%015lu] Data file %s is reached the maximum size %s (MB). TODO Splitting. Sendimg INVALID_REGION_MAP\n",
                                            curr_dt(dt), h->cid, r->r_idx_b[bs]->fname, digit_grouping(mb_val, grouped, sizeof(grouped)));
                                    error_handler(msg, reply, r, INVALID_REGION_MAP, h->cid);
                                    return -15;
                                }
                                result = index_put(r, msg, buf, h, bs, true);

                                if (result == RECORD_TOO_LARGE_RESULT)
                                    goto record_too_large;

                                if (h->realloc_buff && msg->lob_info != 2)
                                    buf = h->buff;

                                if (h->realloc_big_buff && msg->lob_info != 2)
                                    buf = h->big_buff;

                                if (result == UPDATE_OK) {
                                    if (h->record_mutation && h->record_mutation->replacing_marker && msg->lob_info != 2)
                                        r->req_ins_counter++;
                                    else
                                        r->req_upd_counter++;
                                    goto out_GETUPD_OK;
                                }

                                if (result == UPDATE_TOO_BIG) {
                                    uint8_t bs_bkp = bs;

                                    if (msg->msg_size > REAL_MAX_MSG_SIZE) {
                                        if (log_level > 2)
                                            fprintf(stderr, "%s TRACE [BLOCK_HANDLER=%015lu] XXX5 msg_cli->msg_size=%u\n", curr_dt(dt), h->cid, msg->msg_size);
                                        bs = 0;
                                        buf[offsetof(msg_cli_t, lob_info)] = msg->lob_info = 1;
                                    } else {
                                        bs = msg->msg_size / FIX_USEFUL_BLOCK_SIZE;
                                        bs = get_bs_map_pos(r, bs, true, h);
                                    }
                                    msg->type = MSG_PUT;

                                    if (build_idx_vars_old(r, msg, bs, h) == false && msg->type == MSG_PUT) {
                                        while (r->idx_resizer != RESIZE_IDX_DONE)
                                            usleep(100000);

                                        if (build_idx_vars_old(r, msg, bs, h) == false) {
                                            fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Error code 127. Failed insert to index for %s, bs=%d\n", curr_dt(dt), h->cid,
                                                    r->r_idx_b[bs]->fname, bs);
                                            r->stop = true;
                                            finish_thread(r, 127, true, h);
                                        }
                                        build_hash_params(msg, r, bs_bkp);

                                        if (find_k_idx(r, false, 0, bs_bkp, h) == false) {
                                            fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Error code 130. Failed get index for %s, bs=%d\n", curr_dt(dt), h->cid,
                                                    r->r_idx_b[bs]->fname, bs_bkp);
                                            r->stop = true;
                                            finish_thread(r, 130, true, h);
                                        }
                                    }

                                    if (log_level > 2)
                                        fprintf(stderr, "%s TRACE [BLOCK_HANDLER=%015lu] XXX6 msg_cli->msg_size=%u\n", curr_dt(dt), h->cid, msg->msg_size);
                                    result = index_put(r, msg, buf, h, bs, false);

                                    if (result < 0) {
                                        stop_region_after_key_error(r, KEY_EXTRACT_INVALID_STATE, h);
                                        return -16;
                                    }

                                    if (h->realloc_buff && msg->lob_info != 2)
                                        buf = h->buff;

                                    if (h->realloc_big_buff && msg->lob_info != 2)
                                        buf = h->big_buff;
                                    bs = bs_bkp;
                                    msg->type = MSG_DEL_ALL;

                                    if (log_level > 2)
                                        fprintf(stderr, "%s TRACE [BLOCK_HANDLER=%015lu] XXX15 msg_cli->msg_size=%u, result=%d\n", curr_dt(dt), h->cid, msg->msg_size, result);
                                    result = index_put(r, msg, buf, h, bs, false);

                                    if (log_level > 2)
                                        fprintf(stderr, "%s TRACE [BLOCK_HANDLER=%015lu] XXX16 msg_cli->msg_size=%u, result=%d\n", curr_dt(dt), h->cid, msg->msg_size, result);

                                    if (result != UPDATE_OK) {
                                        fprintf(stderr, "%s ERROR [BLOCK_HANDLER] Failed update, region %s has stopped\n", curr_dt(dt), r->uuid);
                                        r->stop = true;
                                        error_handler(NULL, reply, r, INVALID_REGION_MAP, h->cid);
                                        return -16;
                                    }

                                    if (h->record_mutation && h->record_mutation->replacing_marker && msg->lob_info != 2)
                                        r->req_ins_counter++;
                                    else
                                        r->req_upd_counter++;
                                    goto out_GETUPD_OK;
                                }

                                if (result == REC_NOT_FOUND)
                                    continue;

                                if (result == PUT_FAILED) {
                                    error_handler(NULL, reply, r, INVALID_REGION_MAP, h->cid);
                                    return -17;
                                }
                                is_idx_found = true;
                                break;
                            }
                        }
                    }
                }

                if (!is_idx_found) {
                    if (msg->msg_size > REAL_MAX_MSG_SIZE) {
                        bs = 0;
                    } else {
                        bs = msg->msg_size / FIX_USEFUL_BLOCK_SIZE;
                        bs = get_bs_map_pos(r, bs, true, h);
                    }

                    if (build_idx_vars_old(r, msg, bs, h) == false && msg->type == MSG_PUT) {
                        while (r->idx_resizer != RESIZE_IDX_DONE)
                            usleep(100000);

                        if (build_idx_vars_old(r, msg, bs, h) == false) {
                            fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Error code 128. Failed insert to index for %s, bs=%d\n", curr_dt(dt), h->cid, r->r_idx_b[bs]->fname, bs);
                            r->stop = true;
                            finish_thread(r, 128, true, h);
                        }
                    }
                }
            } else {
                r->req_get_counter++;
                r->req_get_counter_sum++;

                for (bs = 0; bs < r->max_idx_b; bs++) {
                    if (r->r_idx_b[bs]->f_idx_curr_size[FBP] == 0)
                        continue;
                    build_hash_params(msg, r, bs);

                    if (log_level > 2 || r->log_level > 1) {
                        uint8_t *key_hex = extract_key(buf);
                        print_full_request_infomation(r, " Ready to find for get ", key_hex, msg, bs, h);
                        free(key_hex);
                    }

                    if (find_k_idx(r, false, 0, bs, h) == true) {
                        if (r->r_idx_b[bs]->idx[FBP][r->r_idx_b[bs]->idx_r_help->k_idx].o_index != 0) {
                            if (log_level > 2)
                                fprintf(stderr, "%s TRACE [BLOCK_HANDLER=%015lu] XXX80 msg_cli->msg_size=%u, msg_cli->lob_info=%d\n", curr_dt(dt), h->cid, msg->msg_size,
                                        msg->lob_info);

                            if (log_level > 2 || r->log_level > 0) {
                                uint8_t *key_hex = extract_key(buf);

                                if (r->log_level == 1)
                                    fprintf(stderr, "%s DEBUG [BLOCK_HANDLER=%015lu] Ready handle get key=%s requested to %s, size=%u, type=%d, bs=%u\n", curr_dt(dt), h->cid,
                                            key_hex, r->uuid, msg->msg_size, msg->type, bs);
                                else
                                    print_full_request_infomation(r, " Ready handle get ", key_hex, msg, bs, h);
                                free(key_hex);
                            }
                            result = index_put(r, msg, buf, h, bs, false);

                            if (result == GET_OK)
                                goto out_GETUPD_OK;
                        }
                    }
                }
                memcpy(reply->resp_buff, buf, msg->msg_size);
                reply->resp_buff[reply->msg_size + MSG_TYPE_OFFSET] = MSG_RES;
                reply->msg_size += msg->msg_size;
                goto out_NOT_FOUND;
            }

            if (r->r_idx_b[bs]->b_offset[r->r_idx_b[bs]->idx_r_help->b] + 1024 >= O_IDX_MAX_SIZE) {
                uint64_t mb_val = r->r_idx_b[bs]->b_offset[FBP] / 1024 / 1024 * r->r_idx_b[bs]->block_size;
                char grouped[64];
                fprintf(stderr, "%s WARN [BLOCK_HANDLER=%015lu] Data file %s is reached the maximum size %s (MB). TODO Splitting. Sendimg INVALID_REGION_MAP\n", curr_dt(dt),
                        h->cid, r->r_idx_b[bs]->fname, digit_grouping(mb_val, grouped, sizeof(grouped)));
                error_handler(msg, reply, r, INVALID_REGION_MAP, h->cid);
                return -18;
            }

            if (log_level > 0 && h->mutation_req_count == 0 || log_level > 2 || r->log_level > 0) {
                uint8_t *key_hex = extract_key(buf);
                uint32_t b_offset = r->r_idx_b[bs]->idx[FBP][r->r_idx_b[bs]->idx_r_help->k_idx].o_index;

                if (b_offset == 0)
                    b_offset = r->r_idx_b[bs]->b_offset[FBP];

                if (log_level > 2 || r->log_level > 1)
                    print_full_request_infomation(r, "Ready insert/delete ", key_hex, msg, bs, h);
                else
                    fprintf(stderr, "%s DEBUG [BLOCK_HANDLER=%015lu] Ready insert/delete first key=%s requested to %s, bs=%d, block=%u, size=%u, type=%d\n", curr_dt(dt), h->cid,
                            key_hex, r->r_idx_b[bs]->fname, bs, b_offset, msg->msg_size, msg->type);
                free(key_hex);
            }
            result = index_put(r, msg, buf, h, bs, false);

            if (result == INSERT_TOO_BIG) {
                bs = get_bs_map_pos(r, msg->msg_size / FIX_USEFUL_BLOCK_SIZE, true, h);

                if (build_idx_vars_old(r, msg, bs, h) == false && msg->type == MSG_PUT) {
                    while (r->idx_resizer != RESIZE_IDX_DONE)
                        usleep(100000);

                    if (build_idx_vars_old(r, msg, bs, h) == false) {
                        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Error code 129. Failed insert to index for %s, bs=%d\n", curr_dt(dt), h->cid, r->r_idx_b[bs]->fname, bs);
                        r->stop = true;
                        finish_thread(r, 129, true, h);
                    }
                }

                if (log_level > 2)
                    fprintf(stderr, "%s TRACE [BLOCK_HANDLER=%015lu] XXX71 msg_cli->msg_size=%u, msg_cli->lob_info=%d, bs=%d, result=%d\n", curr_dt(dt), h->cid, msg->msg_size,
                            msg->lob_info, bs, result);
                result = index_put(r, msg, buf, h, bs, false);
            }

            if (result == RECORD_TOO_LARGE_RESULT)
                goto record_too_large;

            if (result < 0) {
                error_handler(msg, reply, r, INVALID_REGION_MAP, h->cid);
                rc = -17;
                goto client_cleanup;
            }
            r->req_ins_counter++;
        } else {
            s_index_put(r, msg, buf, reply, h->cid);
        }
    out_GETUPD_OK:
        if (h->all_req_count == 0) {
            if (msg->lob_info != 2) {
                if (h->mutation_req_count == 0) {
                    if (type == MSG_PUT || type == MSG_DEL) {
                        if (log_level > 0 || r->log_level > 0)
                            fprintf(stderr, "%s DEBUG [BLOCK_HANDLER=%015lu] Handled first mutation data request, bs=%d, type=%d, r_control=%d, read=%u, write=%u, msg_size=%u\n",
                                    curr_dt(dt), h->cid, bs, msg->type, r->replica->replica_control, msg_srv_read_access(reply), msg_srv_write_access(reply), reply->msg_size);
                        h->mutation_req_count++;
                    }
                }

                if (h->read_req_count == 0) {
                    if (type == MSG_GET) {
                        if (log_level > 0 || r->log_level > 0)
                            fprintf(stderr, "%s DEBUG [BLOCK_HANDLER=%015lu] Handled first read data request %s, bs=%d, type=%d, r_control=%d, read=%u, write=%u, msg_size=%u\n",
                                    curr_dt(dt), h->cid, r->r_idx_b[bs]->fname, bs, msg->type, r->replica->replica_control, msg_srv_read_access(reply), msg_srv_write_access(reply),
                                    reply->msg_size);
                        h->read_req_count++;
                    }
                }

                if (h->mutation_req_count > 0 && h->read_req_count > 0) {
                    h->all_req_count++;
                }
            }
        } else
            h->all_req_count++;

        if (log_level > 1 || r->log_level > 1) {
            if (r->log_level == 2 && log_level == 2) {
                fprintf(stderr, "%s DEBUG [BLOCK_HANDLER=%015lu] Handled data request: bs=%d, type=%d, r_control=%d, read=%u, write=%u, msg_size=%u\n", curr_dt(dt), h->cid, bs,
                        msg->type, r->replica->replica_control, msg_srv_read_access(reply), msg_srv_write_access(reply), reply->msg_size);
            } else
                print_full_request_infomation(r, "Handled data request", "", msg, bs, h);
        }

        if (r->r_idx_b[bs]->fd[r->r_idx_b[bs]->idx_r_help->b] != r->data_000_fd) {
            if (r->r_idx_b[bs]->max_void_count >= 1 && (type == MSG_PUT || type == MSG_DEL) && msg->lob_info != 2) {
                while (atomic_load(&is_shutting_down))
                    sleep(10);
                h->cli_void_count++;

                if (r->idx_resizer == RESIZE_IDX_DONE && msg->lob_info != 2) {
                    if (r->enable_block_compression == BLOCK_COMPRESSION_NONE)
                        merge_voids(r->r_idx_b[bs]->max_void_pos, r, r->r_idx_b[bs]->idx_r_help->b, bs, h);
                    else
                        merge_compressed_voids(r->r_idx_b[bs]->max_void_pos, r, r->r_idx_b[bs]->idx_r_help->b, bs, h);
                }
            }
        }
    out_NOT_FOUND:
        h->cli_req_count++;
        r->req_counter++;
    }
    goto client_cleanup;
record_too_large:
    error_handler(msg, reply, r, RECORD_TOO_LARGE_ERROR, h->cid);
    reply->msg_type = 0;
    rc = RECORD_TOO_LARGE_RESULT;
client_cleanup:
    if (type != MSG_SCN && type != MSG_CNT && msg->lob_info != 2) {
        if (msg->kv->qv->qfl_size > 0)
            free(msg->kv->qv->qfl);

        if (msg->kv->cf_size > 0)
            free(msg->kv->cf);
        msg->kv->qv->qfl = NULL;
        msg->kv->qv->qfl_size = 0;
        msg->kv->cf = NULL;
        msg->kv->key_size = 0;
        msg->kv->cf_size = 0;
    }
    return rc;
}


int block_maker(region_info_t *r_info, uint8_t *buff, uint32_t rsize, table_handler_t *t_handler) {
    record_mutation_t mutation = {0};
    record_mutation_t *outer = t_handler->record_mutation;
    bool logical = rsize >= DISK_RECORD_HEADER_SIZE && (buff[MSG_TYPE_OFFSET] == MSG_PUT || buff[MSG_TYPE_OFFSET] == MSG_PUT_AND_GET || buff[MSG_TYPE_OFFSET] == MSG_DEL) &&
                   buff[offsetof(msg_cli_t, lob_info)] != 2;
    client_record_route_t route;

    if (logical)
        client_record_route_read(buff, rsize, &route);
    t_handler->record_mutation = logical ? &mutation : outer;
    int result = make_block(r_info, buff, rsize, t_handler);
    t_handler->record_mutation = outer;

    if (logical && mutation.changed) {
        if (result < 0 || r_info->stop || r_info->bad_data || !record_version_stage(&r_info->record_versions, mutation.version)) {
            stop_region_after_key_error(r_info, KEY_EXTRACT_INVALID_STATE, t_handler);
            return result < 0 ? result : -16;
        }
        bool remote_pending = r_info->bulk_mode != BUIK_RELICA_OFF && ((r_info->replica->replica1_port > 0 && r_info->replica->r_info1 == NULL) ||
                                                                       (r_info->replica->replica2_port > 0 && r_info->replica->r_info2 == NULL));

        if (!remote_pending && !record_version_commit_pending(&r_info->record_versions)) {
            stop_region_after_key_error(r_info, KEY_EXTRACT_INVALID_STATE, t_handler);
            return -16;
        }
    }

    if (logical && result >= 0 && t_handler->msg_srv->msg_size >= DISK_RECORD_HEADER_SIZE && t_handler->msg_srv->resp_buff[MSG_TYPE_OFFSET] == MSG_RES)
        client_record_route_write((uint8_t *)t_handler->msg_srv->resp_buff, t_handler->msg_srv->msg_size, &route);
    return result;
}
