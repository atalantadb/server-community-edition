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
#include "req_proc.h"
#include "../utils/time_utils.h"
#include "replica_handler.h"
#include "block_handler.h"
#include "../utils/voids_service.h"
static uint64_t request_metric_monotonic_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000U + (uint64_t)ts.tv_nsec / 1000U;
}


static void request_metric_update_max(_Atomic uint64_t *value, uint64_t candidate) {
    uint64_t current = atomic_load(value);

    while (current < candidate && !atomic_compare_exchange_weak(value, &current, candidate)) {
    }
}

static uint32_t request_metric_wait_bucket(uint64_t wait_us) {
    if (wait_us < 10U)
        return 0;

    if (wait_us < 50U)
        return 1;

    if (wait_us < 100U)
        return 2;

    if (wait_us < 250U)
        return 3;

    if (wait_us < 500U)
        return 4;

    if (wait_us < 1000U)
        return 5;

    if (wait_us < 5000U)
        return 6;
    return 7;
}

void close_handler_transport(table_handler_t *t_handler) {
    if (t_handler == NULL)
        return;
    connection_t *conn = t_handler->conn_father;
    connection_transport_t *transport = conn != NULL ? &conn->transport : t_handler->transport;
    transport_close(transport);
    t_handler->transport = NULL;
}


void close_conn(connection_t *conn) {
    if (conn == NULL)
        return;
    atomic_store(&conn->state, STATE_FREED);
    table_handler_t *t_handler = conn->t_handler;

    if (t_handler == NULL)
        return;
    char dt[DT_LOG_SIZE];
    const bool is_client = atomic_load(&t_handler->is_client);
    fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] Closing connection fd=%d, owner_wi=%d, owner_idx=%d\n", curr_dt(dt), t_handler->cid, transport_fd(&conn->transport),
            conn->owner_wi, conn->owner_idx);

    while (atomic_load(&t_handler->resizing) > 0)
        sleep(1);

    if (t_handler->is_replica_client && !t_handler->leader_commit && t_handler->all_req_count > 0)
        set_region_recovery_info(NULL, 1L, t_handler);

    if (is_client && conn->has_table && t_handler->t_info != NULL) {
        for (uint32_t i = 0; i + 1 < t_handler->t_info_size; i++) {
            table_info_t *table = t_handler->t_info[i];

            if (table != NULL && get_t_lock(table, 27, false, t_handler->cid)) {
                del_cli_fd(transport_fd(&conn->transport), table);
                do_t_unlock(table, 29, false, t_handler->cid);
            }
        }
    }
    conn->has_table = false;
    close_handler_transport(t_handler);
    conn->t_handler = NULL;
    t_handler->conn_father = NULL;
    free_t_handler(t_handler);

    if (is_client)
        atomic_fetch_sub(&total_clients, 1);
}


// TODO  malloc-free
bool process_init_msg(connection_transport_t *transport, uint8_t *buff, msg_srv_t *msg_srv, table_handler_t *t_handler, int rsize) {
    char dt[DT_LOG_SIZE];
    int sockfd = transport_fd(transport);
    int chk_init_result = format_validation_check_init_msg(buff, rsize);

    if (chk_init_result < 0) {
        uint8_t msg_type = -1;

        if (rsize >= 5)
            msg_type = buff[MSG_TYPE_OFFSET];
        fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Wrong init msg, result=%d, size=%d, type=%u, sleep 10s and return false\n", curr_dt(dt), t_handler->cid, chk_init_result,
                rsize, msg_type);
        sleep(10);
        return false;
    }
    union i2b_u i2b;
    memcpy(i2b.byte, buff, sizeof(msg_srv->msg_size));
    uint8_t msg_type = buff[MSG_TYPE_OFFSET];
    char *buff_params = NULL;
    region_info_t *r_info = NULL;
    t_handler->client_timeout_ms = 60000;
    uint32_t hdr_size = 0;

    if (msg_type == GET_REGION_MAP) {
        hdr_size = 4 + 1;
    } else {
        hdr_size = 4 + 1 + 4;

        if (rsize < (int)hdr_size) {
            fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Request too short for GET_REGION_MAP_TIMEOUT, rsize=%d, need>=%u\n", curr_dt(dt), t_handler->cid, rsize, hdr_size);
            return false;
        }
        memcpy(&t_handler->client_timeout_ms, buff + 4 + 1, sizeof(t_handler->client_timeout_ms));
    }

    if (t_handler->client_timeout_ms > 2000)
        t_handler->client_timeout_ms = t_handler->client_timeout_ms - 1000;
    fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] Recv REGION_MAP request, size=%d, type=%d, timeout=%u\n", curr_dt(dt), t_handler->cid, i2b.integer_val, msg_type,
            t_handler->client_timeout_ms);

    if (rsize <= (int)hdr_size) {
        fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Empty params. rsize=%d, hdr=%u\n", curr_dt(dt), t_handler->cid, rsize, hdr_size);
        return false;
    }
    int params_len = rsize - (int)hdr_size;
    char cli_ns[MAX_NS_SIZE];
    char cli_table[MAX_TBL_SIZE];
    buff_params = (char *)malloc((size_t)params_len + 1);

    if (!buff_params) {
        fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Failed malloc bulk buff_params=%lu for %s\n", curr_dt(dt), t_handler->cid, (size_t)params_len + 1, r_info->uuid);
        return false;
    }
    memset(buff_params, 0, (size_t)params_len + 1);
    memcpy(buff_params, buff + hdr_size, (size_t)params_len);
    char **ss = NULL;
    int count_tokens = split(buff_params, ':', &ss);

    if (count_tokens < 3 || !ss || !ss[INIT_MSG_CLIENT_NS_POS] || !ss[INIT_MSG_CLIENT_TABLE_POS] || !ss[INIT_MSG_CLIENT_UUIDS_POS]) {
        fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Bad params format: '%s'\n", curr_dt(dt), t_handler->cid, buff_params);
        free(ss);
        free(buff_params);
        return false;
    }
    strncpy(cli_ns, ss[INIT_MSG_CLIENT_NS_POS], MAX_NS_SIZE - 1);
    cli_ns[MAX_NS_SIZE - 1] = '\0';
    strncpy(cli_table, ss[INIT_MSG_CLIENT_TABLE_POS], MAX_TBL_SIZE - 1);
    cli_table[MAX_TBL_SIZE - 1] = '\0';
    const char *uuids_str = ss[INIT_MSG_CLIENT_UUIDS_POS];
    size_t uuids_len = strlen(uuids_str);
    char *cli_uuid = (char *)malloc(uuids_len + 1);

    if (!cli_uuid) {
        free(buff_params);
        return false;
    }
    memcpy(cli_uuid, uuids_str, uuids_len + 1);
    fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] PARAMS = %s:%s:%s\n", curr_dt(dt), t_handler->cid, cli_ns, cli_table, cli_uuid);

    if (cli_uuid[UUID_SIZE - 1] != '0')
        t_handler->is_replica_client = true;
    uint32_t rtt_us;
    int rtt_res = get_tcp_rtt_us_by_fd(sockfd, &rtt_us);

    if (rtt_res == 0) {
        int rtt_score = 0;
        const char *rtt_grade = NULL;
        rate_rtt_us(rtt_us, &rtt_score, &rtt_grade);
        fprintf(stderr, "%s INFO [REQUEST_HANDLER=%015lu] Connection %s:%s to %s:%s stat RTT: %u us, score=%d/5, grade=%s\n", curr_dt(dt), t_handler->cid, cli_ns, cli_table,
                t_handler->ip, t_handler->port, rtt_us, rtt_score, rtt_grade);
    } else {
        fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Connection %s:%s to %s:%s stat RTT: unavailable, score=0/5, grade=Unknown\n", curr_dt(dt), t_handler->cid, cli_ns,
                cli_table, t_handler->ip, t_handler->port);
    }
    char cli_uuid_item[UUID_SIZE + 1];
    msg_srv->msg_size = INIT_MSG_HDR;
    uint32_t msg_offset = INIT_MSG_HDR;
    t_handler->cli_tbl_version = -1;
    uint32_t curr_buff_size = FIX_MAX_MSG_SIZE;
    char *new_buff = (char *)malloc(curr_buff_size);

    if (new_buff == NULL) {
        fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Failed malloc curr_buff_size=%u for %s\n", curr_dt(dt), t_handler->cid, curr_buff_size, cli_uuid);
        free(ss);
        free(cli_uuid);
        free(buff_params);
        sleep(1);
        return false;
    }

    for (int i = 0; i < strlen(cli_uuid); i += (UUID_SIZE + 1)) {
        uint32_t try_count = 0;
        memcpy(cli_uuid_item, cli_uuid + i, UUID_SIZE);
        cli_uuid_item[UUID_SIZE] = 0;

        do {
            r_info = find_region_r_info(cli_ns, cli_table, cli_uuid_item, t_handler, NULL);

            if (r_info == NULL && errno == ENOMEM) {
                fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Cannot prepare region map for %s:%s: insufficient memory, close connection=%d\n", curr_dt(dt), t_handler->cid,
                        cli_ns, cli_table, sockfd);
                free(new_buff);
                free(ss);
                free(cli_uuid);
                free(buff_params);
                return false;
            }

            if (r_info == NULL && (t_handler->t_pos == UINT32_MAX || try_count >= max_retry_num)) {
                fprintf(stderr, "%s INFO [REQUEST_HANDLER=%015lu] Got request to not served table %s. Send INVALID_REGION_MAP, close connection=%d\n", curr_dt(dt), t_handler->cid,
                        cli_table, sockfd);
                memset(new_buff, 0, MSG_SIZE_NEW_REGION_ORDER);
                new_buff[MSG_SIZE_OFFSET] = MSG_SIZE_NEW_REGION_ORDER;
                new_buff[MSG_TYPE_OFFSET] = INVALID_REGION_MAP;
                msg_srv->msg_size = MSG_SIZE_NEW_REGION_ORDER;
                ssize_t sres = transport_write(transport, new_buff, msg_srv->msg_size);

                if (sres != msg_srv->msg_size)
                    fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] INVALID_REGION_MAP response write result is %ld != %d. Connection=%d\n", curr_dt(dt), t_handler->cid, sres,
                            msg_srv->msg_size, sockfd);
                free(new_buff);
                free(ss);
                free(cli_uuid);
                free(buff_params);
                sleep(1);
                return false;
            }

            if (r_info == NULL) {
                try_count++;
                // TODO Send progress to client while assigning
                fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Requested table %s:%s region %s is not ready. Try count = %d\n", curr_dt(dt), t_handler->cid, cli_ns, cli_table,
                        cli_uuid_item, try_count);
#ifdef C_TEST
                sleep(3);
#else
                if (try_count <= 10)
                    usleep(100000);
                else
                    usleep(10000 * try_count);
#endif
            }
        } while (r_info == NULL);

        if (strcmp(cli_uuid_item, r_info->uuid) != 0) {
            fprintf(stderr, "%s INFO [REQUEST_HANDLER=%015lu] Table %s region map is changing. Send INVALID_REGION_MAP, close connection=%d\n", curr_dt(dt), t_handler->cid,
                    cli_table, sockfd);
            do_r_unlock(r_info, 127, false, t_handler);
            memset(new_buff, 0, MSG_SIZE_NEW_REGION_ORDER);
            new_buff[MSG_SIZE_OFFSET] = MSG_SIZE_NEW_REGION_ORDER;
            new_buff[MSG_TYPE_OFFSET] = INVALID_REGION_MAP;
            msg_srv->msg_size = MSG_SIZE_NEW_REGION_ORDER;
            ssize_t sres = transport_write(transport, new_buff, msg_srv->msg_size);

            if (sres != msg_srv->msg_size)
                fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] INVALID_REGION_MAP response write result is %ld != %d. Connection=%d\n", curr_dt(dt), t_handler->cid, sres,
                        msg_srv->msg_size, sockfd);
            free(new_buff);
            free(ss);
            free(cli_uuid);
            free(buff_params);
            return false;
        }
        uint32_t region_map_item_size = sizeof(uint32_t) + REGION_BYTE_RANGE * 2;

        if (msg_offset > UINT32_MAX - region_map_item_size) {
            fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Region map response size overflow. msg_offset=%u, item_size=%u.\n", curr_dt(dt), t_handler->cid, msg_offset,
                    region_map_item_size);
            free(new_buff);
            free(ss);
            free(cli_uuid);
            free(buff_params);
            return false;
        }
        uint32_t required_buff_size = msg_offset + region_map_item_size;

        if (required_buff_size > curr_buff_size) {
            while (curr_buff_size < required_buff_size) {
                if (curr_buff_size > UINT32_MAX / 2) {
                    curr_buff_size = required_buff_size;
                    break;
                }
                curr_buff_size *= 2;
            }
            char *buff_tmp = (char *)realloc(new_buff, curr_buff_size);

            if (buff_tmp == NULL) {
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Cannot realloc buff[%u] == NULL. Memory isn't enough.\n", curr_dt(dt), t_handler->cid, curr_buff_size);
                free(new_buff);
                free(ss);
                free(cli_uuid);
                free(buff_params);
                return false;
            } else
                new_buff = buff_tmp;
        }
        union i2b_u i2b;
        i2b.integer_val = t_handler->r_pos;
        memcpy(new_buff + msg_offset, i2b.byte, sizeof(uint32_t));
        msg_offset += sizeof(i2b.integer_val);

        if (r_info->is_freed) {
            fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Incorrect r_pos = %d to requested table %s:%s region %s\n", curr_dt(dt), t_handler->cid, t_handler->r_pos, cli_ns,
                    cli_table, cli_uuid_item);
            memset(new_buff + msg_offset, 0, REGION_BYTE_RANGE * 2);
            msg_offset += (REGION_BYTE_RANGE * 2);
        } else {
            if (log_level)
                fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] Found r_pos = %d for requested table %s:%s region %s (%s)\n", curr_dt(dt), t_handler->cid, t_handler->r_pos,
                        cli_ns, cli_table, cli_uuid_item, r_info->r_fname);
            memcpy(new_buff + msg_offset, r_info->k_beg, REGION_BYTE_RANGE);
            msg_offset += REGION_BYTE_RANGE;
            memcpy(new_buff + msg_offset, r_info->k_end, REGION_BYTE_RANGE);
            msg_offset += REGION_BYTE_RANGE;
        }
        {
            replica_refresh_region_map(r_info, 1, false, t_handler);
            replica_refresh_region_map(r_info, 2, false, t_handler);
        }

        if (t_handler->is_replica_client) {
            uint32_t recovery_info_path_size = strlen(r_info->r_fname) + strlen(recovery_info_fn) + 2;
            free(t_handler->recovery_info_path);
            t_handler->recovery_info_path = NULL;
            t_handler->recovery_info_path = malloc(recovery_info_path_size);

            if (t_handler->recovery_info_path == NULL) {
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Cannot malloc recovery_info_path. Memory isn't enough.\n", curr_dt(dt), t_handler->cid);
                free(new_buff);
                free(ss);
                free(cli_uuid);
                free(buff_params);
                do_r_unlock(r_info, 108, true, t_handler);
                return false;
            }
            snprintf(t_handler->recovery_info_path, recovery_info_path_size, "%s/%s", r_info->r_fname, recovery_info_fn);
        }
        do_r_unlock(r_info, 109, true, t_handler);
    }
    msg_srv->msg_size = msg_offset;
    i2b.integer_val = msg_srv->msg_size;
    memcpy(new_buff, i2b.byte, sizeof(msg_srv->msg_size));
    new_buff[MSG_TYPE_OFFSET] = CURRENT_REGION_MAP;
    ssize_t sres = transport_write(transport, new_buff, msg_srv->msg_size);

    if (sres != msg_srv->msg_size) {
        fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Init table server response msg result is %ld != %d. Connection=%d\n", curr_dt(dt), t_handler->cid, sres,
                msg_srv->msg_size, sockfd);
        free(new_buff);
        free(ss);
        free(cli_uuid);
        free(buff_params);
        return false;
    } else
        fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] Server sent %d bytes to connection=%d (cli_tbl_version=%d) init table msg OK\n", curr_dt(dt), t_handler->cid, rsize,
                sockfd, t_handler->cli_tbl_version);
    free(new_buff);
    free(ss);
    free(cli_uuid);
    free(buff_params);
    return true;
}

int process_bulk_message(region_info_t *r_info, uint8_t *buff, int rsize, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];

    if (r_info == NULL || buff == NULL || t_handler == NULL || t_handler->msg_srv == NULL || rsize < MSG_TYPE_OFFSET + 1 || (uint32_t)rsize > BULK_MSG_SIZE_LIMIT) {
        return -1;
    }
    uint8_t bulk_type = buff[MSG_TYPE_OFFSET];
    uint32_t bulk_header_size;

    if (bulk_type == MSG_BULK_GET || bulk_type == MSG_BULK_PUT)
        bulk_header_size = 9U;
    else if (bulk_type == MSG_BULK_GET_NIO || bulk_type == MSG_BULK_PUT_NIO)
        bulk_header_size = 13U;
    else
        return -1;
    uint32_t declared_bulk_size = 0;
    memcpy(&declared_bulk_size, buff, sizeof(declared_bulk_size));

    if ((uint32_t)rsize <= bulk_header_size || declared_bulk_size != (uint32_t)rsize) {
        fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Invalid bulk container header: type=%u, declared=%u, received=%d, header=%u\n", curr_dt(dt), t_handler->cid, bulk_type,
                declared_bulk_size, rsize, bulk_header_size);
        return -1;
    }
    uint32_t total_msg_size = 5;
    msg_srv_t *msg_srv = t_handler->msg_srv;
    uint8_t *base = (uint8_t *)msg_srv->resp_buff_base;
    uint32_t cap_total = msg_srv->total_max_msg_size;
    uintptr_t base_addr = (uintptr_t)base;
    uintptr_t tail_addr = (uintptr_t)msg_srv->resp_buff;

    if (base == NULL || msg_srv->resp_buff == NULL || cap_total == 0 || tail_addr < base_addr || tail_addr - base_addr > cap_total) {
        return -1;
    }
    msg_srv->resp_buff = (char *)base;
    msg_srv->total_max_msg_size = cap_total;
    msg_srv->curr_max_msg_size = cap_total;
    const uint32_t MIN_CAP = 1024;

    if (cap_total < MIN_CAP) {
        if (realloc_resp_buff(msg_srv, MIN_CAP, t_handler->cid) != 0)
            return -1;
        base = (uint8_t *)msg_srv->resp_buff_base;
        cap_total = msg_srv->total_max_msg_size;
    }
    uint8_t *curr = base + total_msg_size;
    uint8_t *get_buff = (uint8_t *)t_handler->big_buff;
    uint32_t get_buff_alloc = (uint32_t)t_handler->big_buff_size;
    uint32_t offset = bulk_header_size;
    int res = -1;
    uint32_t bulk_item_num = 0;
    bool standalone_response = false;

    while (1) {
        if (offset > (uint32_t)rsize || sizeof(uint32_t) > (uint32_t)rsize - offset) {
            fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Bulk get offset %u is incorrect, rsize=%d\n", curr_dt(dt), t_handler->cid, offset, rsize);
            break;
        }
        uint32_t msg_get_size = 0;
        memcpy(&msg_get_size, buff + offset, sizeof(msg_get_size));

        if (msg_get_size < MSG_TYPE_OFFSET + 1U) {
            fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Nested bulk msg_size=%u is too small at offset=%u\n", curr_dt(dt), t_handler->cid, msg_get_size, offset);
            break;
        }

        if (msg_get_size > (uint32_t)rsize - offset) {
            fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Bulk get next offset=%u + msg_get_size=%u is incorrect, rsize=%d\n", curr_dt(dt), t_handler->cid, offset,
                    msg_get_size, rsize);
            break;
        }
        uint64_t need64 = (uint64_t)msg_get_size + msg_get_size / 20U + MAX_BSIZE_FACTOR;

        if (need64 > RECORD_SIZE_LIMIT || need64 > UINT32_MAX) {
            res = -1;
            goto bulk_finalize;
        }
        uint32_t need = (uint32_t)need64;

        if (need > get_buff_alloc) {
            if (realloc_buff_or_bbuff(t_handler, &get_buff, need) != 0) {
                res = -1;
                goto bulk_finalize;
            }
            get_buff_alloc = t_handler->big_buff_size;
        }
        memcpy(get_buff, buff + offset, msg_get_size);
        memset(get_buff + msg_get_size, 0, MAX_BSIZE_FACTOR);
        {
            uintptr_t curr_addr = (uintptr_t)curr;
            uintptr_t current_base_addr = (uintptr_t)msg_srv->resp_buff_base;

            if (curr_addr < current_base_addr || curr_addr - current_base_addr > cap_total) {
                res = -1;
                goto bulk_finalize;
            }
            uint32_t used = (uint32_t)(curr_addr - current_base_addr);

            if (used > cap_total) {
                fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] resp_buff invariant broken: used=%u > cap_total=%u\n", curr_dt(dt), t_handler->cid, used, cap_total);
                res = -1;
                goto bulk_finalize;
            }
            uint32_t free_tail = cap_total - used;

            if (free_tail < MAX_BSIZE_FACTOR) {
                msg_srv->resp_buff = msg_srv->resp_buff_base + used;
                msg_srv->curr_max_msg_size = free_tail;

                if (realloc_resp_buff(msg_srv, MAX_BSIZE_FACTOR, t_handler->cid) != 0) {
                    res = -1;
                    goto bulk_finalize;
                }
                base = (uint8_t *)msg_srv->resp_buff_base;
                cap_total = msg_srv->total_max_msg_size;
                curr = msg_srv->resp_buff;
            }
        }
        uintptr_t current_base_addr = (uintptr_t)msg_srv->resp_buff_base;
        uintptr_t curr_addr = (uintptr_t)curr;

        if (curr_addr < current_base_addr || curr_addr - current_base_addr > cap_total) {
            res = -1;
            goto bulk_finalize;
        }
        uint32_t used_before = (uint32_t)(curr_addr - current_base_addr);
        uint32_t free_before = cap_total - used_before;
        msg_srv->resp_buff = (char *)curr;
        msg_srv->curr_max_msg_size = free_before;
        msg_srv->total_max_msg_size = cap_total;
        msg_srv->msg_size = 0;
        res = block_maker(r_info, get_buff, msg_get_size, t_handler);
        get_buff = t_handler->big_buff;
        get_buff_alloc = t_handler->big_buff_size;
        r_info->req_counter--;
        base = (uint8_t *)msg_srv->resp_buff_base;
        cap_total = msg_srv->total_max_msg_size;

        if (!base) {
            res = -1;
            break;
        }

        if (res != 0)
            break;

        if (msg_srv->msg_type == MSG_DATA_CHK_RESULT) {
            uint8_t *response_start = (uint8_t *)msg_srv->resp_buff;
            uint32_t response_size = msg_srv->msg_size;
            uintptr_t base_addr = (uintptr_t)base;
            uintptr_t response_addr = (uintptr_t)response_start;
            uint32_t source_offset = 0;
            uint32_t declared_size = 0;

            if (response_start == NULL || response_addr < base_addr || response_addr - base_addr > cap_total) {
                fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Standalone response pointer is outside response buffer: response=%p, base=%p, capacity=%u\n", curr_dt(dt),
                        t_handler->cid, (void *)response_start, (void *)base, cap_total);
                res = -1;
                break;
            }
            source_offset = (uint32_t)(response_addr - base_addr);

            if (response_size < MSG_SIZE_RESERVE_RS_ANSWER || response_size > cap_total - source_offset) {
                fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Standalone response size is invalid: size=%u, source_offset=%u, capacity=%u\n", curr_dt(dt), t_handler->cid,
                        response_size, source_offset, cap_total);
                res = -1;
                break;
            }
            memcpy(&declared_size, response_start, sizeof(declared_size));

            if (declared_size != response_size || response_start[MSG_TYPE_OFFSET] != MSG_DATA_CHK_RESULT) {
                fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Standalone response header is invalid: declared_size=%u, actual_size=%u, type=%u\n", curr_dt(dt), t_handler->cid,
                        declared_size, response_size, response_start[MSG_TYPE_OFFSET]);
                res = -1;
                break;
            }
            memmove(base, response_start, response_size);
            total_msg_size = response_size;
            curr = base + response_size;
            standalone_response = true;
            break;
        }
        {
            uint8_t *base_after = (uint8_t *)msg_srv->resp_buff_base;
            uint8_t *write_start = (uint8_t *)msg_srv->resp_buff;
            uint32_t written_size = msg_srv->msg_size;
            uintptr_t base_after_addr = (uintptr_t)base_after;
            uintptr_t write_start_addr = (uintptr_t)write_start;

            if (base_after == NULL || write_start == NULL || write_start_addr < base_after_addr || write_start_addr - base_after_addr > msg_srv->total_max_msg_size ||
                written_size > msg_srv->total_max_msg_size - (uint32_t)(write_start_addr - base_after_addr)) {
                fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Bulk response overflow: write_start=%p written_size=%u base=%p total=%u\n", curr_dt(dt), t_handler->cid,
                        write_start, written_size, base_after, msg_srv->total_max_msg_size);
                res = -1;
                break;
            }
            uint32_t total_after = msg_srv->total_max_msg_size;
            uint32_t used_after = (uint32_t)(write_start_addr - base_after_addr) + written_size;
            uint32_t free_after = (total_after > used_after) ? (total_after - used_after) : 0;
            base = base_after;
            cap_total = total_after;
            curr = base + used_after;
            msg_srv->curr_max_msg_size = free_after;
            total_msg_size = used_after;
        }
        offset += msg_get_size;
        bulk_item_num++;

        if (offset >= (uint32_t)rsize)
            break;
    }
bulk_finalize:
    base = msg_srv->resp_buff_base;
    cap_total = msg_srv->total_max_msg_size;

    if (!base) {
        if (r_info->record_versions.pending != 0)
            stop_region_after_key_error(r_info, KEY_EXTRACT_INVALID_STATE, t_handler);
        return -1;
    }

    if (res == 0) {
        uintptr_t base_addr = (uintptr_t)base;
        uintptr_t curr_addr = (uintptr_t)curr;
        uint32_t actual_total_msg_size = 0;

        if (curr_addr < base_addr || curr_addr - base_addr > cap_total || total_msg_size > cap_total) {
            fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Response buffer invariant failed: current=%p, base=%p, capacity=%u, calculated_response_size=%u\n", curr_dt(dt),
                    t_handler->cid, (void *)curr, (void *)base, cap_total, total_msg_size);
            res = -1;
        } else {
            actual_total_msg_size = (uint32_t)(curr_addr - base_addr);
        }

        if (res == 0 && standalone_response) {
            uint32_t declared_size = 0;
            memcpy(&declared_size, base, sizeof(declared_size));

            if (total_msg_size != actual_total_msg_size || total_msg_size < MSG_SIZE_RESERVE_RS_ANSWER || declared_size != total_msg_size ||
                base[MSG_TYPE_OFFSET] != MSG_DATA_CHK_RESULT) {
                fprintf(stderr,
                        "%s ERROR [REQUEST_HANDLER=%015lu] Standalone response invariant failed: declared_size=%u, calculated_response_size=%u, actual_response_size=%u, type=%u\n",
                        curr_dt(dt), t_handler->cid, declared_size, total_msg_size, actual_total_msg_size, base[MSG_TYPE_OFFSET]);
                res = -1;
            }
        } else if (res == 0 && (offset != (uint32_t)rsize || total_msg_size != actual_total_msg_size)) {
            fprintf(stderr,
                    "%s ERROR [REQUEST_HANDLER=%015lu] Bulk container invariant failed: input_offset=%u, input_size=%d, calculated_response_size=%u, actual_response_size=%u, "
                    "items=%u\n",
                    curr_dt(dt), t_handler->cid, offset, rsize, total_msg_size, actual_total_msg_size, bulk_item_num);
            res = -1;
        }
    }

    if (res == 0) {
        if (total_msg_size > cap_total)
            res = -1;
    }

    if (res == 0) {
        msg_srv->resp_buff = (char *)base;
        msg_srv->resp_buff_base = (char *)base;
        msg_srv->msg_size = total_msg_size;
        msg_srv->total_max_msg_size = cap_total;
        msg_srv->curr_max_msg_size = cap_total;
        memcpy(base, &total_msg_size, sizeof(total_msg_size));

        if (msg_srv->msg_type == 0 || msg_srv->msg_type == MSG_RESP_OK)
            base[MSG_TYPE_OFFSET] = MSG_RES_BULK_GET_PUT;
        else
            base[MSG_TYPE_OFFSET] = msg_srv->msg_type;
        msg_srv->msg_type = 0;
    } else {
        msg_srv->resp_buff = (char *)base;
        msg_srv->resp_buff_base = (char *)base;
        msg_srv->msg_size = MSG_SIZE_NEW_REGION_ORDER;
        msg_srv->total_max_msg_size = cap_total;
        memset(base, 0, MSG_SIZE_NEW_REGION_ORDER);
        uint32_t error_msg_size = MSG_SIZE_NEW_REGION_ORDER;
        memcpy(base + MSG_SIZE_OFFSET, &error_msg_size, sizeof(error_msg_size));
        base[MSG_TYPE_OFFSET] = res == RECORD_TOO_LARGE_RESULT ? RECORD_TOO_LARGE_ERROR : INVALID_REGION_MAP;
        msg_srv->curr_max_msg_size = cap_total;
    }

    if (res < 0 && res != -11 && res != -12 && res != RECORD_TOO_LARGE_RESULT && r_info->record_versions.pending != 0)
        stop_region_after_key_error(r_info, KEY_EXTRACT_INVALID_STATE, t_handler);
    return res;
}

table_handler_t *init_connection(void *arg) {
    char dt[DT_LOG_SIZE];
    pthread_arg_t *pthread_arg = (pthread_arg_t *)arg;
    bool total_clients_added = false;
    table_handler_t *t_handler = calloc(1, sizeof(table_handler_t));

    if (!t_handler) {
        fprintf(stderr, "%s ERROR [REQUEST_HANDLER] Failed malloc t_handler\n", curr_dt(dt));
        return NULL;
    }
    t_handler->is_client = true;
    t_handler->is_replica_client = false;
    t_handler->transport = pthread_arg->transport;
    t_handler->t_pos = t_handler->cli_tbl_version = -1;
    t_handler->t_info_size = 1;
    t_handler->t_info_curr = 0;
    t_handler->t_info = malloc(t_handler->t_info_size * sizeof(struct table_info_t *));

    if (!t_handler->t_info) {
        fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Cannot malloc t_info=%lu\n", curr_dt(dt), t_handler->cid, t_handler->t_info_size * sizeof(struct table_info_t *));
        goto fail;
    }
    t_handler->t_info[0] = calloc(1, sizeof(struct table_info_t));

    if (!t_handler->t_info[0]) {
        fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Cannot calloc t_info[0]\n", curr_dt(dt), t_handler->cid);
        goto fail;
    }
    t_handler->t_info[0]->ns = NULL;
    t_handler->t_info[0]->table = NULL;
    t_handler->cli_tbl_versions = malloc(t_handler->t_info_size * sizeof(*t_handler->cli_tbl_versions));

    if (!t_handler->cli_tbl_versions) {
        fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Cannot malloc cli_tbl_versions=%lu\n", curr_dt(dt), t_handler->cid,
                t_handler->t_info_size * sizeof(*t_handler->cli_tbl_versions));
        goto fail;
    }
    t_handler->cli_tbl_versions[0] = UINT32_MAX;
    t_handler->cid = get_new_cid();
    t_handler->cli_req_count = 0;
    t_handler->cli_void_count = 0;
    t_handler->all_req_count = 0;
    t_handler->read_req_count = 0;
    t_handler->mutation_req_count = 0;
    t_handler->resizing = 0;
    size_t ip_len = strnlen(pthread_arg->ip, 256);
    size_t pt_len = strnlen(pthread_arg->port, 32);
    t_handler->ip = malloc(ip_len + 1);
    t_handler->port = malloc(pt_len + 1);

    if (!t_handler->ip || !t_handler->port) {
        fprintf(stderr, "%s ERROR [REQUEST_HANDLER] Failed malloc for ip/port\n", curr_dt(dt));
        goto fail;
    }
    memcpy(t_handler->ip, pthread_arg->ip, ip_len);
    t_handler->ip[ip_len] = '\0';
    memcpy(t_handler->port, pthread_arg->port, pt_len);
    t_handler->port[pt_len] = '\0';
    atomic_fetch_add(&total_clients, 1);
    total_clients_added = true;
    pid_t tid = syscall(SYS_gettid);
    fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] Init client %d (socket=%d) from %s:%s, TID=%u\n", curr_dt(dt), t_handler->cid, total_clients,
            transport_fd(t_handler->transport), t_handler->ip, t_handler->port, tid);
    t_handler->msg_srv = calloc(1, sizeof(struct msg_srv_t));

    if (!t_handler->msg_srv) {
        fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Cannot malloc msg_srv\n", curr_dt(dt), t_handler->cid);
        goto fail;
    }
    t_handler->msg_srv->curr_max_msg_size = CURR_MAX_MSG_SIZE;
    t_handler->msg_srv->resp_buff = malloc(t_handler->msg_srv->curr_max_msg_size);

    if (!t_handler->msg_srv->resp_buff) {
        fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Cannot malloc resp_buff\n", curr_dt(dt), t_handler->cid);
        goto fail;
    }
    t_handler->msg_srv->total_max_msg_size = t_handler->msg_srv->curr_max_msg_size;
    t_handler->msg_srv->resp_buff_base = t_handler->msg_srv->resp_buff;
    memset(t_handler->msg_srv->resp_buff, 0, t_handler->msg_srv->curr_max_msg_size);
    t_handler->msg_srv->msg_size = 0;
    t_handler->msg_srv->msg_type = 0;
    t_handler->d_avg_send_msg_size = 0;
    t_handler->d_avg_recv_msg_size = 0;
    t_handler->msg_srv->scan_keys_list_pos = -1;
    t_handler->msg_srv->p_scan_keys_list = NULL;
    t_handler->msg_srv->allow_multiple_block_scan = 1;
    t_handler->msg_cli = calloc(1, sizeof(struct msg_cli_t));

    if (!t_handler->msg_cli) {
        fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Cannot malloc msg_cli\n", curr_dt(dt), t_handler->cid);
        goto fail;
    }
    t_handler->msg_cli->kv = calloc(1, sizeof(struct msg_kv_t));

    if (!t_handler->msg_cli->kv) {
        fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Cannot malloc msg_cli->kv\n", curr_dt(dt), t_handler->cid);
        goto fail;
    }
    t_handler->msg_cli->kv->qv = (struct msg_qv_t *)calloc(1, sizeof(struct msg_qv_t));

    if (!t_handler->msg_cli->kv->qv) {
        fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Cannot malloc msg_cli->kv->qv\n", curr_dt(dt), t_handler->cid);
        goto fail;
    }
    t_handler->msg_cli->kv->key_size = 0;
    t_handler->msg_cli->kv->cf_size = 0;
    t_handler->realloc_buff = false;
    t_handler->buff_size = CURR_MAX_MSG_SIZE;
    t_handler->buff = malloc(t_handler->buff_size);

    if (!t_handler->buff) {
        fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Cannot malloc buff=%u\n", curr_dt(dt), t_handler->cid, t_handler->buff_size);
        goto fail;
    }
    t_handler->realloc_big_buff = false;
    t_handler->big_buff_size = t_handler->msg_srv->curr_max_msg_size;
    t_handler->big_buff = malloc(t_handler->big_buff_size);

    if (!t_handler->big_buff) {
        fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Cannot malloc big_buff=%u\n", curr_dt(dt), t_handler->cid, t_handler->big_buff_size);
        goto fail;
    }
    t_handler->need_regions_map = true;
    t_handler->rtt_us_avg = 0;
    t_handler->rtt_us_count = 0;
    return t_handler;
fail:
    if (total_clients_added)
        atomic_fetch_sub(&total_clients, 1);

    if (t_handler->msg_cli != NULL) {
        if (t_handler->msg_cli->kv != NULL) {
            free(t_handler->msg_cli->kv->qv);
            free(t_handler->msg_cli->kv);
        }
        free(t_handler->msg_cli);
    }

    if (t_handler->msg_srv != NULL) {
        free(t_handler->msg_srv->resp_buff);
        free(t_handler->msg_srv);
    }
    free(t_handler->big_buff);
    free(t_handler->buff);
    free(t_handler->port);
    free(t_handler->ip);

    if (t_handler->t_info != NULL) {
        free(t_handler->t_info[0]);
        free(t_handler->t_info);
    }
    free(t_handler->cli_tbl_versions);
    free(t_handler);
    return NULL;
}


int process_authentication(table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    connection_transport_t *transport = t_handler->transport;
    uint8_t *buff = t_handler->buff;

    if (log_level > 0)
        fprintf(stderr, "%s INFO [REQUEST_HANDLER=%015lu] Ready to recv auth msg (worker=%d)\n", curr_dt(dt), t_handler->cid, t_handler->conn_father->owner_wi);
    int rsize = (int)transport_read(transport, buff, MSG_SIZE_AUTH_RESP, t_handler->cid);

    if (rsize <= 0) {
        fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Failed read msg, result=%d\n", curr_dt(dt), t_handler->cid, rsize);
        return 0;
    }

    if (log_level > 0)
        fprintf(stderr, "%s INFO [REQUEST_HANDLER=%015lu] Recv auth msg size %d (worker=%d)\n", curr_dt(dt), t_handler->cid, rsize, t_handler->conn_father->owner_wi);

    if (log_level > 6) {
        fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] RECV AUTH MSG rsize=%d\n", curr_dt(dt), t_handler->cid, rsize);

        for (int i = 0; i < rsize; i++) {
            fprintf(stderr, "DEBUG [REQUEST_HANDLER=%015lu] MSG [%d] = %d | %x\n", t_handler->cid, i, buff[i], buff[i]);
        }
    }
    uint32_t msize = 0;
    memcpy(&msize, buff, sizeof(msize));

    if (msize > t_handler->msg_srv->curr_max_msg_size) {
        fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Auth request too big msize=%u, limit = %d\n", curr_dt(dt), t_handler->cid, msize, t_handler->msg_srv->curr_max_msg_size);
        return -1;
    }

    if (msize > t_handler->buff_size)
        return -5;

    while (rsize < (int)msize) {
        int rr = (int)transport_read(transport, buff + rsize, msize - rsize, t_handler->cid);

        if (rr <= 0)
            return -3;
        rsize += rr;
    }

    if (msize != INIT_MSG_HDR || rsize != INIT_MSG_HDR || buff[MSG_TYPE_OFFSET] != MSG_NO_AUTH)
        return -4;
    uint32_t off_result = offsetof(struct msg_rs_result_auth_t, result);
    t_handler->msg_srv->resp_buff[MSG_SIZE_OFFSET] = MSG_SIZE_AUTH_RESP;
    t_handler->msg_srv->resp_buff[MSG_TYPE_OFFSET] = MSG_AUTH;
    t_handler->msg_srv->resp_buff[off_result] = MSG_AUTH_RESULT_OK;
    t_handler->msg_srv->msg_size = MSG_SIZE_AUTH_RESP;

    if (transport_write(transport, t_handler->msg_srv->resp_buff, t_handler->msg_srv->msg_size) < 0)
        return -5;
    fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] Sent result to client (%d bytes, result=%d)\n", curr_dt(dt), t_handler->cid, t_handler->msg_srv->msg_size,
            t_handler->msg_srv->resp_buff[off_result]);
    t_handler->need_regions_map = true;
    return 1;
}

inline static bool region_status_chk(region_info_t *r_info, table_handler_t *t_handler, uint8_t *buff, uint32_t r_pos) {
    char dt[DT_LOG_SIZE];
    int count_stop = 60;

    if (r_info->is_leader && atomic_load(&rs_shutdown_state) != RS_SHUTDOWN_RUNNING) {
        r_info->r_lock_id = 0;
        r_info->curr_cid_lock = 0;
        pthread_mutex_unlock(&r_info->rlock);
        return false;
    }
    while (r_info->stop || (r_info->idx_resizer != RESIZE_IDX_DONE && buff[MSG_TYPE_OFFSET] != MSG_DIR_WRITE && buff[MSG_TYPE_OFFSET] != MSG_DIR_WRITE_TUNC &&
                            buff[MSG_TYPE_OFFSET] != MSG_DIR_WRITE_AND_COMPACT && rand() % 2 == 1)) {
        if (r_info->is_freed) {
            fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] Region %s has freed, region_pos=%d, srv_tbl_version=%d, cli_tbl_version=%d. Sending INVALID_REGION_MAP\n",
                    curr_dt(dt), t_handler->cid, r_info->r_fname, r_pos, t_handler->t_info[t_handler->t_info_curr]->srv_tbl_version, t_handler->cli_tbl_version);
            r_info->r_lock_id = 0;
            r_info->curr_cid_lock = 0;
            pthread_mutex_unlock(&r_info->rlock);
            return false;
        }
        fprintf(stderr,
                "%s DEBUG [REQUEST_HANDLER=%015lu] Cannot get lock %d for %s, region_pos=%d, "
                "srv_tbl_version=%d, cli_tbl_version=%d. Wait %d and will send INVALID_REGION_MAP\n",
                curr_dt(dt), t_handler->cid, REGION_HANDLER_LOCK, r_info->r_fname, r_pos, t_handler->t_info[t_handler->t_info_curr]->srv_tbl_version, t_handler->cli_tbl_version,
                count_stop);
        r_info->r_lock_id = 0;
        r_info->curr_cid_lock = 0;
        pthread_mutex_unlock(&r_info->rlock);

        if (count_stop-- == 0)
            return false;

        while (atomic_load(&is_shutting_down))
            sleep(10);
        usleep(rand() % 1000000);
        pthread_mutex_lock(&r_info->rlock);
        r_info->r_lock_id = NEW_REGION_HANDLER_LOCK;
        r_info->curr_cid_lock = t_handler->cid;

        if (r_info->is_leader && atomic_load(&rs_shutdown_state) != RS_SHUTDOWN_RUNNING) {
            r_info->r_lock_id = 0;
            r_info->curr_cid_lock = 0;
            pthread_mutex_unlock(&r_info->rlock);
            return false;
        }
    }
    return true;
}


static bool normalize_nio_scan(uint8_t *buff, uint32_t *size, uint32_t table_count, bool variable_keys, uint32_t *table_index) {
    const uint32_t table_offset = offsetof(msg_cli_scan_t, block_num);
    const uint32_t key_offset = MSG_CLI_HBASE_SCAN_FILTER_SIZE + sizeof(uint32_t);
    uint32_t declared, index;

    if (*size < key_offset || buff[MSG_TYPE_OFFSET] != MSG_SCN_NIO)
        return false;
    memcpy(&declared, buff, sizeof(declared));
    memcpy(&index, buff + table_offset, sizeof(index));

    if (declared != *size || index >= table_count)
        return false;

    if (variable_keys) {
        uint32_t offset = key_offset;

        for (int key = 0; key < 2; key++) {
            uint32_t length;

            if (*size - offset < sizeof(length))
                return false;
            memcpy(&length, buff + offset, sizeof(length));
            offset += sizeof(length);

            if (length > FIX_BLOCK_SIZE || length > *size - offset)
                return false;
            offset += length;
        }

        if (offset != *size)
            return false;
    } else if (*size != MSG_CLI_SCAN_FILTER_SIZE + sizeof(uint32_t)) {
        return false;
    }
    memmove(buff + table_offset, buff + table_offset + sizeof(index), *size - table_offset - sizeof(index));
    *size -= sizeof(index);
    memcpy(buff, size, sizeof(*size));
    buff[MSG_TYPE_OFFSET] = MSG_SCN;
    *table_index = index;
    return true;
}


static bool prepare_scan_request(table_handler_t *handler, uint32_t *size, bool variable_keys) {
    uint32_t index = 0;

    if (*size < MSG_TYPE_OFFSET + 1U || handler->t_info_size <= 1 || handler->t_info == NULL || handler->cli_tbl_versions == NULL || handler->need_regions_map)
        return false;

    if (handler->buff[MSG_TYPE_OFFSET] == MSG_SCN_NIO) {
        if (!normalize_nio_scan(handler->buff, size, handler->t_info_size - 1, variable_keys, &index))
            return false;
    } else if (handler->buff[MSG_TYPE_OFFSET] != MSG_SCN) {
        return false;
    }

    if (handler->t_info[index] == NULL)
        return false;
    handler->t_info_curr = index;
    return true;
}

static int request_access_mask(const uint8_t *buff, uint32_t size, bool replica) {
    uint32_t declared;

    if (size < MSG_TYPE_OFFSET + 1U)
        return -1;
    memcpy(&declared, buff, sizeof(declared));

    if (declared != size)
        return -1;
    uint8_t type = buff[MSG_TYPE_OFFSET];

    if (type == MSG_BULK_GET || type == MSG_BULK_PUT || type == MSG_BULK_GET_NIO || type == MSG_BULK_PUT_NIO) {
        uint32_t offset = (type == MSG_BULK_GET_NIO || type == MSG_BULK_PUT_NIO) ? 13U : 9U;

        if (size <= offset || size > BULK_MSG_SIZE_LIMIT)
            return -1;
        int mask = (type == MSG_BULK_GET || type == MSG_BULK_GET_NIO) ? 1 : (replica ? 0 : 2);

        while (offset < size) {
            uint32_t item_size;

            if (size - offset < MSG_TYPE_OFFSET + 1U)
                return -1;
            memcpy(&item_size, buff + offset, sizeof(item_size));

            if (item_size < MSG_TYPE_OFFSET + 1U || item_size > size - offset)
                return -1;

            switch (buff[offset + MSG_TYPE_OFFSET]) {
            case MSG_GET:
            case MSG_PUT:
            case MSG_DEL:
            case MSG_PUT_AND_GET:
                break;
            case MSG_DIR_WRITE:
            case MSG_DIR_WRITE_TUNC:
            case MSG_DIR_WRITE_AND_COMPACT:
            case MSG_DATA_FIX:
            case MSG_DATA_CHK:
                if (!replica)
                    return -1;
                break;
            default:
                return -1;
            }
            int item_mask = request_access_mask(buff + offset, item_size, replica);

            if (item_mask < 0)
                return -1;
            mask |= item_mask;
            offset += item_size;
        }
        return mask;
    }

    switch (type) {
    case MSG_GET:
    case MSG_GET_DOUBLE:
        return size >= MIN_REC_SIZE ? 1 : -1;
    case MSG_PUT:
    case MSG_DEL:
    case MSG_DEL_DOUBLE:
    case MSG_DEL_ALL:
        return size >= MIN_REC_SIZE ? 2 : -1;
    case MSG_PUT_AND_GET:
        return size >= MIN_REC_SIZE ? 3 : -1;
    case MSG_SCN:
        return size >= offsetof(msg_cli_t, rec_crc) + sizeof(uint32_t) ? 1 : -1;
    case MSG_SCN_NIO:
        return size >= MSG_CLI_HBASE_SCAN_FILTER_SIZE + sizeof(uint32_t) ? 1 : -1;
    case MSG_CNT:
        return size >= offsetof(msg_cli_count_t, count) ? 1 : -1;
    case MSG_DIR_WRITE:
    case MSG_DIR_WRITE_TUNC:
    case MSG_DIR_WRITE_AND_COMPACT:
    case MSG_DATA_FIX:
    case MSG_UNASSIGN:
        return replica && size >= MSG_RSC_BUFF_OFFSET ? 0 : -1;
    case MSG_DATA_GET:
        return replica && size >= MSG_RSC_BUFF_OFFSET + sizeof(uint32_t) ? 0 : -1;
    case MSG_DATA_CHK:
        return replica && size >= MSG_RSC_BUFF_OFFSET ? 0 : -1;
    case MSG_NO_AUTH:
    case GET_REGION_MAP:
    case GET_REGION_MAP_TIMEOUT:
        return 0;
    default:
        return size >= offsetof(msg_cli_t, rec_crc) + sizeof(uint32_t) ? 0 : -1;
    }
}


static int prepare_unassign_reply(region_info_t *region, const uint8_t *request, uint32_t size, table_handler_t *handler) {
    if (!handler->is_replica_client || size != MSG_RSC_BUFF_OFFSET || region->is_leader || region->has_parent_replica || region->is_freed || region->stop || region->bad_data ||
        region->r_need_data_chk != 0 || region->waiting_count_admin != 0 || region->waiting_count_lock != 0 || region->idx_resizer != RESIZE_IDX_DONE ||
        region->status != STATUS_ASSIGN_REGION || region->uuid == NULL || strnlen(region->uuid, UUID_SIZE + 1U) != UUID_SIZE ||
        memcmp(request + offsetof(msg_rsc_t, uuid), region->uuid, UUID_SIZE) != 0) {
        error_replica_handler(handler->msg_srv, region, handler->cid);
        return -2;
    }
    flush_write_4K_buffer(region, handler->cid);
    uint32_t counter = 0, reply_size = MSG_SIZE_RESERVE_RS_ANSWER;
    memcpy(&counter, request + offsetof(msg_rsc_t, req_count_chk), sizeof(counter));
    counter &= MAX_REPLICA_COUNTER_MASK;
    memcpy(handler->msg_srv->resp_buff, &reply_size, sizeof(reply_size));
    handler->msg_srv->msg_type = handler->msg_srv->resp_buff[MSG_TYPE_OFFSET] = MSG_RESP_OK;
    memcpy(handler->msg_srv->resp_buff + MSG_REPLICA_COUNTER_OFFSET, &counter, MSG_REPLICA_COUNTER_SIZE);
    handler->msg_srv->msg_size = reply_size;
    return 0;
}


static bool process_client_requests_internal(table_handler_t *t_handler, uint32_t rsize) {
    char dt[DT_LOG_SIZE];
    connection_transport_t *transport = t_handler->transport;
    int sockfd = transport_fd(transport);
    uint8_t off_region_pos = offsetof(struct msg_cli_t, region_pos);
    uint8_t off_table_pos = offsetof(struct msg_cli_t, rec_crc);
    region_info_t *r_info = NULL;
    bool need_unlock = false;
    bool table_read = false;
    uint8_t *buff = t_handler->buff;
    uint8_t *big_buff = t_handler->big_buff;
    struct timespec ts;

    if (rsize < MSG_TYPE_OFFSET + 1U)
        return false;
    const uint8_t *request = (buff[MSG_TYPE_OFFSET] == GET_REGION_MAP || buff[MSG_TYPE_OFFSET] == GET_REGION_MAP_TIMEOUT) ? big_buff : buff;

    if (request == NULL)
        return false;
    int access_mask = request_access_mask(request, rsize, t_handler->is_replica_client);

    if (access_mask < 0)
        return false;

    if (t_handler->is_replica_client && buff[MSG_TYPE_OFFSET] == MSG_UNASSIGN) {
        if (rsize != MSG_RSC_BUFF_OFFSET) {
            fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Wrong MSG_UNASSIGN size=%d, expected=%lu\n", curr_dt(dt), t_handler->cid, rsize, MSG_RSC_BUFF_OFFSET);
            set_region_recovery_info(NULL, 1L, t_handler);
            return false;
        }
    }

    if (log_level > 2) {
        pid_t tid = syscall(SYS_gettid);
        fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] RECV MSG rsize = %d, type=%d, TID=%u\n", curr_dt(dt), t_handler->cid, rsize, buff[MSG_TYPE_OFFSET], tid);

        if (log_level > 4) {
            if ((buff[MSG_TYPE_OFFSET] == GET_REGION_MAP || buff[MSG_TYPE_OFFSET] == GET_REGION_MAP_TIMEOUT) && big_buff) {
                for (int i = 0; i < rsize; i++) {
                    fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] RECV_MSG_MAP [%d] = %d | %x\n", dt, t_handler->cid, i, big_buff[i], big_buff[i]);
                }
            } else {
                for (int i = 0; i < rsize; i++) {
                    fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] RECV_MSG_%lu [%d] = %d | %x\n", dt, t_handler->cid, t_handler->all_req_count, i, buff[i], buff[i]);
                }
            }
        }
    }
    uint8_t msg_type =
        ((buff[MSG_TYPE_OFFSET] == GET_REGION_MAP || buff[MSG_TYPE_OFFSET] == GET_REGION_MAP_TIMEOUT) && big_buff) ? big_buff[MSG_TYPE_OFFSET] : buff[MSG_TYPE_OFFSET];

    if (msg_type == GET_REGION_MAP || msg_type == GET_REGION_MAP_TIMEOUT) {
        while (STOP_THE_WORLD == true)
            usleep(10000);

        if (!process_init_msg(transport, big_buff, t_handler->msg_srv, t_handler, rsize)) {
            return false;
        } else {
            t_handler->need_regions_map = false;
        }
    } else if (msg_type == MSG_NO_AUTH) {
        if (rsize != INIT_MSG_HDR) {
            fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Wrong MSG_NO_AUTH size=%d\n", curr_dt(dt), t_handler->cid, rsize);
            return false;
        }
        uint32_t off_result = offsetof(struct msg_rs_result_auth_t, result);
        t_handler->need_regions_map = true;
        t_handler->msg_srv->resp_buff[MSG_SIZE_OFFSET] = MSG_SIZE_AUTH_RESP;
        t_handler->msg_srv->resp_buff[MSG_TYPE_OFFSET] = MSG_AUTH;
        t_handler->msg_srv->resp_buff[off_result] = MSG_AUTH_RESULT_OK;
        t_handler->msg_srv->msg_size = MSG_SIZE_AUTH_RESP;

        if (transport_write(transport, t_handler->msg_srv->resp_buff, t_handler->msg_srv->msg_size) < 0)
            return false;
        return true;
    } else {
        if (t_handler->all_req_count == 0 && log_level > 1) {
            fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] Recv first data request (size %d)\n", curr_dt(dt), t_handler->cid, rsize);
        }
        t_handler->msg_srv->msg_size = 0;
        uint32_t r_pos;
        uint32_t request_table_index = 0;
        memcpy(&r_pos, buff + off_region_pos, sizeof(r_pos));

        if (msg_type == MSG_SCN_NIO || msg_type == MSG_SCN) {
            if (!prepare_scan_request(t_handler, &rsize, hbase_scan_filter)) {
                fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Invalid scan request or unavailable table mapping, type=%u, size=%u\n", curr_dt(dt), t_handler->cid, msg_type,
                        rsize);
                return false;
            }
            request_table_index = t_handler->t_info_curr;

            if (log_level > 1) {
                fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] Scan type=%u, table=%u, region=%u, legacy_size=%u\n", curr_dt(dt), t_handler->cid, msg_type, request_table_index,
                        r_pos, rsize);
            }
            msg_type = MSG_SCN;
        } else {
            if (msg_type != MSG_BULK_GET && msg_type != MSG_BULK_PUT && rsize >= off_table_pos + sizeof(request_table_index)) {
                memcpy(&request_table_index, buff + off_table_pos, sizeof(request_table_index));
            }

            if (request_table_index < t_handler->t_info_size - 1)
                t_handler->t_info_curr = request_table_index;
            else
                t_handler->t_info_curr = 0;
        }

        if (t_handler->need_regions_map) {
            fprintf(stderr, "%s INFO [REQUEST_HANDLER=%015lu] Client did not request the MAP, sleep 10s and close connection\n", curr_dt(dt), t_handler->cid);
            return false;
        }
        while (atomic_load(&is_shutting_down))
            sleep(10);
        begin_tables_read();
        table_read = true;
        int res = -1;

        if (r_pos != (uint32_t)-1 && t_handler->r_pos != (uint32_t)-1) {
            if (t_handler->t_info[t_handler->t_info_curr] != NULL) {
                if (t_handler->t_info[t_handler->t_info_curr]->srv_tbl_version == t_handler->cli_tbl_versions[t_handler->t_info_curr]) {
                    if (r_pos < t_handler->t_info[t_handler->t_info_curr]->region_count_curr) {
                        r_info = t_handler->t_info[t_handler->t_info_curr]->region_info[r_pos];
                        end_tables_read();
                        table_read = false;
                        int count_stop = 60;

                        if (t_handler->t_info[t_handler->t_info_curr]->t_lock_id == LOCK_ID_FIND_REGION) {
                            do {
                                usleep(10000);

                                if (count_stop-- == 0)
                                    goto region_out;
                            } while (t_handler->t_info[t_handler->t_info_curr]->t_lock_id == LOCK_ID_FIND_REGION);
                        }
                        uint64_t client_lock_wait_started_us = request_metric_monotonic_us();
                        bool client_lock_contended = false;
                        bool client_lock_contended_by_compactor = false;
                        bool client_metric_enabled = (t_handler->all_req_count & 1023U) == 0;

                        if (client_metric_enabled)
                            atomic_fetch_add(&append_compact_metric_client_lock_attempts, 1);

                        if (pthread_mutex_trylock(&r_info->rlock) != 0) {
                            client_lock_contended = true;
                            client_metric_enabled = true;
                            client_lock_contended_by_compactor = atomic_load(&r_info->append_compaction_lock_held);
                            atomic_fetch_add(&append_compact_metric_client_lock_contended, 1);

                            if (client_lock_contended_by_compactor)
                                atomic_fetch_add(&append_compact_metric_client_lock_contended_by_compactor, 1);
                            pthread_mutex_lock(&r_info->rlock);
                        }
                        uint64_t client_lock_wait_us = request_metric_monotonic_us() - client_lock_wait_started_us;

                        if (client_lock_contended) {
                            atomic_fetch_add(&append_compact_metric_client_lock_wait_us, client_lock_wait_us);
                            atomic_fetch_add(&append_compact_metric_client_lock_wait_hist[request_metric_wait_bucket(client_lock_wait_us)], 1);
                            request_metric_update_max(&append_compact_metric_client_lock_wait_max_us, client_lock_wait_us);

                            if (client_lock_contended_by_compactor)
                                atomic_fetch_add(&append_compact_metric_client_lock_wait_compactor_us, client_lock_wait_us);
                        }
                        uint64_t client_lock_hold_started_us = client_metric_enabled ? request_metric_monotonic_us() : 0;
                        uint64_t client_block_maker_us = 0;
                        uint64_t client_bulk_process_us = 0;
                        uint64_t client_bulk_replica_us = 0;

                        if (atomic_load(&r_info->append_compaction_requested))
                            atomic_fetch_add(&append_compact_metric_client_acquires_waiting, 1);
                        r_info->r_lock_id = REGION_HANDLER_LOCK;
                        r_info->curr_cid_lock = t_handler->cid;

                        if (region_status_chk(r_info, t_handler, buff, r_pos) == false)
                            goto region_out;

                        while (r_info->waiting_count_lock != 0) {
                            if (atomic_load(&is_shutting_down)) {
                                r_info->r_lock_id = 0;
                                r_info->curr_cid_lock = 0;
                                pthread_mutex_unlock(&r_info->rlock);

                                while (atomic_load(&is_shutting_down)) {
                                    sleep(10);
                                }
                            }
                            fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] Waiting release %s waiting_count_lock=%d\n", curr_dt(dt), t_handler->cid, r_info->r_fname,
                                    buff[MSG_TYPE_OFFSET]);
                            r_info->replica_stoped = true;
                            usleep(10000);
                            r_info->replica_stoped = false;
                        }

                        if (region_status_chk(r_info, t_handler, buff, r_pos) == false)
                            goto region_out;

                        if (atomic_load(&is_shutting_down)) {
                            r_info->r_lock_id = 0;
                            r_info->curr_cid_lock = 0;
                            pthread_mutex_unlock(&r_info->rlock);

                            while (atomic_load(&is_shutting_down)) {
                                sleep(10);
                            }
                        }
                        clock_gettime(CLOCK_REALTIME, &ts);

                        if (ts.tv_sec * 1000L - atomic_load(&t_handler->recv_ts) > t_handler->client_timeout_ms) {
                            fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Client socket timeout exceeded (%lu > %u), break the connection=%d\n", curr_dt(dt), t_handler->cid,
                                    ts.tv_sec * 1000L - atomic_load(&t_handler->recv_ts), t_handler->client_timeout_ms, sockfd);
                            finish_thread(r_info, 32, true, t_handler);
                        }

                        if (t_handler->all_req_count == 0 && log_level > 1 || r_info->log_level > 1) {
                            fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] Ready to handle data request %s, type=%d, size=%u\n", curr_dt(dt), t_handler->cid, r_info->r_fname,
                                    buff[MSG_TYPE_OFFSET], rsize);
                        }
                        int piggyback_result = APPEND_COMPACTION_PIGGYBACK_NONE;
                        piggyback_result = prepare_append_compaction_piggyback(r_info, msg_type, t_handler);

                        if (piggyback_result == APPEND_COMPACTION_PIGGYBACK_ERROR) {
                            res = -1;
                            r_info->r_lock_id_before = r_info->r_lock_id;
                            r_info->curr_cid_lock = r_info->r_lock_id = 0;
                            pthread_mutex_unlock(&r_info->rlock);
                            goto region_out;
                        }
                        r_info->msg_cli_dict_done = r_info->msg_get_dict_done = false;

                        if (msg_type == MSG_UNASSIGN) {
                            res = prepare_unassign_reply(r_info, buff, rsize, t_handler);
                        } else if (msg_type != MSG_BULK_GET && msg_type != MSG_BULK_PUT && msg_type != MSG_BULK_GET_NIO && msg_type != MSG_BULK_PUT_NIO) {
                            uint64_t request_phase_started_us = client_metric_enabled ? request_metric_monotonic_us() : 0;
                            res = block_maker(r_info, buff, rsize, t_handler);

                            if (client_metric_enabled)
                                client_block_maker_us += request_metric_monotonic_us() - request_phase_started_us;
                        } else {
                            uint64_t request_phase_started_us = client_metric_enabled ? request_metric_monotonic_us() : 0;

                            if (msg_type == MSG_BULK_GET || msg_type == MSG_BULK_GET_NIO)
                                res = process_bulk_message(r_info, buff, rsize, t_handler);
                            else {
                                r_info->bulk_mode = BUIK_RELICA_ON;
                                uint8_t bulk_header_size = 9;

                                if (msg_type == MSG_BULK_PUT_NIO)
                                    bulk_header_size = 13;
                                r_info->bulk_buff_offset1 = bulk_header_size;
                                r_info->bulk_buff_offset2 = bulk_header_size;

                                if (ensure_bulk_buffer_capacity(&r_info->bulk_buff1, &r_info->bulk_buff_size1, 0, bulk_header_size, t_handler->cid, "bulk_buff1") != 0 ||
                                    ensure_bulk_buffer_capacity(&r_info->bulk_buff2, &r_info->bulk_buff_size2, 0, bulk_header_size, t_handler->cid, "bulk_buff2") != 0) {
                                    res = -1;
                                } else {
                                    r_info->bulk_buff1[MSG_TYPE_OFFSET] = msg_type;
                                    r_info->bulk_buff2[MSG_TYPE_OFFSET] = msg_type;
                                    res = process_bulk_message(r_info, buff, rsize, t_handler);

                                    if (client_metric_enabled)
                                        client_bulk_process_us += request_metric_monotonic_us() - request_phase_started_us;

                                    if ((res >= 0 || res == -11 || res == -12 || res == RECORD_TOO_LARGE_RESULT) &&
                                        (r_info->bulk_buff_offset1 > bulk_header_size || r_info->bulk_buff_offset2 > bulk_header_size)) {
                                        memcpy(r_info->bulk_buff1, &r_info->bulk_buff_offset1, sizeof(r_info->bulk_buff_offset1));
                                        r_info->bulk_buff1[MSG_TYPE_OFFSET] = msg_type;

                                        if (r_info->bulk_buff_offset1 > bulk_header_size)
                                            memcpy(r_info->bulk_buff1 + off_region_pos, r_info->bulk_buff1 + bulk_header_size + off_region_pos, sizeof(r_pos));
                                        memcpy(r_info->bulk_buff2, &r_info->bulk_buff_offset2, sizeof(r_info->bulk_buff_offset2));
                                        r_info->bulk_buff2[MSG_TYPE_OFFSET] = msg_type;

                                        if (r_info->bulk_buff_offset2 > bulk_header_size)
                                            memcpy(r_info->bulk_buff2 + off_region_pos, r_info->bulk_buff2 + bulk_header_size + off_region_pos, sizeof(r_pos));
                                        request_phase_started_us = client_metric_enabled ? request_metric_monotonic_us() : 0;
                                        bulk_replicas_writer(r_info, t_handler);

                                        if (client_metric_enabled)
                                            client_bulk_replica_us += request_metric_monotonic_us() - request_phase_started_us;
                                    }
                                }
                                r_info->bulk_mode = BUIK_RELICA_OFF;
                                r_info->bulk_buff_offset1 = 0;
                                r_info->bulk_buff_offset2 = 0;
                            }

                            if (client_metric_enabled && (msg_type == MSG_BULK_GET || msg_type == MSG_BULK_GET_NIO))
                                client_bulk_process_us += request_metric_monotonic_us() - request_phase_started_us;
                        }
                        uint64_t client_lock_hold_us = client_metric_enabled ? request_metric_monotonic_us() - client_lock_hold_started_us : 0;

                        if (client_metric_enabled) {
                            atomic_fetch_add(&append_compact_metric_client_lock_hold_us, client_lock_hold_us);
                            atomic_fetch_add(&append_compact_metric_client_lock_hold_samples, 1);
                            atomic_fetch_add(&append_compact_metric_client_block_maker_us, client_block_maker_us);
                            atomic_fetch_add(&append_compact_metric_client_bulk_process_us, client_bulk_process_us);
                            atomic_fetch_add(&append_compact_metric_client_bulk_replica_us, client_bulk_replica_us);
                            request_metric_update_max(&append_compact_metric_client_lock_hold_max_us, client_lock_hold_us);
                        }

                        if (client_metric_enabled && (client_lock_wait_us >= 1000U || client_lock_hold_us >= 2000U)) {
                            uint64_t now_ms = getCurrentTimeMs();
                            uint64_t last_ms = atomic_load(&append_compact_metric_slow_log_last_ms);

                            if (now_ms >= last_ms + 100U && atomic_compare_exchange_strong(&append_compact_metric_slow_log_last_ms, &last_ms, now_ms))
                                fprintf(stderr,
                                        "%s INFO [COMPACT_LOCK=%015lu] Slow region request uuid=%s type=%u size=%u contended=%u by_compactor=%u wait_us=%lu hold_us=%lu "
                                        "block_us=%lu bulk_us=%lu bulk_replica_us=%lu\n",
                                        curr_dt(dt), t_handler->cid, r_info->uuid, msg_type, rsize, client_lock_contended, client_lock_contended_by_compactor, client_lock_wait_us,
                                        client_lock_hold_us, client_block_maker_us, client_bulk_process_us, client_bulk_replica_us);
                        }
                        r_info->r_lock_id_before = r_info->r_lock_id;
                        r_info->curr_cid_lock = r_info->r_lock_id = 0;
                        pthread_mutex_unlock(&r_info->rlock);
                        t_handler->realloc_buff = false;
                        t_handler->realloc_big_buff = false;
                    }
                }
            }
        }
    region_out:
        if (table_read) {
            end_tables_read();
            table_read = false;
        }

        if (res == -1) {
            if (need_unlock)
                pthread_mutex_unlock(&tables_lock);
            t_handler->msg_srv->resp_buff[MSG_SIZE_OFFSET] = MSG_SIZE_NEW_REGION_ORDER;
            t_handler->msg_srv->resp_buff[MSG_TYPE_OFFSET] = INVALID_REGION_MAP;
            t_handler->msg_srv->msg_size = MSG_SIZE_NEW_REGION_ORDER;

            if (t_handler->t_info[t_handler->t_info_curr] != NULL) {
                fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Result -1, region_pos=%d, srv_tbl_version=%d, cli_tbl_version=%d. Sending INVALID_REGION_MAP\n", curr_dt(dt),
                        t_handler->cid, r_pos, t_handler->t_info[t_handler->t_info_curr]->srv_tbl_version, t_handler->cli_tbl_versions[t_handler->t_info_curr]);
            } else {
                fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Invalid table index=%u, registered_tables=%u, region_pos=%d. Sending INVALID_REGION_MAP\n", curr_dt(dt),
                        t_handler->cid, request_table_index, t_handler->t_info_size - 1, r_pos);
            }
        }

        if (res == 0 && !t_handler->is_replica_client && (msg_type == MSG_PUT || msg_type == MSG_BULK_PUT || msg_type == MSG_BULK_PUT_NIO)) {
            uint64_t put_ok = MSG_PUT_OK;
            memcpy(t_handler->msg_srv->resp_buff, &put_ok, MSG_SHORT_ANSWER_SIZE);
            t_handler->msg_srv->msg_size = MSG_SHORT_ANSWER_SIZE;
        }

        if (log_level > 2) {
            fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] SEND MSG msg_size = %d, type=%d\n", curr_dt(dt), t_handler->cid, t_handler->msg_srv->msg_size,
                    t_handler->msg_srv->resp_buff[MSG_TYPE_OFFSET]);

            if (log_level > 4) {
                for (int i = 0; i < t_handler->msg_srv->msg_size; i++) {
                    fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] SEND_MSG_%lu [%d] = %d | %x\n", dt, t_handler->cid, t_handler->all_req_count, i,
                            t_handler->msg_srv->resp_buff[i], t_handler->msg_srv->resp_buff[i]);
                }
            }
        }
        ssize_t wres = transport_write(transport, t_handler->msg_srv->resp_buff, t_handler->msg_srv->msg_size);

        if (wres != t_handler->msg_srv->msg_size) {
            if (msg_type == MSG_DIR_WRITE || msg_type == MSG_DIR_WRITE_TUNC || msg_type == MSG_DIR_WRITE_AND_COMPACT || msg_type == MSG_DATA_FIX || msg_type == MSG_UNASSIGN) {
                if (set_region_recovery_info(r_info, 1L, t_handler) && msg_type == MSG_DIR_WRITE_AND_COMPACT && r_info != NULL)
                    atomic_store(&r_info->r_need_data_chk, 1L);
            }
            fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Failed sent answer wres=%ld != msg_size=%u\n", curr_dt(dt), t_handler->cid, wres, t_handler->msg_srv->msg_size);
            return false;
        }

        if (msg_type == MSG_UNASSIGN) {
            if (res == 0) {
                t_handler->leader_commit = true;

                if (log_level > 1)
                    fprintf(stderr, "%s INFO [REQUEST_HANDLER=%015lu] MSG_UNASSIGN confirmed after buffer flush\n", curr_dt(dt), t_handler->cid);
            }
            return true;
        }

        if (t_handler->all_req_count == 1 && log_level > 1) {
            fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] Sent answer to first data request (size %ld)\n", curr_dt(dt), t_handler->cid, wres);
        }
        while (stop_any_client_requests)
            sleep(3);

        if (t_handler->all_req_count == 0 && t_handler->cli_req_count % rtt_client_get_stat_count == 0) {
            u_int32_t rtt_us;
            int rtt_res = get_tcp_rtt_us_by_fd(sockfd, &rtt_us);

            if (rtt_res == 0) {
                t_handler->rtt_us_count++;
                t_handler->rtt_us_avg += ((double)rtt_us - t_handler->rtt_us_avg) / (double)t_handler->rtt_us_count;

                if (t_handler->cli_req_count % rtt_client_print_stat_count == 0 && log_level > 0)
                    fprintf(stderr, "%s INFO [REQUEST_HANDLER=%015lu] Client fd=%d curr RTT=%u us, avr RTT=%.0f\n", curr_dt(dt), t_handler->cid, sockfd, rtt_us,
                            t_handler->rtt_us_avg);
            } else if (log_level > 0)
                fprintf(stderr, "%s INFO [REQUEST_HANDLER=%015lu] Client fd=%d RTT failed, rtt_res=%d\n", curr_dt(dt), t_handler->cid, sockfd, rtt_res);
        }

        if (t_handler->all_req_count > 0 && t_handler->all_req_count % rtt_replica_get_stat_count == 0) {
            u_int32_t rtt_us;
            int rtt_res = get_tcp_rtt_us_by_fd(sockfd, &rtt_us);

            if (rtt_res == 0) {
                t_handler->rtt_us_count++;
                t_handler->rtt_us_avg += ((double)rtt_us - t_handler->rtt_us_avg) / (double)t_handler->rtt_us_count;

                if (t_handler->all_req_count % rtt_replica_print_stat_count == 0 && log_level > 0)
                    fprintf(stderr, "%s INFO [REQUEST_HANDLER=%015lu] Replica fd=%d curr RTT=%u us, avr RTT=%.0f\n", curr_dt(dt), t_handler->cid, sockfd, rtt_us,
                            t_handler->rtt_us_avg);
            } else if (log_level > 0)
                fprintf(stderr, "%s INFO [REQUEST_HANDLER=%015lu] Replica fd=%d RTT failed, rtt_res=%d\n", curr_dt(dt), t_handler->cid, sockfd, rtt_res);
        }

        if (t_handler->d_avg_send_msg_size > 0.0)
            t_handler->d_avg_send_msg_size = (t_handler->d_avg_send_msg_size * SEND_MSG_WINDOW_SIZE + t_handler->msg_srv->msg_size) / (SEND_MSG_WINDOW_SIZE + 1.0);
        else
            t_handler->d_avg_send_msg_size = (double)t_handler->msg_srv->msg_size;

        if (t_handler->d_avg_recv_msg_size > 0.0)
            t_handler->d_avg_recv_msg_size = (t_handler->d_avg_recv_msg_size * RECV_MSG_WINDOW_SIZE + rsize) / (RECV_MSG_WINDOW_SIZE + 1.0);
        else
            t_handler->d_avg_recv_msg_size = (double)rsize;

        if (t_handler->buff_size > CURR_MAX_MSG_SIZE && (uint32_t)t_handler->d_avg_recv_msg_size < CURR_MAX_MSG_SIZE) {
            void *buff_tmp = realloc(t_handler->buff, CURR_MAX_MSG_SIZE);

            if (buff_tmp)
                t_handler->buff = buff_tmp;
            else
                return false;
            t_handler->buff_size = CURR_MAX_MSG_SIZE;
        }
    }
    return true;
}

bool process_client_requests(table_handler_t *t_handler, uint32_t rsize) {
    if (!t_handler->is_replica_client) {
        bool result = process_client_requests_internal(t_handler, rsize);
        return result;
    }
    bool result;
    rs_shutdown_replica_request_begin();
    pthread_cleanup_push(rs_shutdown_replica_request_end, NULL);
    result = process_client_requests_internal(t_handler, rsize);
    pthread_cleanup_pop(1);
    return result;
}

static int read_exact_message(connection_transport_t *transport, uint8_t *dst, int want, uint64_t cid) {
    if (!dst || want < 0)
        return -1;
    int total = 0;

    while (total < want) {
        int rr = (int)transport_read(transport, dst + total, (size_t)(want - total), cid);

        if (rr > 0) {
            total += rr;
            continue;
        }
        return rr;
    }
    return total;
}


int recv_client_requests(connection_t *conn) {
    char dt[DT_LOG_SIZE];
    table_handler_t *t_handler = conn->t_handler;
    connection_transport_t *transport = &conn->transport;
    uint8_t *buff = t_handler->buff;
    uint8_t *big_buff = t_handler->big_buff;

    while (true) {
        memset(t_handler->msg_srv->resp_buff, 0, INIT_MSG_SIZE + INIT_MSG_TYPE);
        int rsize = 0;
        uint32_t msize = 0;
        uint32_t want_read = FIX_BLOCK_SIZE * 2;
        atomic_store(&conn->state, STATE_WORKING);

        do {
            int rr = read_exact_message(transport, buff, MSG_TYPE_OFFSET + 1, t_handler->cid);

            if (rr <= 0) {
                if (t_handler->is_replica_client && t_handler->all_req_count > 0) {
                    set_region_recovery_info(NULL, 1L, t_handler);
                    return -8;
                }
            }

            if (rr != MSG_TYPE_OFFSET + 1) {
                atomic_store(&conn->state, STATE_AUTHENTICATED);
                return -8;
            }
            rsize = MSG_TYPE_OFFSET + 1;
            memcpy(&msize, buff, sizeof(msize));

            if (msize < 5)
                return -1;

            if (buff[MSG_TYPE_OFFSET] == GET_REGION_MAP || buff[MSG_TYPE_OFFSET] == GET_REGION_MAP_TIMEOUT) {
                if (msize > t_handler->big_buff_size) {
                    uint8_t *big_buff_tmp = realloc(t_handler->big_buff, msize);

                    if (!big_buff_tmp) {
                        fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Realloc big_buff_tmp failed\n", curr_dt(dt), t_handler->cid);
                        return -3;
                    }
                    big_buff = t_handler->big_buff = big_buff_tmp;
                    t_handler->big_buff_size = msize;
                    t_handler->realloc_big_buff = true;
                }
                memcpy(big_buff, buff, rsize);
                rr = read_exact_message(transport, big_buff + rsize, (int)msize - rsize, t_handler->cid);

                if (rr != (int)msize - rsize) {
                    return -4;
                }
                rsize = (int)msize;
            } else {
                size_t old_buff_size = t_handler->buff_size;
                uint64_t required_buff_size = (uint64_t)msize + LAST_CRC16;

                if (required_buff_size > t_handler->buff_size) {
                    uint32_t new_buff_size;

                    if (calc_msg_buff_size(required_buff_size, &new_buff_size, t_handler->cid) == false)
                        return -5;
                    size_t extra_for_buff = new_buff_size - old_buff_size;
                    uint64_t extra_for_resp = new_buff_size > t_handler->msg_srv->curr_max_msg_size ? (uint64_t)new_buff_size - t_handler->msg_srv->curr_max_msg_size : 0;
                    extra_for_resp = extra_for_resp < 0 ? 0 : extra_for_resp;
                    size_t bytes_to_alloc = extra_for_buff + extra_for_resp;
                    unsigned int free_after_pct = 0;

                    if (!mem_check_before_alloc(bytes_to_alloc, mem_free_limit_percentage, &free_after_pct)) {
                        fprintf(stderr, "%s ERROR [REQUEST_HANDLER=%015lu] Free memory %u would fall to %u%% (<%d%%). Allocation aborted\n", curr_dt(dt), t_handler->cid,
                                new_buff_size, free_after_pct, mem_free_limit_percentage);
                        return -6;
                    }
                    t_handler->buff_size = new_buff_size;

                    if (log_level > 1)
                        fprintf(stderr, "%s DEBUG [REQUEST_HANDLER=%015lu] Recv msg size %d. Realloc client buffs up to %u\n", curr_dt(dt), t_handler->cid, msize,
                                t_handler->buff_size);
                    uint8_t *buff_tmp = realloc(buff, t_handler->buff_size);

                    if (!buff_tmp) {
                        fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Realloc buff_tmp failed\n", curr_dt(dt), t_handler->cid);
                        return -7;
                    }
                    buff = t_handler->buff = buff_tmp;
                    // TODO Remove it and test
                    if (t_handler->msg_srv->curr_max_msg_size < t_handler->buff_size) {
                        if (realloc_resp_buff(t_handler->msg_srv, t_handler->buff_size, t_handler->cid) < 0) {
                            return -8;
                        }
                    }
                }
                rr = read_exact_message(transport, buff + rsize, (int)msize - rsize, t_handler->cid);

                if (rr != (int)msize - rsize)
                    return -9;
                rsize = (int)msize;
            }
        } while (rsize != (int)msize);

        if (blocking_socket_mode) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            atomic_store(&t_handler->recv_ts, ts.tv_sec * 1000L);
        }

        if (rsize <= 0) {
            atomic_store(&conn->state, STATE_AUTHENTICATED);
            return -10;
        }

        if (!process_client_requests(t_handler, rsize)) {
            atomic_store(&conn->state, STATE_AUTHENTICATED);
            return -11;
        }
        buff = t_handler->buff;
        big_buff = t_handler->big_buff;
        atomic_store(&conn->state, STATE_AUTHENTICATED);

        if (conn->mode == NONBLOCKING_MODE)
            return 0;
    };
    return 1;
}
