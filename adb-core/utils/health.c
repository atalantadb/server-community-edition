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
#include "health.h"
#include "region_memory.h"
#include "../handle/storage.h"
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <malloc.h>
#include "../handle/replica_handler.h"
#define MAX_NUMBER_OF_DRIVES 256
#define MAX_SHUTDOWN_TABLE_THREADS 64
#define SHUTDOWN_TABLE_LOCK 30
#define SHUTDOWN_REGION_LOCK 115
#define SHUTDOWN_IS_WRITE_WAIT_TRIES 10000
#define SHUTDOWN_TABLES_LOCK_TRIES 10000
#define SHUTDOWN_TABLE_LOCK_PASSES 1000
#define SHUTDOWN_RECOVERY_DROP_WAIT_TRIES 10000
#define SHUTDOWN_WORKER_WAIT_TRIES 1000
#define SHUTDOWN_SSL_LOCK_TRIES 100
#define SHUTDOWN_CONTROL_LOCK_TRIES 1000
#define SHUTDOWN_SECOND_PASS_DELAY_US 100000
#define SHUTDOWN_THIRD_PASS_DELAY_US 500000
typedef struct shutdown_table_arg_t {
    table_info_t *t_info;
    uint32_t region_count;
    uint64_t cid;
    bool result;
    bool thread_started;
} shutdown_table_arg_t;
char *drv_arr[MAX_NUMBER_OF_DRIVES];
_Atomic int curr_drv_arr = 0;
static _Atomic uint32_t recovery_drop_inflight = 0;
static _Atomic bool recovery_marker_failed = false;
uint64_t need_data_chk;
_Atomic int replica_need_data_chk_count;
uint32_t replica_need_data_chk_capacity;
struct region_info_t **r_info_replicas_chk;
pthread_mutex_t r_info_chk_mutex = PTHREAD_MUTEX_INITIALIZER;
void graceful_exit(int user_exit_code) {
    rs_signal_handler(user_exit_code);
    sleep(100);
    exit(-1);
}
#define STATE_BLOCK_OK 1
#define STATE_BLOCK_BAD 0
void calculate_crc(const char *filename) {
    char dt[DT_LOG_SIZE];
    FILE *file = fopen(filename, "rb");

    if (!file) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] CRC32 caclulation failed - File %s open error: %s", curr_dt(dt), 0L, filename, strerror(errno));
        return;
    }
    unsigned char *buffer = (unsigned char *)malloc(CHUNK_SIZE);

    if (!buffer) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] CRC32 caclulation failed - Memory allocation error: %s", curr_dt(dt), 0L, strerror(errno));
        fclose(file);
        return;
    }
    size_t bufferSize = CHUNK_SIZE;
    size_t bytesReadTotal = 0;
    size_t bytesRead;

    while ((bytesRead = fread(buffer + bytesReadTotal, 1, CHUNK_SIZE, file)) > 0) {
        bytesReadTotal += bytesRead;

        if (bytesReadTotal + CHUNK_SIZE > bufferSize) {
            bufferSize *= 2;
            unsigned char *newBuffer = (unsigned char *)realloc(buffer, bufferSize);

            if (!newBuffer) {
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] CRC32 caclulation failed - Memory %lu allocation error: %s", curr_dt(dt), 0L, bufferSize, strerror(errno));
                free(buffer);
                fclose(file);
                return;
            }
            buffer = newBuffer;
        }
    }
    uint32_t crc = crc_32(buffer, bytesReadTotal);
    free(buffer);
    fclose(file);
    unsigned char hash[4];
    memcpy(hash, &crc, 4);
    fprintf(stderr, "CRC32: ");

    for (int i = 0; i < 4; i++)
        fprintf(stderr, "%02X", hash[i]);
    fprintf(stderr, "\n");
}

int get_month_number(const char *month) {
    const char *months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

    for (int i = 0; i < 12; i++) {
        if (strcmp(month, months[i]) == 0) {
            return i + 1;
        }
    }
    return 0;
}

int compare_filenames(const char *path1, const char *path2) {
    const char *file1 = strrchr(path1, '/');
    const char *file2 = strrchr(path2, '/');
    file1 = (file1) ? file1 + 1 : path1;
    file2 = (file2) ? file2 + 1 : path2;
    return strcmp(file1, file2) == 0 ? 1 : 0;
}

uint32_t build_rsc_msg(region_info_t *r_info, uint8_t replica_num, uint8_t msg_type, uint64_t rw_off, uint8_t bs, uint32_t chunk_size) {
    if (msg_type == MSG_DATA_FIX || msg_type == MSG_DIR_WRITE_TUNC || msg_type == MSG_DIR_WRITE || msg_type == MSG_DIR_WRITE_AND_COMPACT) {
        invalidate_replicas_idle_commit(r_info);
        invalidate_record_gc(r_info);
    }
    uint32_t off_uuid = offsetof(struct msg_rsc_t, uuid);
    uint32_t off_offset = off_uuid + UUID_SIZE;
    uint32_t off_bs = off_offset + sizeof(rw_off);
    uint32_t off_buff = off_bs + sizeof(bs);
    uint32_t rsc_msg_size = off_buff;

    if (msg_type == MSG_DATA_FIX)
        rsc_msg_size += chunk_size;

    if (msg_type == MSG_DATA_GET)
        rsc_msg_size += sizeof(chunk_size);
    memcpy(r_info->rsc_msg, &rsc_msg_size, sizeof(rsc_msg_size));
    r_info->rsc_msg[MSG_TYPE_OFFSET] = msg_type;
    uint32_t int_converter = 0;

    if (replica_num == 1)
        int_converter = r_info->replica->replica1_region_pos;

    if (replica_num == 2)
        int_converter = r_info->replica->replica2_region_pos;
    memcpy(r_info->rsc_msg + offsetof(struct msg_rsc_t, region_pos), &int_converter, sizeof(int_converter));
    memcpy(r_info->rsc_msg + off_uuid, r_info->uuid, UUID_SIZE);
    r_info->rsc_msg[off_uuid + UUID_SIZE - 1] = replica_num + '0';

    if (msg_type != MSG_DATA_CHK) {
        memcpy(r_info->rsc_msg + off_offset, &rw_off, sizeof(rw_off));

        if (bs != BS_LOB)
            r_info->rsc_msg[off_bs] = r_info->r_idx_b[bs]->pos;
        else
            r_info->rsc_msg[off_bs] = 0;

        if (msg_type == MSG_DATA_FIX)
            memcpy(r_info->rsc_msg + off_buff, r_info->zbuff, chunk_size);

        if (msg_type == MSG_DATA_GET)
            memcpy(r_info->rsc_msg + off_buff, &chunk_size, sizeof(chunk_size));
    }
    return rsc_msg_size;
}


static bool is_admin_data_check(const region_info_t *r_info, const table_handler_t *t_handler) {
    return r_info != NULL && t_handler != NULL && t_handler->admin_data_check_region == r_info;
}

static bool refresh_data_check_replica(region_info_t *r_info, uint8_t replica_num, table_handler_t *t_handler) {
    bool is_stoppable = is_admin_data_check(r_info, t_handler);
    return replica_refresh_region_map(r_info, replica_num, is_stoppable, t_handler);
}

bool reconnect_replicas(region_info_t *r_info, uint8_t replica_num, int *sock_fd, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];

    if (replica_num == 1) {
        if (r_info->replica->replica1_sock_fd > 0)
            close_socket(r_info, replica_num, r_info->replica->replica1_sock_fd);
        r_info->replica->replica1_sock_fd = -1;
    }

    if (replica_num == 2) {
        if (r_info->replica->replica2_sock_fd > 0)
            close_socket(r_info, replica_num, r_info->replica->replica2_sock_fd);
        r_info->replica->replica2_sock_fd = -1;
    }
    fprintf(stderr, "%s WARN [HEALTH=%015lu] Data check reconnect replica%d %s\n", curr_dt(dt), t_handler->cid, replica_num, r_info->uuid);
    bool result = refresh_data_check_replica(r_info, replica_num, t_handler);

    if (!result) {
        fprintf(stderr, "%s WARN [HEALTH=%015lu] Failed reconnect replica%d %s\n", curr_dt(dt), t_handler->cid, replica_num, r_info->uuid);
        return false;
    }
    union i2b_u i2b;
    uint32_t off_region_pos = offsetof(struct msg_rsc_t, region_pos);

    if (replica_num == 1) {
        i2b.integer_val = r_info->replica->replica1_region_pos;
        memcpy(r_info->rsc_msg + off_region_pos, i2b.byte, sizeof(i2b.integer_val));
        *sock_fd = r_info->replica->replica1_sock_fd;
        return true;
    }

    if (replica_num == 2) {
        i2b.integer_val = r_info->replica->replica2_region_pos;
        memcpy(r_info->rsc_msg + off_region_pos, i2b.byte, sizeof(i2b.integer_val));
        *sock_fd = r_info->replica->replica2_sock_fd;
        return true;
    }
}


static bool retry_data_check_reconnect(region_info_t *r_info, uint8_t replica_num, int *sock_fd, table_handler_t *t_handler, uint32_t *admin_reconnects) {
    if (is_admin_data_check(r_info, t_handler)) {
        if (atomic_load(&is_shutting_down) || *admin_reconnects >= 3U) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Administrative data check reconnect aborted for %s replica%u, attempts=%u, shutdown=%d\n", curr_dt(dt), t_handler->cid,
                    r_info->uuid, replica_num, *admin_reconnects, atomic_load(&is_shutting_down));
            return false;
        }
        ++*admin_reconnects;
        return reconnect_replicas(r_info, replica_num, sock_fd, t_handler);
    }
    while (!reconnect_replicas(r_info, replica_num, sock_fd, t_handler))
        sleep(10);
    return true;
}


static ssize_t read_all_plain(int fd, void *buf, size_t count) {
    size_t bytes_left = count;
    ssize_t total_read = 0;
    uint8_t *p = (uint8_t *)buf;

    while (bytes_left > 0) {
        ssize_t r = read(fd, p, bytes_left);

        if (r < 0) {
            if (errno == EINTR)
                continue;

            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return -1;
            return -1;
        } else if (r == 0) {
            return total_read;
        }
        p += r;
        bytes_left -= (size_t)r;
        total_read += r;
    }
    return total_read;
}

static ssize_t write_all_plain(int fd, const void *buf, size_t count) {
    size_t bytes_left = count;
    ssize_t total_written = 0;
    const uint8_t *p = (const uint8_t *)buf;

    while (bytes_left > 0) {
        ssize_t w = write(fd, p, bytes_left);

        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        } else if (w == 0) {
            return total_written;
        }
        p += w;
        bytes_left -= (size_t)w;
        total_written += w;
    }
    return total_written;
}

int replica_data_fix_exchange(region_info_t *r_info, uint8_t replica_num, uint32_t rsc_msg_size, uint8_t *recv_buff, uint32_t wait_recv_timeout, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    uint64_t cid = t_handler->cid;
    bool data_fix_done = true;
    uint32_t admin_reconnects = 0;
    int sock_fd = 0;

    do {
        if (replica_num == 1) {
            sock_fd = r_info->replica->replica1_sock_fd;
        } else if (replica_num == 2) {
            sock_fd = r_info->replica->replica2_sock_fd;
        }

        if (replica_num == 1 && r_info->replica->r_info1 != NULL) {
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Return from replica_data_fix_exchange replica_num=%d r_info1=%s\n", curr_dt(dt), t_handler->cid, replica_num, r_info->uuid);
            return -1;
        }

        if (replica_num == 2 && r_info->replica->r_info2 != NULL) {
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Return from replica_data_fix_exchange replica_num=%d r_info2=%s\n", curr_dt(dt), t_handler->cid, replica_num, r_info->uuid);
            return -1;
        }
        r_info->req_count_chk++;

        if (r_info->req_count_chk > MAX_REPLICA_COUNTER_MASK)
            r_info->req_count_chk = 1;
        memcpy(r_info->rsc_msg + offsetof(struct msg_rsc_t, req_count_chk), &r_info->req_count_chk, sizeof(r_info->req_count_chk));
        ssize_t wres = write_all_plain(sock_fd, r_info->rsc_msg, rsc_msg_size);

        if (wres < 0 || (uint32_t)wres != rsc_msg_size) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Write to socket %d replica%d %s error %s (written %zd)\n", curr_dt(dt), cid, sock_fd, replica_num, r_info->uuid,
                    (wres < 0) ? strerror(errno) : "partial write", wres);
            data_fix_done = false;
            sleep(1);
        }

        if (!data_fix_done) {
            if (!retry_data_check_reconnect(r_info, replica_num, &sock_fd, t_handler, &admin_reconnects))
                return -1;
            data_fix_done = true;
            continue;
        }
        struct timeval tv;
        tv.tv_sec = wait_recv_timeout;
        tv.tv_usec = 0;
        setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv));
        uint64_t resp_data = 0;
        ssize_t rres = read_all_plain(sock_fd, &resp_data, MSG_SIZE_RESERVE_RS_ANSWER);

        if (rres < 0) {
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Read error from socket %d replica%d %s: %s\n", curr_dt(dt), cid, sock_fd, replica_num, r_info->uuid, strerror(errno));
            data_fix_done = false;
        } else if (rres == 0) {
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Connection closed while reading from socket %d replica%d %s\n", curr_dt(dt), cid, sock_fd, replica_num, r_info->uuid);
            data_fix_done = false;
        } else if (rres < MSG_SIZE_RESERVE_RS_ANSWER) {
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Partial read (%zd of 8 bytes) from socket %d replica%d %s\n", curr_dt(dt), cid, rres, sock_fd, replica_num, r_info->uuid);
            data_fix_done = false;
        }

        if (!data_fix_done) {
            if (!retry_data_check_reconnect(r_info, replica_num, &sock_fd, t_handler, &admin_reconnects))
                return -1;
            data_fix_done = true;
            continue;
        }
        uint32_t msg_size = (resp_data & 0xFFFFFFFFULL);
        uint8_t msg_type = ((resp_data >> 32) & 0xFF);
        uint32_t req_count_chk = ((resp_data >> 40) & 0xFFFFFFULL);

        if (msg_size == MSG_SIZE_RESERVE_RS_ANSWER && msg_type == MSG_RESP_OK && req_count_chk == r_info->req_count_chk) {
            tv.tv_sec = cli_sock_timeout;
            setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv));
            return 0;
        }

        if (msg_size > MSG_SIZE_RESERVE_RS_ANSWER && req_count_chk == r_info->req_count_chk) {
            if (msg_type == MSG_DATA_GET_RESULT || msg_type == MSG_DATA_CHK_RESULT) {
                uint32_t need_to_read = msg_size - MSG_SIZE_RESERVE_RS_ANSWER;

                if (need_to_read > BLOCK_MAX_SIZE) {
                    fprintf(stderr, "%s WARN [HEALTH=%015lu] Msg_size too large (%u), rejecting.\n", curr_dt(dt), cid, msg_size);
                    data_fix_done = false;
                } else {
                    ssize_t rres = read_all_plain(sock_fd, recv_buff, need_to_read);

                    if (rres < 0) {
                        fprintf(stderr, "%s WARN [HEALTH=%015lu] Read error from socket %d replica%d %s: %s\n", curr_dt(dt), cid, sock_fd, replica_num, r_info->uuid,
                                strerror(errno));
                        data_fix_done = false;
                    } else if ((uint32_t)rres < need_to_read) {
                        fprintf(stderr, "%s WARN [HEALTH=%015lu] Connection closed/partial read while reading from socket %d replica%d %s\n", curr_dt(dt), cid, sock_fd,
                                replica_num, r_info->uuid);
                        data_fix_done = false;
                    }
                }
                tv.tv_sec = cli_sock_timeout;
                setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv));

                if (!data_fix_done) {
                    if (!retry_data_check_reconnect(r_info, replica_num, &sock_fd, t_handler, &admin_reconnects))
                        return -1;
                    data_fix_done = true;
                    continue;
                }
                return (msg_size - MSG_SIZE_RESERVE_RS_ANSWER);
            }
        }
        fprintf(stderr, "%s WARN [HEALTH=%015lu] Invalid response (size=%u type=%u req_count=%u) from socket %d replica%d %s, sleep 10s\n", curr_dt(dt), cid, msg_size, msg_type,
                req_count_chk, sock_fd, replica_num, r_info->uuid);
        sleep(10);

        if (!retry_data_check_reconnect(r_info, replica_num, &sock_fd, t_handler, &admin_reconnects))
            return -1;
    } while (true);
}

void stop_region_after_key_error(region_info_t *r_info, int code, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    r_info->stop = true;
    r_info->bad_data = true;
    fprintf(stderr, "%s WARN [HEALTH=%015lu] Key extraction interrupted for %s, code=%d; region stopped for recovery\n", curr_dt(dt), t_handler->cid, r_info->uuid, code);
    set_region_recovery_info(r_info, 1, t_handler);
}


data_quality_result_t check_data_quality(region_info_t *r_info, uint8_t bs, uint64_t file_chk_offset, uint64_t file_size, uint8_t *chk_buff, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    int16_t curr_pos = 0;
    uint32_t zip_val_size = -1;
    uint32_t r_zip_offset = 0;
    uint32_t r_real_offset = 0;
    uint16_t bsize_factor = r_info->r_idx_b[bs]->bsize_factor;
    uint32_t block_size = r_info->r_idx_b[bs]->block_size;
    struct msg_qv_t qv = {0};
    msg_kv_t kv = {.qv = &qv};
    msg_cli_t msg = {.kv = &kv};
    msg_cli_t *msg_get = &msg;
    data_quality_result_t result = DATA_QUALITY_BAD;
    uint8_t *buff = malloc(block_size);

    if (buff == NULL) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Cannot allocate data check buffer\n", curr_dt(dt), cid);
        return DATA_QUALITY_ERROR;
    }
    uint32_t f = file_chk_offset / block_size;
    bool logical_eof_found = false;
    fprintf(stderr, "%s INFO [HEALTH=%015lu] Replica %s data check begin, block=%u, block_size=%u, file_chk_offset=%lu\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname, f,
            block_size, file_chk_offset);
    uint32_t checked_keys = 0;

    while (file_chk_offset < file_size) {
        memset(buff, 0, block_size);
        size_t to_copy = block_size;

        if (file_chk_offset + block_size > file_size)
            to_copy = file_size - file_chk_offset;
        memcpy(buff, chk_buff + file_chk_offset, to_copy);
        r_zip_offset = 0;
        int count_iteration = 0;

        do {
            if (count_iteration++ > block_size) {
                fprintf(stderr, "%s WARN [HEALTH=%015lu] Bad block %u in %s, file_chk_offset=%lu, r_zip_offset=%d, zip_val_size=%d, count_iteration=%d\n", curr_dt(dt), cid, f,
                        r_info->r_idx_b[bs]->fname, file_chk_offset, r_zip_offset, zip_val_size, count_iteration);
                goto data_check_cleanup;
            }
            r_real_offset = r_zip_offset * bsize_factor;

            if (r_info->enable_block_compression == BLOCK_COMPRESSION_NONE && r_info->r_idx_b[bs]->fd[FBP] == r_info->data_000_fd && block_size == FIX_BLOCK_SIZE &&
                is_append_logical_eof_marker(buff + r_real_offset, to_copy > r_real_offset ? to_copy - r_real_offset : 0U)) {
                logical_eof_found = true;
                break;
            }
            free(kv.key);
            free(kv.cf);
            kv.key = NULL;
            kv.cf = NULL;
            kv.key_size = 0;
            kv.cf_size = 0;
            curr_pos = get_key_from_msg_bytes_safety_checked(msg_get, buff + r_real_offset);

            if (curr_pos == KEY_EXTRACT_NOMEM || curr_pos == KEY_EXTRACT_INVALID_STATE) {
                fprintf(stderr, "%s WARN [HEALTH=%015lu] Data check incomplete for %s, block=%u, code=%d\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname, f, curr_pos);
                result = DATA_QUALITY_ERROR;
                goto data_check_cleanup;
            }

            if (curr_pos < 0) {
                fprintf(stderr, "%s WARN [HEALTH=%015lu] Bad block %u in %s, file_chk_offset=%lu, r_zip_offset=%d, zip_val_size=%d, code=%d\n", curr_dt(dt), cid, f,
                        r_info->r_idx_b[bs]->fname, file_chk_offset, r_zip_offset, zip_val_size, curr_pos);
                goto data_check_cleanup;
            }

            if (msg_get->msg_size == 0) {
                if (file_chk_offset != 0) {
                    for (int i = r_real_offset; i < block_size; i++) {
                        if (buff[i] != 0) {
                            fprintf(stderr, "%s TRACE [HEALTH=%015lu] Bad end of block %u in %s, chk_offset=%lu, r_zip=%d, zip_val=%d, buff[%d]=%x\n", curr_dt(dt), cid, f,
                                    r_info->r_idx_b[bs]->fname, file_chk_offset, r_zip_offset, zip_val_size, i, buff[i]);
                            goto data_check_cleanup;
                        }
                    }
                }
                break;
            }
            checked_keys++;
            disk_record_header_t disk_header;

            if (r_real_offset > to_copy || !disk_record_decode_header(buff + r_real_offset, to_copy - r_real_offset, &disk_header) ||
                (disk_header.kind == DISK_RECORD_TOMBSTONE &&
                 (r_info->r_idx_b[bs]->fd[FBP] != r_info->data_000_fd || r_info->enable_block_compression != BLOCK_COMPRESSION_NONE)))
                goto data_check_cleanup;

            if (msg_get->kv->key_size == 0) {
                fprintf(stderr, "%s WARN [HEALTH=%015lu] Bad block %u in %s, key size=0, file_chk_offset=%lu, r_zip_offset=%d, zip_val_size=%d, code=%d\n", curr_dt(dt), cid, f,
                        r_info->r_idx_b[bs]->fname, file_chk_offset, r_zip_offset, zip_val_size, curr_pos);
                goto data_check_cleanup;
            }
            zip_val_size = (uint8_t)((msg_get->msg_size - 1) / bsize_factor + 1);
            r_zip_offset += zip_val_size;
        } while (true);

        if (logical_eof_found)
            break;
        file_chk_offset += block_size;
        f++;

        if (file_chk_offset < 256 * 256 && log_level > 0)
            fprintf(stderr, "%s DEBUG [HEALTH=%015lu] Iterate replica %s to file_chk_offset=%lu\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname, file_chk_offset);
    };
    fprintf(stderr, "%s INFO [HEALTH=%015lu] Replica %s data check done with success, file_chk_offset=%lu, checked_keys=%u\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname,
            file_chk_offset, checked_keys);
    result = DATA_QUALITY_OK;
data_check_cleanup:
    free(kv.key);
    free(kv.cf);
    free(buff);
    return result;
}

void data_fix(struct region_info_t *r_info, uint8_t bs, uint32_t read_block, uint16_t r_real_offset, table_handler_t *t_handler) {
    invalidate_record_gc(r_info);
    char dt[DT_LOG_SIZE];
    uint16_t bsize_factor = r_info->r_idx_b[bs]->bsize_factor;
    uint32_t block_size = r_info->r_idx_b[bs]->block_size;
    uint64_t rw_off = (uint64_t)read_block * block_size;
    bool data_chk_r1 = 0;
    bool data_chk_r2 = 0;
    int32_t chunk_size_ret1 = 0;
    int32_t chunk_size_ret2 = 0;

    if (r_info->replica->replica_num > 0) {
        do {
            uint32_t rsc_msg_size = 0;

            if (r_info->replica->r_info1 != NULL)
                chunk_size_ret1 = read_file(r_info->replica->r_info1->r_idx_b[bs]->fd[FBP], r_info->zbuff, rw_off, block_size, r_info->replica->r_info1, t_handler->cid);
            else {
                rsc_msg_size = build_rsc_msg(r_info, 1, MSG_DATA_GET, rw_off, bs, block_size);
                chunk_size_ret1 = replica_data_fix_exchange(r_info, 1, rsc_msg_size, r_info->zbuff, cli_sock_timeout, t_handler);
            }

            if (chunk_size_ret1 > 0) {
                data_chk_r1 = memcmp(r_info->rbuff, r_info->zbuff, chunk_size_ret1);
                fprintf(stderr, "%s INFO [HEALTH=%015lu] Result %d of comparsion %s leader block=%d vs replica1, chunk_size_ret=%d\n", curr_dt(dt), t_handler->cid, data_chk_r1,
                        r_info->r_idx_b[bs]->fname, read_block, chunk_size_ret1);
                break;
            }
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Incorrect chunk_size_ret1=%d for %s (rsc_msg_size=%u), sleep 1s and repeat\n", curr_dt(dt), t_handler->cid, chunk_size_ret1,
                    r_info->r_idx_b[bs]->fname, rsc_msg_size);
            sleep(1);
        } while (true);
    }

    if (r_info->replica->replica_num > 1) {
        do {
            uint32_t rsc_msg_size = 0;

            if (r_info->replica->r_info2 != NULL)
                chunk_size_ret2 = read_file(r_info->replica->r_info2->r_idx_b[bs]->fd[FBP], r_info->bbuff, rw_off, block_size, r_info->replica->r_info2, t_handler->cid);
            else {
                rsc_msg_size = build_rsc_msg(r_info, 2, MSG_DATA_GET, rw_off, bs, block_size);
                chunk_size_ret2 = replica_data_fix_exchange(r_info, 2, rsc_msg_size, r_info->bbuff, cli_sock_timeout, t_handler);
            }

            if (chunk_size_ret2 > 0) {
                data_chk_r2 = memcmp(r_info->rbuff, r_info->bbuff, chunk_size_ret2);
                fprintf(stderr, "%s INFO [HEALTH=%015lu] Result %d of comparsion %s leader block=%d vs replica2, chunk_size_ret=%d\n", curr_dt(dt), t_handler->cid, data_chk_r2,
                        r_info->r_idx_b[bs]->fname, read_block, chunk_size_ret2);
                break;
            }
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Incorrect chunk_size_ret2=%d for %s (rsc_msg_size=%u), sleep 1s and repeat\n", curr_dt(dt), t_handler->cid, chunk_size_ret2,
                    r_info->r_idx_b[bs]->fname, rsc_msg_size);
            sleep(1);
        } while (true);
    }

    if (data_chk_r1 == 1 && chunk_size_ret1 > MIN_REC_SIZE) {
        data_quality_result_t quality = check_data_quality(r_info, bs, 0, chunk_size_ret1, r_info->zbuff, t_handler->cid);

        if (quality == DATA_QUALITY_ERROR) {
            r_info->stop = true;
            r_info->bad_data = true;
            set_region_recovery_info(r_info, 1, t_handler);
            finish_thread(r_info, 77, true, t_handler);
            return;
        }

        if (quality == DATA_QUALITY_OK) {
            write_file(r_info->r_idx_b[bs]->fd[FBP], rw_off, r_info->zbuff, chunk_size_ret1, bsize_factor, r_info, t_handler->cid);
            uint64_t file_size = get_file_size(r_info->r_idx_b[bs]->fname);
            uint8_t *buff_tmp = malloc(chunk_size_ret1);

            if (buff_tmp == NULL) {
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed malloc %u for data fix %s\n", curr_dt(dt), t_handler->cid, chunk_size_ret1, r_info->r_idx_b[bs]->fname);
                finish_thread(r_info, 78, true, t_handler);
            }
            memcpy(buff_tmp, r_info->zbuff, chunk_size_ret1);

            if (reload_index(r_info, bs, file_size, t_handler)) {
                memcpy(r_info->zbuff, buff_tmp, chunk_size_ret1);
                free(buff_tmp);

                if (r_info->replica->replica_num > 1) {
                    if (memcmp(r_info->zbuff, r_info->bbuff, chunk_size_ret1)) {
                        if (r_info->replica->r_info2 != NULL) {
                            fprintf(stderr, "%s INFO [HEALTH=%015lu] Data fix to local replica2 %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                            write_file(r_info->replica->r_info2->r_idx_b[bs]->fd[FBP], rw_off, r_info->zbuff, chunk_size_ret1, bsize_factor, r_info->replica->r_info2,
                                       t_handler->cid);
                        } else {
                            fprintf(stderr, "%s INFO [HEALTH=%015lu] Data fix to remote replica2 %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                            uint32_t rsc_msg_size = build_rsc_msg(r_info, 2, MSG_DATA_FIX, rw_off, bs, chunk_size_ret1);
                            int fix_result = replica_data_fix_exchange(r_info, 2, rsc_msg_size, r_info->zbuff, cli_sock_timeout, t_handler);

                            if (fix_result != 0) {
                                fprintf(stderr, "%s WARN [HEALTH=%015lu] Incorrect r2 fix_result=%d for %s (rsc_msg_size=%u)\n", curr_dt(dt), t_handler->cid, fix_result,
                                        r_info->r_idx_b[bs]->fname, rsc_msg_size);
                                finish_thread(r_info, 146, true, t_handler);
                            }
                        }
                    }
                }

                if (r_info->r_need_data_chk > 0) {
                    fprintf(stderr, "%s WARN [HEALTH=%015lu] Drop r_need_data_chk=%lu for %s\n", curr_dt(dt), t_handler->cid, r_info->r_need_data_chk,
                            r_info->r_idx_b[bs]->fname);
                    drop_region_recovery_info(r_info, t_handler->cid);
                }
                fprintf(stderr, "%s WARN [HEALTH=%015lu] Data fix from replica1 done in %s offset=%lu, client have to repeat try\n", curr_dt(dt), t_handler->cid,
                        r_info->r_idx_b[bs]->fname, rw_off);
                finish_thread(r_info, 77, true, t_handler);
            }
            free(buff_tmp);
            finish_thread(r_info, 77, true, t_handler);
            return;
        }
    }

    if (data_chk_r2 == 1 && chunk_size_ret2 > MIN_REC_SIZE) {
        data_quality_result_t quality = check_data_quality(r_info, bs, 0, chunk_size_ret2, r_info->bbuff, t_handler->cid);

        if (quality == DATA_QUALITY_ERROR) {
            r_info->stop = true;
            r_info->bad_data = true;
            set_region_recovery_info(r_info, 1, t_handler);
            finish_thread(r_info, 78, true, t_handler);
            return;
        }

        if (quality == DATA_QUALITY_OK) {
            write_file(r_info->r_idx_b[bs]->fd[FBP], rw_off, r_info->bbuff, chunk_size_ret2, bsize_factor, r_info, t_handler->cid);
            uint64_t file_size = get_file_size(r_info->r_idx_b[bs]->fname);

            if (reload_index(r_info, bs, file_size, t_handler)) {
                memcpy(r_info->zbuff, r_info->bbuff, chunk_size_ret2);

                if (r_info->replica->r_info1 != NULL) {
                    fprintf(stderr, "%s INFO [HEALTH=%015lu] Data fix to local replica1 %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                    write_file(r_info->replica->r_info1->r_idx_b[bs]->fd[FBP], rw_off, r_info->zbuff, chunk_size_ret2, bsize_factor, r_info->replica->r_info1, t_handler->cid);
                } else {
                    fprintf(stderr, "%s INFO [HEALTH=%015lu] Data fix to remote replica1 %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                    uint32_t rsc_msg_size = build_rsc_msg(r_info, 1, MSG_DATA_FIX, rw_off, bs, chunk_size_ret2);
                    int fix_result = replica_data_fix_exchange(r_info, 1, rsc_msg_size, r_info->zbuff, cli_sock_timeout, t_handler);

                    if (fix_result != 0) {
                        fprintf(stderr, "%s WARN [HEALTH=%015lu] Incorrect r1 fix_result=%d for %s (rsc_msg_size=%u)\n", curr_dt(dt), t_handler->cid, fix_result,
                                r_info->r_idx_b[bs]->fname, rsc_msg_size);
                        finish_thread(r_info, 147, true, t_handler);
                    }
                }
                fprintf(stderr, "%s WARN [HEALTH=%015lu] Data fix from replica2 done in %s offset=%lu, client have to repeat try\n", curr_dt(dt), t_handler->cid,
                        r_info->r_idx_b[bs]->fname, rw_off);

                if (r_info->r_need_data_chk > 0) {
                    fprintf(stderr, "%s WARN [HEALTH=%015lu] Drop r_need_data_chk=%lu for %s\n", curr_dt(dt), t_handler->cid, r_info->r_need_data_chk,
                            r_info->r_idx_b[bs]->fname);
                    drop_region_recovery_info(r_info, t_handler->cid);
                }
                finish_thread(r_info, 78, true, t_handler);
            }
            finish_thread(r_info, 78, true, t_handler);
            return;
        }
    }
    r_info->stop = true;
    r_info->bad_data = true;
    fprintf(stderr, "%s ERROR [HEALTH=%015lu] Data corruption detected in %s offset=%lu, cannot process data, region stopped. Sleep 60s to avoid DDoS\n", curr_dt(dt),
            t_handler->cid, r_info->r_idx_b[bs]->fname, rw_off + r_real_offset);
    sleep(60);
    finish_thread(r_info, 79, true, t_handler);
}
#if !ENTERPRISE
static bool ce_files_list_has_no_lob(const msg_files_list_t *list, uint64_t cid) {
    if (list == NULL || (list->files_count != 0 && list->fi == NULL))
        return false;

    for (uint32_t i = 0; i < list->files_count; ++i) {
        const msg_file_info_t *file = &list->fi[i];

        if (file->file_name != NULL && file->file_name_size == sizeof("lob.data") && memcmp(file->file_name, "lob.data", sizeof("lob.data")) == 0) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] CE cannot synchronize lob.data; use enterprise\n", curr_dt(dt), cid);
            return false;
        }
    }
    return true;
}
#endif
bool clone_missing_files(region_info_t *r_info, uint8_t replica_num, msg_files_list_t *files_list1, msg_files_list_t *files_list2, struct table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    uint64_t cid = t_handler->cid;
#if !ENTERPRISE
    if (!ce_files_list_has_no_lob(files_list1, cid) || !ce_files_list_has_no_lob(files_list2, cid))
        return false;
#endif
    msg_files_list_t *src_list = NULL, *dst_list = NULL;

    if (files_list1->files_count > files_list2->files_count) {
        src_list = files_list1;
        dst_list = files_list2;
    } else {
        src_list = files_list2;
        dst_list = files_list1;
    }
    fprintf(stderr, "%s INFO [HEALTH=%015lu] clone_missing_files: source files_count=%u, destination files_count=%u\n", curr_dt(dt), cid, src_list->files_count,
            dst_list->files_count);
    int replica_sock_fd = -1;

    if (replica_num == 1)
        replica_sock_fd = r_info->replica->replica1_sock_fd;
    else if (replica_num == 2)
        replica_sock_fd = r_info->replica->replica2_sock_fd;

    for (uint32_t i = 0; i < src_list->files_count; i++) {
        msg_file_info_t *src_file = &src_list->fi[i];
        bool file_found = false;

        for (uint32_t j = 0; j < dst_list->files_count; j++) {
            msg_file_info_t *dst_file = &dst_list->fi[j];

            if (src_file->file_name_size == dst_file->file_name_size &&
                strncmp((const char *)src_file->file_name, (const char *)dst_file->file_name, src_file->file_name_size) == 0) {
                file_found = true;
                break;
            }
        }

        if (!file_found) {
            fprintf(stderr, "%s INFO [HEALTH=%015lu] clone_missing_files: cloning missing file %s, size=%lu, ts=%lu\n", curr_dt(dt), cid, src_file->file_name,
                    src_file->file_size, src_file->file_ts);
            uint16_t bsize_factor = get_bsize_factor(src_file->file_name);
            int8_t bucket_idx = -1;

            for (int bs = 0; bs < r_info->max_idx_b; bs++) {
                if (compare_filenames(r_info->r_idx_b[bs]->fname, src_file->file_name)) {
                    bucket_idx = bs;
                    break;
                }
            }

            if (bucket_idx < 0) {
                {
                    uint16_t pos = bsize_factor * MAX_BSIZE_FACTOR / FIX_BLOCK_SIZE;
                    get_bs_map_pos(r_info, pos - 1, true, t_handler);

                    for (int bs = 0; bs < r_info->max_idx_b; bs++) {
                        if (compare_filenames(r_info->r_idx_b[bs]->fname, src_file->file_name)) {
                            bucket_idx = bs;
                            break;
                        }
                    }

                    if (bucket_idx < 0) {
                        fprintf(stderr, "%s ERROR [HEALTH=%015lu] clone_missing_files: cannot find BS %s, max_idx_b=%u\n", curr_dt(dt), cid, src_file->file_name,
                                r_info->max_idx_b);
                        return false;
                    }
                }
            }
            uint64_t file_size = src_file->file_size;
            uint64_t offset = 0;
            uint32_t chunk_size = BLOCK_MAX_SIZE;
            int received = 0;
            uint8_t *recv_buff = malloc(chunk_size);

            if (recv_buff == NULL) {
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] clone_missing_files: cannot allocate receive buffer for file %s\n", curr_dt(dt), cid, src_file->file_name);
                return false;
            }

            if (src_list == files_list1) {
                if (log_level > 0)
                    fprintf(stderr, "%s DEBUG [HEALTH=%015lu] Data fix from leader to replica%d %s\n", curr_dt(dt), t_handler->cid, replica_num, r_info->uuid);

                if (file_size == 0) {
                    if (replica_num == 1 && r_info->replica->r_info1 != NULL) {
                        size_t len1 = strlen(r_info->replica->r_info1->r_fname);
                        size_t len2 = strlen(src_file->file_name);
                        char fname[len1 + 1 + len2 + 1];
                        snprintf(fname, sizeof(fname), "%s/%s", r_info->replica->r_info1->r_fname, src_file->file_name);
                        open_file(fname, cid);
                    }

                    if (replica_num == 2 && r_info->replica->r_info2 != NULL) {
                        size_t len1 = strlen(r_info->replica->r_info2->r_fname);
                        size_t len2 = strlen(src_file->file_name);
                        char fname[len1 + 1 + len2 + 1];
                        snprintf(fname, sizeof(fname), "%s/%s", r_info->replica->r_info2->r_fname, src_file->file_name);
                        open_file(fname, cid);
                    }
                    uint32_t rsc_msg_size = build_rsc_msg(r_info, replica_num, MSG_DATA_FIX, 0, bucket_idx, 0);
                    int fix_result = replica_data_fix_exchange(r_info, replica_num, rsc_msg_size, recv_buff, cli_sock_timeout, t_handler);

                    if (fix_result != 0) {
                        fprintf(stderr, "%s ERROR [HEALTH=%015lu] clone_missing_files: failed to create empty file %s on replica%d, result=%d\n", curr_dt(dt), cid,
                                src_file->file_name, replica_num, fix_result);
                        free(recv_buff);
                        return false;
                    }
                }
                while (offset < file_size) {
                    if (offset + BLOCK_MAX_SIZE > file_size)
                        chunk_size = file_size - offset;
                    else
                        chunk_size = BLOCK_MAX_SIZE;
                    int read_size = read_file(r_info->r_idx_b[bucket_idx]->fd[FBP], r_info->zbuff, offset, chunk_size, r_info, cid);

                    if (read_size != (int)chunk_size) {
                        fprintf(stderr, "%s ERROR [HEALTH=%015lu] clone_missing_files: failed to read file %s at offset %lu, read=%d expected=%u\n", curr_dt(dt), cid,
                                src_file->file_name, offset, read_size, chunk_size);
                        free(recv_buff);
                        return false;
                    }
                    uint32_t rsc_msg_size = build_rsc_msg(r_info, replica_num, MSG_DATA_FIX, offset, bucket_idx, chunk_size);
                    int fix_result = replica_data_fix_exchange(r_info, replica_num, rsc_msg_size, recv_buff, cli_sock_timeout, t_handler);

                    if (fix_result != 0) {
                        fprintf(stderr, "%s ERROR [HEALTH=%015lu] clone_missing_files: failed to send file %s at offset %lu to replica%d, result=%d\n", curr_dt(dt), cid,
                                src_file->file_name, offset, replica_num, fix_result);
                        free(recv_buff);
                        return false;
                    }
                    offset += chunk_size;
                }
            } else {
                while (offset < file_size) {
                    if (offset + BLOCK_MAX_SIZE > file_size)
                        chunk_size = file_size - offset;
                    else
                        chunk_size = BLOCK_MAX_SIZE;
                    uint32_t rsc_msg_size = build_rsc_msg(r_info, replica_num, MSG_DATA_GET, offset, bucket_idx, chunk_size);
                    received = replica_data_fix_exchange(r_info, replica_num, rsc_msg_size, recv_buff, cli_sock_timeout, t_handler);

                    if (received <= 0 || received > (int)chunk_size) {
                        fprintf(stderr, "%s ERROR [HEALTH=%015lu] clone_missing_files: failed to receive data for file %s at offset %lu, received=%d expected=1..%u\n",
                                curr_dt(dt), cid, src_file->file_name, offset, received, chunk_size);
                        break;
                    }
                    write_file(r_info->r_idx_b[bucket_idx]->fd[FBP], offset, recv_buff, (uint32_t)received, bsize_factor, r_info, cid);
                    offset += (uint32_t)received;

                    if ((uint32_t)received < chunk_size)
                        break;
                }
            }

            if (offset != file_size) {
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] clone_missing_files: incomplete clone of file %s, copied=%lu expected=%lu\n", curr_dt(dt), cid, src_file->file_name,
                        offset, file_size);
                free(recv_buff);
                return false;
            }
            fprintf(stderr, "%s INFO [HEALTH=%015lu] clone_missing_files: successfully cloned file %s\n", curr_dt(dt), cid, src_file->file_name);
            free(recv_buff);
        }
    }
    return true;
}

bool reload_index(region_info_t *r_info, uint8_t bs, uint32_t file_size, struct table_handler_t *t_handler) {
    invalidate_record_gc(r_info);
    char dt[DT_LOG_SIZE];
    uint8_t b = FBP;
    keys_double_t **saved_keepers = r_info->k_dbl;
    uint8_t *saved_wal = r_info->wal_lob_buff;
    keys_double_t **keepers = calloc(BLOCK_MAX_SIZE_M, sizeof(*keepers));
    uint8_t *wal = NULL;
    unsigned keeper_count = r_info->max_idx_b > bs ? r_info->max_idx_b : (unsigned)bs + 1U;

    if (keeper_count > BLOCK_MAX_SIZE_M)
        goto preparation_failed;

    if (keepers == NULL)
        goto preparation_failed;
    region_info_t owner = {.k_dbl = keepers};

    for (unsigned i = 0; i < keeper_count; ++i)
        if (!create_double_keys_keeper(&owner, i))
            goto preparation_failed;
    r_info->k_dbl = keepers;
    r_info->wal_lob_buff = wal;
    memset(r_info->r_idx_b[bs]->idx[b], 0, r_info->r_idx_b[bs]->f_idx_size[b] * sizeof(struct idx_t));
    memset(r_info->r_idx_b[bs]->coll_keeper[b], 0, r_info->r_idx_b[bs]->coll_count_tot[b] * ((uint32_t)COLL_KEEPER_SIZE) * sizeof(uint8_t));

    if (r_info->r_idx_b[bs]->k1_index[b] != NULL)
        memset(r_info->r_idx_b[bs]->k1_index[b], 0, r_info->r_idx_b[bs]->f_idx_size[b] * sizeof(uint8_t));

    if (r_info->r_idx_b[bs]->k2_index[b] != NULL)
        memset(r_info->r_idx_b[bs]->k2_index[b], 0, r_info->r_idx_b[bs]->f_idx_size[b] * sizeof(uint8_t));
    r_info->r_idx_b[bs]->f_idx_curr_size[b] = 0;
    r_info->r_idx_b[bs]->coll_count_curr[b] = 0;
    r_info->r_idx_b[bs]->max_void_count = 0;
    r_info->r_idx_b[bs]->max_void_pos = 0;
    r_info->r_idx_b[bs]->count_max_idx_shift_x2 = 0;
    r_info->r_idx_b[bs]->count_max_idx_jump_x2 = 0;

    for (int v = 0; v < VOIDS; v++) {
        r_info->r_idx_b[bs]->void_info[b][v].void_count_tot = 0;
        r_info->r_idx_b[bs]->void_info[b][v].void_count_curr = 0;

        for (int i = 0; i < MAX_VOID_ELEMENTS; i++)
            r_info->r_idx_b[bs]->void_info[b][v].void_arr[i] = 0;
    }
    bool result_reload_idx = scan_index(r_info, b, file_size, bs, t_handler);

    if (result_reload_idx)
        result_reload_idx = finish_index_recovery(r_info, r_info->r_idx_b[bs]->block_size == BLOCK_MAX_SIZE, t_handler);

    for (unsigned i = 0; i < keeper_count; ++i) {
        free_double_keys_keeper(r_info, i);
    }
    free(keepers);
    free(wal);
    r_info->k_dbl = saved_keepers;
    r_info->wal_lob_buff = saved_wal;

    if (result_reload_idx)
        return true;
    goto failed;
preparation_failed:
    if (keepers != NULL) {
        region_info_t owner = {.k_dbl = keepers};

        for (unsigned i = 0; i < BLOCK_MAX_SIZE_M; ++i)
            free_double_keys_keeper(&owner, i);
    }
    free(keepers);
    free(wal);
failed:
    r_info->stop = true;
    r_info->bad_data = true;
    set_region_recovery_info(r_info, 1, t_handler);
    fprintf(stderr, "%s ERROR [HEALTH=%015lu] Index reload failed for %s, bs=%u\n", curr_dt(dt), t_handler->cid, r_info->uuid, bs);
    return false;
}

static bool file_info_name_valid(const msg_file_info_t *file_info) {
    if (file_info == NULL || file_info->file_name == NULL || file_info->file_name_size < 2)
        return false;

    if (file_info->file_name[file_info->file_name_size - 1] != 0)
        return false;
    return memchr(file_info->file_name, 0, file_info->file_name_size - 1) == NULL;
}

static bool find_unique_file_info(msg_files_list_t *files_list, const uint8_t *file_name, uint32_t file_name_size, msg_file_info_t **result) {
    if (result == NULL)
        return false;
    *result = NULL;

    if (files_list == NULL || file_name == NULL || file_name_size < 2 || file_name[file_name_size - 1] != 0 || memchr(file_name, 0, file_name_size - 1) != NULL)
        return false;

    if (files_list->files_count > 0 && files_list->fi == NULL)
        return false;

    for (uint32_t i = 0; i < files_list->files_count; i++) {
        msg_file_info_t *file_info = &files_list->fi[i];

        if (!file_info_name_valid(file_info))
            return false;

        if (file_info->file_name_size == file_name_size && memcmp(file_info->file_name, file_name, file_name_size) == 0) {
            if (*result != NULL)
                return false;
            *result = file_info;
        }
    }
    return *result != NULL;
}

bool compare_fix_files_lists(region_info_t *r_info, uint8_t replica_num, msg_files_list_t *files_list1, msg_files_list_t *files_list2, struct table_handler_t *t_handler) {
    uint64_t cid = t_handler->cid;
    char dt[DT_LOG_SIZE];

    if (!files_list1 || !files_list2) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Compare_files_lists vs replica%d: one struct is NULL\n", curr_dt(dt), cid, replica_num);
        return false;
    }

    if ((files_list1->files_count > 0 && files_list1->fi == NULL) || (files_list2->files_count > 0 && files_list2->fi == NULL)) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Compare_files_lists vs replica%d: file info array is NULL\n", curr_dt(dt), cid, replica_num);
        return false;
    }
#if !ENTERPRISE
    if (!ce_files_list_has_no_lob(files_list1, cid) || !ce_files_list_has_no_lob(files_list2, cid))
        return false;
#endif
    if (files_list1->files_count != files_list2->files_count) {
        fprintf(stderr, "%s WARN [HEALTH=%015lu] Compare_files_lists vs replica%d: different number of files, files_list1=%u, files_list2=%u\n", curr_dt(dt), cid, replica_num,
                files_list1->files_count, files_list2->files_count);

        if (!clone_missing_files(r_info, replica_num, files_list1, files_list2, t_handler))
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Compare_files_lists vs replica%d: missing file clone failed, recovery marker remains active\n", curr_dt(dt), cid,
                    replica_num);
        return false;
    }

    for (uint32_t i = 0; i < files_list1->files_count; i++) {
        msg_file_info_t *file1 = &files_list1->fi[i];
        msg_file_info_t *file1_unique = NULL;
        msg_file_info_t *file2 = NULL;

        if (!find_unique_file_info(files_list1, file1->file_name, file1->file_name_size, &file1_unique) || file1_unique != file1) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Compare_files_lists vs replica%d: invalid or duplicated leader filename at position=%u\n", curr_dt(dt), cid, replica_num,
                    i);
            return false;
        }

        if (!find_unique_file_info(files_list2, file1->file_name, file1->file_name_size, &file2)) {
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Compare_files_lists vs replica%d: file %s is missing, duplicated or invalid in replica list\n", curr_dt(dt), cid,
                    replica_num, file1->file_name);
            return false;
        }
        bool leader_changed_need_reload_index_this = false;
        bool leader_changed_need_reload_dict = false;
        uint8_t bs_fixed = -1;

        if (file1->file_size != file2->file_size) {
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Compare_files_lists vs replica%d: file size mismatch, file1=%s, size1=%lu, size2=%lu\n", curr_dt(dt), cid, replica_num,
                    file1->file_name, file1->file_size, file2->file_size);
        }

        if (file1->file_ts != file2->file_ts) {
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Compare_files_lists vs replica%d: modification time mismatch, file1=%s, ts1=%lu, ts2=%lu\n", curr_dt(dt), cid, replica_num,
                    file1->file_name, file1->file_ts, file2->file_ts);

            if (log_level > 1) {
                void *bt[1024];
                int bt_size = backtrace(bt, sizeof(bt) / sizeof(bt[0]));
                backtrace_symbols_fd(bt, bt_size, STDERR_FILENO);
            }
        }

        if (file1->num_blocks != file2->num_blocks) {
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Compare_files_lists vs replica%d: number of blocks mismatch, file1=%s, num_blocks1=%zu, num_blocks2=%zu\n", curr_dt(dt), cid,
                    replica_num, file1->file_name, file1->num_blocks, file2->num_blocks);
        }

        if (file1->num_blocks == 0 && file2->num_blocks > 0 && file2->msg_hsh_info[0]->data_state == false) {
            for (int bs = 0; bs < r_info->max_idx_b; bs++) {
                if (compare_filenames(r_info->r_idx_b[bs]->fname, file1->file_name)) {
                    uint8_t *recv_buff = malloc(FIX_BLOCK_SIZE);

                    if (r_info->replica->replica_num > 0) {
                        if (r_info->replica->r_info1 == NULL) {
                            if (log_level > 0)
                                fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix by truncate from leader to remote replica1 %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                            uint32_t rsc_msg_size = build_rsc_msg(r_info, 1, MSG_DIR_WRITE_TUNC, 0, bs, 0);

                            if (replica_data_fix_exchange(r_info, 1, rsc_msg_size, recv_buff, cli_sock_timeout, t_handler) == -1)
                                return false;
                        } else {
                            if (log_level > 0)
                                fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix by truncate from leader to local replica1 %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                            truncate_file(r_info->replica->r_info1, r_info->replica->r_info1->r_idx_b[bs]->fd[FBP], 0, t_handler->cid);
                        }
                    }

                    if (r_info->replica->replica_num > 1) {
                        if (r_info->replica->r_info2 == NULL) {
                            if (log_level > 0)
                                fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix by truncate from leader to remote replica2 %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                            uint32_t rsc_msg_size = build_rsc_msg(r_info, 2, MSG_DIR_WRITE_TUNC, 0, bs, 0);

                            if (replica_data_fix_exchange(r_info, 2, rsc_msg_size, recv_buff, cli_sock_timeout, t_handler) == -1)
                                return false;
                        } else {
                            if (log_level > 0)
                                fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix by truncate from leader to local replica2 %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                            truncate_file(r_info->replica->r_info2, r_info->replica->r_info2->r_idx_b[bs]->fd[FBP], 0, t_handler->cid);
                        }
                    }
                    free(recv_buff);
                    break;
                }
            }
        }
        bool leader_is_truth_for_this_file = true;

        for (size_t j = 0; j < file1->num_blocks; j++) {
            const msg_hsh_info_t *block1 = file1->msg_hsh_info[j];

            if (block1->xxh128 == NULL) {
                leader_is_truth_for_this_file = false;
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] Compare_files_lists: block1->xxh128 == NULL, replica%d %s file=%s, size=%lu, chunk=%lu\n", curr_dt(dt), cid,
                        replica_num, r_info->uuid, file1->file_name, file1->file_size, j);
                break; // А такое возможно? TODO выяснить
            }
            bool need_replicate = false;

            if (j >= file2->num_blocks) {
                fprintf(stderr, "%s WARN [HEALTH=%015lu] file1->num_blocks=%lu > file2->num_blocks=%lu, replica%d %s file=%s, fsize1=%lu, fsize2=%lu\n", curr_dt(dt), cid,
                        file1->num_blocks, file2->num_blocks, replica_num, r_info->uuid, file1->file_name, file1->file_size, file2->file_size);
                need_replicate = true; // Я забыл что это значит, TODO проверить этот кейс
            } else {
                const msg_hsh_info_t *block2 = file2->msg_hsh_info[j];

                if (block2 == NULL || block2->xxh128 == NULL) {
                    leader_is_truth_for_this_file = false;
                    fprintf(stderr, "%s ERROR [HEALTH=%015lu] Compare_files_lists: invalid replica%d hash metadata for %s file=%s, chunk=%lu\n", curr_dt(dt), cid, replica_num,
                            r_info->uuid, file1->file_name, j);
                    break;
                }
                need_replicate = memcmp(block1->xxh128, block2->xxh128, XXH128_SIZE);
                char sha_str1[XXH128_SIZE * 2 + 1] = {0};
                char sha_str2[XXH128_SIZE * 2 + 1] = {0};

                for (int k = 0; k < XXH128_SIZE; k++) {
                    snprintf(&sha_str1[k * 2], 3, "%02x", block1->xxh128[k]);
                    snprintf(&sha_str2[k * 2], 3, "%02x", block2->xxh128[k]);
                }
                sha_str1[XXH128_SIZE * 2] = '\0';
                sha_str2[XXH128_SIZE * 2] = '\0';

                if (need_replicate) {
                    fprintf(stderr, "%s WARN [HEALTH=%015lu] Cmp %s xxh128 file=%s, position=%lu, replica%d, local_xxh128=%s vs remote_xxh128=%s is not ok\n", curr_dt(dt), cid,
                            r_info->uuid, file1->file_name, block1->position, replica_num, sha_str1, sha_str2);
                } else if (log_level > 1)
                    fprintf(stderr, "%s WARN [HEALTH=%015lu] Cmp  %s xxh128 file=%s, position=%lu, replica%d, local_xxh128=%s vs remote_xxh128=%s is ok\n", curr_dt(dt), cid,
                            r_info->uuid, file1->file_name, block1->position, replica_num, sha_str1, sha_str2);

                if (!need_replicate && (!block1->data_state || !block2->data_state)) {
                    leader_is_truth_for_this_file = false;
                    fprintf(stderr,
                            "%s ERROR [HEALTH=%015lu] Structurally invalid identical data in leader and replica%d %s file=%s, chunk=%lu, leader_state=%u, replica_state=%u\n",
                            curr_dt(dt), cid, replica_num, r_info->uuid, file1->file_name, j, block1->data_state, block2->data_state);
                    break;
                }
            }

            if (need_replicate) {
                fprintf(stderr, "%s WARN [HEALTH=%015lu] Compare_files_lists: xxh128 mismatch, replica%d %s file=%s, size=%lu, ts=%lu, position=%lu\n", curr_dt(dt), cid,
                        replica_num, r_info->uuid, file1->file_name, file1->file_size, file1->file_ts, block1->position);

                for (int bs = 0; bs < r_info->max_idx_b; bs++) {
                    if (compare_filenames(r_info->r_idx_b[bs]->fname, file1->file_name)) {
                        bs_fixed = bs;
                        uint32_t chunk_size = BLOCK_MAX_SIZE;
                        uint64_t offset = block1->position * CHUNK_SIZE;
                        uint64_t total_fixed = 0;
                        uint8_t *recv_buff = malloc(chunk_size);
                        uint8_t r0_data_state = block1->data_state;
                        uint8_t r0_close_state = r_info->r_need_data_chk == 0;
                        fprintf(stderr, "%s INFO [HEALTH=%015lu] Data states for %s r0.data_state=%d, r0.close_state=%d, r0.file_ts=%lu\n", curr_dt(dt), t_handler->cid,
                                r_info->uuid, r0_data_state, r0_close_state, file1->file_ts);
                        uint8_t rr_data_state = 2;
                        uint8_t rr_close_state = files_list2->close_state;

                        if (j < file2->num_blocks)
                            rr_data_state = file2->msg_hsh_info[j]->data_state;
                        fprintf(stderr, "%s INFO [HEALTH=%015lu] Data states for %s rr.data_state=%d, rr.close_state=%d, rr.file_ts=%lu\n", curr_dt(dt), t_handler->cid,
                                r_info->uuid, rr_data_state, rr_close_state, file2->file_ts);
                        uint16_t bsize_factor = r_info->r_idx_b[bs]->bsize_factor;

                        if (r0_data_state == false) {
                            if (j >= file2->num_blocks) {
                                if (truncate_file(r_info, r_info->r_idx_b[bs]->fd[FBP], file2->file_size, t_handler->cid) < 0) {
                                    fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed truncate leader %s to size=%lu\n", curr_dt(dt), t_handler->cid, r_info->r_idx_b[bs]->fname,
                                            file2->file_size);
                                    free(recv_buff);
                                    return false;
                                }
                                free(recv_buff);
                                break;
                            }

                            if (file2->msg_hsh_info[j]->data_state == false) {
                                leader_is_truth_for_this_file = false;

                                if (r_info->replica->replica_num > 1)
                                    fprintf(stderr, "%s ERROR [HEALTH=%015lu] Bad leader data quality and bad from replica%d %s, skip it. The other replica will be checked\n",
                                            curr_dt(dt), cid, replica_num, r_info->r_idx_b[bs]->fname);
                                else
                                    fprintf(stderr, "%s ERROR [HEALTH=%015lu] Bad leader data quality and bad from replica%d %s, skip it\n", curr_dt(dt), cid, replica_num,
                                            r_info->r_idx_b[bs]->fname);
                                free(recv_buff);
                                break;
                            }
                        }

                        if (r0_data_state == true) {
                            if (r_info->stop == true) {
                                fprintf(stderr, "%s WARN [HEALTH=%015lu] Leader %s has stopped, cannot fix to replicas\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                                free(recv_buff);
                                break;
                            }
                            fprintf(stderr, "%s INFO [HEALTH=%015lu] Data fix from leader to replica(s) %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);

                            do {
                                if (offset + BLOCK_MAX_SIZE > file1->file_size)
                                    chunk_size = file1->file_size - offset;

                                if (log_level > 0)
                                    fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix from leader at offset=%lu len %u for %s\n", curr_dt(dt), t_handler->cid, offset,
                                            chunk_size, r_info->uuid);
                                read_file(r_info->r_idx_b[bs]->fd[FBP], r_info->zbuff, offset, chunk_size, r_info, cid);

                                if (r_info->replica->replica_num > 0) {
                                    if (r_info->replica->r_info1 == NULL) {
                                        if (log_level > 0)
                                            fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix from leader to remote replica1 %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                                        uint32_t rsc_msg_size = build_rsc_msg(r_info, 1, MSG_DATA_FIX, offset, bs, chunk_size);

                                        if (replica_data_fix_exchange(r_info, 1, rsc_msg_size, recv_buff, cli_sock_timeout, t_handler) == -1)
                                            return false;
                                    } else {
                                        if (log_level > 0)
                                            fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix from leader to local replica1 %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                                        write_file(r_info->replica->r_info1->r_idx_b[bs]->fd[FBP], offset, r_info->zbuff, chunk_size, bsize_factor, r_info->replica->r_info1,
                                                   cid);
                                    }
                                }

                                if (r_info->replica->replica_num > 1) {
                                    if (r_info->replica->r_info2 == NULL) {
                                        if (log_level > 0)
                                            fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix from leader to remote replica2 %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                                        uint32_t rsc_msg_size = build_rsc_msg(r_info, 2, MSG_DATA_FIX, offset, bs, chunk_size);

                                        if (replica_data_fix_exchange(r_info, 2, rsc_msg_size, recv_buff, cli_sock_timeout, t_handler) == -1)
                                            return false;
                                    } else {
                                        if (log_level > 0)
                                            fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix from leader to local replica2 %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                                        write_file(r_info->replica->r_info2->r_idx_b[bs]->fd[FBP], offset, r_info->zbuff, chunk_size, bsize_factor, r_info->replica->r_info2,
                                                   cid);
                                    }
                                }
                                total_fixed += chunk_size;
                                offset += chunk_size;
                            } while (offset < (block1->position + 1) * CHUNK_SIZE && offset < file1->file_size);

                            if (bs != BS_LOB && file2->file_size > file1->file_size) {
                                if ((replica_num == 1 && r_info->replica->r_info1 == NULL) || (replica_num == 2 && r_info->replica->r_info2 == NULL)) {
                                    if (log_level > 0)
                                        fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix by truncate remote replica%d %s to size=%lu\n", curr_dt(dt), t_handler->cid,
                                                replica_num, r_info->uuid, file1->file_size);
                                    uint32_t rsc_msg_size = build_rsc_msg(r_info, replica_num, MSG_DIR_WRITE_TUNC, file1->file_size, bs, 0);

                                    if (replica_data_fix_exchange(r_info, replica_num, rsc_msg_size, recv_buff, cli_sock_timeout, t_handler) == -1)
                                        return false;
                                } else {
                                    int trc = 0;

                                    if (replica_num == 1) {
                                        if (log_level > 0)
                                            fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix by truncate local replica1 %s to size=%lu\n", curr_dt(dt), t_handler->cid,
                                                    r_info->uuid, file1->file_size);
                                        trc = truncate_file(r_info->replica->r_info1, r_info->replica->r_info1->r_idx_b[bs]->fd[FBP], file1->file_size, t_handler->cid);
                                    } else if (replica_num == 2) {
                                        if (log_level > 0)
                                            fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix by truncate local replica2 %s to size=%lu\n", curr_dt(dt), t_handler->cid,
                                                    r_info->uuid, file1->file_size);
                                        trc = truncate_file(r_info->replica->r_info2, r_info->replica->r_info2->r_idx_b[bs]->fd[FBP], file1->file_size, t_handler->cid);
                                    }

                                    if (trc != 0) {
                                        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed ftruncate replica%d %s fix to size=%lu\n", curr_dt(dt), cid, replica_num,
                                                r_info->r_idx_b[bs]->fname, file1->file_size);
                                        free(recv_buff);
                                        return false;
                                    }
                                }
                            }
                            fprintf(stderr, "%s INFO [HEALTH=%015lu] Data fix to replica(s) %s success, total_fixed=%lu, from %lu\n", curr_dt(dt), cid,
                                    r_info->r_idx_b[bs]->fname, total_fixed, block1->position * CHUNK_SIZE);
                        } else {
                            fprintf(stderr, "%s INFO [HEALTH=%015lu] Data fix to leader from replica%d %s\n", curr_dt(dt), t_handler->cid, replica_num, r_info->uuid);
                            uint16_t bsize_factor = get_bsize_factor(file1->file_name);
                            uint32_t chunk_size_ret = 0;

                            do {
                                chunk_size = BLOCK_MAX_SIZE;

                                if (offset + BLOCK_MAX_SIZE > file2->file_size)
                                    chunk_size = file2->file_size - offset;
                                uint32_t rsc_msg_size;

                                if ((replica_num == 1 && r_info->replica->r_info1 == NULL) || (replica_num == 2 && r_info->replica->r_info2 == NULL)) {
                                    if (log_level > 0)
                                        fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix to leader from remote replica%d %s\n", curr_dt(dt), t_handler->cid, replica_num,
                                                r_info->uuid);
                                    rsc_msg_size = build_rsc_msg(r_info, replica_num, MSG_DATA_GET, offset, bs, chunk_size);
                                    chunk_size_ret = replica_data_fix_exchange(r_info, replica_num, rsc_msg_size, r_info->zbuff, cli_sock_timeout, t_handler);

                                    if (chunk_size_ret == -1)
                                        return false;
                                } else {
                                    if (log_level > 0)
                                        fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix to leader from local replica%d %s\n", curr_dt(dt), t_handler->cid, replica_num,
                                                r_info->uuid);

                                    if (replica_num == 1)
                                        read_file(r_info->replica->r_info1->r_idx_b[bs]->fd[FBP], r_info->zbuff, offset, chunk_size, r_info->replica->r_info1, cid);

                                    if (replica_num == 2)
                                        read_file(r_info->replica->r_info2->r_idx_b[bs]->fd[FBP], r_info->zbuff, offset, chunk_size, r_info->replica->r_info2, cid);
                                    chunk_size_ret = chunk_size;
                                }

                                if (replica_num == 1 && r_info->replica->replica_num > 1) {
                                    if (r_info->replica->r_info2 != NULL) {
                                        if (log_level > 0)
                                            fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix from replica1 to local replica2 %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                                        write_file(r_info->replica->r_info2->r_idx_b[bs]->fd[FBP], offset, r_info->zbuff, chunk_size_ret, bsize_factor, r_info->replica->r_info2,
                                                   cid);
                                    } else {
                                        if (log_level > 0)
                                            fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix from replica1 to remote replica2 %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                                        uint32_t rsc_msg_size = build_rsc_msg(r_info, 2, MSG_DATA_FIX, offset, bs, chunk_size_ret);

                                        if (replica_data_fix_exchange(r_info, 2, rsc_msg_size, recv_buff, cli_sock_timeout, t_handler) == -1)
                                            return false;
                                    }
                                }

                                if (replica_num == 2 && r_info->replica->replica_num > 0) {
                                    if (r_info->replica->r_info1 != NULL) {
                                        if (log_level > 0)
                                            fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix from replica2 to local replica1 %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                                        write_file(r_info->replica->r_info1->r_idx_b[bs]->fd[FBP], offset, r_info->zbuff, chunk_size_ret, bsize_factor, r_info->replica->r_info1,
                                                   cid);
                                    } else {
                                        if (log_level > 0)
                                            fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix from replica2 to remote replica1 %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
                                        uint32_t rsc_msg_size = build_rsc_msg(r_info, 1, MSG_DATA_FIX, offset, bs, chunk_size_ret);

                                        if (replica_data_fix_exchange(r_info, 1, rsc_msg_size, recv_buff, cli_sock_timeout, t_handler) == -1)
                                            return false;
                                    }
                                }

                                if (log_level > 0)
                                    fprintf(stderr, "%s TRACE [HEALTH=%015lu] Data fix to leader at offset=%lu, chunk_size=%u chunk_size_ret=%u for %s\n", curr_dt(dt),
                                            t_handler->cid, offset, chunk_size, chunk_size_ret, r_info->uuid);
                                // Лидер пишет данные себе TODO fsync?
                                write_file(r_info->r_idx_b[bs]->fd[FBP], offset, r_info->zbuff, chunk_size_ret, bsize_factor, r_info, cid);
                                leader_changed_need_reload_index_this = true;

                                if (offset == 0)
                                    leader_changed_need_reload_dict = true;
                                total_fixed += chunk_size_ret;
                                offset += chunk_size_ret;

                                if (chunk_size_ret < chunk_size) {
                                    break;
                                }
                            } while (offset < (block1->position + 1) * CHUNK_SIZE && offset < file2->file_size);

                            if (file1->file_size != file2->file_size) {
                                if (truncate_file(r_info, r_info->r_idx_b[bs]->fd[FBP], file2->file_size, t_handler->cid) != 0) {
                                    fprintf(stderr, "%s ERROR [HEALTH=%015lu] ftruncate failed for %s to size=%lu\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fname,
                                            file2->file_size);
                                    return false;
                                }
                            }
                            fprintf(stderr, "%s INFO [HEALTH=%015lu] Data fix from replica%d %s success, total_fixed=%lu, offset=%lu, from %lu\n", curr_dt(dt), cid, replica_num,
                                    r_info->r_idx_b[bs]->fname, total_fixed, offset, block1->position * CHUNK_SIZE);
                        }
                        free(recv_buff);
                        break;
                    }
                }
            }
        }

        if (leader_is_truth_for_this_file == true && file2->num_blocks > file1->num_blocks && file2->file_size > file1->file_size) {
            int bs_extra = -1;

            for (int i = 0; i < r_info->max_idx_b; i++) {
                if (compare_filenames(r_info->r_idx_b[i]->fname, file1->file_name)) {
                    bs_extra = i;
                    break;
                }
            }

            if (bs_extra >= 0) {
                fprintf(stderr,
                        "%s WARN [HEALTH=%015lu] Replica%d %s file=%s has extra full block(s): "
                        "leader_blocks=%lu replica_blocks=%lu, leader_size=%lu replica_size=%lu. Truncating replica\n",
                        curr_dt(dt), cid, replica_num, r_info->uuid, file1->file_name, file1->num_blocks, file2->num_blocks, file1->file_size, file2->file_size);
                uint8_t *recv_buff_trunc = malloc(FIX_BLOCK_SIZE);

                if (recv_buff_trunc == NULL) {
                    fprintf(stderr, "%s ERROR [HEALTH=%015lu] malloc failed for truncate buffer, replica%d %s file=%s\n", curr_dt(dt), cid, replica_num, r_info->uuid,
                            file1->file_name);
                    return false;
                }

                if ((replica_num == 1 && r_info->replica->r_info1 == NULL) || (replica_num == 2 && r_info->replica->r_info2 == NULL)) {
                    if (log_level > 0)
                        fprintf(stderr, "%s TRACE [HEALTH=%015lu] Post-loop truncate remote replica%d %s file=%s to size=%lu\n", curr_dt(dt), t_handler->cid, replica_num,
                                r_info->uuid, file1->file_name, file1->file_size);
                    uint32_t rsc_msg_size = build_rsc_msg(r_info, replica_num, MSG_DIR_WRITE_TUNC, file1->file_size, bs_extra, 0);

                    if (replica_data_fix_exchange(r_info, replica_num, rsc_msg_size, recv_buff_trunc, cli_sock_timeout, t_handler) == -1) {
                        free(recv_buff_trunc);
                        return false;
                    }
                } else {
                    int trc = -1;

                    if (replica_num == 1) {
                        if (log_level > 0)
                            fprintf(stderr, "%s TRACE [HEALTH=%015lu] Post-loop truncate local replica1 %s file=%s to size=%lu\n", curr_dt(dt), t_handler->cid, r_info->uuid,
                                    file1->file_name, file1->file_size);
                        trc = truncate_file(r_info->replica->r_info1, r_info->replica->r_info1->r_idx_b[bs_extra]->fd[FBP], file1->file_size, t_handler->cid);
                    } else if (replica_num == 2) {
                        if (log_level > 0)
                            fprintf(stderr, "%s TRACE [HEALTH=%015lu] Post-loop truncate local replica2 %s file=%s to size=%lu\n", curr_dt(dt), t_handler->cid, r_info->uuid,
                                    file1->file_name, file1->file_size);
                        trc = truncate_file(r_info->replica->r_info2, r_info->replica->r_info2->r_idx_b[bs_extra]->fd[FBP], file1->file_size, t_handler->cid);
                    }

                    if (trc != 0) {
                        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Post-loop ftruncate failed, replica%d %s file=%s to size=%lu\n", curr_dt(dt), cid, replica_num, r_info->uuid,
                                file1->file_name, file1->file_size);
                        free(recv_buff_trunc);
                        return false;
                    }
                }
                free(recv_buff_trunc);
            } else {
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] Post-loop truncate skipped: bs not found for replica%d %s file=%s\n", curr_dt(dt), cid, replica_num, r_info->uuid,
                        file1->file_name);
                return false;
            }
        }
        {
            if (leader_changed_need_reload_index_this == true && bs_fixed != -1) {
                if (file1->file_size > file2->file_size) {
                    truncate_file(r_info, r_info->r_idx_b[bs_fixed]->fd[FBP], file2->file_size, t_handler->cid);
                    file1->file_size = file2->file_size;
                }

                if (file1->file_size > r_info->r_idx_b[bs_fixed]->block_size && !reload_index(r_info, bs_fixed, file1->file_size, t_handler))
                    return false;

                if (r_info->enable_dict == 1 && leader_changed_need_reload_dict == true) {
                    if (!load_dict(r_info, t_handler))
                        return false;
                }
            }
        }

        if (!leader_is_truth_for_this_file) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Data check cannot select a valid source for replica%d %s file=%s\n", curr_dt(dt), cid, replica_num, r_info->uuid,
                    file1->file_name);
            return false;
        }
    }
    fprintf(stderr, "%s TRACE [HEALTH=%015lu] Compare has done\n", curr_dt(dt), cid);
    return true;
}

bool check_replica(struct region_info_t *r_info, int replica_num, struct table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];

    if (r_info == NULL || t_handler == NULL || r_info->replica == NULL)
        return false;
    uint64_t cid = t_handler->cid;
    msg_files_list_t *files_list = r_info->files_list;

    if (files_list == NULL) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] File list is empty %s, replica%d check failed\n", curr_dt(dt), cid, r_info->uuid, replica_num);
        return false;
    }

    if (replica_num != 1 && replica_num != 2) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Incorrect replica_num=%d for %s data check\n", curr_dt(dt), cid, replica_num, r_info->uuid);
        return false;
    }
    print_files_list(files_list, r_info->uuid, t_handler->cid);
    uint32_t serialized_size = 0;
    uint8_t *serialized_data = serialize_files_list(files_list, &serialized_size, t_handler->cid);
    bool result_data_check_and_fix;
    bool result = false;
    const uint32_t max_attempts = 3U;

    for (uint32_t attempt = 1; attempt <= max_attempts; attempt++) {
        if (replica_num == 1) {
            if (r_info->replica->r_info1 != NULL) {
                msg_files_list_t *files_list_replica = get_files_list(r_info->replica->r_info1, true, t_handler);

                if (files_list_replica == NULL) {
                    fprintf(stderr, "%s ERROR [HEALTH=%015lu] Cannot create local replica1 files list for %s\n", curr_dt(dt), cid, r_info->uuid);
                    break;
                }
                print_files_list(files_list, r_info->uuid, t_handler->cid);
                result_data_check_and_fix = compare_fix_files_lists(r_info, replica_num, files_list, files_list_replica, t_handler);
                free_files_list(files_list_replica);

                if (!result_data_check_and_fix)
                    continue;
            } else if (r_info->replica->replica1_port > 0) {
                if (r_info->replica->replica1_sock_fd == -1)
                    if (!refresh_data_check_replica(r_info, replica_num, t_handler))
                        continue;

                if (check_remote_replica(r_info, 1, files_list, t_handler) != true)
                    continue;
            } else {
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] Replica1 is required but neither local replica nor remote port exists for %s\n", curr_dt(dt), cid, r_info->uuid);
                break;
            }
            result = true;
            break;
        } else if (replica_num == 2) {
            if (r_info->replica->r_info2 != NULL) {
                msg_files_list_t *files_list_replica = get_files_list(r_info->replica->r_info2, true, t_handler);

                if (files_list_replica == NULL) {
                    fprintf(stderr, "%s ERROR [HEALTH=%015lu] Cannot create local replica2 files list for %s\n", curr_dt(dt), cid, r_info->uuid);
                    break;
                }
                print_files_list(files_list, r_info->uuid, t_handler->cid);
                result_data_check_and_fix = compare_fix_files_lists(r_info, replica_num, files_list, files_list_replica, t_handler);
                free_files_list(files_list_replica);

                if (!result_data_check_and_fix)
                    continue;
            } else if (r_info->replica->replica2_port > 0) {
                if (r_info->replica->replica2_sock_fd == -1)
                    if (!refresh_data_check_replica(r_info, replica_num, t_handler))
                        continue;

                if (check_remote_replica(r_info, 2, files_list, t_handler) != true)
                    continue;
            } else {
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] Replica2 is required but neither local replica nor remote port exists for %s\n", curr_dt(dt), cid, r_info->uuid);
                break;
            }
            result = true;
            break;
        }
    }

    if (!result) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Data check failed for replica%d %s after %u attempts\n", curr_dt(dt), cid, replica_num, r_info->uuid, max_attempts);
    }
    free(serialized_data);
    return result;
}


static uint64_t add_region_file_size(uint64_t total, uint64_t size) {
    return size > UINT64_MAX - total ? UINT64_MAX : total + size;
}

static uint32_t data_check_timeout_for_size(uint64_t bytes) {
    uint64_t gib = bytes / (UINT64_C(1024) * 1024 * 1024);

    if (gib == 0)
        gib = 1;
    int configured = atomic_load(&replica_sock_timeout);
    uint32_t base = configured > 0 ? (uint32_t)configured : 1U;
    return gib > INT_MAX / base ? INT_MAX : (uint32_t)(gib * base);
}


static uint64_t files_list_total_size(const msg_files_list_t *files_list) {
    uint64_t total = 0;

    for (uint32_t f = 0; f < files_list->files_count; ++f)
        total = add_region_file_size(total, files_list->fi[f].file_size);
    return total;
}

static bool is_data_file_name(const char *file_name);
static uint64_t replica_check_region_size(region_info_t *r_info, uint64_t cid) {
    DIR *dir = opendir(r_info->r_fname);
    uint64_t total = 0;
    int error = dir == NULL ? errno : 0;

    if (dir != NULL) {
        for (;;) {
            errno = 0;
            struct dirent *entry = readdir(dir);

            if (entry == NULL) {
                if (errno != 0)
                    error = errno;
                break;
            }

            if (!is_data_file_name(entry->d_name))
                continue;
            struct stat st;

            if (fstatat(dirfd(dir), entry->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
                error = errno;
                continue;
            }

            if (S_ISREG(st.st_mode) && st.st_size > 0)
                total = add_region_file_size(total, (uint64_t)st.st_size);
        }
        closedir(dir);
    }

    if (error != 0) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s WARN [HEALTH=%015lu] Incomplete region size for retry timeout %s: %s; timeout still grows\n", curr_dt(dt), cid, r_info->uuid, strerror(error));
    }
    return total;
}


static bool set_replica_receive_timeout(int fd, uint32_t seconds, uint64_t cid) {
    struct timeval tv = {.tv_sec = seconds, .tv_usec = 0};

    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == 0)
        return true;
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s WARN [HEALTH=%015lu] Cannot set replica receive timeout fd=%d seconds=%u: %s\n", curr_dt(dt), cid, fd, seconds, strerror(errno));
    return false;
}

bool apply_replica_reply_timeout(region_info_t *r_info, uint8_t replica_num, int fd, uint64_t cid) {
    uint32_t seconds = r_info->replica->reply_retry_timeout[replica_num - 1];
    return seconds == 0 || set_replica_receive_timeout(fd, seconds, cid);
}

void grow_replica_reply_timeout(region_info_t *r_info, uint8_t replica_num, int fd, uint64_t cid) {
    uint32_t *saved = &r_info->replica->reply_retry_timeout[replica_num - 1];
    int configured = atomic_load(&cli_sock_timeout);
    uint64_t previous = configured > 0 ? (uint32_t)configured : 1U;

    if (*saved > previous)
        previous = *saved;
    struct timeval tv;
    socklen_t len = sizeof(tv);

    if (getsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, &len) == 0 && tv.tv_sec > 0 && (uint64_t)tv.tv_sec > previous)
        previous = (uint64_t)tv.tv_sec;
    uint64_t bytes = replica_check_region_size(r_info, cid);
    uint32_t next = previous > INT_MAX / 2U ? INT_MAX : (uint32_t)(previous * 2);
    uint32_t size_timeout = data_check_timeout_for_size(bytes);

    if (next < size_timeout)
        next = size_timeout;
    *saved = next;
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s WARN [HEALTH=%015lu] Replica reply timeout %s replica%u: previous=%lu sec, next=%u sec, region_bytes=%lu\n", curr_dt(dt), cid, r_info->uuid, replica_num,
            previous, next, bytes);
}

void reset_replica_reply_timeout(region_info_t *r_info, uint8_t replica_num, uint64_t cid) {
    uint32_t *saved = &r_info->replica->reply_retry_timeout[replica_num - 1];

    if (*saved == 0)
        return;
    int fd = replica_num == 1 ? r_info->replica->replica1_sock_fd : r_info->replica->replica2_sock_fd;
    bool local = replica_num == 1 ? r_info->replica->r_info1 != NULL : r_info->replica->r_info2 != NULL;

    if (local || fd < 0 || set_replica_receive_timeout(fd, (uint32_t)atomic_load(&cli_sock_timeout), cid))
        *saved = 0;
}

bool check_remote_replica(struct region_info_t *r_info, uint8_t replica_num, msg_files_list_t *files_list, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    uint64_t cid = t_handler->cid;
    uint32_t rsc_msg_size = build_rsc_msg(r_info, replica_num, MSG_DATA_CHK, 0, 0, 0);
    bool result_data_check_and_fix;
    uint64_t total_region_size = files_list_total_size(files_list);
    uint32_t rtimeo = data_check_timeout_for_size(total_region_size);
    fprintf(stderr, "%s INFO [HEALTH=%015lu] Data check waiting response from %s replica%d will be %u sec\n", curr_dt(dt), cid, r_info->uuid, replica_num, rtimeo);
    memset(r_info->zbuff, 0, MSG_SIZE_RESERVE_RS_ANSWER);
    int received = replica_data_fix_exchange(r_info, replica_num, rsc_msg_size, r_info->zbuff + MSG_SIZE_RESERVE_RS_ANSWER, rtimeo, t_handler);

    if (received <= 0) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Data check %s failed for replica%d\n", curr_dt(dt), cid, r_info->uuid, replica_num);
        return false;
    }
    uint32_t rsize = (uint32_t)received + MSG_SIZE_RESERVE_RS_ANSWER;
    msg_files_list_t *files_list_replica = deserialize_files_list(r_info->zbuff, rsize, cid);

    if (files_list_replica == NULL) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed deserialize_files_list %d bytes from replica%d\n", curr_dt(dt), cid, rsize, replica_num);
        return false;
    }
    result_data_check_and_fix = compare_fix_files_lists(r_info, replica_num, files_list, files_list_replica, t_handler);
    free_files_list(files_list_replica);
    return result_data_check_and_fix;
}

uint64_t calc_num_blocks(long file_size) {
    if (file_size <= 0) {
        return 0;
    }
    return (uint64_t)((file_size + CHUNK_SIZE - 1) / CHUNK_SIZE);
}

uint16_t get_bsize_factor(const char *filename) {
    if (strcmp(filename, "lob.data") == 0)
        return 256;
    int num;

    if (sscanf(filename, "%3d.data", &num) == 1) {
        if (num >= 0 && num <= 15) {
            return (uint16_t)(16 * (num + 1));
        }
    }
    return 0;
}


static void free_file_hash_info(msg_file_info_t *file_info) {
    if (file_info == NULL)
        return;

    if (file_info->msg_hsh_info != NULL) {
        for (uint64_t i = 0; i < file_info->num_blocks; i++) {
            if (file_info->msg_hsh_info[i] != NULL) {
                free(file_info->msg_hsh_info[i]->xxh128);
                free(file_info->msg_hsh_info[i]);
            }
        }
        free(file_info->msg_hsh_info);
    }
    file_info->msg_hsh_info = NULL;
    file_info->num_blocks = 0;
}

bool calculate_xxh128_for_file(struct region_info_t *r_info, bool calc_xxh, const char *filename, msg_file_info_t *file_info, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    FILE *fp = fopen(filename, "rb");

    if (!fp) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed to open file %s: %s\n", curr_dt(dt), cid, filename, strerror(errno));
        return false;
    }
    int8_t bucket_idx = -1;

    for (int bs = 0; bs < r_info->max_idx_b; bs++) {
        if (compare_filenames(r_info->r_idx_b[bs]->fname, file_info->file_name)) {
            bucket_idx = bs;
            break;
        }
    }
    uint64_t num_blocks = (file_info->file_size + CHUNK_SIZE - 1) / CHUNK_SIZE;

    if (num_blocks == 0 || num_blocks > SIZE_MAX / sizeof(*file_info->msg_hsh_info)) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Invalid number of chunks %lu for file %s\n", curr_dt(dt), cid, num_blocks, filename);
        fclose(fp);
        return false;
    }
    file_info->num_blocks = num_blocks;
    file_info->msg_hsh_info = (msg_hsh_info_t **)calloc(num_blocks, sizeof(*file_info->msg_hsh_info));

    if (!file_info->msg_hsh_info) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Memory allocation error (msg_hsh_info)\n", curr_dt(dt), cid);
        fclose(fp);
        file_info->num_blocks = 0;
        return false;
    }
    unsigned char *buffer = (unsigned char *)malloc(CHUNK_SIZE);

    if (!buffer) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Memory allocation error (buffer)\n", curr_dt(dt), cid);
        free_file_hash_info(file_info);
        fclose(fp);
        return false;
    }
    fprintf(stderr, "%s INFO [HEALTH=%015lu] Found %lu chunks in %s, bs=%d, calc_xxh=%d\n", curr_dt(dt), cid, num_blocks, filename, bucket_idx, calc_xxh);

    for (uint64_t i = 0; i < num_blocks; i++) {
        msg_hsh_info_t *hsh_info = (msg_hsh_info_t *)calloc(1, sizeof(msg_hsh_info_t));

        if (!hsh_info) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Memory allocation error (hsh_info)\n", curr_dt(dt), cid);
            free(buffer);
            free_file_hash_info(file_info);
            fclose(fp);
            return false;
        }
        hsh_info->xxh128 = (uint8_t *)malloc(XXH128_SIZE);

        if (!hsh_info->xxh128) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Memory allocation error (xxh128)\n", curr_dt(dt), cid);
            free(hsh_info);
            free(buffer);
            free_file_hash_info(file_info);
            fclose(fp);
            return false;
        }
        file_info->msg_hsh_info[i] = hsh_info;

        if (calc_xxh) {
            uint64_t current_pos = i * CHUNK_SIZE;
            uint64_t bytes_left = file_info->file_size - current_pos;
            size_t bytes_to_read = bytes_left < CHUNK_SIZE ? (size_t)bytes_left : CHUNK_SIZE;
            size_t chunk_size_read = fread(buffer, 1, bytes_to_read, fp);

            if (chunk_size_read != bytes_to_read) {
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] File %s block chunk_size_read %zu vs %zu error: %s\n", curr_dt(dt), cid, filename, chunk_size_read, bytes_to_read,
                        ferror(fp) ? strerror(errno) : "unexpected EOF");
                free(buffer);
                free_file_hash_info(file_info);
                fclose(fp);
                return false;
            }
            hsh_info->data_state = true;

            if (bucket_idx > 0) {
                if (log_level > 0)
                    fprintf(stderr, "%s DEBUG [HEALTH=%015lu] Replica %s data check begin chunk %lu, chunk_size_read=%zu\n", curr_dt(dt), cid, r_info->r_idx_b[bucket_idx]->fname,
                            i, chunk_size_read);
                uint32_t meta_size = 0;

                if (i == 0) {
                    meta_size = r_info->r_idx_b[bucket_idx]->block_size;

                    if (chunk_size_read < meta_size || check_meta_quality(buffer, meta_size, meta_size, cid) == false)
                        hsh_info->data_state = false;
                }

                if (hsh_info->data_state) {
                    data_quality_result_t quality = check_data_quality(r_info, bucket_idx, 0, chunk_size_read - meta_size, buffer + meta_size, cid);

                    if (quality == DATA_QUALITY_ERROR) {
                        free(buffer);
                        free_file_hash_info(file_info);
                        fclose(fp);
                        return false;
                    }
                    hsh_info->data_state = quality == DATA_QUALITY_OK;
                }
            } else if (log_level > 0)
                fprintf(stderr, "%s DEBUG [HEALTH=%015lu] Replica %s data hash caluclate begin chunk %lu, chunk_size_read=%zu\n", curr_dt(dt), cid, filename, i, chunk_size_read);
            XXH128_hash_t hash = XXH3_128bits(buffer, chunk_size_read);
            memcpy(hsh_info->xxh128, &hash, XXH128_SIZE);
        } else {
            hsh_info->data_state = false;
            memset(hsh_info->xxh128, 0, XXH128_SIZE);
        }
        hsh_info->position = i;
    }
    free(buffer);

    if (fclose(fp) != 0) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed to close file %s: %s\n", curr_dt(dt), cid, filename, strerror(errno));
        free_file_hash_info(file_info);
        return false;
    }
    return true;
}

static bool is_data_file_name(const char *file_name) {
    if (file_name == NULL)
        return false;
    const char *ext = strrchr(file_name, '.');
    return ext != NULL && strcmp(ext, ".data") == 0;
}


msg_files_list_t *get_files_list(struct region_info_t *r_info, bool calc_xxh, struct table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    DIR *dir = NULL;
    struct dirent *entry;
    struct stat file_stat;
    uint64_t cid = t_handler->cid;
    const char *directory = r_info->r_fname;
#if !ENTERPRISE
    if (!ce_region_has_no_lob(directory, cid))
        return NULL;
#endif
    flush_write_4K_buffer(r_info, cid);
    fprintf(stderr, "%s INFO [HEALTH=%015lu] Creating get_files_list %s\n", curr_dt(dt), cid, r_info->uuid);

    if (log_level > 1) {
        void *bt[1024];
        int bt_size = backtrace(bt, sizeof(bt) / sizeof(bt[0]));
        backtrace_symbols_fd(bt, bt_size, STDERR_FILENO);
    }
    msg_files_list_t *files_list = (msg_files_list_t *)calloc(1, sizeof(msg_files_list_t));

    if (!files_list) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Memory allocation error (files_list)\n", curr_dt(dt), cid);
        return NULL;
    }
    files_list->msg_size = 0;
    files_list->type = MSG_DATA_CHK;
    files_list->close_state = r_info->r_need_data_chk == 0;
    files_list->files_count = 0;
    files_list->fi = NULL;
    dir = opendir(directory);

    if (!dir) {
        if (log_level > 1)
            fprintf(stderr, "%s INFO [HEALTH=%015lu] Directory %s is not exist or open error: %s\n", curr_dt(dt), cid, directory, strerror(errno));
        free(files_list);
        return NULL;
    }

    for (;;) {
        errno = 0;
        entry = readdir(dir);

        if (entry == NULL) {
            if (errno != 0) {
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed to read directory %s: %s\n", curr_dt(dt), cid, directory, strerror(errno));
                goto error_cleanup;
            }
            break;
        }

        if (!is_data_file_name(entry->d_name))
            continue;
#if !ENTERPRISE
        if (strcmp(entry->d_name, "lob.data") == 0)
            continue;
#endif
        if (fstatat(dirfd(dir), entry->d_name, &file_stat, AT_SYMLINK_NOFOLLOW) != 0) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Stat error for file %s/%s: %s\n", curr_dt(dt), cid, directory, entry->d_name, strerror(errno));
            goto error_cleanup;
        }

        if (!S_ISREG(file_stat.st_mode))
            continue;

        if (files_list->files_count == UINT32_MAX) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Too many *.data files in directory %s\n", curr_dt(dt), cid, directory);
            goto error_cleanup;
        }
        files_list->files_count++;
    }

    if (files_list->files_count > 0) {
        files_list->fi = (msg_file_info_t *)calloc(files_list->files_count, sizeof(msg_file_info_t));

        if (!files_list->fi) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Memory allocation error (fi)\n", curr_dt(dt), cid);
            goto error_cleanup;
        }
    }
    rewinddir(dir);
    uint32_t index = 0;

    for (;;) {
        errno = 0;
        entry = readdir(dir);

        if (entry == NULL) {
            if (errno != 0) {
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed to read directory %s: %s\n", curr_dt(dt), cid, directory, strerror(errno));
                goto error_cleanup;
            }
            break;
        }

        if (!is_data_file_name(entry->d_name))
            continue;
#if !ENTERPRISE
        if (strcmp(entry->d_name, "lob.data") == 0)
            continue;
#endif
        if (fstatat(dirfd(dir), entry->d_name, &file_stat, AT_SYMLINK_NOFOLLOW) != 0) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Stat error for file %s/%s: %s\n", curr_dt(dt), cid, directory, entry->d_name, strerror(errno));
            goto error_cleanup;
        }

        if (!S_ISREG(file_stat.st_mode))
            continue;

        if (index >= files_list->files_count) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Directory %s changed while creating files list: found more than %u *.data files\n", curr_dt(dt), cid, directory,
                    files_list->files_count);
            goto error_cleanup;
        }
        char filepath[PATH_MAX];
        int path_size = snprintf(filepath, sizeof(filepath), "%s/%s", directory, entry->d_name);

        if (path_size < 0 || (size_t)path_size >= sizeof(filepath)) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] File path is too long for %s/%s\n", curr_dt(dt), cid, directory, entry->d_name);
            goto error_cleanup;
        }
        fprintf(stderr, "%s INFO [HEALTH=%015lu] Process filepath %s\n", curr_dt(dt), cid, filepath);
        msg_file_info_t *file_info = &files_list->fi[index];
        size_t file_name_size = strlen(entry->d_name) + 1;

        if (file_name_size > UINT32_MAX) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] File name is too long for %s\n", curr_dt(dt), cid, filepath);
            goto error_cleanup;
        }
        file_info->file_name_size = (uint32_t)file_name_size;
        file_info->file_name = (uint8_t *)calloc(file_info->file_name_size, 1);

        if (!file_info->file_name) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Memory allocation error (file_name)\n", curr_dt(dt), cid);
            goto error_cleanup;
        }
        memcpy(file_info->file_name, entry->d_name, file_info->file_name_size);

        if (file_stat.st_size < 0) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Negative file size for %s\n", curr_dt(dt), cid, filepath);
            goto error_cleanup;
        }
        file_info->file_size = (uint64_t)file_stat.st_size;
        file_info->file_ts = (uint64_t)file_stat.st_mtim.tv_sec * 1000 + (uint64_t)file_stat.st_mtim.tv_nsec / 1000000;

        if (file_info->file_size > 0 && !calculate_xxh128_for_file(r_info, calc_xxh, filepath, file_info, cid)) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed to calculate XXH128 for %s\n", curr_dt(dt), cid, filepath);
            goto error_cleanup;
        }
        index++;
    }

    if (index != files_list->files_count) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Directory %s changed while creating files list: expected %u, processed %u *.data files\n", curr_dt(dt), cid, directory,
                files_list->files_count, index);
        goto error_cleanup;
    }

    if (log_level > 1)
        fprintf(stderr, "%s DEBUG [HEALTH=%015lu] Creating get_files_list done %s\n", curr_dt(dt), cid, r_info->uuid);

    if (closedir(dir) != 0) {
        dir = NULL;
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed to close directory %s: %s\n", curr_dt(dt), cid, directory, strerror(errno));
        goto error_cleanup;
    }
    return files_list;
error_cleanup:
    if (dir != NULL)
        closedir(dir);
    free_files_list(files_list);
    return NULL;
}

uint8_t *serialize_files_list(msg_files_list_t *files_list, uint32_t *output_size, uint64_t cid) {
    if (!files_list)
        return NULL;
    char dt[DT_LOG_SIZE];
    uint32_t total_size = 0;
    total_size += sizeof(files_list->msg_size);
    total_size += sizeof(files_list->type);
    total_size += sizeof(files_list->req_count_chk1);
    total_size += sizeof(files_list->req_count_chk2);
    total_size += sizeof(files_list->req_count_chk3);
    total_size += sizeof(files_list->files_count);
    total_size += sizeof(files_list->close_state);

    for (uint32_t i = 0; i < files_list->files_count; i++) {
        const msg_file_info_t *f = &files_list->fi[i];
        total_size += sizeof(f->file_name_size);
        total_size += f->file_name_size;
        total_size += sizeof(f->file_size) + sizeof(f->file_ts);
        total_size += sizeof(uint64_t);

        for (uint64_t b = 0; b < f->num_blocks; b++) {
            total_size += sizeof(uint64_t);
            total_size += sizeof(uint8_t);
            total_size += XXH128_SIZE;
        }
    }
    files_list->msg_size = total_size;
    *output_size = total_size;
    uint8_t *buffer = (uint8_t *)malloc(total_size);

    if (!buffer) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed to allocate memory for serialization\n", curr_dt(dt), cid);
        return NULL;
    }
    uint8_t *ptr = buffer;
    uint32_t msg_size_network = total_size;
    memcpy(ptr, &msg_size_network, sizeof(msg_size_network));
    ptr += sizeof(msg_size_network);
    memcpy(ptr, &files_list->type, sizeof(files_list->type));
    ptr += sizeof(files_list->type);
    memcpy(ptr, &files_list->req_count_chk1, sizeof(files_list->req_count_chk1));
    ptr += sizeof(files_list->req_count_chk1);
    memcpy(ptr, &files_list->req_count_chk2, sizeof(files_list->req_count_chk2));
    ptr += sizeof(files_list->req_count_chk2);
    memcpy(ptr, &files_list->req_count_chk3, sizeof(files_list->req_count_chk3));
    ptr += sizeof(files_list->req_count_chk3);
    memcpy(ptr, &files_list->files_count, sizeof(files_list->files_count));
    ptr += sizeof(files_list->files_count);
    memcpy(ptr, &files_list->close_state, sizeof(files_list->close_state));
    ptr += sizeof(files_list->close_state);

    for (uint32_t i = 0; i < files_list->files_count; i++) {
        const msg_file_info_t *f = &files_list->fi[i];
        memcpy(ptr, &f->file_name_size, sizeof(f->file_name_size));
        ptr += sizeof(f->file_name_size);
        memcpy(ptr, f->file_name, f->file_name_size);
        ptr += f->file_name_size;
        memcpy(ptr, &f->file_size, sizeof(f->file_size));
        ptr += sizeof(f->file_size);
        memcpy(ptr, &f->file_ts, sizeof(f->file_ts));
        ptr += sizeof(f->file_ts);
        uint64_t nb = (uint64_t)f->num_blocks;
        memcpy(ptr, &nb, sizeof(nb));
        ptr += sizeof(nb);

        for (uint64_t b = 0; b < f->num_blocks; b++) {
            const msg_hsh_info_t *h = f->msg_hsh_info[b];
            memcpy(ptr, &h->position, sizeof(h->position));
            ptr += sizeof(h->position);
            memcpy(ptr, &h->data_state, sizeof(h->data_state));
            ptr += sizeof(h->data_state);
            memcpy(ptr, h->xxh128, XXH128_SIZE);
            ptr += XXH128_SIZE;
        }
    }
    return buffer;
}


void free_files_list(msg_files_list_t *files_list) {
    if (!files_list)
        return;

    if (files_list->fi) {
        for (uint32_t i = 0; i < files_list->files_count; i++) {
            msg_file_info_t *f = &files_list->fi[i];

            if (f->file_name) {
                free(f->file_name);
            }

            if (f->msg_hsh_info) {
                for (uint64_t b = 0; b < f->num_blocks; b++) {
                    if (f->msg_hsh_info[b]) {
                        if (f->msg_hsh_info[b]->xxh128) {
                            free(f->msg_hsh_info[b]->xxh128);
                        }
                        free(f->msg_hsh_info[b]);
                    }
                }
                free(f->msg_hsh_info);
            }
        }
        free(files_list->fi);
    }
    free(files_list);
}


msg_files_list_t *deserialize_files_list(const uint8_t *buffer, uint32_t size, uint64_t cid) {
    char dt[DT_LOG_SIZE];

    if (!buffer || size < 9) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Input buffer too small\n", curr_dt(dt), cid);
        return NULL;
    }
    const uint8_t *ptr = buffer;
    const uint8_t *buffer_end = buffer + size;
#define CHECK_REMAINING(required)                                                                                                                                                \
    do {                                                                                                                                                                         \
        if ((size_t)(buffer_end - ptr) < (required)) {                                                                                                                           \
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] deserialize_files_list: insufficient buffer (needed %zu, available %zu bytes)\n", curr_dt(dt), cid, (size_t)(required),    \
                    (size_t)(buffer_end - ptr));                                                                                                                                 \
            goto error_cleanup;                                                                                                                                                  \
        }                                                                                                                                                                        \
    } while (0)
    msg_files_list_t *files_list = calloc(1, sizeof(msg_files_list_t));

    if (!files_list) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Memory allocation error (files_list)\n", curr_dt(dt), cid);
        return NULL;
    }
    CHECK_REMAINING(sizeof(files_list->msg_size));
    memcpy(&files_list->msg_size, ptr, sizeof(files_list->msg_size));
    ptr += sizeof(files_list->msg_size);
    CHECK_REMAINING(sizeof(files_list->type));
    memcpy(&files_list->type, ptr, sizeof(files_list->type));
    ptr += sizeof(files_list->type);
    CHECK_REMAINING(sizeof(files_list->req_count_chk1));
    memcpy(&files_list->req_count_chk1, ptr, sizeof(files_list->req_count_chk1));
    ptr += sizeof(files_list->req_count_chk1);
    CHECK_REMAINING(sizeof(files_list->req_count_chk2));
    memcpy(&files_list->req_count_chk2, ptr, sizeof(files_list->req_count_chk2));
    ptr += sizeof(files_list->req_count_chk2);
    CHECK_REMAINING(sizeof(files_list->req_count_chk3));
    memcpy(&files_list->req_count_chk3, ptr, sizeof(files_list->req_count_chk3));
    ptr += sizeof(files_list->req_count_chk3);
    CHECK_REMAINING(sizeof(files_list->files_count));
    memcpy(&files_list->files_count, ptr, sizeof(files_list->files_count));
    ptr += sizeof(files_list->files_count);
    CHECK_REMAINING(sizeof(files_list->close_state));
    memcpy(&files_list->close_state, ptr, sizeof(files_list->close_state));
    ptr += sizeof(files_list->close_state);

    if (files_list->files_count > 0) {
        if (files_list->files_count > SIZE_MAX / sizeof(msg_file_info_t)) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] files_count too large (overflow)\n", curr_dt(dt), cid);
            goto error_cleanup;
        }
        files_list->fi = calloc(files_list->files_count, sizeof(msg_file_info_t));

        if (!files_list->fi) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Memory allocation error (fi)\n", curr_dt(dt), cid);
            free(files_list);
            return NULL;
        }
    }

    for (uint32_t i = 0; i < files_list->files_count; i++) {
        msg_file_info_t *file_info = &files_list->fi[i];
        CHECK_REMAINING(sizeof(file_info->file_name_size));
        memcpy(&file_info->file_name_size, ptr, sizeof(file_info->file_name_size));
        ptr += sizeof(file_info->file_name_size);

        if (file_info->file_name_size > 0) {
            CHECK_REMAINING(file_info->file_name_size);
            file_info->file_name = malloc(file_info->file_name_size);

            if (!file_info->file_name) {
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] Memory allocation error for file_name\n", curr_dt(dt), cid);
                goto error_cleanup;
            }
            memcpy(file_info->file_name, ptr, file_info->file_name_size);
            ptr += file_info->file_name_size;
        }
        CHECK_REMAINING(sizeof(file_info->file_size));
        memcpy(&file_info->file_size, ptr, sizeof(file_info->file_size));
        ptr += sizeof(file_info->file_size);
        CHECK_REMAINING(sizeof(file_info->file_ts));
        memcpy(&file_info->file_ts, ptr, sizeof(file_info->file_ts));
        ptr += sizeof(file_info->file_ts);
        CHECK_REMAINING(sizeof(uint64_t));
        uint64_t nb = 0;
        memcpy(&nb, ptr, sizeof(nb));
        ptr += sizeof(nb);
        file_info->num_blocks = nb;

        if (file_info->num_blocks > 0) {
            if (file_info->num_blocks > SIZE_MAX / sizeof(msg_hsh_info_t *)) {
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] num_blocks too large (overflow)\n", curr_dt(dt), cid);
                goto error_cleanup;
            }
            file_info->msg_hsh_info = calloc(file_info->num_blocks, sizeof(msg_hsh_info_t *));

            if (!file_info->msg_hsh_info) {
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] Memory allocation error for msg_hsh_info\n", curr_dt(dt), cid);
                goto error_cleanup;
            }
        }

        for (uint64_t b = 0; b < file_info->num_blocks; b++) {
            msg_hsh_info_t *hsh = calloc(1, sizeof(msg_hsh_info_t));

            if (!hsh) {
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] Memory allocation error for msg_hsh_info_t\n", curr_dt(dt), cid);
                goto error_cleanup;
            }
            CHECK_REMAINING(sizeof(hsh->position));
            memcpy(&hsh->position, ptr, sizeof(hsh->position));
            ptr += sizeof(hsh->position);
            CHECK_REMAINING(sizeof(hsh->data_state));
            memcpy(&hsh->data_state, ptr, sizeof(hsh->data_state));
            ptr += sizeof(hsh->data_state);
            CHECK_REMAINING(XXH128_SIZE);
            hsh->xxh128 = malloc(XXH128_SIZE);

            if (!hsh->xxh128) {
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] Memory allocation error for xxh128\n", curr_dt(dt), cid);
                free(hsh);
                goto error_cleanup;
            }
            memcpy(hsh->xxh128, ptr, XXH128_SIZE);
            ptr += XXH128_SIZE;
            file_info->msg_hsh_info[b] = hsh;
        }
    }
#undef CHECK_REMAINING
    return files_list;
error_cleanup:
#undef CHECK_REMAINING
    free_files_list(files_list);
    return NULL;
}


void print_files_list(const msg_files_list_t *files_list, char *uuid, uint64_t cid) {
    char dt[DT_LOG_SIZE];

    if (!files_list) {
        fprintf(stderr, "%s ERROR [HEALTH=%015" PRIu64 "] files_list is NULL\n", curr_dt(dt), cid);
        return;
    }
    const char *region_uuid = uuid ? uuid : "<null>";

    if (!files_list->fi) {
        fprintf(stderr, "%s ERROR [HEALTH=%015" PRIu64 "] files_list->fi is NULL for region %s\n", curr_dt(dt), cid, region_uuid);
        return;
    }
    fprintf(stderr, "%s INFO [HEALTH=%015" PRIu64 "] Total region %s message size: %u bytes\n", curr_dt(dt), cid, region_uuid, files_list->msg_size);
    fprintf(stderr, "%s INFO [HEALTH=%015" PRIu64 "] Number region %s of files: %u\n", curr_dt(dt), cid, region_uuid, files_list->files_count);

    for (uint32_t i = 0; i < files_list->files_count; i++) {
        const msg_file_info_t *file_info = &files_list->fi[i];
        const char *file_name = (file_info->file_name) ? (const char *)file_info->file_name : "<unknown>";
        fprintf(stderr, "%s INFO [HEALTH=%015" PRIu64 "] File %s region %s size: %" PRIu64 " bytes\n", curr_dt(dt), cid, file_name, region_uuid, file_info->file_size);
        fprintf(stderr, "%s INFO [HEALTH=%015" PRIu64 "] File %s region %s modification time: %" PRIu64 "\n", curr_dt(dt), cid, file_name, region_uuid, file_info->file_ts);

        if (log_level > 0) {
            if (!file_info->msg_hsh_info) {
                if (file_info->file_size != 0)
                    fprintf(stderr, "%s ERROR [HEALTH=%015" PRIu64 "] File %s region %s has NULL msg_hsh_info\n", curr_dt(dt), cid, file_name, region_uuid);
            } else {
                for (uint64_t j = 0; j < file_info->num_blocks; j++) {
                    if (!file_info->msg_hsh_info[j]) {
                        fprintf(stderr, "%s ERROR [HEALTH=%015" PRIu64 "] File %s region %s block %" PRIu64 " has NULL hash info pointer\n", curr_dt(dt), cid, file_name,
                                region_uuid, j);
                        continue;
                    }
                    char xxh_str[XXH128_SIZE * 2 + 1] = {0};

                    for (int k = 0; k < XXH128_SIZE; k++)
                        snprintf(&xxh_str[k * 2], 3, "%02x", file_info->msg_hsh_info[j]->xxh128[k]);
                    xxh_str[XXH128_SIZE * 2] = '\0';
                    fprintf(stderr, "%s DEBUG [HEALTH=%015" PRIu64 "] File %s region %s state=%d block %" PRIu64 " xxh128 #%llu: %s\n", curr_dt(dt), cid, file_name, region_uuid,
                            file_info->msg_hsh_info[j]->data_state, j, (unsigned long long)file_info->msg_hsh_info[j]->position, xxh_str);
                }
            }
        }
    }
}

static int is_number(const char *s, int *out) {
    char *end = NULL;
    errno = 0;
    long v = strtol(s, &end, 10);

    if (errno != 0 || end == s || *end != '\0')
        return 0;

    if (v < 0 || v > INT_MAX)
        return 0;
    *out = (int)v;
    return 1;
}

long count_open_fds_linux(void) {
    DIR *d = opendir("/proc/self/fd");

    if (!d)
        return -1;
    int dfd = dirfd(d);
    long count = 0;
    errno = 0;

    for (;;) {
        struct dirent *ent = readdir(d);

        if (!ent)
            break;

        if (ent->d_name[0] == '.' && (ent->d_name[1] == '\0' || (ent->d_name[1] == '.' && ent->d_name[2] == '\0')))
            continue;
        int fd = -1;

        if (!is_number(ent->d_name, &fd))
            continue;

        if (fd == dfd)
            continue;
        ++count;
    }
    int saved_errno = errno;
    closedir(d);

    if (saved_errno != 0) {
        errno = saved_errno;
        return -1;
    }
    return count;
}


int warn_if_near_nofile(double threshold_ratio, uint64_t cid) {
    struct rlimit rl;

    if (getrlimit(RLIMIT_NOFILE, &rl) != 0)
        return -1;
    long used = count_open_fds_linux();

    if (used < 0)
        return -1;

    if (rl.rlim_cur != RLIM_INFINITY) {
        double ratio = (double)used / (double)rl.rlim_cur;

        if (ratio >= threshold_ratio) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Open fds=%ld, RLIMIT_NOFILE=%llu (%.1f%% used), stop new client requests\n", curr_dt(dt), cid, used,
                    (unsigned long long)rl.rlim_cur, ratio * 100.0);
            stop_new_client_requests = 1;
        } else {
            if (stop_new_client_requests == 1) {
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s WARN [HEALTH=%015lu] Open fds=%ld, RLIMIT_NOFILE=%llu (%.1f%% used), allow new client requests\n", curr_dt(dt), cid, used,
                        (unsigned long long)rl.rlim_cur, ratio * 100.0);
            }
            stop_new_client_requests = 0;
        }
    }
    return 0;
}

int get_open_files_limit(rlim_t *soft, rlim_t *hard) {
    if (!soft || !hard) {
        errno = EINVAL;
        return -1;
    }
    struct rlimit rl;

    if (getrlimit(RLIMIT_NOFILE, &rl) != 0) {
        return -1;
    }
    *soft = rl.rlim_cur;
    *hard = rl.rlim_max;
    return 0;
}


bool region_needs_restart_marker(const char *dir_path, uint64_t health_ts, uint64_t cid) {
    if (health_ts == 0)
        return false;
    char dt[DT_LOG_SIZE];
    DIR *dir = opendir(dir_path);

    if (dir == NULL) {
        fprintf(stderr, "%s WARN [HEALTH=%015lu] Cannot inspect restart timestamps in %s: %s; keep recovery marker policy\n", curr_dt(dt), cid, dir_path, strerror(errno));
        return true;
    }
    bool needed = true;
    bool data_entry_seen = false;

    for (;;) {
        errno = 0;
        struct dirent *entry = readdir(dir);

        if (entry == NULL) {
            if (errno != 0)
                fprintf(stderr, "%s WARN [HEALTH=%015lu] Cannot list restart timestamps in %s: %s; keep recovery marker policy\n", curr_dt(dt), cid, dir_path, strerror(errno));
            else if (!data_entry_seen) {
                needed = false;
                fprintf(stderr, "%s INFO [HEALTH=%015lu] Skip restart recovery marker for %s: no .data files\n", curr_dt(dt), cid, dir_path);
            }
            break;
        }
        size_t name_size = strlen(entry->d_name);

        if (name_size < 5 || strcmp(entry->d_name + name_size - 5, ".data") != 0)
            continue;
        data_entry_seen = true;
        struct stat st;
        int result;

        do {
            result = fstatat(dirfd(dir), entry->d_name, &st, 0);
        } while (result != 0 && errno == EINTR);

        if (result != 0) {
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Cannot stat restart file %s/%s: %s; keep recovery marker policy\n", curr_dt(dt), cid, dir_path, entry->d_name,
                    strerror(errno));
            break;
        }

        if (!S_ISREG(st.st_mode) || st.st_mtim.tv_sec < 0 || (uint64_t)st.st_mtim.tv_sec > (UINT64_MAX - 999U) / 1000U)
            continue;
        uint64_t file_ts = (uint64_t)st.st_mtim.tv_sec * 1000U + (uint64_t)st.st_mtim.tv_nsec / 1000000U;

        if (file_ts > health_ts && file_ts - health_ts > 60000U) {
            fprintf(stderr, "%s INFO [HEALTH=%015lu] Skip restart recovery marker for %s: file=%s mtime=%lu health_ts=%lu delta_ms=%lu\n", curr_dt(dt), cid, dir_path,
                    entry->d_name, file_ts, health_ts, file_ts - health_ts);
            needed = false;
            break;
        }
    }
    closedir(dir);
    return needed;
}


bool check_last_modify_time(region_info_t *r_info, char *dir_path, uint64_t data_chk_ts, table_handler_t *t_handler) {
    uint64_t cid = t_handler->cid;
    char dt[DT_LOG_SIZE];
    struct dirent *entry;
    uint64_t last_modify_ts;
    DIR *dir = opendir(dir_path);

    if (dir == NULL) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed to open directory %s : error %s\n", curr_dt(dt), cid, dir_path, strerror(errno));
        r_info->stop = true;
        r_info->bad_data = true;
        finish_thread(r_info, 81, true, t_handler);
    }
    fprintf(stderr, "%s INFO [HEALTH=%015lu] Check last_modify_ts files with *.data extension in directory %s:\n", curr_dt(dt), cid, dir_path);

    while ((entry = readdir(dir)) != NULL) {
        if (strstr(entry->d_name, ".data") != NULL) {
            char full_path[PATH_MAX];
            snprintf(full_path, PATH_MAX, "%s/%s", dir_path, entry->d_name);
            last_modify_ts = get_last_modify_ts(full_path, cid);
            uint64_t difference = last_modify_ts > data_chk_ts ? last_modify_ts - data_chk_ts : data_chk_ts - last_modify_ts;

            if (difference < replicas_last_change_time_check_diff_ms) {
                fprintf(stderr, "%s INFO [HEALTH=%015lu] Difference %lu for file %s less then %u\n", curr_dt(dt), cid, difference, full_path,
                        replicas_last_change_time_check_diff_ms);
                return true;
            } else if (log_level > 0)
                fprintf(stderr, "%s INFO [HEALTH=%015lu] Difference %lu for file %s more or equal then %u\n", curr_dt(dt), cid, difference, full_path,
                        replicas_last_change_time_check_diff_ms);
        }
    }
    closedir(dir);
    return false;
}

static bool write_recovery_info_full(int fd, const char *buff, size_t size) {
    size_t written = 0;

    while (written < size) {
        ssize_t result = write(fd, buff + written, size - written);

        if (result > 0) {
            written += (size_t)result;
            continue;
        }

        if (result < 0 && errno == EINTR)
            continue;
        return false;
    }
    return true;
}


static bool fsync_recovery_info_dir(const char *path) {
    char dir_path[MAX_FN_SIZE];
    size_t path_size = strlen(path);

    if (path_size == 0 || path_size >= sizeof(dir_path)) {
        errno = ENAMETOOLONG;
        return false;
    }
    memcpy(dir_path, path, path_size + 1);
    char *slash = strrchr(dir_path, '/');

    if (slash == NULL) {
        strcpy(dir_path, ".");
    } else if (slash == dir_path) {
        slash[1] = 0;
    } else {
        *slash = 0;
    }
    int dir_fd;

    do {
        dir_fd = open(dir_path, O_RDONLY | O_DIRECTORY);
    } while (dir_fd < 0 && errno == EINTR);

    if (dir_fd < 0)
        return false;
    int result;

    do {
        result = fsync(dir_fd);
    } while (result != 0 && errno == EINTR);
    int fsync_errno = errno;
    int close_result = close(dir_fd);

    if (result != 0) {
        errno = fsync_errno;
        return false;
    }
    return close_result == 0;
}

static bool recovery_info_region_dir_missing(const char *path) {
    char dir_path[MAX_FN_SIZE];
    size_t path_size = strlen(path);

    if (path_size == 0 || path_size >= sizeof(dir_path))
        return false;
    memcpy(dir_path, path, path_size + 1);
    char *slash = strrchr(dir_path, '/');

    if (slash == NULL)
        strcpy(dir_path, ".");
    else if (slash == dir_path)
        slash[1] = 0;
    else
        *slash = 0;
    struct stat st;
    int result;

    do {
        result = stat(dir_path, &st);
    } while (result != 0 && errno == EINTR);
    return result != 0 && errno == ENOENT;
}

static bool ignore_missing_replica_region(table_handler_t *t_handler, const char *path, int saved_errno) {
    if (!t_handler->is_replica_client || saved_errno != ENOENT || !recovery_info_region_dir_missing(path))
        return false;
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s INFO [HEALTH=%015lu] Region directory for %s does not exist, recovery marker is not needed after region unassign\n", curr_dt(dt), t_handler->cid, path);
    return true;
}


static void recovery_info_error(region_info_t *r_info, table_handler_t *t_handler, int error_code, int call_id, const char *operation, const char *path, int saved_errno) {
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s ERROR [HEALTH=%015lu] Error code %d. Failed %s recovery file %s: %s (errno: %d)\n", curr_dt(dt), t_handler->cid, error_code, operation, path,
            strerror(saved_errno), saved_errno);

    if (t_handler->is_replica_client) {
        bool expected = false;

        if (atomic_compare_exchange_strong(&recovery_marker_failed, &expected, true))
            fprintf(stderr, "%s FATAL [HEALTH=%015lu] Cannot persist recovery marker for a reserve replica. Fail-safe shutdown enabled; health file must be preserved\n",
                    curr_dt(dt), t_handler->cid);
        rs_signal_handler(100);
    } else if (r_info != NULL) {
        graceful_exit(-2);
    }
}


bool set_region_recovery_info(region_info_t *r_info, uint64_t data_chk_ts, table_handler_t *t_handler) {
    invalidate_record_gc(r_info);

    if (data_check_enabled != 1)
        return true;

    if (t_handler == NULL)
        return false;
    uint64_t cid = t_handler->cid;
    char dt[DT_LOG_SIZE];
    char region_recovery_info[MAX_FN_SIZE];
    int path_result;

    if (t_handler->is_replica_client) {
        if (t_handler->recovery_info_path == NULL) {
            errno = EINVAL;
            recovery_info_error(NULL, t_handler, 123, 82, "resolve path for", recovery_info_fn, errno);
            return false;
        }
        path_result = snprintf(region_recovery_info, sizeof(region_recovery_info), "%s", t_handler->recovery_info_path);
    } else {
        if (r_info == NULL || r_info->r_fname == NULL) {
            errno = EINVAL;
            recovery_info_error(r_info, t_handler, 123, 82, "resolve path for", recovery_info_fn, errno);
            return false;
        }
        path_result = snprintf(region_recovery_info, sizeof(region_recovery_info), "%s/%s", r_info->r_fname, recovery_info_fn);
    }

    if (path_result < 0 || (size_t)path_result >= sizeof(region_recovery_info)) {
        errno = ENAMETOOLONG;
        recovery_info_error(r_info, t_handler, 123, 82, "resolve path for", recovery_info_fn, errno);
        return false;
    }
    fprintf(stderr, "%s INFO [HEALTH=%015lu] Check file %s\n", curr_dt(dt), cid, region_recovery_info);
    uint64_t marker_ts = data_chk_ts == 0 ? 1 : data_chk_ts;
    int fd = -1;
    bool new_file = false;

    while (fd < 0) {
        do {
            fd = open(region_recovery_info, O_RDWR);
        } while (fd < 0 && errno == EINTR);

        if (fd >= 0)
            break;

        if (errno != ENOENT)
            break;

        if (!t_handler->is_replica_client && data_chk_ts != 1) {
            if (check_last_modify_time(r_info, r_info->r_fname, data_chk_ts, t_handler) == false)
                return true;
        }

        do {
            fd = open(region_recovery_info, O_RDWR | O_CREAT | O_EXCL, 0666);
        } while (fd < 0 && errno == EINTR);

        if (fd >= 0) {
            new_file = true;
            break;
        }

        if (errno != EEXIST)
            break;
    }

    if (fd < 0) {
        int saved_errno = errno;

        if (ignore_missing_replica_region(t_handler, region_recovery_info, saved_errno))
            return true;
        recovery_info_error(r_info, t_handler, 123, 82, "open", region_recovery_info, saved_errno);
        return false;
    }
    bool rewrite_file = new_file;
    uint64_t stored_ts = 0;

    if (!new_file) {
        char stored_value[64];
        ssize_t read_size;

        do {
            read_size = pread(fd, stored_value, sizeof(stored_value) - 1U, 0);
        } while (read_size < 0 && errno == EINTR);

        if (read_size > 0) {
            stored_value[read_size] = 0;
            errno = 0;
            char *end = NULL;
            unsigned long long parsed = strtoull(stored_value, &end, 10);

            while (end != NULL && (*end == ' ' || *end == '\t' || *end == '\r' || *end == '\n'))
                end++;

            if (errno == 0 && end != stored_value && end != NULL && *end == 0 && parsed != 0)
                stored_ts = (uint64_t)parsed;
        }

        if (stored_ts == 0)
            rewrite_file = true;
    }
    bool file_ok = true;
    int saved_errno = 0;

    if (rewrite_file) {
        char marker[64];
        int marker_size = snprintf(marker, sizeof(marker), "%lu\n", marker_ts);

        if (marker_size < 0 || (size_t)marker_size >= sizeof(marker)) {
            file_ok = false;
            saved_errno = EOVERFLOW;
        } else if (ftruncate(fd, 0) != 0 || lseek(fd, 0, SEEK_SET) < 0 || !write_recovery_info_full(fd, marker, marker_size)) {
            file_ok = false;
            saved_errno = errno;
        } else {
            stored_ts = marker_ts;
        }
    }

    if (file_ok) {
        int result;

        do {
            result = fsync(fd);
        } while (result != 0 && errno == EINTR);

        if (result != 0) {
            file_ok = false;
            saved_errno = errno;
        }
    }

    if (close(fd) != 0 && file_ok) {
        file_ok = false;
        saved_errno = errno;
    }
    bool dir_ok = fsync_recovery_info_dir(region_recovery_info);
    int dir_errno = errno;

    if (!file_ok) {
        recovery_info_error(r_info, t_handler, 124, 83, "write or sync", region_recovery_info, saved_errno);
        return false;
    }

    if (!dir_ok) {
        if (ignore_missing_replica_region(t_handler, region_recovery_info, dir_errno))
            return true;
        recovery_info_error(r_info, t_handler, 127, 87, "sync directory for", region_recovery_info, dir_errno);
        return false;
    }

    if (!t_handler->is_replica_client)
        atomic_store(&r_info->r_need_data_chk, stored_ts);
    fprintf(stderr, "%s INFO [HEALTH=%015lu] Recovery ts=%lu is synced for region %s\n", curr_dt(dt), cid, stored_ts, region_recovery_info);
    return true;
}

bool shutdown_recovery_marker_valid(struct region_info_t *r_info, uint64_t *marker_ts) {
    if (marker_ts != NULL)
        *marker_ts = 0;

    if (r_info == NULL || atomic_load(&r_info->recovery_path_ready) == 0 || r_info->r_fname == NULL || recovery_info_fn == NULL)
        return false;
    char region_recovery_info[MAX_FN_SIZE];
    int path_result = snprintf(region_recovery_info, sizeof(region_recovery_info), "%s/%s", r_info->r_fname, recovery_info_fn);

    if (path_result < 0 || (size_t)path_result >= sizeof(region_recovery_info))
        return false;
    int fd;

    do {
        fd = open(region_recovery_info, O_RDONLY);
    } while (fd < 0 && errno == EINTR);

    if (fd < 0)
        return false;
    char stored_value[64];
    ssize_t read_size;

    do {
        read_size = pread(fd, stored_value, sizeof(stored_value), 0);
    } while (read_size < 0 && errno == EINTR);
    bool result = false;
    uint64_t stored_ts = 0;

    if (read_size > 0 && (size_t)read_size < sizeof(stored_value) && memchr(stored_value, 0, (size_t)read_size) == NULL) {
        stored_value[read_size] = 0;
        char *start = stored_value;

        while (*start == ' ' || *start == '\t' || *start == '\r' || *start == '\n')
            start++;

        if (*start >= '0' && *start <= '9') {
            errno = 0;
            char *end = NULL;
            unsigned long long parsed = strtoull(start, &end, 10);

            while (end != NULL && (*end == ' ' || *end == '\t' || *end == '\r' || *end == '\n'))
                end++;

            if (errno == 0 && end != start && end != NULL && *end == 0 && parsed != 0) {
                stored_ts = (uint64_t)parsed;
                result = true;
            }
        }
    }

    if (close(fd) != 0)
        result = false;

    if (result && marker_ts != NULL)
        *marker_ts = stored_ts;
    return result;
}

void drop_region_recovery_info(struct region_info_t *r_info, uint64_t cid) {
    char dt[DT_LOG_SIZE];

    while (atomic_load(&is_shutting_down)) {
        sleep(10);
    }
    atomic_fetch_add(&recovery_drop_inflight, 1);

    if (atomic_load(&is_shutting_down)) {
        atomic_fetch_sub(&recovery_drop_inflight, 1);

        while (atomic_load(&is_shutting_down)) {
            sleep(10);
        }
        return;
    }
    char region_recovery_info[MAX_FN_SIZE];
    snprintf(region_recovery_info, MAX_FN_SIZE, "%s/%s", r_info->r_fname, recovery_info_fn);

    if (access(region_recovery_info, F_OK) != 0) {
        if (errno == ENOENT) {
            r_info->r_need_data_chk = 0;
            fprintf(stderr, "%s INFO [HEALTH=%015lu] Recovery point file %s is not found\n", curr_dt(dt), cid, region_recovery_info);
        } else
            fprintf(stderr, "%s INFO [HEALTH=%015lu] Cannot check recovery file %s: %s\n", curr_dt(dt), cid, region_recovery_info, strerror(errno));
        atomic_fetch_sub(&recovery_drop_inflight, 1);
        return;
    } else {
        if (remove(region_recovery_info) == 0) {
            bool dir_ok = fsync_recovery_info_dir(region_recovery_info);

            if (dir_ok) {
                r_info->r_need_data_chk = 0;
                fprintf(stderr, "%s WARN [HEALTH=%015lu] Recovery point file %s is removed\n", curr_dt(dt), cid, region_recovery_info);
                atomic_fetch_sub(&recovery_drop_inflight, 1);
                return;
            }
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed fsunc dir for %s \n", curr_dt(dt), cid, region_recovery_info);
        }
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Recovery point file %s is not removed, recommended check permissions and remove it\n", curr_dt(dt), cid, region_recovery_info);
    }
    atomic_fetch_sub(&recovery_drop_inflight, 1);
}


bool save_data_check(uint64_t cid, bool print_ts_info) {
    char dt[DT_LOG_SIZE];

    if (health_file_name == NULL) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Health file name is NULL\n", curr_dt(dt), cid);
        return false;
    }

    for (int attempt = 1; attempt <= 3; attempt++) {
        struct timeval tv;

        if (gettimeofday(&tv, NULL) != 0) {
            int saved_errno = errno;
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed gettimeofday for file %s, attempt=%d/3: %s (errno: %d)\n", curr_dt(dt), cid, health_file_name, attempt,
                    strerror(saved_errno), saved_errno);
        } else {
            uint64_t timestamp = (uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec / 1000;
            timestamp -= 1000;
            FILE *file = fopen(health_file_name, "w");

            if (file == NULL) {
                int saved_errno = errno;
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed to create file %s, attempt=%d/3: %s (errno: %d)\n", curr_dt(dt), cid, health_file_name, attempt,
                        strerror(saved_errno), saved_errno);
            } else {
                bool write_ok = true;
                int saved_errno = 0;

                if (fprintf(file, "%lu\n", timestamp) < 0) {
                    write_ok = false;
                    saved_errno = errno;
                } else if (fflush(file) != 0) {
                    write_ok = false;
                    saved_errno = errno;
                } else if (fsync(fileno(file)) != 0) {
                    write_ok = false;
                    saved_errno = errno;
                }

                if (fclose(file) != 0 && write_ok) {
                    write_ok = false;
                    saved_errno = errno;
                }

                if (write_ok) {
                    if (print_ts_info)
                        fprintf(stderr, "%s INFO [HEALTH=%015lu] File %s is updated by ts=%lu\n", curr_dt(dt), cid, health_file_name, timestamp);
                    return true;
                }
                fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed to update file %s, attempt=%d/3: %s (errno: %d)\n", curr_dt(dt), cid, health_file_name, attempt,
                        strerror(saved_errno), saved_errno);
            }
        }

        if (attempt < 3)
            usleep((useconds_t)attempt * 100000);
    }
    fprintf(stderr, "%s ERROR [HEALTH=%015lu] Failed to update file %s after 3 attempts\n", curr_dt(dt), cid, health_file_name);
    return false;
}

uint64_t get_file_mod_time_ms(const char *filepath) {
    struct stat file_stat;

    if (stat(filepath, &file_stat) != 0)
        return 0;
    struct timespec ts;
    ts = file_stat.st_mtim;
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}


uint64_t get_data_check_ts(struct region_info_t *r_info, struct table_handler_t *t_handler, char *filename, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    struct stat fileStat;

    if (stat(filename, &fileStat) == -1)
        return 0;
    FILE *file = fopen(filename, "r");

    if (!file) {
        if (cid == 0) {
            fprintf(stderr, "%s FATAL [HEALTH=%015lu] Failed to open file %s: %s\n", curr_dt(dt), cid, filename, strerror(errno));
            exit(-1);
        }
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Error code 270. Failed to open file %s: %s\n", curr_dt(dt), cid, filename, strerror(errno));
        r_info->stop = true;
        r_info->bad_data = true;
        finish_thread(r_info, 86, true, t_handler);
    }
    uint64_t timestamp = 0;

    if (fscanf(file, "%lu", &timestamp) != 1) {
        fprintf(stderr, "%s WARN [HEALTH=%015lu] Error reading from file %s: %s (errno: %d)\n", curr_dt(dt), cid, filename, strerror(errno), errno);
        fclose(file);
        uint64_t ts = get_file_mod_time_ms(filename);
        fprintf(stderr, "%s INFO [HEALTH=%015lu] Timestamp file %s is %lu\n", curr_dt(dt), cid, filename, ts);
        return ts;
    }
    fclose(file);
    fprintf(stderr, "%s INFO [HEALTH=%015lu] Read check ts=%lu for %s\n", curr_dt(dt), cid, timestamp, filename);
    return timestamp;
}


uint64_t get_last_modify_ts(const char *filename, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    FILE *file = fopen(filename, "r");

    if (!file)
        return 0;
    fclose(file);
    struct stat fileStat;

    if (stat(filename, &fileStat) == -1) {
        perror("stat");
        return 0;
    }
    struct timespec mod_time_spec = fileStat.st_mtim;
    uint64_t timestamp = (uint64_t)mod_time_spec.tv_sec * 1000 + (uint64_t)mod_time_spec.tv_nsec / 1000000;
    time_t mod_time_sec = mod_time_spec.tv_sec;
    struct tm time_info;
    localtime_r(&mod_time_sec, &time_info);
    char formatted_time[64];
    strftime(formatted_time, sizeof(formatted_time), "%Y-%m-%d %H:%M:%S", &time_info);
    fprintf(stderr, "%s INFO [HEALTH=%015lu] Last modification of %s: %s.%03lu (ts=%lu)\n", curr_dt(dt), cid, filename, formatted_time, mod_time_spec.tv_nsec / 1000000,
            timestamp);
    return timestamp;
}

void add_path_to_drv_arr(char *path, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    char res[256];
    char *token, *str, *tofree;
    tofree = str = strdup(path);
    token = strsep(&str, "/");
    token = strsep(&str, "/");
    snprintf(res, sizeof(res) - 1, "/%s", token);

    for (int i = 0; i < curr_drv_arr; i++) {
        if (strcmp(drv_arr[i], res) == 0) {
            free(tofree);
            return;
        }
    }

    if (curr_drv_arr < MAX_NUMBER_OF_DRIVES - 1) {
        drv_arr[curr_drv_arr] = malloc(strlen(res) + 1);
        strcpy(drv_arr[curr_drv_arr], res);
        fprintf(stderr, "%s INFO [HEALTH=%015lu] Added drive %s to free space check\n", curr_dt(dt), cid, drv_arr[curr_drv_arr]);
        curr_drv_arr++;
    } else
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Unable to add new disk to check free space, limit %d reached\n", curr_dt(dt), cid, curr_drv_arr);
    free(tofree);
}


bool check_drive_free(char *drive_check, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    struct statfs dinfo;
    statfs(drive_check, &dinfo);
    uint64_t free_drive_size = dinfo.f_bavail * dinfo.f_bsize / (1024 * 1024);
    fprintf(stderr, "%s INFO [HEALTH=%015lu] Check drive %s space for user %lu MB (drive_free_mb_limit %d MB)\n", curr_dt(dt), cid, drive_check, free_drive_size,
            drive_free_mb_limit);

    if (free_drive_size < drive_free_mb_limit) {
        stop_write = 1;
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Cannot put data, drive free space is too small for %s, free space for %ld MB (drive_free_mb_limit %d MB)\n", curr_dt(dt), cid,
                drive_check, free_drive_size, drive_free_mb_limit);
        return false;
    }
    return true;
}


static int parse_socket_inode_from_fd_link(const char *link, uint64_t *inode_out) {
    if (link == NULL || inode_out == NULL)
        return -1;
    unsigned long long inode_ull = 0;

    if (sscanf(link, "socket:[%llu]", &inode_ull) == 1) {
        *inode_out = (uint64_t)inode_ull;
        return 0;
    }
    return -1;
}

void log_current_process_tcp_fds(uint64_t cid) {
    DIR *dir = opendir("/proc/self/fd");

    if (dir == NULL) {
        return;
    }
    int socket_fd_count = 0;
    int tcp_fd_count = 0;
    int failed_stat_tcp_fd_count = 0;
    uint32_t rtt_us_total = 0;
    uint32_t idle_skipped_count = 0;
    struct dirent *de;

    while ((de = readdir(dir)) != NULL) {
        if (de->d_name[0] == '.')
            continue;
        char *endptr = NULL;
        errno = 0;
        long fd_long = strtol(de->d_name, &endptr, 10);

        if (errno != 0 || endptr == de->d_name || *endptr != '\0')
            continue;

        if (fd_long < 0 || fd_long > INT32_MAX)
            continue;
        int fd = (int)fd_long;
        int dir_fd = dirfd(dir);

        if (fd == dir_fd) {
            continue;
        }
        char fd_path[256];
        char link_buf[512];
        snprintf(fd_path, sizeof(fd_path), "/proc/self/fd/%d", fd);
        ssize_t link_len = readlink(fd_path, link_buf, sizeof(link_buf) - 1);

        if (link_len < 0)
            continue;
        link_buf[link_len] = '\0';
        uint64_t inode = 0;

        if (parse_socket_inode_from_fd_link(link_buf, &inode) != 0)
            continue;
        socket_fd_count++;
        int sock_type = 0;
        socklen_t sock_type_len = sizeof(sock_type);

        if (getsockopt(fd, SOL_SOCKET, SO_TYPE, &sock_type, &sock_type_len) != 0) {
            continue;
        }

        if (sock_type != SOCK_STREAM) {
            continue;
        }
        struct tcp_info ti;
        socklen_t ti_len = sizeof(ti);
        memset(&ti, 0, sizeof(ti));

        if (getsockopt(fd, IPPROTO_TCP, TCP_INFO, &ti, &ti_len) != 0)
            continue;

        if (ti.tcpi_last_data_sent > rtt_fresh_idle_limit_ms && ti.tcpi_last_data_recv > rtt_fresh_idle_limit_ms) {
            idle_skipped_count++;
            continue;
        }

        if (ti.tcpi_state == TCP_ESTABLISHED) {
            tcp_fd_count++;
            uint32_t rtt_us;
            int rtt_res = get_tcp_rtt_us_by_fd(fd, &rtt_us);

            if (rtt_res == 0)
                rtt_us_total += rtt_us;
            else
                failed_stat_tcp_fd_count++;
        }
    }
    closedir(dir);

    if (tcp_fd_count > 0 && rtt_us_total > 0) {
        int rtt_score = 0;
        const char *rtt_grade = NULL;
        rtt_us_total /= tcp_fd_count;
        rate_rtt_us(rtt_us_total, &rtt_score, &rtt_grade);
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s INFO [HEALTH=%015lu] Connections average RTT=%u us, grade=%s (tcp_fd_count=%u, failed_stat_tcp_fd_count=%u, idle_skipped_count=%u)\n", curr_dt(dt),
                cid, rtt_us_total, rtt_grade, tcp_fd_count, failed_stat_tcp_fd_count, idle_skipped_count);
    } else {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s INFO [HEALTH=%015lu] Connections RTT unknown, failed_stat_tcp_fd_count=%u, idle_skipped_count=%u\n", curr_dt(dt), cid, failed_stat_tcp_fd_count,
                idle_skipped_count);
    }
}

void *pthread_health() {
    int res;
    char dt[DT_LOG_SIZE];
    int i = 0;
    uint64_t cid = get_new_cid();
    bool check_drive_free_is_ok;

    for (;;) {
        res = getpgid(parent_pid);

        if (res < 0) {
            stop_write = 1;
            fprintf(stderr, "%s FATAL [HEALTH=%015lu] PARENT %d HAS GONE (%d), SUICIDE\n", curr_dt(dt), cid, parent_pid, res);
            graceful_exit(0);
        }
        sleep(1);

        if (i++ == h_count) {
            i = 0;
            fprintf(stderr, "%s INFO [HEALTH=%015lu] Parent %d check\n", curr_dt(dt), cid, parent_pid);
            log_current_process_tcp_fds(cid);
            check_drive_free_is_ok = true;

            for (int j = 0; j < curr_drv_arr; j++) {
                if (!check_drive_free(drv_arr[j], cid))
                    check_drive_free_is_ok = false;
            }

            if (check_drive_free_is_ok)
                stop_write = 0;
            else
                stop_write = 1;
        }
    }
}


static uint64_t flush_monotonic_time_ms(void) {
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
        return 0;
    return (uint64_t)ts.tv_sec * 1000U + (uint64_t)ts.tv_nsec / 1000000U;
}


static bool try_get_flush_region_lock(region_info_t *r_info, table_handler_t *t_handler) {
    if (pthread_mutex_trylock(&r_info->rlock) != 0)
        return false;
    pthread_mutex_lock(&r_info->control_mutex);

    if (r_info->is_freed || r_info->r_lock_id != 0 || r_info->waiting_count_lock != 0) {
        pthread_mutex_unlock(&r_info->control_mutex);
        pthread_mutex_unlock(&r_info->rlock);
        return false;
    }
    r_info->r_lock_id = FLUSH_REGION_LOCK;
    r_info->curr_cid_lock = t_handler->cid;
    pthread_mutex_unlock(&r_info->control_mutex);
    return true;
}

static bool write_4K_buffer_is_due(region_info_t *r_info, uint64_t now_ms, uint64_t flush_age_ms) {
    if (r_info == NULL || r_info->is_freed || r_info->write_4K_buff_used == 0)
        return false;
    uint64_t buffered_since = atomic_load(&r_info->write_4K_buffered_since_ms);
    return buffered_since != 0 && now_ms >= buffered_since && now_ms - buffered_since >= flush_age_ms;
}

static void flush_due_write_4K_buffer(region_info_t *r_info, region_info_t *lock_owner, uint64_t now_ms, uint64_t flush_age_ms, uint64_t cid) {
    if (!write_4K_buffer_is_due(r_info, now_ms, flush_age_ms))
        return;

    if (log_level > 2) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s TRACE [HEALTH=%015lu] Ready to flush 4K buff %s under %s lock, fd=%d, offset=%lu\n", curr_dt(dt), cid, r_info->uuid, lock_owner->uuid,
                r_info->write_4K_fd, r_info->write_4K_file_offset);
    }
    flush_write_4K_buffer(r_info, cid);
}

void *pthread_flush_write_4K_buffers(void *arg) {
    (void)arg;
    const uint64_t flush_age_ms = 100;
    const struct timespec poll_interval = {.tv_sec = 0, .tv_nsec = 100000000L};
    uint64_t cid = get_new_cid();
    table_handler_t *t_handler;

    while ((t_handler = create_t_handler(0)) == NULL)
        sleep(1);
    t_handler->cid = cid;

    while (atomic_load(&is_shutting_down) == 0) {
        nanosleep(&poll_interval, NULL);

        if (atomic_load(&is_shutting_down) != 0)
            break;
        uint64_t now_ms = flush_monotonic_time_ms();
        pthread_mutex_lock(&tables_lock);

        for (int t = 0; t < table_count_curr; t++) {
            table_info_t *t_info = table_info[t];

            if (pthread_mutex_trylock(&t_info->tlock) != 0)
                continue;

            if (t_info->t_lock_id != 0) {
                pthread_mutex_unlock(&t_info->tlock);
                continue;
            }
            t_info->t_lock_id = FLUSH_TABLE_LOCK;
            pthread_mutex_unlock(&tables_lock);

            for (int r = 0; r < t_info->region_count_curr; r++) {
                region_info_t *r_info = t_info->region_info[r];

                if (!try_get_flush_region_lock(r_info, t_handler))
                    continue;

                if (atomic_load(&r_info->has_parent_replica)) {
                    do_r_unlock(r_info, 129, false, t_handler);
                    continue;
                }
                flush_due_write_4K_buffer(r_info, r_info, now_ms, flush_age_ms, cid);

                if (r_info->is_leader && r_info->replica != NULL) {
                    region_info_t *replica1 = r_info->replica->r_info1;
                    region_info_t *replica2 = r_info->replica->r_info2;

                    if (replica1 != NULL && replica1 != r_info && atomic_load(&replica1->has_parent_replica)) {
                        flush_due_write_4K_buffer(replica1, r_info, now_ms, flush_age_ms, cid);
                    }

                    if (replica2 != NULL && replica2 != r_info && replica2 != replica1 && atomic_load(&replica2->has_parent_replica)) {
                        flush_due_write_4K_buffer(replica2, r_info, now_ms, flush_age_ms, cid);
                    }
                }

                if (now_ms != 0)
                    refresh_region_memory_locked(r_info, now_ms);
                do_r_unlock(r_info, 129, false, t_handler);
            }
            do_t_unlock(t_info, 31, false, cid);
            pthread_mutex_lock(&tables_lock);

            if (atomic_load(&is_shutting_down) != 0)
                break;
        }
        pthread_mutex_unlock(&tables_lock);
    }
    free_t_handler(t_handler);
    return NULL;
}


static bool shutdown_sync_close_fd(region_info_t *r_info, int volatile *fd_slot, int buffered_fd, bool flush_succeeded, uint64_t cid) {
    int fd = *fd_slot;

    if (fd < 0)
        return true;
    char dt[DT_LOG_SIZE];
    const char *uuid = r_info->uuid != NULL ? r_info->uuid : "(null)";

    if (!flush_succeeded && fd == buffered_fd) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown keeps fd=%d open for region %s because write-4K flush failed\n", curr_dt(dt), cid, fd, uuid);
        return false;
    }
    int sync_result;

    do {
        sync_result = fsync(fd);
    } while (sync_result != 0 && errno == EINTR);

    if (sync_result != 0) {
        int saved_errno = errno;
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown fsync failed for fd=%d region %s: %s (errno: %d)\n", curr_dt(dt), cid, fd, uuid, strerror(saved_errno), saved_errno);
        return false;
    }
    int close_result = close(fd);
    int saved_errno = errno;
    *fd_slot = -1;

    if (r_info->write_4K_fd == fd)
        r_info->write_4K_fd = -1;

    if (close_result != 0) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown close failed for fd=%d region %s: %s (errno: %d)\n", curr_dt(dt), cid, fd, uuid, strerror(saved_errno), saved_errno);
        return false;
    }
    return true;
}


bool shutdown_close_locked_region(struct region_info_t *r_info, uint64_t cid) {
    if (r_info == NULL)
        return false;
    char dt[DT_LOG_SIZE];
    const char *uuid = r_info->uuid != NULL ? r_info->uuid : "(null)";

    if (r_info->r_idx_b == NULL) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown cannot close region %s because r_idx_b is NULL\n", curr_dt(dt), cid, uuid);
        return false;
    }
    int max_idx_b = atomic_load(&r_info->max_idx_b);

    if (max_idx_b < 0 || max_idx_b > BLOCK_MAX_SIZE_M) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown cannot close region %s with max_idx_b=%d\n", curr_dt(dt), cid, uuid, max_idx_b);
        return false;
    }

    for (int bs = 0; bs < max_idx_b; bs++) {
        if (r_info->r_idx_b[bs] == NULL) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown cannot close region %s because r_idx_b[%d] is NULL\n", curr_dt(dt), cid, uuid, bs);
            return false;
        }
    }

    if (r_info->r_idx_b[BS_LOB] == NULL) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown cannot close region %s because LOB index is NULL\n", curr_dt(dt), cid, uuid);
        return false;
    }

    for (int attempt = 0; atomic_load(&r_info->is_write) != 0; attempt++) {
        if (attempt >= SHUTDOWN_IS_WRITE_WAIT_TRIES) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown timed out waiting is_write for region %s\n", curr_dt(dt), cid, uuid);
            return false;
        }
        usleep(100);
    }
    bool result = true;
    int buffered_fd = r_info->write_4K_fd;
    bool flush_succeeded = flush_write_4K_buffer_on_exit(r_info);

    if (!flush_succeeded)
        result = false;

    for (int bs = 0; bs < max_idx_b; bs++) {
        if (!shutdown_sync_close_fd(r_info, &r_info->r_idx_b[bs]->fd[FBP], buffered_fd, flush_succeeded, cid))
            result = false;
    }

    if (!shutdown_sync_close_fd(r_info, &r_info->r_idx_b[BS_LOB]->fd[FBP], buffered_fd, flush_succeeded, cid))
        result = false;
    return result;
}

bool shutdown_close_locked_group(struct region_info_t *leader, uint64_t cid) {
    if (leader == NULL)
        return false;
    char dt[DT_LOG_SIZE];

    if (atomic_load(&leader->has_parent_replica)) {
        atomic_store(&leader->shutdown_region_state, SHUTDOWN_REGION_NEED_MARK);
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown group called for child region %s\n", curr_dt(dt), cid, leader->uuid != NULL ? leader->uuid : "(null)");
        return false;
    }
    region_info_t *members[3] = {leader, NULL, NULL};

    if (leader->replica != NULL) {
        members[1] = leader->replica->r_info1;
        members[2] = leader->replica->r_info2;
    }
    bool result = true;

    for (int i = 0; i < 3; i++) {
        region_info_t *member = members[i];

        if (member == NULL)
            continue;

        if ((i > 0 && member == leader) || (i == 2 && member == members[1]))
            continue;

        if (i > 0 && !atomic_load(&member->has_parent_replica)) {
            atomic_store(&member->shutdown_region_state, SHUTDOWN_REGION_NEED_MARK);
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown local replica %s has no parent flag\n", curr_dt(dt), cid, member->uuid != NULL ? member->uuid : "(null)");
            result = false;
            continue;
        }
        bool member_result = shutdown_close_locked_region(member, cid);
        atomic_store(&member->shutdown_region_state, member_result ? SHUTDOWN_REGION_CLOSED : SHUTDOWN_REGION_NEED_MARK);

        if (!member_result)
            result = false;
    }
    return result;
}

static bool shutdown_try_tables_lock(uint64_t cid) {
    for (int attempt = 0; attempt < SHUTDOWN_TABLES_LOCK_TRIES; attempt++) {
        int lock_result = pthread_mutex_trylock(&tables_lock);

        if (lock_result == 0)
            return true;

        if (lock_result != EBUSY) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown cannot lock tables_lock: %s (error: %d)\n", curr_dt(dt), cid, strerror(lock_result), lock_result);
            return false;
        }
        usleep(100);
    }
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown cannot lock tables_lock after %d attempts\n", curr_dt(dt), cid, SHUTDOWN_TABLES_LOCK_TRIES);
    return false;
}

static bool shutdown_lock_tables(int table_count, uint64_t cid) {
    bool result = true;
    int unlocked_tables = table_count;

    for (int pass = 0; pass < SHUTDOWN_TABLE_LOCK_PASSES && unlocked_tables > 0; pass++) {
        unlocked_tables = 0;

        for (int t = 0; t < table_count; t++) {
            table_info_t *t_info = table_info[t];

            if (t_info == NULL) {
                unlocked_tables++;
                continue;
            }

            if (atomic_load(&t_info->t_lock_id) == SHUTDOWN_TABLE_LOCK)
                continue;
            int lock_result = pthread_mutex_trylock(&t_info->tlock);

            if (lock_result == 0) {
                int old_lock_id = atomic_load(&t_info->t_lock_id);

                if (old_lock_id != 0) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown locked table %s with nonzero t_lock_id=%d\n", curr_dt(dt), cid,
                            t_info->fullname != NULL ? t_info->fullname : "(null)", old_lock_id);
                    result = false;
                }
                atomic_store(&t_info->t_lock_id, SHUTDOWN_TABLE_LOCK);
                continue;
            }
            unlocked_tables++;

            if (lock_result != EBUSY)
                result = false;
        }

        if (unlocked_tables > 0)
            usleep(1000);
    }

    for (int t = 0; t < table_count; t++) {
        table_info_t *t_info = table_info[t];

        if (t_info == NULL || atomic_load(&t_info->t_lock_id) != SHUTDOWN_TABLE_LOCK) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown cannot lock table position=%d name=%s\n", curr_dt(dt), cid, t,
                    t_info != NULL && t_info->fullname != NULL ? t_info->fullname : "(null)");
            result = false;
        }
    }
    return result;
}

static bool shutdown_wait_recovery_drops(uint64_t cid) {
    for (int attempt = 0; attempt < SHUTDOWN_RECOVERY_DROP_WAIT_TRIES; attempt++) {
        if (atomic_load(&recovery_drop_inflight) == 0)
            return true;
        usleep(100);
    }
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown recovery drop barrier timed out, inflight=%u\n", curr_dt(dt), cid, atomic_load(&recovery_drop_inflight));
    return false;
}


static bool shutdown_wake_remote_replica(region_info_t *r_info, uint8_t replica_num, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    int fd = -1;
    fd = replica_num == 1 ? atomic_load(&r_info->replica->replica1_sock_fd) : atomic_load(&r_info->replica->replica2_sock_fd);

    if (fd >= 0 && shutdown(fd, SHUT_RDWR) != 0 && errno != ENOTCONN && errno != EBADF) {
        int saved_errno = errno;
        fprintf(stderr, "%s WARN [HEALTH=%015lu] Shutdown failed to wake replica=%u fd=%d region %s: %s (errno: %d)\n", curr_dt(dt), cid, replica_num, fd,
                r_info->uuid != NULL ? r_info->uuid : "(null)", strerror(saved_errno), saved_errno);
        return false;
    }
    return true;
}


static bool shutdown_prepare_region(region_info_t *r_info, table_handler_t *t_handler) {
    if (r_info == NULL)
        return false;
    bool result = true;
    char dt[DT_LOG_SIZE];
    uint64_t cid = t_handler->cid;
    atomic_store(&r_info->stop, true);
    atomic_store(&r_info->shutdown_region_state, SHUTDOWN_REGION_IDLE);

    if (atomic_load(&r_info->is_freed))
        return true;

    if (atomic_load(&r_info->is_replicate)) {
        if (atomic_load(&r_info->recovery_path_ready) && r_info->r_fname != NULL)
            set_region_recovery_info(r_info, 1L, t_handler);
        else {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown cannot mark active replication for region %s because recovery path is not ready\n", curr_dt(dt), cid,
                    r_info->uuid != NULL ? r_info->uuid : "(null)");
            result = false;
        }
    }

    if (atomic_load(&r_info->has_parent_replica))
        return result;

    if (r_info->replica == NULL) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown found region %s without replica state\n", curr_dt(dt), cid, r_info->uuid != NULL ? r_info->uuid : "(null)");
        return false;
    }
    int replica_num = atomic_load(&r_info->replica->replica_num);

    if (replica_num < 0 || replica_num > 2) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown found invalid replica_num=%d for region %s\n", curr_dt(dt), cid, replica_num,
                r_info->uuid != NULL ? r_info->uuid : "(null)");
        return false;
    }

    if (replica_num >= 1 && r_info->replica->r_info1 == NULL && !shutdown_wake_remote_replica(r_info, 1, cid))
        result = false;

    if (replica_num >= 2 && r_info->replica->r_info2 == NULL && !shutdown_wake_remote_replica(r_info, 2, cid))
        result = false;
    return result;
}

static bool shutdown_snapshot_region_count(table_info_t *t_info, uint32_t *region_count, uint64_t cid) {
    *region_count = 0;

    if (t_info == NULL)
        return false;
    int count = atomic_load(&t_info->region_count_curr);
    int total = atomic_load(&t_info->region_count_total);

    if (count < 0 || total < 0 || count > total || (count > 0 && t_info->region_info == NULL)) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown found invalid region topology for table %s: count=%d total=%d region_info=%p\n", curr_dt(dt), cid,
                t_info->fullname != NULL ? t_info->fullname : "(null)", count, total, (void *)t_info->region_info);
        return false;
    }
    *region_count = (uint32_t)count;
    return true;
}

static bool shutdown_region_belongs_to_table(shutdown_table_arg_t *arg, region_info_t *member) {
    if (member == NULL)
        return false;

    for (uint32_t r = 0; r < arg->region_count; r++) {
        if (arg->t_info->region_info[r] == member)
            return true;
    }
    return false;
}


static bool shutdown_validate_table_topology(shutdown_table_arg_t *arg) {
    bool result = true;
    char dt[DT_LOG_SIZE];

    for (uint32_t r = 0; r < arg->region_count; r++) {
        region_info_t *r_info = arg->t_info->region_info[r];

        if (r_info == NULL) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown found NULL region at table %s position=%u\n", curr_dt(dt), arg->cid,
                    arg->t_info->fullname != NULL ? arg->t_info->fullname : "(null)", r);
            result = false;
            continue;
        }

        if (atomic_load(&r_info->is_freed))
            continue;

        if (r_info->replica == NULL) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown found region %s without replica state\n", curr_dt(dt), arg->cid, r_info->uuid != NULL ? r_info->uuid : "(null)");
            result = false;
            continue;
        }
        int replica_num = atomic_load(&r_info->replica->replica_num);

        if (replica_num < 0 || replica_num > 2) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown found invalid replica_num=%d for region %s\n", curr_dt(dt), arg->cid, replica_num,
                    r_info->uuid != NULL ? r_info->uuid : "(null)");
            result = false;
        }

        if (atomic_load(&r_info->has_parent_replica))
            continue;
        region_info_t *replica1 = r_info->replica->r_info1;
        region_info_t *replica2 = r_info->replica->r_info2;

        if ((replica1 != NULL && replica_num < 1) || (replica2 != NULL && replica_num < 2)) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown found local replica outside replica_num=%d for region %s\n", curr_dt(dt), arg->cid, replica_num,
                    r_info->uuid != NULL ? r_info->uuid : "(null)");
            result = false;
        }

        if (replica1 != NULL &&
            (replica1 == r_info || !shutdown_region_belongs_to_table(arg, replica1) || atomic_load(&replica1->is_freed) || !atomic_load(&replica1->has_parent_replica))) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown found invalid local replica1 topology for region %s\n", curr_dt(dt), arg->cid,
                    r_info->uuid != NULL ? r_info->uuid : "(null)");
            result = false;
        }

        if (replica2 != NULL && (replica2 == r_info || replica2 == replica1 || !shutdown_region_belongs_to_table(arg, replica2) || atomic_load(&replica2->is_freed) ||
                                 !atomic_load(&replica2->has_parent_replica))) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown found invalid local replica2 topology for region %s\n", curr_dt(dt), arg->cid,
                    r_info->uuid != NULL ? r_info->uuid : "(null)");
            result = false;
        }
    }

    for (uint32_t r = 0; r < arg->region_count; r++) {
        region_info_t *child = arg->t_info->region_info[r];

        if (child == NULL || atomic_load(&child->is_freed) || !atomic_load(&child->has_parent_replica))
            continue;
        uint32_t parent_count = 0;

        for (uint32_t p = 0; p < arg->region_count; p++) {
            region_info_t *parent = arg->t_info->region_info[p];

            if (parent == NULL || atomic_load(&parent->is_freed) || atomic_load(&parent->has_parent_replica) || parent->replica == NULL)
                continue;

            if (parent->replica->r_info1 == child)
                parent_count++;

            if (parent->replica->r_info2 == child)
                parent_count++;
        }

        if (parent_count != 1) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown local replica %s has parent_count=%u in table %s\n", curr_dt(dt), arg->cid,
                    child->uuid != NULL ? child->uuid : "(null)", parent_count, arg->t_info->fullname != NULL ? arg->t_info->fullname : "(null)");
            result = false;
        }
    }
    return result;
}

static bool shutdown_claim_region_lock(region_info_t *r_info, uint64_t cid) {
    int lock_result = EBUSY;

    for (int attempt = 0; attempt < SHUTDOWN_CONTROL_LOCK_TRIES; attempt++) {
        lock_result = pthread_mutex_trylock(&r_info->control_mutex);

        if (lock_result == 0)
            break;

        if (lock_result != EBUSY)
            break;
        usleep(100);
    }
    char dt[DT_LOG_SIZE];

    if (lock_result != 0) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown cannot lock control_mutex for region %s: %s (error: %d)\n", curr_dt(dt), cid,
                r_info->uuid != NULL ? r_info->uuid : "(null)", strerror(lock_result), lock_result);
        return false;
    }
    int old_lock_id = atomic_load(&r_info->r_lock_id);
    int waiting_count = atomic_load(&r_info->waiting_count_lock);

    if (old_lock_id != 0 || waiting_count != 0)
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown acquired rlock for region %s with r_lock_id=%d, waiting_count_lock=%d, owner=%lu\n", curr_dt(dt), cid,
                r_info->uuid != NULL ? r_info->uuid : "(null)", old_lock_id, waiting_count, atomic_load(&r_info->curr_cid_lock));
    atomic_store(&r_info->r_lock_id, SHUTDOWN_REGION_LOCK);
    atomic_store(&r_info->curr_cid_lock, cid);
    pthread_mutex_unlock(&r_info->control_mutex);
    return true;
}


static bool shutdown_process_table_pass(shutdown_table_arg_t *arg, bool retry_only) {
    bool has_retry = false;
    char dt[DT_LOG_SIZE];

    for (uint32_t r = 0; r < arg->region_count; r++) {
        region_info_t *r_info = arg->t_info->region_info[r];

        if (r_info == NULL)
            continue;
        atomic_store(&r_info->stop, true);

        if (atomic_load(&r_info->is_freed) || atomic_load(&r_info->has_parent_replica))
            continue;
        int state = atomic_load(&r_info->shutdown_region_state);

        if (retry_only) {
            if (state != SHUTDOWN_REGION_RETRY_LOCK)
                continue;
        } else if (state != SHUTDOWN_REGION_IDLE) {
            continue;
        }
        int lock_result = pthread_mutex_trylock(&r_info->rlock);

        if (lock_result == EBUSY) {
            atomic_store(&r_info->shutdown_region_state, SHUTDOWN_REGION_RETRY_LOCK);
            has_retry = true;
            continue;
        }

        if (lock_result != 0) {
            atomic_store(&r_info->shutdown_region_state, SHUTDOWN_REGION_NEED_MARK);
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown cannot lock region %s: %s (error: %d)\n", curr_dt(dt), arg->cid, r_info->uuid != NULL ? r_info->uuid : "(null)",
                    strerror(lock_result), lock_result);
            continue;
        }

        if (!shutdown_claim_region_lock(r_info, arg->cid)) {
            atomic_store(&r_info->shutdown_region_state, SHUTDOWN_REGION_NEED_MARK);
            continue;
        }
        shutdown_close_locked_group(r_info, arg->cid);
    }
    return has_retry;
}

static void *shutdown_table_worker(void *arg_ptr) {
    shutdown_table_arg_t *arg = arg_ptr;

    if (!shutdown_process_table_pass(arg, false))
        return NULL;
    usleep(SHUTDOWN_SECOND_PASS_DELAY_US);

    if (!shutdown_process_table_pass(arg, true))
        return NULL;
    usleep(SHUTDOWN_THIRD_PASS_DELAY_US);
    shutdown_process_table_pass(arg, true);
    return NULL;
}


static bool shutdown_mark_region(region_info_t *r_info, table_handler_t *t_handler) {
    if (r_info == NULL)
        return false;

    if (atomic_load(&r_info->is_freed))
        return true;
    char dt[DT_LOG_SIZE];
    atomic_store(&r_info->shutdown_region_state, SHUTDOWN_REGION_NEED_MARK);

    if (atomic_load(&r_info->recovery_path_ready) == 0 || r_info->r_fname == NULL) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown cannot create recovery marker for region %s because recovery path is not ready\n", curr_dt(dt), t_handler->cid,
                r_info->uuid != NULL ? r_info->uuid : "(null)");
        return false;
    }
    set_region_recovery_info(r_info, 1L, t_handler);
    uint64_t marker_ts = 0;

    if (!shutdown_recovery_marker_valid(r_info, &marker_ts)) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown cannot validate recovery marker for region %s\n", curr_dt(dt), t_handler->cid,
                r_info->uuid != NULL ? r_info->uuid : "(null)");
        return false;
    }
    atomic_store(&r_info->shutdown_region_state, SHUTDOWN_REGION_MARKED);
    return true;
}


static bool shutdown_recover_table(shutdown_table_arg_t *arg, table_handler_t *t_handler) {
    bool topology_ok = arg->result;
    bool coverage_ok = true;

    for (uint32_t r = 0; r < arg->region_count; r++) {
        region_info_t *r_info = arg->t_info->region_info[r];

        if (r_info == NULL) {
            coverage_ok = false;
            continue;
        }

        if (atomic_load(&r_info->is_freed))
            continue;
        int state = atomic_load(&r_info->shutdown_region_state);

        if (state != SHUTDOWN_REGION_CLOSED && state != SHUTDOWN_REGION_MARKED && !shutdown_mark_region(r_info, t_handler))
            coverage_ok = false;
    }
    arg->result = topology_ok && coverage_ok;
    return arg->result;
}


bool shutdown_regions_parallel(table_handler_t *t_handler) {
    if (t_handler == NULL)
        return false;
    char dt[DT_LOG_SIZE];
    uint64_t cid = t_handler->cid;

    if (atomic_load(&is_shutting_down) == 0 || t_handler->is_replica_client) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] shutdown_regions_parallel called outside the health shutdown coordinator\n", curr_dt(dt), cid);
        return false;
    }

    if (!shutdown_try_tables_lock(cid))
        return false;
    int table_count = atomic_load(&table_count_curr);

    if (table_count < 0 || (uint32_t)table_count > table_count_total || (table_count > 0 && table_info == NULL)) {
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown found invalid table topology: count=%d total=%u table_info=%p\n", curr_dt(dt), cid, table_count, table_count_total,
                (void *)table_info);
        return false;
    }
    bool result = shutdown_lock_tables(table_count, cid);

    if (!shutdown_wait_recovery_drops(cid))
        result = false;

    for (int t = 0; t < table_count; t++) {
        table_info_t *t_info = table_info[t];

        if (t_info == NULL || atomic_load(&t_info->t_lock_id) != SHUTDOWN_TABLE_LOCK)
            continue;
        uint32_t region_count = 0;

        if (!shutdown_snapshot_region_count(t_info, &region_count, cid)) {
            result = false;
            continue;
        }

        for (uint32_t r = 0; r < region_count; r++) {
            if (!shutdown_prepare_region(t_info->region_info[r], t_handler))
                result = false;
        }
    }
    int worker_wait_left = SHUTDOWN_WORKER_WAIT_TRIES;

    while (atomic_load(&worker_count) > 0 && worker_wait_left > 0) {
        usleep(1000);
        worker_wait_left--;
    }

    if (atomic_load(&worker_count) > 0)
        fprintf(stderr, "%s WARN [HEALTH=%015lu] Shutdown continues with worker_count=%d after bounded wait\n", curr_dt(dt), cid, atomic_load(&worker_count));
    static pthread_t shutdown_threads[MAX_SHUTDOWN_TABLE_THREADS];
    static shutdown_table_arg_t shutdown_args[MAX_SHUTDOWN_TABLE_THREADS];
    int table_pos = 0;

    while (table_pos < table_count) {
        uint32_t batch_count = 0;
        bool join_failed = false;

        while (table_pos < table_count && batch_count < MAX_SHUTDOWN_TABLE_THREADS) {
            table_info_t *t_info = table_info[table_pos++];

            if (t_info == NULL || atomic_load(&t_info->t_lock_id) != SHUTDOWN_TABLE_LOCK)
                continue;
            shutdown_table_arg_t *arg = &shutdown_args[batch_count++];
            memset(arg, 0, sizeof(*arg));
            arg->t_info = t_info;
            arg->cid = cid;
            arg->result = shutdown_snapshot_region_count(t_info, &arg->region_count, cid);

            if (arg->result)
                arg->result = shutdown_validate_table_topology(arg);

            if (!arg->result)
                result = false;
        }

        for (uint32_t i = 0; i < batch_count; i++) {
            shutdown_table_arg_t *arg = &shutdown_args[i];

            if (!arg->result)
                continue;
            int create_result = pthread_create(&shutdown_threads[i], NULL, shutdown_table_worker, arg);

            if (create_result == 0) {
                arg->thread_started = true;
                continue;
            }
            fprintf(stderr, "%s WARN [HEALTH=%015lu] Shutdown cannot create table worker for %s, use synchronous fallback: %s (error: %d)\n", curr_dt(dt), cid,
                    arg->t_info->fullname != NULL ? arg->t_info->fullname : "(null)", strerror(create_result), create_result);
            shutdown_table_worker(arg);
        }

        for (uint32_t i = 0; i < batch_count; i++) {
            shutdown_table_arg_t *arg = &shutdown_args[i];

            if (!arg->thread_started)
                continue;
            int join_result = pthread_join(shutdown_threads[i], NULL);

            if (join_result == 0) {
                arg->thread_started = false;
                continue;
            }
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Shutdown cannot join table worker for %s: %s (error: %d)\n", curr_dt(dt), cid,
                    arg->t_info->fullname != NULL ? arg->t_info->fullname : "(null)", strerror(join_result), join_result);
            arg->result = false;
            result = false;
            join_failed = true;
        }

        for (uint32_t i = 0; i < batch_count; i++) {
            shutdown_table_arg_t *arg = &shutdown_args[i];

            if (arg->thread_started) {
                result = false;
                continue;
            }

            if (!shutdown_recover_table(arg, t_handler))
                result = false;
        }

        if (join_failed)
            break;
    }
    fprintf(stderr, "%s INFO [HEALTH=%015lu] Shutdown regions processed: tables=%d, worker_count=%d, result=%d\n", curr_dt(dt), cid, table_count, atomic_load(&worker_count),
            result);
    return result;
}


bool close_fd(region_info_t *r_info, bool is_locked, bool cache_lock_held, uint64_t cid) {
    invalidate_record_gc(r_info);
    bool result = true;

    while (atomic_load(&r_info->is_write)) {
        usleep(100);
    }
    int buffered_fd = r_info->write_4K_fd;
    bool flush_succeeded = flush_write_4K_buffer_on_exit(r_info);

    if (!flush_succeeded)
        result = false;

    for (int bs = 0; bs < r_info->max_idx_b; bs++) {
        int fd = r_info->r_idx_b[bs]->fd[FBP];

        if (fd > 0 && fsync(fd) != 0)
            result = false;

        if (is_locked) {
            if (fd > 0) {
                if (!flush_succeeded && fd == buffered_fd)
                    continue;

                if (close(fd) != 0)
                    result = false;

                if (cache_lock_held)
                    unbind_data_000_fd(r_info, fd, cid);
                r_info->r_idx_b[bs]->fd[FBP] = -1;
            }
        }
    }
    return result;
}

uint32_t stop_all_regions(bool get_lock, uint32_t closed_socks_count, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    struct linger lo = {1, 0};
    struct table_info_t *t_info;
    struct region_info_t *r_info;
    uint32_t region_count_curr;
    uint32_t region_count_total = 0;
    uint32_t locked_region_count = 0;
    int fd;

    for (int t = 0; t < table_count_curr; t++) {
        t_info = table_info[t]; // TODO Если в этот момент начнет выполняться resize_table практически нулевая
        region_count_curr = t_info->region_count_curr;

        for (int r = 0; r < region_count_curr; r++) {
            r_info = t_info->region_info[r];
            bool is_locked = true;

            if (get_lock) {
                region_count_total++;

                if (pthread_mutex_trylock(&r_info->rlock) != 0) {
                    locked_region_count++;
                    is_locked = false;
                }
            }

            if (!r_info->is_freed) {
                invalidate_replicas_idle_commit(r_info);
                invalidate_record_gc(r_info);

                if (r_info->replica->r_info1 == NULL) {
                    if (r_info->replica->replica1_sock_fd > 0) {
                        setsockopt(r_info->replica->replica1_sock_fd, SOL_SOCKET, SO_LINGER, &lo, sizeof(lo));
                        close(r_info->replica->replica1_sock_fd);
                        r_info->replica->replica1_sock_fd = -1;
                        closed_socks_count++;
                    }
                } else if (!close_fd(r_info->replica->r_info1, is_locked, false, t_handler->cid))
                    set_region_recovery_info(r_info, 1000006L, t_handler);

                if (r_info->replica->r_info2 == NULL) {
                    if (r_info->replica->replica2_sock_fd > 0) {
                        setsockopt(r_info->replica->replica2_sock_fd, SOL_SOCKET, SO_LINGER, &lo, sizeof(lo));
                        close(r_info->replica->replica2_sock_fd);
                        r_info->replica->replica2_sock_fd = -1;
                        closed_socks_count++;
                    }
                } else if (!close_fd(r_info->replica->r_info2, is_locked, false, t_handler->cid))
                    set_region_recovery_info(r_info, 1000007L, t_handler);

                if (!close_fd(r_info, is_locked, get_lock && is_locked, t_handler->cid))
                    set_region_recovery_info(r_info, 1000008L, t_handler);

                if (get_lock && is_locked)
                    r_info->data_000_fd = -1;
            }
        }
    }

    if (get_lock) {
        if (region_count_total - locked_region_count > 0)
            fprintf(stderr, "%s WARN [RS_PROCESSOR] Cannot get lock for %d regions, total %d\n", curr_dt(dt), region_count_total - locked_region_count, region_count_total);
        else
            fprintf(stderr, "%s INFO [RS_PROCESSOR] Locked all %d regions\n", curr_dt(dt), region_count_total);
    }
    return closed_socks_count;
}


bool process_admin_replicas_data_chk(region_info_t *r_info, table_handler_t *t_handler) {
    if (r_info == NULL || t_handler == NULL || r_info->curr_cid_lock != t_handler->cid ||
        (r_info->r_lock_id != REGION_ADMIN_START_STOP_LOCK && r_info->r_lock_id != TRY_REGION_ADMIN_START_STOP_LOCK)) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [HEALTH] Administrative data check requires ownership of the START/STOP region lock\n", curr_dt(dt));
        return false;
    }
    region_info_t *previous = t_handler->admin_data_check_region;
    t_handler->admin_data_check_region = r_info;
    bool result = procces_replicas_data_chk(r_info, t_handler);
    t_handler->admin_data_check_region = previous;
    return result;
}

bool procces_replicas_data_chk(region_info_t *r_info, table_handler_t *t_handler) {
    invalidate_record_gc(r_info);
    char dt[DT_LOG_SIZE];
    uint64_t cid = t_handler->cid;
    fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Data check %s begin\n", curr_dt(dt), cid, r_info->uuid);
    bool result = true;

    if (r_info->files_list != NULL)
        free_files_list(r_info->files_list);
    r_info->files_list = get_files_list(r_info, true, t_handler);

    if (r_info->files_list == NULL) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Cannot create final files list for data check of %s\n", curr_dt(dt), cid, r_info->uuid);
        finish_thread(r_info, 76, true, t_handler);
    }

    if (r_info->replica == NULL || r_info->replica->replica_num < 0 || r_info->replica->replica_num > 2) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Incorrect replica state for %s data check\n", curr_dt(dt), cid, r_info->uuid);
        result = false;
        goto cleanup;
    }

    if (r_info->replica->replica_num > 0) {
        if (!check_replica(r_info, 1, t_handler))
            result = false;

        if (r_info->is_freed) {
            result = false;
            goto cleanup;
        }
    }

    if (r_info->replica->replica_num > 1) {
        if (!check_replica(r_info, 2, t_handler))
            result = false;

        if (r_info->is_freed) {
            result = false;
            goto cleanup;
        }
    }
cleanup:
    if (r_info->files_list != NULL)
        free_files_list(r_info->files_list);
    r_info->files_list = NULL;
    fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Data check done for region %s, result=%d\n", curr_dt(dt), cid, r_info->uuid, result);
    return result;
}

static bool del_r_info_chk_element_locked(region_info_t *r_info_chk) {
    int replica_count = replica_need_data_chk_count;

    for (int j = 0; j < replica_count; j++) {
        if (r_info_replicas_chk[j] == r_info_chk) {
            r_info_replicas_chk[j] = r_info_replicas_chk[replica_count - 1];
            r_info_replicas_chk[replica_count - 1] = NULL;
            replica_need_data_chk_count--;
            return true;
        }
    }
    return false;
}


void del_r_info_chk_element(region_info_t *r_info_chk) {
    pthread_mutex_lock(&r_info_chk_mutex);
    int replica_count = replica_need_data_chk_count;

    if (replica_count < 0 || (uint32_t)replica_count > replica_need_data_chk_capacity || (replica_count > 0 && r_info_replicas_chk == NULL)) {
        fprintf(stderr, "%s ERROR [HEALTH] Invalid replicas data check array state count=%d, capacity=%u, array=%p\n", curr_dt((char[DT_LOG_SIZE]){0}), replica_count,
                replica_need_data_chk_capacity, (void *)r_info_replicas_chk);
        pthread_mutex_unlock(&r_info_chk_mutex);
        return;
    }
    del_r_info_chk_element_locked(r_info_chk);
    pthread_mutex_unlock(&r_info_chk_mutex);
}


void process_r_info_replicas_chk(table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    uint64_t cid = t_handler->cid;
    pthread_mutex_lock(&r_info_chk_mutex);
    int replica_count = replica_need_data_chk_count;

    if (replica_count < 0 || (uint32_t)replica_count > replica_need_data_chk_capacity || (replica_count > 0 && r_info_replicas_chk == NULL)) {
        pthread_mutex_unlock(&r_info_chk_mutex);
        fprintf(stderr, "%s ERROR [HEALTH=%015lu] Invalid replicas data check array state count=%d, capacity=%u, array=%p\n", curr_dt(dt), cid, replica_count,
                replica_need_data_chk_capacity, (void *)r_info_replicas_chk);
        return;
    }

    if (replica_count == 0) {
        pthread_mutex_unlock(&r_info_chk_mutex);
        return;
    }
    region_info_t *r_info_chk = NULL;

    for (int i = 0; i < replica_need_data_chk_count;) {
        r_info_chk = r_info_replicas_chk[i];

        if (r_info_chk == NULL) {
            fprintf(stderr, "%s WARN [HEALTH=%015lu] r_info_replicas_chk[%d] == NULL\n", curr_dt(dt), cid, i);
            del_r_info_chk_element_locked(NULL);
            continue;
        }

        if (pthread_mutex_trylock(&r_info_chk->rlock) != 0) {
            r_info_chk = NULL;
            i++;
            continue;
        }

        if (r_info_chk->is_freed || r_info_chk->status != STATUS_ASSIGN_REGION || r_info_chk->uuid == NULL || r_info_chk->replica == NULL) {
            del_r_info_chk_element_locked(r_info_chk);
            pthread_mutex_unlock(&r_info_chk->rlock);
            r_info_chk = NULL;
            continue;
        }

        if (r_info_chk->stop) {
            pthread_mutex_unlock(&r_info_chk->rlock);
            r_info_chk = NULL;
            i++;
            continue;
        }

        if (r_info_chk->r_lock_id != 0) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Replica %s r_lock_id=%d\n", curr_dt(dt), cid, r_info_chk->uuid, r_info_chk->r_lock_id);
            r_info_chk->stop = true;
            r_info_chk->bad_data = true;
        }
        r_info_chk->r_lock_id = REGION_HEALTH_LOCK;
        r_info_chk->curr_cid_lock = t_handler->cid;
        break;
    }
    pthread_mutex_unlock(&r_info_chk_mutex);

    if (r_info_chk == NULL)
        return;
    fprintf(stderr, "%s INFO [HEALTH=%015lu] Replicas to data check count %d\n", curr_dt(dt), cid, replica_count);
    fprintf(stderr, "%s INFO [HEALTH=%015lu] Selected to replicas data check %s\n", curr_dt(dt), cid, r_info_chk->uuid);
    int remaining_count;
    {
        bool result_data_check_and_fix = true;

        if (r_info_chk->replica->r_info1 != NULL) {
            if (r_info_chk->replica->r_info1->r_need_data_chk != 0) {
                fprintf(stderr, "%s INFO [HEALTH=%015lu] Local replica1 data check %s begin\n", curr_dt(dt), cid, r_info_chk->uuid);
                bool replica1_data_check_and_fix = false;
                msg_files_list_t *files_list = get_files_list(r_info_chk, true, t_handler);

                if (files_list != NULL) {
                    print_files_list(files_list, r_info_chk->uuid, t_handler->cid);
                    msg_files_list_t *files_list_replica = get_files_list(r_info_chk->replica->r_info1, true, t_handler);

                    if (files_list_replica != NULL) {
                        replica1_data_check_and_fix = compare_fix_files_lists(r_info_chk, 1, files_list, files_list_replica, t_handler);
                        free_files_list(files_list_replica);
                    }
                    free_files_list(files_list);
                }

                if (replica1_data_check_and_fix)
                    drop_region_recovery_info(r_info_chk->replica->r_info1, cid);
                else
                    result_data_check_and_fix = false;
            }
        } else {
            if (r_info_chk->replica->replica1_sock_fd == -1) {
                for (int bs = 0; bs < r_info_chk->max_idx_b; bs++) {
                    if (r_info_chk->r_idx_b[bs]->fd[FBP] == 0) {
                        // TODO WTF?
                        continue;
                    }
                    uint64_t file_chk_size = get_file_size(r_info_chk->r_idx_b[bs]->fname);

                    if (file_chk_size > USED_META) {
                        char buf_tmp[1];
                        replicas_writer(r_info_chk, buf_tmp, BLOCK_ARRAY_FIRST_POS, META_OFFSET, 0, false, FBP, true, t_handler);
                        close_socket(r_info_chk, 1, r_info_chk->replica->replica1_sock_fd);
                        ;
                        break;
                    }
                }
            }
        }

        if (r_info_chk->replica->replica_num > 1) {
            if (r_info_chk->replica->r_info2 != NULL) {
                if (r_info_chk->replica->r_info2->r_need_data_chk != 0) {
                    fprintf(stderr, "%s INFO [HEALTH=%015lu] Local replica1 data check %s begin\n", curr_dt(dt), cid, r_info_chk->uuid);
                    bool replica2_data_check_and_fix = false;
                    msg_files_list_t *files_list = get_files_list(r_info_chk, true, t_handler);

                    if (files_list != NULL) {
                        print_files_list(files_list, r_info_chk->uuid, t_handler->cid);
                        msg_files_list_t *files_list_replica = get_files_list(r_info_chk->replica->r_info2, true, t_handler);

                        if (files_list_replica != NULL) {
                            replica2_data_check_and_fix = compare_fix_files_lists(r_info_chk, 2, files_list, files_list_replica, t_handler);
                            free_files_list(files_list_replica);
                        }
                        free_files_list(files_list);
                    }

                    if (replica2_data_check_and_fix)
                        drop_region_recovery_info(r_info_chk->replica->r_info2, cid);
                    else
                        result_data_check_and_fix = false;
                }
            } else {
                if (r_info_chk->replica->replica2_sock_fd == -1) {
                    for (int bs = 0; bs < r_info_chk->max_idx_b; bs++) {
                        if (r_info_chk->r_idx_b[bs]->fd[FBP] == 0) {
                            // TODO WTF?
                            continue;
                        }
                        uint64_t file_chk_size = get_file_size(r_info_chk->r_idx_b[bs]->fname);

                        if (file_chk_size > USED_META) {
                            char buf_tmp[1];
                            replicas_writer(r_info_chk, buf_tmp, BLOCK_ARRAY_FIRST_POS, META_OFFSET, 0, false, FBP, true, t_handler);
                            close_socket(r_info_chk, 2, r_info_chk->replica->replica2_sock_fd);
                            ;
                            break;
                        }
                    }
                }
            }
        }

        if (result_data_check_and_fix)
            drop_region_recovery_info(r_info_chk, cid);
        pthread_mutex_lock(&r_info_chk_mutex);

        if (result_data_check_and_fix)
            del_r_info_chk_element_locked(r_info_chk);
        remaining_count = replica_need_data_chk_count;
        pthread_mutex_unlock(&r_info_chk_mutex);
        r_info_chk->r_lock_id_before = REGION_HEALTH_LOCK;
        r_info_chk->r_lock_id = 0;
        r_info_chk->curr_cid_lock = 0;
        pthread_mutex_unlock(&r_info_chk->rlock);
    }
    fprintf(stderr, "%s INFO [HEALTH=%015lu] End of replicas data check cycle, count=%d\n", curr_dt(dt), cid, remaining_count);
}


static void replicas_chk_restart(void *arg) {
    pthread_t *thr = (pthread_t *)arg;

    if (pthread_create(thr, NULL, pthread_replicas_chk, thr) != 0) {
        fprintf(stderr, "%s ERROR [HEALTH] Error pthread_create REPLICAS_CHECK %s\n", curr_dt((char[DT_LOG_SIZE]){0}), strerror(errno));
    }
}

void *pthread_replicas_chk(void *arg) {
    pthread_cleanup_push(replicas_chk_restart, arg);
    pthread_detach(pthread_self());
    uint64_t cid = get_new_cid();
    table_handler_t *t_handler;

    while ((t_handler = create_t_handler(0)) == NULL)
        sleep(1);
    t_handler->cid = cid;
    int count_replicas_chk_curr = 0;

    for (;;) {
        sleep(10);

        if (count_replicas_chk_curr >= replicas_chk_delay_sec) {
            process_r_info_replicas_chk(t_handler);
        }

        if (replica_need_data_chk_count > 0)
            count_replicas_chk_curr += 10;
    }
    pthread_cleanup_pop(1);
    return NULL;
}


static void safe_log(uint64_t cid, int priority, const char *fmt, ...) {
    char buf[1024];
    char line[1200];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    struct tm tm;
    localtime_r(&ts.tv_sec, &tm);
    char tsbuf[64];
    strftime(tsbuf, sizeof(tsbuf), "%Y-%m-%d %H:%M:%S", &tm);

    if (priority == LOG_INFO)
        snprintf(line, sizeof(line), "%s.%03ld INFO [HEALTH=%015lu] %s\n", tsbuf, ts.tv_nsec / 1000000, cid, buf);

    if (priority == LOG_WARNING)
        snprintf(line, sizeof(line), "%s.%03ld WARN [HEALTH=%015lu] %s\n", tsbuf, ts.tv_nsec / 1000000, cid, buf);

    if (priority == LOG_ERR)
        snprintf(line, sizeof(line), "%s.%03ld ERROR [HEALTH=%015lu] %s\n", tsbuf, ts.tv_nsec / 1000000, cid, buf);
    ssize_t ignored = write(STDERR_FILENO, line, strnlen(line, sizeof(line)));
    openlog("yourprog", LOG_PID | LOG_NDELAY, LOG_USER);
    syslog(priority, "%s", buf);
    closelog();
}

static const char *errname(void) {
    return strerror(errno);
}

int reopen_stderr(uint64_t cid) {
    const int max_retries = 5;
    const long base_sleep_ms = 50;

    if (cid != 0)
        safe_log(cid, LOG_INFO, "reopen_stderr: begin (target: %s)", log_path);
    int attempt;
    int fd = -1;

    for (attempt = 0; attempt < max_retries; ++attempt) {
        errno = 0;
        fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);

        if (fd >= 0)
            break;
        int e = errno;
        safe_log(cid, LOG_WARNING, "reopen_stderr: open('%s') failed (attempt %d/%d): %s", log_path, attempt + 1, max_retries, strerror(e));

        if (e == EINTR || e == EAGAIN || e == ETIMEDOUT || e == EMFILE || e == ENFILE || e == ENOSPC || e == EBUSY) {
            long ms = base_sleep_ms << attempt;
            struct timespec req = {.tv_sec = ms / 1000, .tv_nsec = (ms % 1000) * 1000000L};
            nanosleep(&req, NULL);
            continue;
        }
        return -1;
    }

    if (fd < 0) {
        safe_log(cid, LOG_ERR, "reopen_stderr: giving up after %d attempts", max_retries);
        return -1;
    }

    if (dup2(fd, STDERR_FILENO) == -1) {
        int e = errno;
        safe_log(cid, LOG_ERR, "reopen_stderr: dup2(fd=%d -> STDERR) failed: %s", fd, strerror(e));
        close(fd);
        return -1;
    }

    if (close(fd) == -1) {
        safe_log(cid, LOG_WARNING, "reopen_stderr: close(temp-fd) failed: %s", errname());
    }
    int flags = fcntl(STDERR_FILENO, F_GETFD);

    if (flags != -1) {
        if (fcntl(STDERR_FILENO, F_SETFD, flags | FD_CLOEXEC) == -1) {
            safe_log(cid, LOG_WARNING, "reopen_stderr: fcntl(F_SETFD, FD_CLOEXEC) failed: %s", errname());
        }
    }

    if (cid != 0)
        safe_log(cid, LOG_INFO, "reopen_stderr: done");
    return 0;
}

static unsigned long long get_reclaimable_cache_bytes(void) {
    unsigned long long cached_kb = 0, sreclaim_kb = 0;
    FILE *f = fopen("/proc/meminfo", "r");

    if (!f)
        return 0ULL;
    char line[256];

    while (fgets(line, sizeof line, f)) {
        if (sscanf(line, "Cached: %llu kB", &cached_kb) == 1)
            continue;

        if (sscanf(line, "SReclaimable: %llu kB", &sreclaim_kb) == 1)
            continue;

        if (cached_kb && sreclaim_kb)
            break;
    }
    fclose(f);
    return (cached_kb + sreclaim_kb) * 1024ULL;
}

int mem_check_before_alloc(size_t bytes_to_alloc, int mem_free_limit_percentage, unsigned int *out_free_after) {
    struct sysinfo si;

    if (sysinfo(&si) != 0) {
        return 0;
    }
    unsigned long long free_now = (unsigned long long)(si.freeram + si.bufferram) * si.mem_unit;
    free_now += get_reclaimable_cache_bytes();
    unsigned long long total_ram = (unsigned long long)si.totalram * si.mem_unit;
    unsigned long long free_after = (free_now > bytes_to_alloc) ? (free_now - bytes_to_alloc) : 0ULL;
    unsigned int free_after_pct = (unsigned int)(100.0 * free_after / total_ram);

    if (out_free_after)
        *out_free_after = free_after_pct;
    return free_after_pct >= (unsigned int)mem_free_limit_percentage;
}

void *pthread_data_care() {
    char dt[DT_LOG_SIZE];
    uint64_t cid = get_new_cid();
    struct stat fileStat;
    int count_try_get_lock = 10000;
    uint32_t closed_socks_count = 0;
    bool got_tables_lock = true;
    table_info_t *t_info;
    region_info_t *r_info;
    uint32_t region_count_curr;
    uint64_t chk_count = 1;
    uint32_t sec_count = 0;
    unsigned int free_after_pct;
    bool health_check_result;
    table_handler_t *t_handler;

    while ((t_handler = create_t_handler(0)) == NULL)
        sleep(1);
    t_handler->cid = cid;
    pid_t tid = syscall(SYS_gettid);
    fprintf(stderr, "%s INFO [HEALTH=%015lu] Data care thread started, TID=%d\n", curr_dt(dt), cid, tid);

    for (;;) {
        if (atomic_load(&is_shutting_down) != 0) {
            bool shutdown_result = shutdown_regions_parallel(t_handler);
            bool marker_failed = atomic_load(&recovery_marker_failed);

            if (shutdown_result && data_check_enabled && !marker_failed) {
                if (remove(health_file_name) == 0) {
                    fprintf(stderr, "%s INFO [HEALTH=%015lu] File '%s' has been deleted successfully\n", curr_dt(dt), cid, health_file_name);
                } else {
                    fprintf(stderr, "%s ERROR [HEALTH=%015lu] Cannot delete %s\n", curr_dt(dt), cid, health_file_name);

                    if (get_data_check_ts(NULL, NULL, health_file_name, 0L))
                        fprintf(stderr, "%s INFO [HEALTH=%015lu] File %s is exist\n", curr_dt(dt), cid, health_file_name);
                    else {
                        fprintf(stderr, "%s INFO [HEALTH=%015lu] File %s is not exist\n", curr_dt(dt), cid, health_file_name);
                    }
                }
            } else if (marker_failed)
                fprintf(stderr, "%s FATAL [HEALTH=%015lu] Health file %s is preserved because a reserve replica recovery marker could not be persisted\n", curr_dt(dt), cid,
                        health_file_name);

            if (h_count <= 16)
                fprintf(stderr, "%s DONE\n", curr_dt(dt));
            else
                fprintf(stderr, "%s DONE \n", curr_dt(dt));
            exit(marker_failed ? EXIT_FAILURE : EXIT_SUCCESS);
        }
        sleep(1);

        if (need_reopen) {
            reopen_stderr(cid);
            need_reopen = 0;
        }

        if (log_path != NULL) {
            if (sec_count == log_rotation_seconds) {
                time_t t = time(NULL);
                struct tm tm;
                localtime_r(&t, &tm);
                char ts[32];
                strftime(ts, sizeof(ts), "%Y%m%d_%H%M%S", &tm);
                size_t len = strlen(log_path) + 1 + strlen(ts) + 1;
                char *log_path_bkp = malloc(len);

                if (!log_path_bkp)
                    return NULL;
                snprintf(log_path_bkp, len, "%s_%s", log_path, ts);

                if (rename(log_path, log_path_bkp) != 0)
                    fprintf(stderr, "%s ERROR [HEALTH=%015lu] Rename %s to %s failed: %s\n", curr_dt(dt), cid, log_path, log_path_bkp, strerror(errno));
                else
                    reopen_stderr(cid);
                sec_count = 0;
            }
            sec_count++;
        }

        if (data_check_enabled) {
            if (chk_count % 10 == 0)
                health_check_result = save_data_check(cid, true);
            else
                health_check_result = save_data_check(cid, false);

            if (stat(exit_file_name, &fileStat) != -1 || health_check_result == false)
                rs_signal_handler(100);
        }
        chk_count++;

        if (!mem_check_before_alloc(1024 * 1024, mem_free_limit_percentage, &free_after_pct)) {
            fprintf(stderr, "%s ERROR [HEALTH=%015lu] Free memory is not enough (less then %u percent) so any client requests stopped until free\n", curr_dt(dt), t_handler->cid,
                    mem_free_limit_percentage);
            stop_any_client_requests = 1;
        } else
            stop_any_client_requests = 0;
        warn_if_near_nofile(0.99, t_handler->cid);
    }
}
