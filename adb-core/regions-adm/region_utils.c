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
#include "region_utils.h"
#include "../utils/region_memory.h"
#include <malloc.h>
#include "../index/index_builder.h"
#include "../utils/voids_service.h"
static _Atomic bool malloc_trim_requested = false;
static table_info_t **retired_table_info = NULL;
static size_t retired_table_count = 0;
static size_t retired_table_capacity = 0;
void request_malloc_trim(void) {
    atomic_store(&malloc_trim_requested, true);
}


void *pthread_malloc_trim(void *arg) {
    (void)arg;

    while (atomic_load(&is_shutting_down) == 0) {
        for (int i = 0; i < 10 && atomic_load(&is_shutting_down) == 0; i++)
            sleep(1);

        if (atomic_load(&is_shutting_down) != 0)
            break;

        if (atomic_exchange(&malloc_trim_requested, false))
            malloc_trim(0);
    }
    return NULL;
}


bool try_get_t_lock(table_info_t *t_info, uint32_t new_t_lock_id) {
    bool lock_success = false;

    for (int i = 0; i < 10; i++) {
        if (pthread_mutex_trylock(&t_info->tlock) == 0) {
            lock_success = true;
            t_info->t_lock_id = new_t_lock_id;
            break;
        }
        while (atomic_load(&is_shutting_down))
            sleep(10);
        usleep(1000);
    }
    return lock_success;
}


bool get_t_lock(table_info_t *t_info, uint32_t new_t_lock_id, bool do_log, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    int count_try = 0;

    while (pthread_mutex_trylock(&t_info->tlock) != 0) {
        while (atomic_load(&is_shutting_down))
            sleep(10);
        usleep(10000);

        if (count_try++ == 1000 && new_t_lock_id == FIND_REGION_T_LOCK) {
            return false;
        }
    }

    if (t_info->t_lock_id != 0) {
        fprintf(stderr, "%s FATAL [REGION_UTILS=%015lu] Wrong lock %s try new_t_lock_id=%d (t_info->t_lock_id=%d != 0)\n", curr_dt(dt), cid, t_info->fullname, new_t_lock_id,
                t_info->t_lock_id);
        char *ret;

        if ((ret = (char *)malloc(20)) == NULL) {
            perror("malloc() error");
            graceful_exit(-1);
        }
        strcpy(ret, "Thread finished");
        pthread_exit(ret);
    } else {
        t_info->t_lock_id = new_t_lock_id;

        if (do_log)
            fprintf(stderr, "%s DEBUG [REGION_UTILS=%015lu] Lock table %s t_info->t_lock_id == %d\n", curr_dt(dt), cid, t_info->fullname, t_info->t_lock_id);
    }
    return true;
}

void do_t_unlock(table_info_t *t_info, uint32_t t_call_id, bool do_log, uint64_t cid) {
    char dt[DT_LOG_SIZE];

    if (t_info->t_lock_id == 0) {
        fprintf(stderr, "%s ERROR [REGION_UTILS=%015lu] Wrong unlock %s try t_lock_id == 0 (t_lock_id_before=%d, t_call_id_unlock=%d). Call id %d\n", curr_dt(dt), cid, t_info->fullname,
                t_info->t_lock_id_before, t_info->t_call_id_unlock, t_call_id);
        char *ret;

        if ((ret = (char *)malloc(20)) == NULL) {
            perror("malloc() error");
            graceful_exit(-3);
        }
        strcpy(ret, "Thread finished");
        pthread_exit(ret);
    } else {
        if (do_log)
            fprintf(stderr, "%s DEBUG [REGION_UTILS=%015lu] Unlock table %s t_info->t_lock_id = %d. Call id %d\n", curr_dt(dt), cid, t_info->fullname, t_info->t_lock_id, t_call_id);
        t_info->t_lock_id_before = t_info->t_lock_id;
        t_info->t_call_id_unlock = t_call_id;
        t_info->t_lock_id = 0;
    }
    pthread_mutex_unlock(&t_info->tlock);
}


bool get_r_lock(region_info_t *r_info, uint32_t new_r_lock_id, bool do_log, bool force_lock, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    uint16_t count_tries = 0;
    uint16_t count_tries_lim = 200;
    uint32_t count_tries_print = 0;
    uint64_t cid = t_handler->cid;
    uint64_t recv_ts = t_handler->recv_ts;
    uint64_t client_timeout_ms = t_handler->client_timeout_ms;
    int cli_socket = transport_fd(t_handler->transport);

    while (true) {
        int ret = pthread_mutex_trylock(&r_info->rlock);

        if (ret == 0)
            break;

        while (atomic_load(&is_shutting_down))
            sleep(10);

        if (r_info->is_freed) {
            fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Try lock failed and region has freed: %s\n", curr_dt(dt), cid, strerror(ret));
            sleep(1);

            if (new_r_lock_id == NEW_REGION_LOCK) // TODO Проанализировать все вызовы и возможно нужно сначала разлочить таблицу где-то еще
                return false;
            else
                finish_thread(r_info, 66, false, t_handler);
        }

        if (r_info->replica != NULL && force_lock) {
            pthread_mutex_lock(&r_info->control_mutex);

            if (r_info->r_lock_id != 0 && (r_info->replica->replica1_sock_waiting_conn || r_info->replica->replica2_sock_waiting_conn || r_info->replica_stoped)) {
                r_info->waiting_count_lock++;
                fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Region %s already locked by cid=%015lu, id=%u, count = %d, pass (force lock)\n", curr_dt(dt), cid, r_info->r_fname,
                        r_info->curr_cid_lock, r_info->r_lock_id, r_info->waiting_count_lock);
                pthread_mutex_unlock(&r_info->control_mutex);
                break;
            }
            pthread_mutex_unlock(&r_info->control_mutex);
        }
        usleep(1000);

        if (++count_tries % count_tries_lim == 0) {
            if (count_tries_print % 100 == 0) {
                fprintf(stderr,
                        "%s WARN [REGION_UTILS=%015lu] Lock waiting %s, new_r_lock_id=%d, "
                        "r_info->r_lock_id = %d, r_info->waiting_count_lock = %d, "
                        "r_info->r_lock_id_before = %d, r_info->r_call_id_unlock = %d, "
                        "r1_sock_waiting_conn=%d, r2_sock_waiting_conn=%d, owner=%lu\n",
                        curr_dt(dt), cid, r_info->r_fname, new_r_lock_id, r_info->r_lock_id, r_info->waiting_count_lock, r_info->r_lock_id_before, r_info->r_call_id_unlock,
                        r_info->replica->replica1_sock_waiting_conn, r_info->replica->replica2_sock_waiting_conn, r_info->curr_cid_lock);
            }
            count_tries_print++;

            if (count_tries >= (MAX_REGION_WAIT_LOCK_M * 10)) {
                count_tries = 10;
            }
            usleep(1000 * count_tries);
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);

            if (ts.tv_sec * 1000L - recv_ts > client_timeout_ms) {
                fprintf(stderr, "%s ERROR [REGION_UTILS=%015lu] Client socket timeout exceeded (%lu > %lu), sleep 1s and break the connection=%d\n", curr_dt(dt), cid,
                        ts.tv_sec * 1000L - recv_ts, client_timeout_ms, cli_socket);
                sleep(1);
                finish_thread(r_info, 67, false, t_handler);
            }
        }
    }
    pthread_mutex_lock(&r_info->control_mutex);

    if (r_info->r_lock_id != 0 && r_info->waiting_count_lock == 0) {
        pthread_mutex_unlock(&r_info->control_mutex);
        fprintf(stderr, "%s ERROR [REGION_UTILS=%015lu] Error code 117. Wrong lock try new_r_lock_id=%d (r_lock_id=%d != 0), owner=%lu\n", curr_dt(dt), cid, new_r_lock_id,
                r_info->r_lock_id, r_info->curr_cid_lock);
        r_info->stop = true;
        r_info->bad_data = true;
        finish_thread(r_info, 68, false, t_handler);
    } else {
        uint8_t r_lock_id_check = r_info->r_lock_id;
        r_info->r_lock_id = new_r_lock_id;
        r_info->curr_cid_lock = cid;

        if (do_log) {
            fprintf(stderr,
                    "%s DEBUG [REGION_UTILS=%015lu] Lock region %s, r_info->r_lock_id == %d "
                    "(r_lock_id_check=%d), r_info->r_lock_id_before = %d, r_info->r_call_id_unlock = %d\n",
                    curr_dt(dt), cid, r_info->r_fname, r_info->r_lock_id, r_lock_id_check, r_info->r_lock_id_before, r_info->r_call_id_unlock);
        }
    }
    pthread_mutex_unlock(&r_info->control_mutex);
    return true;
}


bool do_r_unlock(struct region_info_t *r_info, uint32_t r_call_id, bool do_log, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    uint64_t cid = t_handler->cid;
    pthread_mutex_lock(&r_info->control_mutex);

    if (r_info->r_lock_id == 0 && r_info->waiting_count_lock == 0) {
        pthread_mutex_unlock(&r_info->control_mutex);
        fprintf(stderr, "%s ERROR [REGION_UTILS=%015lu] Error code 118. Wrong unlock %s try r_lock_id == 0 (r_lock_id_before=%d, r_call_id_unlock=%d), call id %d, owner=%lu\n",
                curr_dt(dt), cid, r_info->uuid, r_info->r_lock_id_before, r_info->r_call_id_unlock, r_call_id, r_info->curr_cid_lock);
        r_info->stop = true;
        r_info->bad_data = true;
        finish_thread(r_info, 69, true, t_handler);
    } else {
        if (r_info->waiting_count_lock == 0) {
            if (do_log)
                fprintf(stderr, "%s DEBUG [REGION_UTILS=%015lu] Unlock region %s r_info->r_lock_id=%d. Call id %d\n", curr_dt(dt), cid, r_info->r_fname, r_info->r_lock_id, r_call_id);
            r_info->r_lock_id_before = r_info->r_lock_id;
            r_info->r_call_id_unlock = r_call_id;
            r_info->r_lock_id = 0;
            r_info->curr_cid_lock = 0;
            pthread_mutex_unlock(&r_info->control_mutex);
            pthread_mutex_unlock(&r_info->rlock);
        } else {
            r_info->waiting_count_lock--;
            fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Region %s count = %d, owner=%lu, pass\n", curr_dt(dt), cid, r_info->r_fname, r_info->waiting_count_lock,
                    r_info->curr_cid_lock);
            pthread_mutex_unlock(&r_info->control_mutex);
        }
    }
    return true;
}


bool mutex_creator(pthread_mutex_t *lock) {
    char dt[DT_LOG_SIZE];

    for (int a = 0; a < 10; a++) {
        if (pthread_mutex_init(lock, NULL) != 0)
            sleep(1);
        else
            return true;
    }
    // TODO Find out what to do
    fprintf(stderr, "%s FATAL [REGION_HANDLER] Cannot init mutex\n", curr_dt(dt));
    graceful_exit(-4);
}


static table_info_t *create_empty_table_slot(uint64_t cid) {
    char dt[DT_LOG_SIZE];
    table_info_t *t_info = calloc(1, sizeof(table_info_t));

    if (t_info == NULL) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Failed malloc empty table slot\n", curr_dt(dt), cid);
        return NULL;
    }
    mutex_creator(&t_info->tlock);
    return t_info;
}

static bool retire_empty_table(table_info_t *t_info, uint64_t cid) {
    char dt[DT_LOG_SIZE];

    if (t_info == NULL || t_info->retired || atomic_load(&t_info->region_count_curr_assigned) != 0 || t_info->count_cli_fd != 0)
        return false;

    for (int r = 0; r < t_info->region_count_curr; r++) {
        if (!t_info->region_info[r]->is_freed) {
            fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Cannot retire table %s: region slot %d is not freed despite zero assigned count\n", curr_dt(dt), cid, t_info->fullname, r);
            return false;
        }
    }
    pthread_mutex_lock(&tables_lock);
    int table_count = atomic_load(&table_count_curr);
    int table_pos = -1;

    for (int t = 0; t < table_count; t++) {
        if (table_info[t] == t_info) {
            table_pos = t;
            break;
        }
    }

    if (table_pos < 0) {
        pthread_mutex_unlock(&tables_lock);
        return false;
    }

    if (retired_table_count == retired_table_capacity) {
        size_t new_capacity = retired_table_capacity == 0 ? 8 : retired_table_capacity * 2;

        if (new_capacity < retired_table_capacity || new_capacity > SIZE_MAX / sizeof(*retired_table_info)) {
            fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Cannot grow retired tables list count=%zu, capacity=%zu\n", curr_dt(dt), cid, retired_table_count, retired_table_capacity);
            pthread_mutex_unlock(&tables_lock);
            return false;
        }
        table_info_t **retired_tmp = realloc(retired_table_info, new_capacity * sizeof(*retired_table_info));

        if (retired_tmp == NULL) {
            fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Cannot realloc retired tables list to %zu elements\n", curr_dt(dt), cid, new_capacity);
            pthread_mutex_unlock(&tables_lock);
            return false;
        }
        retired_table_info = retired_tmp;
        retired_table_capacity = new_capacity;
    }
    begin_tables_resize();
    t_info->retired = true;
    table_info_t *empty_slot = table_count < (int)table_count_total ? table_info[table_count] : NULL;
    table_info[table_pos] = table_info[table_count - 1];
    table_info[table_count - 1] = empty_slot;

    if (table_count < (int)table_count_total)
        table_info[table_count] = NULL;
    retired_table_info[retired_table_count++] = t_info;
    size_t retired_count = retired_table_count;
    atomic_store(&table_count_curr, table_count - 1);
    end_tables_resize();
    pthread_mutex_unlock(&tables_lock);
    fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Table %s retired, active_tables=%d, retired_tables=%zu\n", curr_dt(dt), cid, t_info->fullname, table_count - 1, retired_count);
    return true;
}

void init_table_region_slots() {
    char dt[DT_LOG_SIZE];
    mutex_creator(&tables_lock);
    pthread_mutex_lock(&tables_lock);
    table_count_curr = 0;
    table_count_total = MAX_TABLES_LIMIT;
    table_info = (table_info_t **)malloc(table_count_total * sizeof(table_info_t *));

    if (table_info == NULL) {
        fprintf(stderr, "%s FATAL [REGION_HANDLER] Failed malloc table_info\n", curr_dt(dt));
        graceful_exit(-1);
    }

    for (int t = 0; t < table_count_total; t++) {
        table_info[t] = (table_info_t *)malloc(sizeof(table_info_t));

        if (table_info[t] == NULL) {
            fprintf(stderr, "%s FATAL [REGION_HANDLER] Filed malloc table_info[%d]\n", curr_dt(dt), t);
            graceful_exit(-6);
        }
        memset(table_info[t], 0, sizeof(table_info_t));
        mutex_creator(&table_info[t]->tlock);
    }
    pthread_mutex_unlock(&tables_lock);
}

void begin_tables_read(void) {
    for (;;) {
        while (atomic_load(&tables_resize_pending))
            usleep(100);
        atomic_fetch_add(&tables_readers, 1);

        if (!atomic_load(&tables_resize_pending))
            return;
        atomic_fetch_sub(&tables_readers, 1);
    }
}

void end_tables_read(void) {
    atomic_fetch_sub(&tables_readers, 1);
}


void begin_tables_resize(void) {
    atomic_store(&tables_resize_pending, true);

    while (atomic_load(&tables_readers) != 0)
        usleep(100);
}

void end_tables_resize(void) {
    atomic_store(&tables_resize_pending, false);
}

bool take_tables_snapshot(tables_snapshot_t *snapshot, uint64_t cid) {
    snapshot->count = 0;

    while (!atomic_load(&is_shutting_down)) {
        begin_tables_read();
        int count = atomic_load(&table_count_curr);

        if (count < 0 || (size_t)count > SIZE_MAX / sizeof(*snapshot->tables)) {
            end_tables_read();
            return false;
        }

        if ((size_t)count <= snapshot->capacity) {
            if (count != 0)
                memcpy(snapshot->tables, table_info, (size_t)count * sizeof(*snapshot->tables));
            snapshot->count = count;
            end_tables_read();
            return true;
        }
        end_tables_read();
        table_info_t **tables = realloc(snapshot->tables, (size_t)count * sizeof(*tables));

        if (tables == NULL) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [REGION_UTILS=%015lu] Cannot allocate table snapshot count=%d\n", curr_dt(dt), cid, count);
            return false;
        }
        snapshot->tables = tables;
        snapshot->capacity = (size_t)count;
    }
    return false;
}


void free_tables_snapshot(void *arg) {
    tables_snapshot_t *snapshot = arg;
    free(snapshot->tables);
    snapshot->tables = NULL;
    snapshot->capacity = 0;
    snapshot->count = 0;
}


void reserve_replica_pointer_check(table_info_t *t_info, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    struct region_info_t *r_info;

    if (t_info == NULL)
        return;

    for (int r = 0; r < t_info->region_count_curr; r++) {
        r_info = t_info->region_info[r];

        if (r_info->is_freed == false && r_info->replica->r_info1 != NULL) {
            if (r_info->replica->r_info1->is_freed == false)
                if (strncmp(r_info->replica->r_info1->uuid, r_info->uuid, UUID_SIZE - 1) != 0) {
                    fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Error code 118. Invalid replica1 %s pointer to %s\n", curr_dt(dt), t_handler->cid, r_info->replica->r_info1->uuid,
                            r_info->uuid);
                    r_info->stop = true;
                    r_info->bad_data = true;
                    finish_thread(r_info, 70, false, t_handler);
                };
        }

        if (r_info->is_freed == false && r_info->replica->r_info2 != NULL) {
            if (r_info->replica->r_info2->is_freed == false)
                if (strncmp(r_info->replica->r_info2->uuid, r_info->uuid, UUID_SIZE - 1) != 0) {
                    fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Error code 119. Invalid replica2 %s pointer to %s\n", curr_dt(dt), t_handler->cid, r_info->replica->r_info2->uuid,
                            r_info->uuid);
                    r_info->stop = true;
                    r_info->bad_data = true;
                    finish_thread(r_info, 71, true, t_handler);
                };
        }
    }
}

table_info_t *find_region_t_info(char *ns, char *table, char *region_uuid, table_handler_t *t_handler, table_info_t *t_info) {
    char dt[DT_LOG_SIZE];
    struct region_info_t *r_info;
    int32_t ver_begin = -1;
    int32_t ver_end = -1;
retry_table_lookup:
    t_handler->t_pos = -1;
    t_handler->r_pos = -1;

    do {
        ver_begin = -1;
        ver_end = -1;

        while (atomic_load(&is_shutting_down))
            sleep(10);
        pthread_mutex_lock(&tables_lock);

        for (int t = 0; t < table_count_curr; t++) {
            t_info = table_info[t];
            pthread_mutex_unlock(&tables_lock);

            while (STOP_THE_WORLD == true) {
                usleep(10000);
            }

            if (strcmp(ns, t_info->ns) == 0 && strcmp(table, t_info->table) == 0) {
                t_handler->t_pos = t;
                get_t_lock(t_info, 10, true, t_handler->cid);

                if (t_info->retired) {
                    do_t_unlock(t_info, 14, false, t_handler->cid);
                    goto retry_table_lookup;
                }
                fprintf(stderr, "%s DEBUG [REGION_UTILS=%015lu] Found table %s on position=%d\n", curr_dt(dt), t_handler->cid, t_info->fullname, t);
                ver_begin = t_info->srv_tbl_version;

                for (int r = 0; r < t_info->region_count_curr; r++) {
                    r_info = t_info->region_info[r];

                    if (r_info->status == STATUS_ASSIGN_REGION) {
                        if (strcmp(region_uuid, r_info->uuid) == 0) {
                            t_handler->r_pos = r;
                            fprintf(stderr, "%s DEBUG [REGION_UTILS=%015lu] Found region %s on position=%d\n", curr_dt(dt), t_handler->cid, r_info->uuid, r);
                            t_info->need_lock = true; // TODO Бля, ну ож же и так залочен! Попробуй удалить
                            usleep(10000);
                            return t_info;
                        }
                    }
                }
                ver_end = t_info->srv_tbl_version;
                return t_info;
            }
            while (atomic_load(&is_shutting_down))
                sleep(10);
            pthread_mutex_lock(&tables_lock);
        }
        pthread_mutex_unlock(&tables_lock);
    } while (ver_begin != ver_end);
    return NULL;
}

int add_cli_fd(int fd, table_info_t *t_info) {
    if (!t_info)
        return -1;

    for (size_t i = 0; i < t_info->count_cli_fd; ++i) {
        if (t_info->cli_fd[i] == fd) {
            return -1;
        }
    }
    size_t new_count = t_info->count_cli_fd + 1;
    int *tmp = (int *)realloc(t_info->cli_fd, new_count * sizeof(int));

    if (!tmp) {
        return -1;
    }
    t_info->cli_fd = tmp;
    t_info->cli_fd[t_info->count_cli_fd] = fd;
    t_info->count_cli_fd = new_count;
    return 0;
}

int del_cli_fd(int fd, table_info_t *t_info) {
    if (!t_info)
        return -1;

    if (t_info->count_cli_fd == 0)
        return -1;
    size_t idx = (size_t)-1;

    for (size_t i = 0; i < t_info->count_cli_fd; ++i) {
        if (t_info->cli_fd[i] == fd) {
            idx = i;
            break;
        }
    }

    if (idx == (size_t)-1)
        return -1;
    size_t last = t_info->count_cli_fd - 1;

    if (idx != last) {
        t_info->cli_fd[idx] = t_info->cli_fd[last];
    }
    size_t new_count = last;

    if (new_count == 0) {
        free(t_info->cli_fd);
        t_info->cli_fd = NULL;
        t_info->count_cli_fd = 0;
        return 0;
    }
    t_info->count_cli_fd = new_count;
    return 0;
}


region_info_t *find_region_r_info(char *ns, char *table, char *region_uuid, table_handler_t *t_handler, struct table_info_t *t_info) {
    char dt[DT_LOG_SIZE];
retry_table_lookup:
    t_handler->t_pos = -1;
    t_handler->r_pos = -1;
    struct region_info_t *r_info;

    while (atomic_load(&is_shutting_down))
        sleep(10);
    pthread_mutex_lock(&tables_lock);

    for (int t = 0; t < table_count_curr; t++) {
        t_info = table_info[t];
        pthread_mutex_unlock(&tables_lock);

        if (strcmp(ns, t_info->ns) == 0 && strcmp(table, t_info->table) == 0) {
            t_handler->t_pos = t;
            bool local_need_lock = false;
            local_need_lock = true;
            get_t_lock(t_info, LOCK_ID_FIND_REGION, true, t_handler->cid);

            if (t_info->retired) {
                do_t_unlock(t_info, 14, false, t_handler->cid);
                goto retry_table_lookup;
            }

            for (int r = 0; r < t_info->region_count_curr; r++) {
                r_info = t_info->region_info[r];

                if (local_need_lock) {
                    local_need_lock = false;
                    do_t_unlock(t_info, 14, true, t_handler->cid);
                }

                if (r_info->status == STATUS_ASSIGN_REGION && r_info->is_freed == false) {
                    if (strcmp(region_uuid, r_info->uuid) == 0) {
                        while (r_info->waiting_count_lock != 0)
                            usleep(10000);
                        get_r_lock(r_info, FIND_REGION_LOCK, true, false, t_handler);

                        if (r_info->is_freed) {
                            do_r_unlock(r_info, 122, true, t_handler);
                            errno = EAGAIN;
                            return NULL;
                        }
                        bool table_registered = false;
                        uint32_t table_index = UINT32_MAX;

                        for (uint32_t i = 0; i + 1 < t_handler->t_info_size; i++) {
                            if (t_handler->t_info[i] == t_info) {
                                table_registered = true;
                                table_index = i;
                                break;
                            }
                        }

                        if (!table_registered) {
                            uint32_t t_info_size_new = t_handler->t_info_size + 1;
                            struct table_info_t *t_info_empty = calloc(1, sizeof(struct table_info_t));

                            if (t_info_empty == NULL) {
                                do_r_unlock(r_info, 124, true, t_handler);
                                errno = ENOMEM;
                                return NULL;
                            }
                            struct table_info_t **t_info_tmp = malloc(t_info_size_new * sizeof(*t_info_tmp));
                            uint32_t *cli_tbl_versions_tmp = malloc(t_info_size_new * sizeof(*cli_tbl_versions_tmp));

                            if (t_info_tmp == NULL || cli_tbl_versions_tmp == NULL) {
                                free(t_info_tmp);
                                free(cli_tbl_versions_tmp);
                                free(t_info_empty);
                                do_r_unlock(r_info, 125, true, t_handler);
                                errno = ENOMEM;
                                return NULL;
                            }
                            uint32_t registered_count = t_handler->t_info_size - 1;
                            memcpy(t_info_tmp, t_handler->t_info, registered_count * sizeof(*t_info_tmp));
                            memcpy(cli_tbl_versions_tmp, t_handler->cli_tbl_versions, registered_count * sizeof(*cli_tbl_versions_tmp));
                            table_index = registered_count;
                            t_info_tmp[table_index] = t_info;
                            cli_tbl_versions_tmp[table_index] = UINT32_MAX;
                            t_info_tmp[t_info_size_new - 1] = t_info_empty;
                            cli_tbl_versions_tmp[t_info_size_new - 1] = UINT32_MAX;
                            free(t_handler->t_info[t_handler->t_info_size - 1]);
                            free(t_handler->t_info);
                            free(t_handler->cli_tbl_versions);
                            t_handler->t_info = t_info_tmp;
                            t_handler->cli_tbl_versions = cli_tbl_versions_tmp;
                            t_handler->t_info_size = t_info_size_new;
                        }

                        if (get_t_lock(t_info, FIND_REGION_T_LOCK, false, t_handler->cid) == false) {
                            do_r_unlock(r_info, 123, true, t_handler);
                            errno = EAGAIN;
                            return NULL;
                        }
                        t_handler->conn_father->has_table = true;
                        add_cli_fd(transport_fd(&t_handler->conn_father->transport), t_info);
                        do_t_unlock(t_info, 30, false, t_handler->cid);

                        if (t_handler->cli_tbl_version == -1)
                            t_handler->cli_tbl_version = t_info->srv_tbl_version;
                        t_handler->cli_tbl_versions[table_index] = t_handler->cli_tbl_version;
                        t_handler->r_pos = r;
                        return r_info;
                    }
                }

                if (t_info->need_lock) { // TODO Нах?
                    local_need_lock = true;
                    get_t_lock(t_info, 13, true, t_handler->cid);
                }
            }

            if (local_need_lock) {
                local_need_lock = false;
                do_t_unlock(t_info, 15, true, t_handler->cid);
            }
        }
        while (atomic_load(&is_shutting_down))
            sleep(10);
        pthread_mutex_lock(&tables_lock);
    }
    pthread_mutex_unlock(&tables_lock);
    errno = ENOENT;
    return NULL;
}

table_info_t *build_new_table(char *ns, char *table, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    table_info_t *t_info;
    char *ns_old;
    char *table_old;
    char *fullname_old;
    region_info_t **region_info_old;
    uint32_t region_count_total_old;
    int r_alloc = 0;
    bool ns_alloc = false;
    bool table_alloc = false;
    bool fullname_alloc = false;
    bool region_info_alloc = false;
retry_table_lookup:
    while (atomic_load(&is_shutting_down))
        sleep(10);
    pthread_mutex_lock(&tables_lock);

    for (int t = 0; t < table_count_curr; t++) {
        if (strcmp(ns, table_info[t]->ns) == 0 && strcmp(table, table_info[t]->table) == 0) {
            t_info = table_info[t];
            pthread_mutex_unlock(&tables_lock);
            get_t_lock(t_info, 23, true, cid);

            if (t_info->retired) {
                do_t_unlock(t_info, 29, false, cid);
                goto retry_table_lookup;
            }
            return t_info;
        }
    }
    uint32_t t_pos = table_count_curr;

    if (t_pos >= table_count_total)
        resize_tables(cid);
    t_info = table_info[t_pos];

    if (t_info == NULL) {
        t_info = create_empty_table_slot(cid);

        if (t_info == NULL) {
            pthread_mutex_unlock(&tables_lock);
            return NULL;
        }
        table_info[t_pos] = t_info;
    }
    get_t_lock(t_info, 24, true, cid);
    ns_old = t_info->ns;
    table_old = t_info->table;
    fullname_old = t_info->fullname;
    region_info_old = t_info->region_info;
    region_count_total_old = t_info->region_count_total;
    fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Start to create table %s:%s on %d position\n", curr_dt(dt), cid, ns, table, t_pos);
    t_info->ns = malloc((strlen(ns) + 1) * sizeof(char));

    if (t_info->ns == NULL) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Failed malloc table_info[%d]->ns\n", curr_dt(dt), cid, t_pos);
        goto build_new_table_error;
    }
    ns_alloc = true;
    strcpy(t_info->ns, ns);
    t_info->table = malloc((strlen(table) + 1) * sizeof(char));

    if (t_info->table == NULL) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Failed malloc table_info[%d]->table\n", curr_dt(dt), cid, t_pos);
        goto build_new_table_error;
    }
    table_alloc = true;
    strcpy(t_info->table, table);
    size_t fullname_size = strlen(ns) + 1 + strlen(table) + 1;
    t_info->fullname = malloc(fullname_size);

    if (t_info->fullname == NULL) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Failed malloc table_info[%d]->fullname\n", curr_dt(dt), cid, t_pos);
        goto build_new_table_error;
    }
    fullname_alloc = true;
    snprintf(t_info->fullname, fullname_size, "%s:%s", ns, table);
    t_info->region_count_total = MAX_REGIONS_LIMIT;
    t_info->region_info = (region_info_t **)malloc(t_info->region_count_total * sizeof(region_info_t *));

    if (t_info->region_info == NULL) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Failed malloc table_info[%d]->region_info\n", curr_dt(dt), cid, t_pos);
        goto build_new_table_error;
    }
    region_info_alloc = true;

    for (int r = 0; r < t_info->region_count_total; r++) {
        t_info->region_info[r] = (region_info_t *)calloc(1, sizeof(struct region_info_t));

        if (t_info->region_info[r] == NULL) {
            fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Failed malloc table_info[%d]->region_info[%d]\n", curr_dt(dt), cid, t_pos, r);
            goto build_new_table_error;
        }
        r_alloc++;
        memset(t_info->region_info[r], 0, sizeof(region_info_t));

        if (t_info->region_info[r]->freed != MAGIC_NUM) {
            mutex_creator(&t_info->region_info[r]->rlock);
            mutex_creator(&t_info->region_info[r]->control_mutex);
        } else
            fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Reuse region %d\n", curr_dt(dt), cid, r);
        t_info->region_info[r]->has_parent_replica = false;
        t_info->region_info[r]->replica = malloc(sizeof(struct replica_info_t));

        if (t_info->region_info[r]->replica == NULL) {
            fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Failed malloc table_info[%d]->region_info[%d]->replica\n", curr_dt(dt), cid, t_pos, r);
            goto build_new_table_error;
        }
        t_info->region_info[r]->status = STATUS_UNASSIGN_REGION;
    }
    t_info->region_count_curr = 0;
    t_info->count_cli_fd = 0;
    t_info->cli_fd = NULL;
    table_count_curr++;
    pthread_mutex_unlock(&tables_lock);
    fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Table %s:%s on position %d has created \n", curr_dt(dt), cid, ns, table, t_pos);
    return t_info;
build_new_table_error:
    if (region_info_alloc) {
        for (int r = 0; r < r_alloc; r++) {
            free(t_info->region_info[r]->replica);

            if (t_info->region_info[r]->freed != MAGIC_NUM) {
                pthread_mutex_destroy(&t_info->region_info[r]->control_mutex);
                pthread_mutex_destroy(&t_info->region_info[r]->rlock);
            }
            free(t_info->region_info[r]);
        }
        free(t_info->region_info);
    }

    if (fullname_alloc)
        free(t_info->fullname);

    if (table_alloc)
        free(t_info->table);

    if (ns_alloc)
        free(t_info->ns);
    t_info->ns = ns_old;
    t_info->table = table_old;
    t_info->fullname = fullname_old;
    t_info->region_info = region_info_old;
    t_info->region_count_total = region_count_total_old;
    do_t_unlock(t_info, 29, false, cid);
    pthread_mutex_unlock(&tables_lock);
    return NULL;
}

bool set_replica_local(struct region_info_t *r_info, struct table_info_t *t_info, char *r_replica, char *r_uuid, uint8_t r_num, table_handler_t *t_handler, bool *same_instance) {
    *same_instance = false;
    char dt[DT_LOG_SIZE];
    struct region_info_t *r_info_join;
    struct hostent_t *server_host = thread_safe_gethostname(r_replica);

    if (server_host == NULL) {
        fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Could not resolve hostname %s\n", curr_dt(dt), t_handler->cid, r_replica);
        return false;
    }
    struct in_addr **addr_list = (struct in_addr **)server_host->hp->h_addr_list;
    char ip[100];
    uint64_t cid = t_handler->cid;
    strcpy(ip, inet_ntoa(*addr_list[0]));
    fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Check replica%d IP address %s\n", curr_dt(dt), cid, r_num, ip);

    for (int j = 0; j < local_ip_num; j++) {
        fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Cmp IP %s to %s\n", curr_dt(dt), cid, ip, local_ip[j]);

        if (strcmp(ip, local_ip[j]) == 0) {
            *same_instance = true;
            fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Found local IP address %s\n", curr_dt(dt), cid, ip);

            if (r_num == 1)
                r_uuid[strlen(r_uuid) - 1] = '1';

            if (r_num == 2)
                r_uuid[strlen(r_uuid) - 1] = '2';

            for (int r = 0; r < t_info->region_count_curr; r++) {
                r_info_join = t_info->region_info[r];

                if (r_info_join != r_info) {
                    if (r_info_join->is_freed == false) {
                        r_info_join->dont_free_now = true;

                        if (strcmp(r_uuid, r_info_join->uuid) == 0) {
                            r_info_join->dont_free_now = false;
                            usleep(10000);
                            get_r_lock(r_info_join, SET_REPLICA_LOCK, true, true, t_handler);
                            fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Cmp r_uuid=%s and r_info_join->uuid=%s (pos=%d)\n", curr_dt(dt), cid, r_uuid, r_info_join->uuid, r);

                            while (r_info_join->status != STATUS_ASSIGN_REGION) {
                                fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Check t_info->region_info[%d]->status=%d\n", curr_dt(dt), cid, r, r_info_join->status);
                                sleep(RETRY_JOIN_REPLICA_SLEEP);
                            }
                            invalidate_replicas_idle_commit(r_info);
                            invalidate_record_gc(r_info);

                            if (r_num == 1) {
                                r_info_join->has_parent_replica = true;
                                r_info->replica->r_info1 = r_info_join;
                                r_info->replica->replica1_port = -1;
                                fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Joined replica1 %s to %s\n", curr_dt(dt), cid, r_info->replica->r_info1->r_fname, r_info->r_fname);
                            }

                            if (r_num == 2) {
                                r_info_join->has_parent_replica = true;
                                r_info->replica->r_info2 = r_info_join;
                                r_info->replica->replica2_port = -1;
                                fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Joined replica2 %s to %s\n", curr_dt(dt), cid, r_info->replica->r_info2->r_fname, r_info->r_fname);
                            }
                            do_r_unlock(r_info_join, 116, true, t_handler);
                            free(server_host->hostbuf);
                            free(server_host->tmphstbuf);
                            free(server_host);
                            return true;
                        }
                        r_info_join->dont_free_now = false;
                    }
                }
            }
        }
    }

    if (log_level > 0)
        fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Cannot find replica%d for %s\n", curr_dt(dt), cid, r_num, r_info->uuid);
    free(server_host->hostbuf);
    free(server_host->tmphstbuf);
    free(server_host);
    return false;
}

bool replica_info_builder(region_info_t *r_info, table_info_t *t_info, char *r_replica1, int32_t r_replica1_port, char *r_replica2, int32_t r_replica2_port, char *r_uuid,
                          table_handler_t *t_handler) {
    bool same_instance[2] = {false, false};
    invalidate_replicas_idle_commit(r_info);
    char dt[DT_LOG_SIZE];
    invalidate_record_gc(r_info);
    uint64_t cid = t_handler->cid;
    r_info->replica->replica1 = NULL;
    r_info->replica->replica2 = NULL;
    r_info->replica->request_params = NULL;
    r_info->replica->request_params_size = 0;
    r_info->replica->r_info1 = NULL;
    r_info->replica->r_info2 = NULL;
    r_info->replica->replica1_sock_fd = -1;
    r_info->replica->replica2_sock_fd = -1;
    r_info->replica->reply_retry_timeout[0] = 0;
    r_info->replica->reply_retry_timeout[1] = 0;
    pthread_mutex_lock(&r_info->control_mutex);
    r_info->replica->replica1_sock_waiting_conn = false;
    r_info->replica->replica2_sock_waiting_conn = false;
    pthread_mutex_unlock(&r_info->control_mutex);
    r_info->replica->replica_num = 0;

    if (r_replica1_port > 0) {
        r_info->replica->replica1 = (char *)malloc((strlen(r_replica1) + 1) * sizeof(char));

        if (r_info->replica->replica1 == NULL)
            goto replica_allocation_failed;
        strcpy(r_info->replica->replica1, r_replica1);
        r_info->replica->replica1_port = r_replica1_port;
        r_info->replica->replica_num++;
        fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Region %s replica_num=%d\n", curr_dt(dt), cid, r_info->uuid, r_info->replica->replica_num);
    } else {
        r_info->replica->replica1 = NULL;
        r_info->replica->replica1_port = -1;
        fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Region %s set replica1_port=-1\n", curr_dt(dt), cid, r_info->uuid);
    }

    if (r_replica2_port > 0) {
        r_info->replica->replica2 = (char *)malloc((strlen(r_replica2) + 1) * sizeof(char));

        if (r_info->replica->replica2 == NULL)
            goto replica_allocation_failed;
        strcpy(r_info->replica->replica2, r_replica2);
        r_info->replica->replica2_port = r_replica2_port;
        r_info->replica->replica_num++;
        fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Region %s replica_num=%d\n", curr_dt(dt), cid, r_info->uuid, r_info->replica->replica_num);
    } else {
        r_info->replica->replica2 = NULL;
        r_info->replica->replica2_port = -1;
        fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Region %s set replica2_port=-1\n", curr_dt(dt), cid, r_info->uuid);
    }
    fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] TID=%d replica_info_builder=%p\n", curr_dt(dt), t_handler->cid, (int)syscall(SYS_gettid), (void *)t_handler);
    r_info->replica->request_params_size = 0;

    if (r_info->replica->replica_num > 0) {
        if (r_info->uuid == NULL || strnlen(r_info->uuid, UUID_SIZE + 1U) != UUID_SIZE)
            goto replica_allocation_failed;
        uint32_t ns_size = strlen(t_info->ns);
        uint32_t table_size = strlen(t_info->table);
        uint32_t replica_request_size = INIT_MSG_HDR + INT_SIZE + ns_size + 1 + table_size + 1 + UUID_SIZE + 1;
        r_info->replica->request_params_size = replica_request_size;
        r_info->replica->request_params = malloc(replica_request_size * sizeof(char));

        if (r_info->replica->request_params == NULL)
            goto replica_allocation_failed;
        union i2b_u i2b;
        i2b.integer_val = replica_request_size;
        uint32_t curr = 0;
        memcpy(r_info->replica->request_params + curr, i2b.byte, INIT_MSG_SIZE);
        curr += INIT_MSG_SIZE;
        r_info->replica->request_params[curr] = GET_REGION_MAP_TIMEOUT;
        curr += INIT_MSG_TYPE;
        i2b.integer_val = worker_replica_idle_timeout * 1000;
        memcpy(r_info->replica->request_params + curr, i2b.byte, INIT_MSG_SIZE);
        curr += INT_SIZE;
        memcpy(r_info->replica->request_params + curr, t_info->ns, ns_size);
        curr += ns_size;
        r_info->replica->request_params[curr++] = ':';
        memcpy(r_info->replica->request_params + curr, t_info->table, table_size);
        curr += table_size;
        r_info->replica->request_params[curr++] = ':';
        memcpy(r_info->replica->request_params + curr, r_info->uuid, UUID_SIZE);
        curr += UUID_SIZE;
        r_info->replica->request_params[curr++] = 0;
    }
    r_info->replica->r_info1 = NULL;
    r_info->replica->r_info2 = NULL;

    if (r_replica1_port == client_port)
        set_replica_local(r_info, t_info, r_replica1, r_uuid, 1, t_handler, &same_instance[0]);

    if (r_replica2_port == client_port)
        set_replica_local(r_info, t_info, r_replica2, r_uuid, 2, t_handler, &same_instance[1]);
    return true;
replica_allocation_failed:
    free(r_info->replica->replica1);
    free(r_info->replica->replica2);
    free(r_info->replica->request_params);
    r_info->replica->replica1 = r_info->replica->replica2 = NULL;
    r_info->replica->request_params = NULL;
    r_info->replica->request_params_size = 0;
    r_info->replica->replica_num = 0;
    r_info->replica->replica1_port = r_info->replica->replica2_port = -1;
    return false;
}

uint32_t idx_builder(struct region_info_t *r_info, uint8_t bs, uint32_t block_size, table_handler_t *t_handler) {
    uint64_t cid = t_handler->cid;
    idx_region_info_t *index = index_memory_calloc(&r_info->index_memory_bytes, &r_info->index_object_bytes[bs], 1, sizeof(*index));

    if (index == NULL)
        goto allocation_failed;
    index->dir_name = malloc(strlen(r_info->r_fname) + 1);

    if (index->dir_name == NULL) {
        index_memory_free(&r_info->index_memory_bytes, &r_info->index_object_bytes[bs], index);
        goto allocation_failed;
    }

    for (int b = 0; b < BUCKETS; b++)
        index->fd[b] = -1;
    r_info->r_idx_b[bs] = index;
    r_info->r_idx_b[bs]->block_size = block_size;
    r_info->r_idx_b[bs]->bsize_factor = block_size / FIX_BLOCK_SIZE * FIX_BSIZE_FACTOR;
    r_info->r_idx_b[bs]->pos = block_size / FIX_BLOCK_SIZE;
    r_info->r_idx_b[bs]->block_size_zip = 255;
    r_info->r_idx_b[bs]->block_useful_size = r_info->r_idx_b[bs]->block_size - r_info->r_idx_b[bs]->bsize_factor;
    strcpy(r_info->r_idx_b[bs]->dir_name, r_info->r_fname);
    r_info->req_count_chk = rand() & MAX_REPLICA_COUNTER_MASK;
    r_info->r_idx_b[bs]->max_void_count = 0;
    r_info->r_idx_b[bs]->max_void_pos = 0;
    r_info->r_idx_b[bs]->count_max_idx_shift_x2 = 0;
    r_info->r_idx_b[bs]->count_max_idx_jump_x2 = 0;

    for (int b = 0; b < BUCKETS; b++) {
        r_info->r_idx_b[bs]->f_idx_curr_size[b] = 0;
        r_info->r_idx_b[bs]->f_idx_size[b] = 0;
    }

    if (build_index(r_info, bs, t_handler) == -1) {
        free(index->dir_name);
        index_memory_free(&r_info->index_memory_bytes, &r_info->index_object_bytes[bs], index);
        r_info->r_idx_b[bs] = NULL;
        return -1; // TODO Handle it
    }
    r_info->max_idx_b++;
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Index[%d] for region %s (%s) has built. Total idx = %d\n", curr_dt(dt), cid, bs, r_info->r_fname, r_info->r_idx_b[bs]->fname,
            r_info->max_idx_b);
    return 0;
allocation_failed:
    r_info->stop = true;
    r_info->bad_data = true;
    fprintf(stderr, "ERROR [REGION_HANDLER=%015lu] Cannot allocate initial index for %s\n", cid, r_info->uuid);
    return (uint32_t)-1;
}

static bool delete_key_doubles_prefix(struct region_info_t *r_info, keys_double_t *kd, uint8_t bs_del, uint64_t count, bool consume, table_handler_t *t_handler) {
    if (count == 0)
        return true;
    char dt[DT_LOG_SIZE];
    struct msg_kv_t kv = {0};
    struct msg_cli_t message = {.kv = &kv};
    struct msg_cli_t *msg_del = &message;
    msg_del->kv->key = malloc(BLOCK_MAX_SIZE);
    uint8_t *buff = malloc(BLOCK_MAX_SIZE);
    bool success = false;

    if (msg_del->kv->key == NULL || buff == NULL)
        goto cleanup;
    uint64_t offset = 0;

    for (uint64_t i = 0; i < count; i++) {
        uint16_t key_sz;
        memcpy(&key_sz, kd->key_sizes + i * 2, sizeof(key_sz));
        char key_str[key_sz * 3 + 1];
        memset(key_str, 0, sizeof(key_str));
        char *ptr = key_str;

        for (int j = 0; j < key_sz; j++)
            ptr += sprintf(ptr, "%02x_", (unsigned char)kd->keys[offset + j]);

        if (ptr != key_str)
            *(ptr - 1) = '\0';
        fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Handle %lu double key=%s, block_del=%u\n", curr_dt(dt), t_handler->cid, (unsigned long)i, key_str, kd->block_del[i]);
        memcpy(msg_del->kv->key, kd->keys + offset, key_sz);
        msg_del->kv->key_size = key_sz;
        uint32_t block_del = kd->block_del[i];
        msg_del->type = MSG_DEL_DOUBLE;
        int result = get_put(r_info, block_del, msg_del, buff, t_handler, -1, bs_del);

        if (result == GET_PUT_EXTRACT_ERROR) {
            goto cleanup;
        }

        if (result != UPDATE_OK) {
            fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Error code 120. Cannot delete double key in bs_del=%d, result=%d\n", curr_dt(dt), t_handler->cid, bs_del, result);
            goto cleanup;
        }
        offset += key_sz;
    }

    if (consume) {
        uint64_t remaining = kd->k_count - count;
        memmove(kd->keys, kd->keys + offset, kd->keys_offset - offset);
        memmove(kd->key_sizes, kd->key_sizes + count * sizeof(uint16_t), remaining * sizeof(uint16_t));
        memmove(kd->block_del, kd->block_del + count, remaining * sizeof(*kd->block_del));
        kd->keys_offset -= offset;
        kd->k_count = remaining;
        kd->wal_count = kd->wal_count > count ? kd->wal_count - count : 0;
    }
    success = true;
cleanup:
    free(msg_del->kv->key);
    free(buff);

    if (!success) {
        r_info->stop = true;
        r_info->bad_data = true;
        set_region_recovery_info(r_info, 1, t_handler);
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Duplicate cleanup failed for %s\n", curr_dt(dt), t_handler->cid, r_info->uuid);
    }
    return success;
}

bool delete_all_key_doubles(struct region_info_t *r_info, keys_double_t *kd, uint8_t bs_del, table_handler_t *t_handler) {
    return delete_key_doubles_prefix(r_info, kd, bs_del, kd->k_count, false, t_handler);
}


bool finish_index_recovery(region_info_t *r_info, bool recover_lob_wal, table_handler_t *t_handler) {
    for (uint8_t bs = 0; bs < BLOCK_MAX_SIZE_M - 1; ++bs) {
        keys_double_t *kd = r_info->k_dbl[bs];

        if (kd != NULL && !delete_key_doubles_prefix(r_info, kd, bs, kd->wal_count, true, t_handler))
            return false;
    }

    for (uint8_t bs = 0; bs < BLOCK_MAX_SIZE_M - 1; ++bs) {
        keys_double_t *kd = r_info->k_dbl[bs];

        if (kd != NULL && !delete_all_key_doubles(r_info, kd, bs, t_handler))
            return false;
    }
    return true;
}

void free_double_keys_keeper(struct region_info_t *r_info, uint8_t bs) {
    if (r_info->k_dbl == NULL || r_info->k_dbl[bs] == NULL)
        return;
    free(r_info->k_dbl[bs]->keys);
    free(r_info->k_dbl[bs]->key_sizes);
    free(r_info->k_dbl[bs]->block_del);
    free(r_info->k_dbl[bs]);
    r_info->k_dbl[bs] = NULL;
}


static bool allocate_double_keys_keeper(keys_double_t **slot) {
    (*slot) = (keys_double_t *)malloc(sizeof(struct keys_double_t));

    if (!(*slot))
        return false;
    memset((*slot), 0, sizeof(keys_double_t));
    (*slot)->max_key_count = 1024;
    (*slot)->keys_mem_limit = 1024;
    (*slot)->keys_offset = 0;
    (*slot)->keys = malloc((*slot)->keys_mem_limit);
    (*slot)->key_sizes = malloc((*slot)->max_key_count * sizeof(uint16_t));
    (*slot)->block_del = malloc((*slot)->max_key_count * sizeof(uint32_t));

    if ((*slot)->keys == NULL || (*slot)->key_sizes == NULL || (*slot)->block_del == NULL) {
        free((*slot)->keys);
        free((*slot)->key_sizes);
        free((*slot)->block_del);
        free(*slot);
        *slot = NULL;
        return false;
    }
    return true;
}

bool create_double_keys_keeper(struct region_info_t *r_info, uint8_t bs) {
    return allocate_double_keys_keeper(&r_info->k_dbl[bs]);
}


static void free_prepared_region_keepers(keys_double_t **keepers) {
    if (keepers == NULL)
        return;

    for (int bs = 0; bs < BLOCK_MAX_SIZE_M - 1; bs++) {
        keys_double_t *keeper = keepers[bs];

        if (keeper == NULL)
            continue;
        free(keeper->keys);
        free(keeper->key_sizes);
        free(keeper->block_del);
        free(keeper);
    }
    free(keepers);
}

bool load_dict(region_info_t *r_info, table_handler_t *t_handler) {
    const unsigned capacity = 256U * 256U;
    char **dict = calloc(capacity, sizeof(*dict));
    uint16_t *sizes = malloc(capacity * sizeof(*sizes));
    unsigned count = 0, total = 0, max_len = 0;
    const int fd = r_info->r_idx_b[BLOCK_ARRAY_FIRST_POS]->fd[FBP];
    const uint32_t limit = r_info->r_idx_b[BLOCK_ARRAY_FIRST_POS]->block_size;

    if (dict == NULL || sizes == NULL)
        goto failed;

    for (;;) {
        uint16_t size = 0;
        ssize_t received = pread(fd, &size, sizeof(size), USED_META + total);

        if (received == 0 && total == 0)
            break;

        if (received != sizeof(size))
            goto failed;

        if (size == 0)
            break;

        if (count == capacity || USED_META + total > limit || sizeof(size) + (uint32_t)size > limit - (USED_META + total))
            goto failed;
        dict[count] = malloc(size);

        if (dict[count] == NULL)
            goto failed;
        sizes[count] = size;
        count++;

        if (pread(fd, dict[count - 1], size, USED_META + total + sizeof(size)) != size)
            goto failed;
        total += sizeof(size) + size;

        if (size > max_len)
            max_len = size;
    }

    for (int i = 0; i < r_info->qfl_dic_lens; i++)
        free(r_info->qfl_dic[i]);
    free(r_info->qfl_dic);
    free(r_info->qfl_dic_size);
    r_info->qfl_dic = dict;
    r_info->qfl_dic_size = sizes;
    r_info->qfl_dic_lens = count;
    r_info->qfl_dic_total_size = total;
    r_info->max_dict_qfl_len = max_len;
    return true;
failed:
    for (unsigned i = 0; i < count; i++)
        free(dict[i]);
    free(dict);
    free(sizes);
    r_info->stop = true;
    r_info->bad_data = true;
    set_region_recovery_info(r_info, 1, t_handler);
    fprintf(stderr, "ERROR [REGION_HANDLER=%015lu] Cannot load dictionary for %s\n", t_handler->cid, r_info->uuid);
    return false;
}

static char *trim(char *s) {
    char *end;

    while (*s && isspace((unsigned char)*s))
        s++;

    if (*s == '\0')
        return s;
    end = s + strlen(s) - 1;

    while (end > s && isspace((unsigned char)*end))
        *end-- = '\0';
    return s;
}

int load_regioninfo(region_info_t *r_info, const char *path) {
    FILE *f = fopen(path, "rb");

    if (!f)
        return -1;

    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return -1;
    }
    long len = ftell(f);

    if (len < 0) {
        fclose(f);
        return -1;
    }
    rewind(f);
    char *orig = (char *)malloc((size_t)len + 1);

    if (!orig) {
        fclose(f);
        return -1;
    }
    size_t rd = fread(orig, 1, (size_t)len, f);
    fclose(f);

    if (rd != (size_t)len) {
        free(orig);
        return -1;
    }
    orig[len] = '\0';
    char *lower = (char *)malloc((size_t)len + 1);

    if (!lower) {
        free(orig);
        return -1;
    }

    for (long i = 0; i < len; ++i) {
        lower[i] = (char)tolower((unsigned char)orig[i]);
    }
    lower[len] = '\0';
    char *work = (char *)malloc((size_t)len + 1);

    if (!work) {
        free(orig);
        free(lower);
        return -1;
    }
    memcpy(work, lower, (size_t)len + 1);
    char *saveptr = NULL;

    for (char *line = strtok_r(work, "\r\n", &saveptr); line != NULL; line = strtok_r(NULL, "\r\n", &saveptr)) {
        char *pound = strchr(line, '#');

        if (pound)
            *pound = '\0';
        line = trim(line);

        if (*line == '\0')
            continue;
        char *eq = strchr(line, '=');

        if (!eq)
            continue;
        *eq = '\0';
        char *key = trim(line);
        char *val = trim(eq + 1);

        if (strcmp(key, "compression") == 0) {
            if (strcmp(val, "lz") == 0)
                r_info->enable_block_compression = BLOCK_COMPRESSION_LZ;

            if (strcmp(val, "snappy") == 0)
                r_info->enable_block_compression = BLOCK_COMPRESSION_SNAPPY;

            if (strcmp(val, "none") == 0)
                r_info->enable_block_compression = BLOCK_COMPRESSION_NONE;
        } else if (strcmp(key, "record_compression") == 0) {
            r_info->enable_record_compression = (strcmp(val, "lz") == 0) ? 1 : 0;
        } else if (strcmp(key, "dictionary") == 0) {
            r_info->enable_dict = (strcmp(val, "true") == 0) ? 1 : 0;
        } else if (strcmp(key, "record_target_count") == 0) {
            if (*val != '\0') {
                char *endptr = NULL;
                errno = 0;
                unsigned long long parsed = strtoull(val, &endptr, 10);

                if (errno == 0 && endptr != val && *endptr == '\0') {
                    if (parsed <= SIZE_MAX)
                        r_info->record_target_count = (size_t)parsed;
                }
            }
        }
    }
    free(orig);
    free(lower);
    free(work);
    return 0;
}

static void free_new_region_message(msg_cli_t *msg) {
    if (msg == NULL)
        return;

    if (msg->kv != NULL) {
        if (msg->kv->qv != NULL) {
            free(msg->kv->qv->qfl);
            free(msg->kv->qv);
        }
        free(msg->kv->key);
        free(msg->kv->cf);
        free(msg->kv);
    }
    free(msg);
}

static void free_unpublished_region_buffers(region_info_t *r_info) {
    free_new_region_message(r_info->msg_get);
    free_new_region_message(r_info->msg_get_put);
    free_new_region_message(r_info->msg_get_put_link);
    free_new_region_message(r_info->msg_get_link);
    free_new_region_message(r_info->msg_get_rotate);
    r_info->msg_get = r_info->msg_get_put = r_info->msg_get_put_link = NULL;
    r_info->msg_get_link = r_info->msg_get_rotate = NULL;

    if (r_info->scan_keys_list != NULL) {
        for (int i = 0; i < r_info->scan_keys_list_count_tot; i++) {
            if (r_info->scan_keys_list[i] == NULL)
                continue;
            free(r_info->scan_keys_list[i]->keys);
            free(r_info->scan_keys_list[i]->key_sizes);
            free(r_info->scan_keys_list[i]);
        }
        free(r_info->scan_keys_list);
    }
    r_info->scan_keys_list = NULL;
    r_info->scan_keys_list_count_tot = r_info->scan_keys_list_count_curr = 0;
#define FREE_NEW_REGION_FIELD(field)                                                                                                                                                    \
    do {                                                                                                                                                                                \
        free(r_info->field);                                                                                                                                                            \
        r_info->field = NULL;                                                                                                                                                           \
    } while (0)
    FREE_NEW_REGION_FIELD(scan_vars);
    FREE_NEW_REGION_FIELD(r_idx_b);
    FREE_NEW_REGION_FIELD(rbuff);
    FREE_NEW_REGION_FIELD(write_4K_buff);
    FREE_NEW_REGION_FIELD(zbuff);
    FREE_NEW_REGION_FIELD(bbuff);
    FREE_NEW_REGION_FIELD(lob_buff);
    FREE_NEW_REGION_FIELD(wal_lob_buff);
    FREE_NEW_REGION_FIELD(rsc_msg);
    FREE_NEW_REGION_FIELD(resize_buff);
    FREE_NEW_REGION_FIELD(resize_buff_size);
    FREE_NEW_REGION_FIELD(k_beg);
    FREE_NEW_REGION_FIELD(k_end);
    FREE_NEW_REGION_FIELD(uuid);
    FREE_NEW_REGION_FIELD(r_fname);
#undef FREE_NEW_REGION_FIELD
    r_info->rbuff_size = r_info->zbuff_size = r_info->bbuff_size = 0;
    r_info->write_4K_buff_used = 0;
    r_info->write_4K_fd = -1;
    record_version_state_init(&r_info->record_versions);
    reset_record_gc(r_info);
    r_info->pending_delete = 0;
}

region_info_t *build_new_region(struct table_info_t *t_info, char *r_uuid, char *r_fname, uint32_t r_cmd, char *r_key_beg, char *r_key_end, char *r_replica1, int32_t r_replica1_port,
                                char *r_replica2, int32_t r_replica2_port, char *r_compress, table_handler_t *t_handler) {
    uint64_t cid = t_handler->cid;
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Start to build the new region %s on table %s\n", curr_dt(dt), cid, r_uuid, t_info->fullname);
    struct region_info_t *r_info = NULL;
    bool region_locked = false;
    bool resize_coll_ready = false, resize_help_ready = false;
    keys_double_t **prepared_keepers = calloc(BLOCK_MAX_SIZE_M, sizeof(*prepared_keepers));
    lob_map_t *prepared_lob_map = NULL;
    idx_region_info_t *prepared_lob_idx = NULL;
#if !ENTERPRISE
    if (!ce_region_has_no_lob(r_fname, cid))
        goto region_prepare_failed;
#endif
    if (prepared_keepers == NULL)
        goto region_prepare_failed;

    for (int bs = 0; bs < BLOCK_MAX_SIZE_M - 1; bs++) {
        if (!allocate_double_keys_keeper(&prepared_keepers[bs]))
            goto region_prepare_failed;
    }
    prepared_lob_idx = calloc(1, sizeof(*prepared_lob_idx));

    if (prepared_lob_idx == NULL)
        goto region_prepare_failed;
    prepared_lob_idx->bsize_factor = MAX_BSIZE_FACTOR;

    for (int b = 0; b < BUCKETS; b++)
        prepared_lob_idx->fd[b] = -1;
    t_info->srv_tbl_version++;
    uint32_t r_pos = -1;

    for (int r = 0; r < t_info->region_count_curr; r++) {
        r_info = t_info->region_info[r];

        if (r_info->is_freed) {
            if (pthread_mutex_trylock(&r_info->rlock) == 0) {
                pthread_mutex_unlock(&r_info->rlock);
            } else {
                fprintf(stderr, "%s DEBUG [REGION_UTILS=%015lu] Lock busy %u\n", curr_dt(dt), cid, r_info->hash_idx);
                continue;
            }
            r_pos = r;
            break;
        }
    }

    if (r_pos == -1) {
        if (t_info->region_count_curr == t_info->region_count_total && !resize_regions(t_info, cid))
            goto region_prepare_failed;
        r_pos = t_info->region_count_curr++;
        fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Increasing table %s->region_count_curr=%d\n", curr_dt(dt), cid, t_info->fullname, t_info->region_count_curr);
    }
    r_info = t_info->region_info[r_pos];

    if (r_pos + 1 < t_info->region_count_curr)
        usleep((rand() % 100 + 10) * MAX_REGION_WAIT_LOCK_M);

    if (get_r_lock(r_info, NEW_REGION_LOCK, false, true, t_handler) == false) {
        goto region_prepare_failed;
    }
    region_locked = true;
    reset_region_memory_snapshot(r_info);
    invalidate_replicas_idle_commit(r_info);
    atomic_store(&r_info->replicas_idle_retry_after_ms, 0);
    r_info->is_freed = true;
    r_info->stop = true;
    record_version_state_init(&r_info->record_versions);
    reset_record_gc(r_info);
    r_info->pending_delete = 0;
    r_info->uuid = r_info->r_fname = NULL;
    r_info->k_beg = r_info->k_end = NULL;
    r_info->rbuff = r_info->write_4K_buff = r_info->zbuff = r_info->bbuff = NULL;
    r_info->lob_buff = r_info->wal_lob_buff = r_info->rsc_msg = NULL;
    r_info->resize_buff = NULL;
    r_info->resize_buff_size = NULL;
    r_info->msg_get = r_info->msg_get_put = r_info->msg_get_put_link = NULL;
    r_info->msg_get_link = r_info->msg_get_rotate = NULL;
    r_info->r_idx_b = NULL;
    r_info->scan_vars = NULL;
    r_info->scan_keys_list = NULL;
    r_info->scan_keys_list_count_tot = r_info->scan_keys_list_count_curr = 0;
    atomic_store(&r_info->recovery_path_ready, 0);
    atomic_store(&r_info->shutdown_region_state, SHUTDOWN_REGION_IDLE);
    // TODO Universal memory allocator with blackjack and prostitution
    r_info->uuid = (char *)malloc((strlen(r_uuid) + 1) * sizeof(char));

    if (r_info->uuid == NULL) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Malloc %s[%d]->uuid == NULL\n", curr_dt(dt), cid, r_uuid, r_pos);
        goto region_prepare_failed;
    }
    strcpy(r_info->uuid, r_uuid);

    if (r_info->uuid[strlen(r_info->uuid) - 1] == '0')
        r_info->is_leader = true;
    else
        r_info->is_leader = false;

    if (strlen(r_fname) + 256 > MAX_FN_SIZE) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Error code 38. Wrong %s r_fname size=%lu\n", curr_dt(dt), cid, r_info->uuid, strlen(r_fname));
        goto region_prepare_failed;
    }
    r_info->r_fname = (char *)malloc((strlen(r_fname) + 1) * sizeof(char));

    if (r_info->r_fname == NULL) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Error code 39. Malloc %s[%d]->r_name == NULL\n", curr_dt(dt), cid, r_info->uuid, r_pos);
        goto region_prepare_failed;
    }
    strcpy(r_info->r_fname, r_fname);
    atomic_store(&r_info->recovery_path_ready, 1);
    r_info->waiting_count_lock = 0;
    r_info->waiting_count_admin = 0;
    r_info->waiting_count_reconnect = 0;
    r_info->replica_stoped = false;
    r_info->dont_free_now = false;
    r_info->req_counter = 0;
    r_info->req_get_counter = 0;
    r_info->req_ins_counter = 0;
    r_info->req_upd_counter = 0;
    r_info->req_del_counter = 0;
    r_info->req_scn_counter = 0;
    r_info->req_rep_counter = 0;
    r_info->req_get_counter_sum = 0;
    r_info->req_put_counter_sum = 0;
    r_info->req_del_counter_sum = 0;
    r_info->req_scn_counter_sum = 0;

    if (r_info->append_compact_dirty_map != NULL)
        memset(r_info->append_compact_dirty_map, 0, r_info->append_compact_dirty_map_size);
    r_info->append_compact_dirty_segments = 0;
    r_info->append_compact_garbage_bytes = 0;
    r_info->append_compact_occupied_bytes = 0;
    r_info->append_compact_cursor_segment = 0;
    atomic_store(&r_info->append_compaction_requested, false);
    atomic_store(&r_info->append_compaction_requested_since_ms, 0);
    atomic_store(&r_info->append_compaction_retry_after_ms, 0);
    atomic_store(&r_info->append_compact_truncate_pending, false);
    atomic_store(&r_info->append_compact_last_activity_sum, 0);
    atomic_store(&r_info->append_compact_idle_streak, 0);
    r_info->enable_dict = false;
    size_t k_beg_len = 0;
    size_t k_end_len = 0;
    size_t key_base64_len = 4 * ((REGION_BYTE_RANGE + 2) / 3);

    if (strlen(r_key_beg) != key_base64_len || strlen(r_key_end) != key_base64_len) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Wrong base64 borders length for %s\n", curr_dt(dt), cid, r_info->uuid);
        goto region_prepare_failed;
    }
    r_info->k_beg = base64_decode(r_key_beg, key_base64_len, &k_beg_len);
    r_info->k_end = base64_decode(r_key_end, key_base64_len, &k_end_len);

    if (r_info->k_beg == NULL || r_info->k_end == NULL || k_beg_len != REGION_BYTE_RANGE || k_end_len != REGION_BYTE_RANGE) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Wrong base64 borders for %s\n", curr_dt(dt), cid, r_info->uuid);
        free(r_info->k_beg);
        free(r_info->k_end);
        r_info->k_beg = NULL;
        r_info->k_end = NULL;
        goto region_prepare_failed;
    }

    for (int i = 0; i < REGION_BYTE_RANGE; i++) {
        if (r_info->k_beg[i] < r_info->k_end[i])
            break;

        if (r_info->k_beg[i] > r_info->k_end[i]) {
            fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Wrong borders for %s\n", curr_dt(dt), cid, r_info->uuid);
            goto region_prepare_failed;
        }
    }
    r_info->rbuff_size = BLOCK_MAX_SIZE * 2;
    r_info->rbuff = malloc(r_info->rbuff_size);

    if (r_info->rbuff == NULL)
        goto region_prepare_failed;
    r_info->write_4K_buff = malloc(FIX_BLOCK_SIZE);

    if (r_info->write_4K_buff == NULL) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Malloc write_4K_buff failed for %s\n", curr_dt(dt), cid, r_info->uuid);
        free(r_info->rbuff);
        r_info->rbuff = NULL;
        goto region_prepare_failed;
    }
    r_info->write_4K_buff_used = 0;
    atomic_store(&r_info->write_4K_buffered_since_ms, 0);
    r_info->write_4K_file_offset = 0;
    r_info->write_4K_fd = -1;
    r_info->write_4K_cache_ok = false;
    r_info->write_4K_cache_block_offset = 0;
    r_info->zbuff_size = 0;
    r_info->zbuff = NULL;
    r_info->block_comp_ins_buff_size = 0;
    r_info->block_comp_ins_buff = NULL;
    r_info->block_comp_buff_size = 0;
    r_info->block_comp_buff = NULL;
    r_info->snappy_work_buff = NULL;
    r_info->z_stat_size = 0;
    r_info->z_stat = NULL;
    r_info->scan_key = NULL;
    r_info->scan_key_size = 0;
    r_info->resize_buff_size = 0;
    r_info->resize_buff = NULL;
    r_info->rsc_msg = NULL;
    r_info->wal_lob_buff = NULL;

    if (r_info->is_leader) {
        r_info->zbuff_size = BLOCK_MAX_SIZE * 3;
        r_info->zbuff = malloc(r_info->zbuff_size);

        if (r_info->zbuff == NULL)
            goto region_prepare_failed;
        memset(r_info->zbuff, 0, r_info->zbuff_size);
        r_info->bbuff_size = BLOCK_MAX_SIZE * 10;
        r_info->bbuff = malloc(r_info->bbuff_size);

        if (r_info->bbuff == NULL)
            goto region_prepare_failed;
        r_info->lob_in_bbuff = false;
        r_info->rsc_msg = malloc(RSC_MSG_BUFFER_SIZE);

        if (r_info->rsc_msg == NULL)
            goto region_prepare_failed;
        r_info->resize_buff = malloc(8 * sizeof(*r_info->resize_buff));

        if (r_info->resize_buff == NULL)
            goto region_prepare_failed;
        r_info->resize_buff_size = malloc(8 * sizeof(uint32_t));

        if (r_info->resize_buff_size == NULL)
            goto region_prepare_failed;
    } else
        r_info->files_list = NULL;
    r_info->msg_get = calloc(1, sizeof(struct msg_cli_t));

    if (r_info->msg_get == NULL)
        goto region_prepare_failed;
    r_info->msg_get->kv = calloc(1, sizeof(struct msg_kv_t));

    if (r_info->msg_get->kv == NULL)
        goto region_prepare_failed;
    r_info->msg_get->kv->qv = calloc(1, sizeof(struct msg_qv_t));

    if (r_info->msg_get->kv->qv == NULL)
        goto region_prepare_failed;
    r_info->msg_get->kv->key_size = 0;
    r_info->msg_get->kv->cf_size = 0;
    r_info->msg_get_put = calloc(1, sizeof(struct msg_cli_t));

    if (r_info->msg_get_put == NULL)
        goto region_prepare_failed;
    r_info->msg_get_put->kv = calloc(1, sizeof(struct msg_kv_t));

    if (r_info->msg_get_put->kv == NULL)
        goto region_prepare_failed;
    r_info->msg_get_put->kv->key_size = 0;
    r_info->msg_get_put->kv->cf_size = 0;
    r_info->msg_get_put_allocated_key_size = 8;
    r_info->msg_get_put_allocated_cf_size = 2;
    r_info->msg_get_put_allocated_qfl_size = 2;
    r_info->msg_get_put->kv->key = malloc(r_info->msg_get_put_allocated_key_size);

    if (r_info->msg_get_put->kv->key == NULL) {
        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Cannot allocate memory r_info->msg_get_put->kv->key for %s\n", curr_dt(dt), cid, r_info->uuid);
        goto region_prepare_failed;
    }
    r_info->msg_get_put->kv->cf = malloc(r_info->msg_get_put_allocated_cf_size);

    if (r_info->msg_get_put->kv->cf == NULL) {
        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Cannot allocate memory r_info->msg_get_put->kv->cf for %s\n", curr_dt(dt), cid, r_info->uuid);
        goto region_prepare_failed;
    }
    r_info->msg_get_put->kv->qv = calloc(1, sizeof(struct msg_qv_t));

    if (r_info->msg_get_put->kv->qv == NULL) {
        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Cannot allocate memory r_info->msg_get_put->kv->qv for %s\n", curr_dt(dt), cid, r_info->uuid);
        goto region_prepare_failed;
    }
    r_info->msg_get_put->kv->qv->qfl_size = 0;
    r_info->msg_get_put->kv->qv->qfl = malloc(r_info->msg_get_put_allocated_qfl_size);

    if (r_info->msg_get_put->kv->qv->qfl == NULL) {
        fprintf(stderr, "%s ERROR [IDX_BUILDER=%015lu] Cannot allocate memory r_info->msg_get_put->kv->qv->qfl for %s\n", curr_dt(dt), cid, r_info->uuid);
        goto region_prepare_failed;
    }
    r_info->msg_get_rotate = calloc(1, sizeof(struct msg_cli_t));

    if (r_info->msg_get_rotate == NULL)
        goto region_prepare_failed;
    r_info->msg_get_rotate->kv = calloc(1, sizeof(struct msg_kv_t));

    if (r_info->msg_get_rotate->kv == NULL)
        goto region_prepare_failed;
    r_info->msg_get_rotate->kv->qv = calloc(1, sizeof(struct msg_qv_t));

    if (r_info->msg_get_rotate->kv->qv == NULL)
        goto region_prepare_failed;
    r_info->msg_get_rotate->kv->key_size = 0;
    r_info->msg_get_rotate->kv->cf_size = 0;
    r_info->idx_resizer = RESIZE_IDX_DONE;
    r_info->r_idx_b = calloc(BLOCK_MAX_SIZE_M + 1, sizeof(*r_info->r_idx_b));

    if (r_info->r_idx_b == NULL)
        goto region_prepare_failed;
    r_info->scan_keys_list_count_tot = 1;
    r_info->scan_keys_list_count_curr = 0;
    r_info->scan_vars = calloc(1, sizeof(scan_vars_t));

    if (r_info->scan_vars == NULL)
        goto region_prepare_failed;
    r_info->scan_keys_list = calloc(r_info->scan_keys_list_count_tot, sizeof(*r_info->scan_keys_list));

    if (r_info->scan_keys_list == NULL)
        goto region_prepare_failed;

    for (int i = 0; i < r_info->scan_keys_list_count_tot; i++) {
        r_info->scan_keys_list[i] = (scan_keys_list_t *)calloc(1, sizeof(struct scan_keys_list_t));

        if (r_info->scan_keys_list[i] == NULL)
            goto region_prepare_failed;
        r_info->scan_keys_list[i]->cid = 0;
        r_info->scan_keys_list[i]->keys = malloc(1);

        if (r_info->scan_keys_list[i]->keys == NULL)
            goto region_prepare_failed;
        r_info->scan_keys_list[i]->key_sizes = malloc(1);

        if (r_info->scan_keys_list[i]->key_sizes == NULL)
            goto region_prepare_failed;
        r_info->scan_keys_list[i]->keys_mem_usage_limit = 1;
        r_info->scan_keys_list[i]->max_key_count = 0;
    }
    r_info->max_idx_b = 0;

    for (int bs = 0; bs < BLOCK_MAX_SIZE_M; bs++) {
        r_info->r_idx_b[bs] = NULL;
        r_info->bs_map[bs] = 0;
    }
    r_info->r_idx_b[BS_LOB] = NULL;
    r_info->drop_lob = false;
    r_info->max_lob_id = 0;
    r_info->bulk_mode = BUIK_RELICA_OFF;
    r_info->bulk_buff1 = NULL;
    r_info->bulk_buff_size1 = 0;
    r_info->bulk_buff_offset1 = 0;
    r_info->bulk_buff2 = NULL;
    r_info->bulk_buff_size2 = 0;
    r_info->bulk_buff_offset2 = 0;
    r_info->qfl_info = NULL;
    r_info->is_write = 0;
    r_info->is_replicate = 0;

    if (r_info->is_leader) {
        if (pthread_mutex_init(&r_info->idx_resize_coll_lock, NULL) != 0)
            goto region_prepare_failed;
        resize_coll_ready = true;

        if (pthread_mutex_init(&r_info->idx_resize_help_lock, NULL) != 0)
            goto region_prepare_failed;
        resize_help_ready = true;
    }

    if (!replica_info_builder(r_info, t_info, r_replica1, r_replica1_port, r_replica2, r_replica2_port, r_uuid, t_handler))
        goto region_prepare_failed;
    r_info->is_freed = false;
    r_info->stop = false;
    r_info->data_000_fd = -1;
    t_info->srv_tbl_version++;
    r_info->k_dbl = prepared_keepers;
    r_info->lob_map = prepared_lob_map;
    r_info->r_idx_b[BS_LOB] = prepared_lob_idx;
    r_info->lob_count_total = 10;
    r_info->lob_count_curr = 0;
    prepared_keepers = NULL;
    prepared_lob_map = NULL;
    prepared_lob_idx = NULL;
    t_info->region_count_curr_assigned++;
    char tname[MAX_FN_SIZE];
    snprintf(tname, MAX_FN_SIZE, "%s", t_info->fullname);
    uint32_t srv_tbl_version = t_info->srv_tbl_version;
    do_t_unlock(t_info, 16, true, cid);

    if (data_check_enabled == 1) {
        char region_recovery_info[MAX_FN_SIZE];
        snprintf(region_recovery_info, MAX_FN_SIZE, "%s/%s", r_info->r_fname, recovery_info_fn);
        r_info->r_need_data_chk = get_data_check_ts(r_info, t_handler, region_recovery_info, cid);

        if (need_data_chk > 0 && r_info->r_need_data_chk == 0 && region_needs_restart_marker(r_info->r_fname, need_data_chk, cid))
            set_region_recovery_info(r_info, 1L, t_handler);
    }
#ifdef COLL_LOB_TEST
#endif
    if (r_info->is_leader) {
        r_info->files_list = get_files_list(r_info, false, t_handler);

        if (r_info->r_need_data_chk != 0 && r_info->files_list == NULL) {
            fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Cannot create initial files list for recovery of %s\n", curr_dt(dt), cid, r_info->r_fname);
            goto region_initialization_failed;
        }
    }
    char fname[MAX_FN_SIZE];
    r_info->bad_data = false;
    r_info->already_finishing = false;
    r_info->enable_block_compression = enable_block_compression;
    r_info->enable_record_compression = enable_compression;
    r_info->enable_dict = enable_dict;
    snprintf(fname, MAX_FN_SIZE, "%s/.regioninfo", r_info->r_fname);
    load_regioninfo(r_info, fname);

    if (r_info->enable_block_compression == BLOCK_COMPRESSION_SNAPPY) {
        r_info->snappy_work_buff = malloc(1 << SNAPPY_BLOCK_SIZE_POW2);

        if (!r_info->snappy_work_buff) {
            fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Cannot malloc snappy_work_buff %s\n", curr_dt(dt), cid, fname);
            goto region_initialization_failed;
        }
    }

    if (r_info->enable_block_compression > BLOCK_COMPRESSION_NONE) {
        r_info->enable_record_compression = 0;

        if (r_info->enable_block_compression == BLOCK_COMPRESSION_LZ)
            fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Enabled block compression LZ for %s\n", curr_dt(dt), cid, fname);

        if (r_info->enable_block_compression == BLOCK_COMPRESSION_SNAPPY)
            fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Enabled block compression SNAPPY for %s\n", curr_dt(dt), cid, fname);
    } else {
        if (r_info->enable_record_compression > 0)
            fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Enabled record compression for %s\n", curr_dt(dt), cid, fname);
    }
    uint8_t enable_dict_orig = r_info->enable_dict;
    r_info->enable_dict = 0;

    if (r_info->enable_block_compression) {
        r_info->block_comp_ins_buff_size = 1024;
        r_info->block_comp_ins_buff = malloc(r_info->block_comp_ins_buff_size);

        if (r_info->block_comp_ins_buff == NULL) {
            fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Cannot malloc block_comp_ins_buff %s\n", curr_dt(dt), cid, fname);
            goto region_initialization_failed;
        }
        r_info->block_comp_buff_size = 1024;
        r_info->block_comp_buff = malloc(r_info->block_comp_buff_size);

        if (r_info->block_comp_buff == NULL) {
            fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Cannot malloc block_comp_buff %s\n", curr_dt(dt), cid, fname);
            goto region_initialization_failed;
        }
        r_info->scan_key = NULL;
        r_info->scan_key_size = 0;
    }

    if (idx_builder(r_info, 0, BLOCK_MAX_SIZE, t_handler) != 0)
        goto region_index_failed;

    for (int bs = 0; bs < BLOCK_MAX_SIZE_M - 1; bs++) {
        snprintf(fname, MAX_FN_SIZE, "%s/%03d.data", r_info->r_fname, bs);
        fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Check is file %s exist\n", curr_dt(dt), cid, fname);
        uint64_t file_size = get_file_size(fname);
        fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] File %s size %lu\n", curr_dt(dt), cid, fname, file_size);

        if (file_size > 1024) {
            get_bs_map_pos(r_info, bs, true, t_handler);
        }
    }
    r_info->enable_dict = enable_dict_orig;

    if (r_compress != NULL)
        if (strcmp(r_compress + strlen(COMRESSION_STR), "dic") == 0)
            r_info->enable_dict = 1;

    if (r_info->enable_dict == 1 && !load_dict(r_info, t_handler))
        goto region_index_failed;

    if (r_info->is_leader && r_info->wal_lob_buff != NULL) {
        free(r_info->wal_lob_buff);
        r_info->wal_lob_buff = NULL;
    }
    r_info->status = r_cmd;
    r_info->stop = false;
    refresh_region_memory_locked(r_info, 0);
    fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] The new region %s->%s is ready. Position = %d, srv_tbl_version = %d\n", curr_dt(dt), cid, tname, r_info->uuid, r_pos,
            srv_tbl_version);
    return r_info;
region_index_failed:
    r_info->stop = true;
    r_info->bad_data = true;
    set_region_recovery_info(r_info, 1, t_handler);
    free_prepared_region_keepers(r_info->k_dbl);
    r_info->k_dbl = NULL;
    free(r_info->wal_lob_buff);
    r_info->wal_lob_buff = NULL;
    do_r_unlock(r_info, 118, true, t_handler);
    return NULL;
region_initialization_failed:
    r_info->stop = true;
    r_info->bad_data = true;
    free_files_list(r_info->files_list);
    r_info->files_list = NULL;
    free_prepared_region_keepers(r_info->k_dbl);
    r_info->k_dbl = NULL;
    free(r_info->snappy_work_buff);
    r_info->snappy_work_buff = NULL;
    free(r_info->block_comp_ins_buff);
    r_info->block_comp_ins_buff = NULL;
    r_info->block_comp_ins_buff_size = 0;
    free(r_info->block_comp_buff);
    r_info->block_comp_buff = NULL;
    r_info->block_comp_buff_size = 0;
    free(r_info->wal_lob_buff);
    r_info->wal_lob_buff = NULL;
    free(r_info->resize_buff);
    r_info->resize_buff = NULL;
    free(r_info->resize_buff_size);
    r_info->resize_buff_size = NULL;
    fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Region %s initialization failed; stopped until UNASSIGN\n", curr_dt(dt), cid, r_info->uuid);
    do_r_unlock(r_info, 118, true, t_handler);
    return NULL;
region_prepare_failed:
    if (region_locked) {
        if (resize_help_ready)
            pthread_mutex_destroy(&r_info->idx_resize_help_lock);

        if (resize_coll_ready)
            pthread_mutex_destroy(&r_info->idx_resize_coll_lock);
        free_unpublished_region_buffers(r_info);
        r_info->is_freed = true;
        r_info->status = STATUS_UNASSIGN_REGION;
        r_info->stop = false;
        atomic_store(&r_info->recovery_path_ready, 0);
    }
    free_prepared_region_keepers(prepared_keepers);
    free(prepared_lob_map);
    free(prepared_lob_idx);
    fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Cannot prepare new region %s\n", curr_dt(dt), cid, r_uuid);
    do_t_unlock(t_info, 18, true, cid);

    if (region_locked)
        do_r_unlock(r_info, 118, true, t_handler);
    return NULL;
}


bool resize_regions(struct table_info_t *t_info, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    int old_count = t_info->region_count_total;

    if (old_count <= 0 || old_count > INT_MAX / 2 || (size_t)old_count > SIZE_MAX / 2 / sizeof(region_info_t *)) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Invalid region capacity for %s\n", curr_dt(dt), cid, t_info->fullname);
        return false;
    }
    int new_count = old_count * 2;
    int rlocks_ready = old_count, controls_ready = old_count;

    while (atomic_load(&is_shutting_down))
        sleep(10);
    pthread_mutex_lock(&tables_lock);
    begin_tables_resize();
    region_info_t **region_info_tmp = calloc((size_t)new_count, sizeof(*region_info_tmp));

    if (region_info_tmp == NULL)
        goto allocation_failed;
    memcpy(region_info_tmp, t_info->region_info, (size_t)old_count * sizeof(*region_info_tmp));

    for (int r = old_count; r < new_count; r++) {
        region_info_tmp[r] = calloc(1, sizeof(*region_info_tmp[r]));

        if (region_info_tmp[r] == NULL)
            goto allocation_failed;
        region_info_tmp[r]->replica = calloc(1, sizeof(*region_info_tmp[r]->replica));

        if (region_info_tmp[r]->replica == NULL)
            goto allocation_failed;

        if (pthread_mutex_init(&region_info_tmp[r]->rlock, NULL) != 0)
            goto allocation_failed;
        rlocks_ready++;

        if (pthread_mutex_init(&region_info_tmp[r]->control_mutex, NULL) != 0)
            goto allocation_failed;
        controls_ready++;
        region_info_tmp[r]->is_freed = true;
        region_info_tmp[r]->status = STATUS_UNASSIGN_REGION;
    }
    region_info_t **old_regions = t_info->region_info;
    t_info->region_info = region_info_tmp;
    t_info->region_count_total = new_count;
    free(old_regions);
    end_tables_resize();
    pthread_mutex_unlock(&tables_lock);
    fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Resize table regions %s from %d to %d slots done\n", curr_dt(dt), cid, t_info->fullname, old_count, new_count);
    return true;
allocation_failed:
    if (region_info_tmp != NULL) {
        for (int r = old_count; r < new_count; r++) {
            if (region_info_tmp[r] == NULL)
                continue;

            if (r < controls_ready)
                pthread_mutex_destroy(&region_info_tmp[r]->control_mutex);

            if (r < rlocks_ready)
                pthread_mutex_destroy(&region_info_tmp[r]->rlock);
            free(region_info_tmp[r]->replica);
            free(region_info_tmp[r]);
        }
        free(region_info_tmp);
    }
    end_tables_resize();
    pthread_mutex_unlock(&tables_lock);
    fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Cannot prepare region slots for %s\n", curr_dt(dt), cid, t_info->fullname);
    return false;
}

bool resize_tables(uint64_t cid) {
    char dt[DT_LOG_SIZE];
    begin_tables_resize();
    fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Resize tables_info from %d to %d slots\n", curr_dt(dt), cid, table_count_total, table_count_total * 2);
    uint32_t new_count = table_count_total * 2;
    uint32_t new_mem_size = new_count * sizeof(table_info_t *);
    table_info_t **new_table_info = malloc(new_mem_size);

    if (new_table_info == NULL) {
        fprintf(stderr, "%s FATAL [REGION_HANDLER=%015lu] Cannot allocate new memory for table_info\n", curr_dt(dt), cid);
        graceful_exit(-9);
    }

    for (uint32_t i = 0; i < table_count_total; i++)
        new_table_info[i] = table_info[i];

    for (uint32_t i = table_count_total; i < new_count; i++) {
        new_table_info[i] = (table_info_t *)malloc(sizeof(table_info_t));

        if (new_table_info[i] == NULL) {
            fprintf(stderr, "%s FATAL [REGION_HANDLER=%015lu] Failed malloc new table_info[%d]\n", curr_dt(dt), cid, i);
            graceful_exit(-10);
        }
        memset(new_table_info[i], 0, sizeof(table_info_t));
        mutex_creator(&new_table_info[i]->tlock);
        new_table_info[i]->srv_tbl_version = 0;
        fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] New tables_info[%d]->srv_tbl_version=%u\n", curr_dt(dt), cid, i, new_table_info[i]->srv_tbl_version);
    }
    table_info_t **old_table_info = table_info;

    while (atomic_load(&is_shutting_down))
        sleep(10);
    table_info = new_table_info;
    table_count_total = new_count;
    usleep(10000);

    while (atomic_load(&is_shutting_down))
        sleep(10);
    free(old_table_info);
    fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Resize tables_info to %d slots has done\n", curr_dt(dt), cid, table_count_total);
    end_tables_resize();
    return true;
}

void set_replica_pointer(struct table_info_t *t_info, region_info_t *r_info, region_info_t *r_info_child, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    region_info_t *r_info_leader = NULL;
    uint8_t region_uuid_len = strlen(r_info->uuid);
    uint8_t replica_num = 0;

    if (r_info->uuid[region_uuid_len - 1] == '1')
        replica_num = 1;

    if (r_info->uuid[region_uuid_len - 1] == '2')
        replica_num = 2;

    if (replica_num != 0) {
        fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Replica num = %d. Check parents replica %s\n", curr_dt(dt), cid, replica_num, r_info->r_fname);

        for (int r = 0; r < t_info->region_count_curr; r++) {
            r_info_leader = t_info->region_info[r];

            if (r_info_leader->is_freed == false) {
                if (r_info_leader->uuid[region_uuid_len - 1] == '0') {
                    if (strncmp(r_info_leader->uuid, r_info->uuid, region_uuid_len - 1) == 0) {
                        fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Found parents replica\n", curr_dt(dt), cid);
                        invalidate_replicas_idle_commit(r_info_leader);
                        invalidate_record_gc(r_info_leader);

                        if (replica_num == 1)
                            r_info_leader->replica->r_info1 = r_info_child;

                        if (replica_num == 2)
                            r_info_leader->replica->r_info2 = r_info_child;

                        if (r_info_child == NULL)
                            fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Parent %s->replica->r_info%d = NULL\n", curr_dt(dt), cid, r_info_leader->r_fname, replica_num);
                        else
                            fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Parent %s->replica->r_info%d = %s\n", curr_dt(dt), cid, r_info_leader->r_fname, replica_num,
                                    r_info_child->uuid);
                        break;
                    }
                }
            }
        }
    }
}


table_handler_t *create_t_handler(int new_socket_fd) {
    struct table_handler_t *t_handler = calloc(1, sizeof(struct table_handler_t));

    if (t_handler == NULL)
        goto allocation_failed;
    t_handler->is_client = false;
    t_handler->is_replica_client = false;
    t_handler->leader_commit = true;
    t_handler->r_pos = -1;
    t_handler->t_pos = -1;
    (void)new_socket_fd;
    t_handler->msg_srv = calloc(1, sizeof(struct msg_srv_t));

    if (t_handler->msg_srv == NULL)
        goto allocation_failed;
    t_handler->msg_srv->curr_max_msg_size = FIX_MAX_MSG_SIZE * 16 + MAX_BSIZE_FACTOR; // TODO сделать изначальный размер меньше и реалок при необходимости
    t_handler->msg_cli = calloc(1, sizeof(struct msg_cli_t));

    if (t_handler->msg_cli == NULL)
        goto allocation_failed;
    t_handler->msg_cli->kv = calloc(1, sizeof(msg_kv_t));

    if (t_handler->msg_cli->kv == NULL)
        goto allocation_failed;
    t_handler->msg_cli->kv->qv = calloc(1, sizeof(msg_qv_t));

    if (t_handler->msg_cli->kv->qv == NULL)
        goto allocation_failed;
    t_handler->msg_cli->kv->key = NULL;
    t_handler->msg_cli->kv->key_size = 0;
    t_handler->msg_cli->kv->cf_size = 0;
    t_handler->msg_cli_allocated_key_size = 0;
    t_handler->msg_srv->resp_buff = malloc(t_handler->msg_srv->curr_max_msg_size);

    if (t_handler->msg_srv->resp_buff == NULL)
        goto allocation_failed;
    t_handler->msg_srv->total_max_msg_size = t_handler->msg_srv->curr_max_msg_size;
    t_handler->msg_srv->resp_buff_base = t_handler->msg_srv->resp_buff;
    t_handler->recovery_info_path = NULL;
    memset(t_handler->msg_srv->resp_buff, 0, t_handler->msg_srv->curr_max_msg_size);
    t_handler->msg_srv->msg_size = 0;
    t_handler->ip = NULL;
    t_handler->port = NULL;
    t_handler->resizing = 0;
    t_handler->conn_father = NULL;
    t_handler->big_buff = NULL;
    t_handler->need_regions_map = true;
    t_handler->client_timeout_ms = 60000;
    t_handler->rtt_us_avg = 0;
    t_handler->rtt_us_count = 0;
    t_handler->append_compaction_piggyback = NULL;
    t_handler->admin_data_check_region = NULL;
    t_handler->t_info_size = 1;
    t_handler->t_info_curr = 0;
    t_handler->t_info = malloc(t_handler->t_info_size * sizeof(struct table_info_t *));

    if (t_handler->t_info == NULL)
        goto allocation_failed;
    t_handler->cli_tbl_versions = NULL;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    t_handler->recv_ts = ts.tv_sec * 1000L;
    return t_handler;
allocation_failed:
    free_t_handler(t_handler);
    fprintf(stderr, "ERROR [REGION_HANDLER] Cannot allocate table handler\n");
    return NULL;
}

void free_t_handler(table_handler_t *t_handler) {
    if (t_handler == NULL)
        return;
    const bool is_client = atomic_load(&t_handler->is_client);
    free_append_compaction_piggyback(t_handler);

    if (t_handler->msg_srv != NULL) {
        free(t_handler->msg_srv->resp_buff_base);
        free(t_handler->msg_srv);
    }

    if (t_handler->msg_cli != NULL) {
        if (t_handler->msg_cli->kv != NULL) {
            if (is_client) {
                free(t_handler->msg_cli->kv->key);
                free(t_handler->msg_cli->kv->cf);

                if (t_handler->msg_cli->kv->qv != NULL)
                    free(t_handler->msg_cli->kv->qv->qfl);
            }
            free(t_handler->msg_cli->kv->qv);
            free(t_handler->msg_cli->kv);
        }
        free(t_handler->msg_cli);
    }
    free(t_handler->recovery_info_path);
    free(t_handler->big_buff);
    free(t_handler->ip);
    free(t_handler->port);

    if (is_client) {
        free(t_handler->buff);
        free(t_handler->cli_tbl_versions);

        if (t_handler->t_info != NULL && t_handler->t_info_size != 0)
            free(t_handler->t_info[t_handler->t_info_size - 1]);
    }
    free(t_handler->t_info);
    free(t_handler);
}


static bool calc_usage_stat(struct msg_stat_t *msg_stat, size_t allocated_stat_size, unsigned long cid) {
    char dt[DT_LOG_SIZE];
    size_t stat_pos = 0;
    bool result = true;
    tables_snapshot_t snapshot = {0};

    if (!take_tables_snapshot(&snapshot, cid)) {
        free_tables_snapshot(&snapshot);
        return false;
    }

    for (int t = snapshot.count; t > 0;) {
        table_info_t *t_info = snapshot.tables[--t];

        if (t_info == NULL || !get_t_lock(t_info, 25, false, cid))
            continue;

        if (t_info->retired) {
            do_t_unlock(t_info, 27, false, cid);
            continue;
        }
        int current_region_count = t_info->region_count_curr;

        for (int r = 0; r < current_region_count; r++) {
            region_info_t *r_info_stat = t_info->region_info[r];

            if (r_info_stat == NULL)
                continue;

            if (!r_info_stat->is_freed && r_info_stat->uuid != NULL && strnlen(r_info_stat->uuid, UUID_SIZE + 1) == UUID_SIZE) {
                uint64_t tot_f_idx_curr_size = 0;
                uint8_t max_bs = atomic_load_explicit(&r_info_stat->max_idx_b, memory_order_acquire);

                if (max_bs != 0 && r_info_stat->r_idx_b != NULL) {
                    for (uint8_t bs = 0; bs < max_bs; bs++) {
                        if (r_info_stat->r_idx_b[bs] == NULL)
                            continue;

                        for (int b = 0; b < BUCKETS; b++) {
                            tot_f_idx_curr_size += atomic_load_explicit(&r_info_stat->r_idx_b[bs]->f_idx_curr_size[b], memory_order_relaxed);
                        }
                    }
                }
                uint64_t memory_bytes =
                    atomic_load_explicit(&r_info_stat->index_memory_bytes, memory_order_relaxed) + atomic_load_explicit(&r_info_stat->auxiliary_memory_bytes, memory_order_relaxed);
                char stat_text[160];
                int written = snprintf(stat_text, sizeof(stat_text), ":%" PRIu64 ":%" PRIu64 ":%ld:%ld:%ld;", tot_f_idx_curr_size, memory_bytes,
                                       atomic_load(&r_info_stat->req_get_counter_sum), atomic_load(&r_info_stat->req_put_counter_sum), atomic_load(&r_info_stat->req_del_counter_sum));

                if (written < 0 || (size_t)written >= sizeof(stat_text) || stat_pos > SIZE_MAX - UUID_SIZE - (size_t)written - 1) {
                    fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Usage-stat entry size overflow\n", curr_dt(dt), cid);
                    result = false;
                } else {
                    size_t required = stat_pos + UUID_SIZE + (size_t)written + 1;

                    if (required > allocated_stat_size) {
                        size_t new_size = allocated_stat_size;

                        while (new_size < required && new_size <= SIZE_MAX / 2) {
                            new_size *= 2;
                        }

                        if (new_size < required)
                            new_size = required;
                        char *new_stat = realloc(msg_stat->full_stat, new_size);

                        if (new_stat == NULL) {
                            fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Cannot reallocate %zu bytes for usage stat\n", curr_dt(dt), cid, new_size);
                            result = false;
                        } else {
                            msg_stat->full_stat = new_stat;
                            allocated_stat_size = new_size;
                        }
                    }

                    if (result) {
                        memcpy(msg_stat->full_stat + stat_pos, r_info_stat->uuid, UUID_SIZE);
                        stat_pos += UUID_SIZE;
                        memcpy(msg_stat->full_stat + stat_pos, stat_text, (size_t)written);
                        stat_pos += (size_t)written;
                        msg_stat->full_stat[stat_pos] = '\0';
                    }
                }
            }

            if (!result)
                break;
        }
        do_t_unlock(t_info, 27, false, cid);

        if (!result)
            break;
    }
    free_tables_snapshot(&snapshot);
    return result;
}


static inline void admin_region_cleanup(region_info_t *r_info, bool flags_set, bool simple_lock, uint32_t r_call_id, table_handler_t *t_handler) {
    if (r_info == NULL)
        return;
    invalidate_record_gc(r_info);
    atomic_store(&r_info->waiting_count_admin, 0);

    if (flags_set && r_info->replica != NULL) {
        pthread_mutex_lock(&r_info->control_mutex);
        r_info->replica->replica1_sock_waiting_conn = false;
        r_info->replica->replica2_sock_waiting_conn = false;
        pthread_mutex_unlock(&r_info->control_mutex);
    }

    if (simple_lock) {
        r_info->r_lock_id_before = r_info->r_lock_id;
        r_info->r_lock_id = 0;
        r_info->curr_cid_lock = 0;
        r_info->r_call_id_unlock = r_call_id;
        pthread_mutex_unlock(&r_info->rlock);
    } else {
        do_r_unlock(r_info, r_call_id, true, t_handler);
    }
}


void send_answer_java(pthread_arg_rs_t *pthread_arg_rs, uint32_t answer, char *buff_rs, char **ss, table_info_t *t_info, uint64_t cid, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];

    if (t_handler != NULL && t_handler->r_pos != -2)
        reserve_replica_pointer_check(t_info, t_handler);

    if (t_info != NULL) {
        t_info->need_lock = false;
        do_t_unlock(t_info, 24, true, cid);
    }
    uint8_t msg_rs_len = sizeof(struct msg_rs_answer_t);
    char msg_rs[msg_rs_len];
    memset(msg_rs, 0, msg_rs_len);

    if (answer <= 1) {
        msg_rs[0] = msg_rs_len;
        msg_rs[4] = MSG_RS;
        msg_rs[5] = answer;
        int res = (int)transport_write(&pthread_arg_rs->transport, msg_rs, msg_rs_len);

        if (res != msg_rs_len)
            fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Failed send cmd answer = %s : %d, res=%d (msg_rs_len=%d)\n", curr_dt(dt), cid, buff_rs, answer, res, msg_rs_len);
        else
            fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] RS cmd = %s : %d\n", curr_dt(dt), cid, buff_rs, answer);
    } else {
        int res = (int)transport_write(&pthread_arg_rs->transport, buff_rs, answer);

        if (res != answer)
            fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu]  Failed send ping answer %d, res=%d (len=%d)\n", curr_dt(dt), cid, answer, res, answer);
        else
            fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] RS ping size %d\n", curr_dt(dt), cid, answer);
    }
    transport_close(&pthread_arg_rs->transport);
    free_t_handler(t_handler);
    free(pthread_arg_rs);
    free(buff_rs);
    free(ss);
    rs_cmd_processing--;
}

static bool parse_rs_parent(const char *port_text, const char *pid_text, int *port, int *pid) {
    if (port_text == NULL || pid_text == NULL || *port_text < '0' || *port_text > '9' || *pid_text < '0' || *pid_text > '9')
        return false;
    char *end;
    errno = 0;
    long port_value = strtol(port_text, &end, 10);

    if (errno == ERANGE || *end != '\0' || port_value < 1 || port_value > 65535)
        return false;
    errno = 0;
    long pid_value = strtol(pid_text, &end, 10);

    if (errno == ERANGE || (*end != '\0' && *end != '@') || pid_value < 1 || pid_value > INT_MAX)
        return false;

    if (*end == '@' && end[1] == '\0')
        return false;
    *port = (int)port_value;
    *pid = (int)pid_value;
    return true;
}

static bool update_rs_replica_name(char **owner, const char *name) {
    if (name == NULL)
        return false;
    size_t length = strlen(name);

    if (*owner == NULL || strlen(*owner) < length) {
        char *next = realloc(*owner, length + 1U);

        if (next == NULL)
            return false;
        *owner = next;
    }
    memcpy(*owner, name, length + 1U);
    return true;
}


static void *region_cmd(void *arg) {
    char dt[DT_LOG_SIZE];
    pthread_arg_rs_t *req = arg;
    uint64_t cid = req->cid;
    table_info_t *t_info = NULL;
    char *buf;
    buf = malloc(MAX_RS_MSG_SIZE);

    if (!buf) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Cannot allocate administrative request buffer\n", curr_dt(dt), cid);
        transport_close(&req->transport);
        free(req);
        rs_cmd_processing--;
        return NULL;
    }
    memset(buf, 0, MAX_RS_MSG_SIZE);
    pid_t tid = syscall(SYS_gettid);
    fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Going to get cmd, TID=%d (rs_cmd_processing = %d)\n", curr_dt(dt), cid, tid, rs_cmd_processing);

    if (!transport_accept_server(&req->transport, req->cid)) {
        fprintf(stderr, "%s ERROR [RS_PROCESSOR=%015lu] TLS handshake failed, fd=%d\n", curr_dt(dt), req->cid, transport_fd(&req->transport));
        transport_close(&req->transport);
        free(buf);
        free(req);
        rs_cmd_processing--;
        return NULL;
    }
    int rsize = (int)transport_read(&req->transport, buf, MAX_RS_MSG_SIZE, req->cid);

    if (rsize <= 0 || rsize == MAX_RS_MSG_SIZE) {
        fprintf(stderr, "%s ERROR [RS_PROCESSOR=%015lu] connection closed/read failed, fd=%d\n", curr_dt(dt), req->cid, transport_fd(&req->transport));
    } else {
        if (strncmp(buf, "authentication_port", strlen("authentication_port")) != 0)
            fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Got cmd = %s\n", curr_dt(dt), cid, buf);
        else
            fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Found parent RegionServer\n", curr_dt(dt), cid);
        table_handler_t *h = create_t_handler(transport_fd(&req->transport));

        if (!h) {
            send_answer_java(req, 0, buf, NULL, NULL, cid, NULL);
            return NULL;
        }
        h->transport = &req->transport;
        h->cid = cid;
        char **ss = NULL;

        if (is_rs_shutdown_command(buf)) {
            bool result = process_rs_shutdown(h);
            send_answer_java(req, result, buf, NULL, NULL, cid, h);
            return NULL;
        }
        int ntokens = split(buf, ':', &ss);

        if (ntokens == -1) {
            fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Error code 70. Cannot allocate memory to parse cmd\n", curr_dt(dt), cid);
            send_answer_java(req, 0, buf, ss, t_info, cid, h);
            return NULL;
        }

        if (!ss || ss[0] == NULL) {
            fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Bad RS cmd format: %s\n", curr_dt(dt), cid, buf);
            send_answer_java(req, 0, buf, ss, t_info, cid, h);
            return NULL;
        }

        if (strncmp(buf, "authentication_port", strlen("authentication_port")) != 0) {
            if (log_level > 1)
                fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Str count_tokens: %d\n", curr_dt(dt), cid, ntokens);
        }

        if (ntokens == 1) {
            if (strcmp(ss[0], "ping") == 0) {
                msg_stat_t *msg_stat = calloc(1, sizeof(*msg_stat));

                if (!msg_stat) {
                    fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Cannot allocate usage-stat message\n", curr_dt(dt), cid);
                    send_answer_java(req, 0, buf, ss, NULL, cid, h);
                    return NULL;
                }
                msg_stat->type = MSG_STAT;
                struct timeval tv;
                gettimeofday(&tv, NULL);
                msg_stat->ts_stat = tv.tv_sec;

                while (atomic_load(&is_shutting_down))
                    sleep(10);
                uint32_t allocated_stat_size = 512;
                msg_stat->full_stat = calloc(1, allocated_stat_size);

                if (msg_stat->full_stat == NULL) {
                    fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Cannot allocate usage-stat buffer\n", curr_dt(dt), cid);
                    free(msg_stat);
                    send_answer_java(req, 0, buf, ss, NULL, cid, h);
                    return NULL;
                }

                if (!calc_usage_stat(msg_stat, allocated_stat_size, cid)) {
                    free(msg_stat->full_stat);
                    free(msg_stat);
                    send_answer_java(req, 0, buf, ss, NULL, cid, h);
                } else {
                    size_t stat_length = strlen(msg_stat->full_stat);

                    if (stat_length > UINT32_MAX - MSG_USAGE_STAT_HEADER) {
                        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Usage-stat message is too large\n", curr_dt(dt), cid);
                        free(msg_stat->full_stat);
                        free(msg_stat);
                        send_answer_java(req, 0, buf, ss, NULL, cid, h);
                        return NULL;
                    }
                    msg_stat->msg_size = MSG_USAGE_STAT_HEADER + (uint32_t)stat_length;
                    char *stat_buff = malloc(msg_stat->msg_size);

                    if (!stat_buff) {
                        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Cannot allocate serialized usage stat\n", curr_dt(dt), cid);
                        free(msg_stat->full_stat);
                        free(msg_stat);
                        send_answer_java(req, 0, buf, ss, NULL, cid, h);
                        return NULL;
                    }
                    memset(stat_buff, 0, msg_stat->msg_size);
                    stat_msg_to_bytes(msg_stat, (uint8_t *)stat_buff);
                    uint32_t stat_msg_size = msg_stat->msg_size;
                    free(msg_stat->full_stat);
                    free(msg_stat);
                    send_answer_java(req, stat_msg_size, stat_buff, ss, NULL, cid, h);
                    free(buf);
                }
                return NULL;
            }
            send_answer_java(req, 0, buf, ss, NULL, cid, h);
            return NULL;
        }

        if (strcmp(ss[0], "authentication_port") == 0) {
            if (ntokens < 3 || ss[1] == NULL || ss[2] == NULL) {
                fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Bad authentication_port cmd format: %s\n", curr_dt(dt), cid, buf);
                send_answer_java(req, 0, buf, ss, t_info, cid, h);
                return NULL;
            }
            int auth_port, auth_parent_pid;

            if (!parse_rs_parent(ss[1], ss[2], &auth_port, &auth_parent_pid)) {
                fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Invalid authentication_port fields\n", curr_dt(dt), cid);
                send_answer_java(req, 0, buf, ss, t_info, cid, h);
                return NULL;
            }
            (void)auth_port;
            parent_pid = auth_parent_pid;
            pthread_attr_t pthread_attr;
            pthread_t pthread;

            if (pthread_attr_init(&pthread_attr) != 0) {
                perror("pthread_attr_init");
                exit(1016);
            }

            if (pthread_attr_setdetachstate(&pthread_attr, PTHREAD_CREATE_DETACHED) != 0) {
                perror("pthread_attr_setdetachstate");
                exit(1017);
            }

            if (pthread_create(&pthread, &pthread_attr, (void *)pthread_health, NULL) != 0) {
                perror("pthread_create STAT");
                exit(1014);
            }
            send_answer_java(req, 1, buf, ss, t_info, cid, h);
            return NULL;
        }
        uint8_t r_cmd;

        if (ntokens < MIN_REGION_MAP_TOKEN_COUNT || ss[0] == NULL || ss[1] == NULL || ss[2] == NULL || ss[3] == NULL || ss[4] == NULL || ss[5] == NULL || ss[6] == NULL) {
            fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Bad region cmd format: %s\n", curr_dt(dt), cid, buf);
            send_answer_java(req, 0, buf, ss, t_info, cid, h);
            return NULL;
        }
        char *ns = ss[0];
        char *table = ss[1];
        char *r_uuid = ss[2];

        if (strlen(r_uuid) != UUID_SIZE) {
            fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Invalid region UUID length\n", curr_dt(dt), cid);
            send_answer_java(req, 0, buf, ss, NULL, cid, h);
            return NULL;
        }
        char *r_fname = ss[5];
        char *r_key_beg = ss[3];
        char *r_key_end = ss[4];
        char *r_block_size = NULL;
        char *r_compress = NULL;
        uint8_t add_params = 0;

        if (strstr(ss[6], DATA_BLOCK_SIZE_STR) != NULL) {
            if (ntokens < MIN_REGION_MAP_TOKEN_COUNT + 2 || ss[7] == NULL || ss[8] == NULL) {
                fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Bad region cmd format: %s\n", curr_dt(dt), cid, buf);
                send_answer_java(req, 0, buf, ss, t_info, cid, h);
                return NULL;
            }
            r_block_size = ss[6];
            r_compress = ss[7];
            add_params = 2;
        }
        r_cmd = atoi(ss[6 + add_params]);

        if (r_cmd == STATUS_SHUTDOWN || !rs_shutdown_admin_enter(r_uuid)) {
            fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Reject RS region command: cmd=%u uuid=%s shutdown_state=%d (shutdown command must be ::::::4)\n", curr_dt(dt), cid, r_cmd,
                    r_uuid, atomic_load(&rs_shutdown_state));
            send_answer_java(req, 0, buf, ss, NULL, cid, h);
            return NULL;
        }

        if (r_cmd != STATUS_ASSIGN_REGION && r_cmd != STATUS_UNASSIGN_REGION && r_cmd != STATUS_START_REGION && r_cmd != STATUS_STOP_REGION) {
            send_answer_java(req, 0, buf, ss, NULL, cid, h);
            return NULL;
        }
        char *r_replica1 = NULL;
        char *r_replica2 = NULL;
        int32_t r_replica1_port = -1;
        int32_t r_replica2_port = -1;

        if ((r_cmd == STATUS_ASSIGN_REGION || r_cmd == STATUS_START_REGION) && ntokens > MIN_REGION_MAP_TOKEN_COUNT + add_params) {
            if (ntokens <= 8 + add_params || ss[7 + add_params] == NULL || ss[8 + add_params] == NULL) {
                fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Bad replica1 cmd format: %s\n", curr_dt(dt), cid, buf);
                send_answer_java(req, 0, buf, ss, t_info, cid, h);
                return NULL;
            }
            r_replica1 = ss[7 + add_params];
            r_replica1_port = atoi(ss[8 + add_params]); // TODO Check r_replica1_port < MAX_PORT_NUM
            fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Set r_replica1_port=%d for region %s\n", curr_dt(dt), cid, r_replica1_port, r_uuid);
        }

        if ((r_cmd == STATUS_ASSIGN_REGION || r_cmd == STATUS_START_REGION) && ntokens > MIN_REGION_MAP_TOKEN_COUNT + add_params + 2) {
            if (ntokens <= 10 + add_params || ss[9 + add_params] == NULL || ss[10 + add_params] == NULL) {
                fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Bad replica2 cmd format: %s\n", curr_dt(dt), cid, buf);
                send_answer_java(req, 0, buf, ss, t_info, cid, h);
                return NULL;
            }
            r_replica2 = ss[9 + add_params];
            r_replica2_port = atoi(ss[10 + add_params]); // TODO Check r_replica1_port < MAX_PORT_NUM
            fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Set r_replica2_port=%d for region %s\n", curr_dt(dt), cid, r_replica2_port, r_uuid);
        }
        while (STOP_THE_WORLD == true)
            usleep(10000);
        t_info = find_region_t_info(ns, table, r_uuid, h, t_info);
        int32_t t_pos = h->t_pos;
        int32_t r_pos = h->r_pos;

        while (atomic_load(&is_shutting_down))
            sleep(10);
        pthread_mutex_lock(&tables_lock);

        if (t_info && t_pos >= 0 && r_pos >= 0 && r_pos < t_info->region_count_curr) {
            if (r_cmd == t_info->region_info[r_pos]->status) {
                fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Got the same command %d for %s:%s-%s!\n", curr_dt(dt), cid, r_cmd, ns, table, r_uuid);
                pthread_mutex_unlock(&tables_lock);
                send_answer_java(req, 1, buf, ss, t_info, cid, h);
                return NULL;
            }
        }
        pthread_mutex_unlock(&tables_lock);

        while (atomic_load(&is_shutting_down))
            sleep(10);

        if (r_cmd == STATUS_STOP_REGION || r_cmd == STATUS_START_REGION) {
            if (t_pos < 0) {
                fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Has tried to start/stop region %s from table %s:%s that not belong to this core\n", curr_dt(dt), cid, r_uuid, ns,
                        table);
                send_answer_java(req, 1, buf, ss, t_info, cid, h);
                return NULL;
            }

            if (r_pos < 0) {
                fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Has tried to start/stop region %s that not belong to this core\n", curr_dt(dt), cid, r_uuid);
                send_answer_java(req, 1, buf, ss, t_info, cid, h);
                return NULL;
            }
            region_info_t *r_info = NULL;

            if (r_pos < t_info->region_count_curr) {
                r_info = t_info->region_info[r_pos];
            } else {
                fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] r_pos=%d < t_info->region_count_curr=%d, t_info->t_lock_id=%d, r_info->r_lock_id=%d\n", curr_dt(dt), cid, r_pos,
                        t_info->region_count_curr, t_info->t_lock_id, (r_info ? (int)atomic_load(&r_info->r_lock_id) : -1));
                do_t_unlock(t_info, 28, true, cid);
                send_answer_java(req, 0, buf, ss, t_info, cid, h);
                return NULL;
            }
            do_t_unlock(t_info, 21, true, cid);
            bool result = true;
            bool simple_lock = false;
            bool flags_set = false;
            bool waited_for_new_region = false;

            for (int i = 0; i < 100; i++) {
                if (pthread_mutex_trylock(&r_info->rlock) == 0) {
                    r_info->r_lock_id = TRY_REGION_ADMIN_START_STOP_LOCK;
                    r_info->curr_cid_lock = cid;
                    simple_lock = true;
                    fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Simple locked region %s\n", curr_dt(dt), cid, r_info->uuid);
                    break;
                }
                usleep(10000);
            }

            if (!simple_lock) {
                atomic_store(&r_info->waiting_count_admin, 1);

                if (r_info->replica != NULL) {
                    int fd1 = atomic_load(&r_info->replica->replica1_sock_fd);
                    int fd2 = atomic_load(&r_info->replica->replica2_sock_fd);

                    if (fd1 > 0)
                        (void)shutdown(fd1, SHUT_RDWR);

                    if (fd2 > 0)
                        (void)shutdown(fd2, SHUT_RDWR);
                }
                {
                    int loops = (int)(cli_sock_timeout * 200);

                    if (loops < 200)
                        loops = 200;

                    for (int k = 0; k < loops; k++) {
                        bool fd1_ok = true, fd2_ok = true;

                        if (r_info->replica != NULL) {
                            if (r_info->replica->replica1_port > 0) {
                                fd1_ok = (atomic_load(&r_info->replica->replica1_sock_fd) < 0);
                            }

                            if (r_info->replica->replica2_port > 0) {
                                fd2_ok = (atomic_load(&r_info->replica->replica2_sock_fd) < 0);
                            }
                        }

                        if ((fd1_ok && fd2_ok) || r_info->replica_stoped == true)
                            break;
                        usleep(10000);
                    }
                }

                if (r_info->replica != NULL) {
                    pthread_mutex_lock(&r_info->control_mutex);
                    r_info->replica->replica1_sock_waiting_conn = true;
                    r_info->replica->replica2_sock_waiting_conn = true;
                    pthread_mutex_unlock(&r_info->control_mutex);
                    flags_set = true;
                }
                get_r_lock(r_info, REGION_ADMIN_START_STOP_LOCK, true, true, h);
                bool print_waiting = true;

                while (r_info->waiting_count_lock != 0 && r_info->r_lock_id != 0 && r_info->replica_stoped != true) {
                    if (print_waiting) {
                        fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Waiting release %s, waiting_count_lock=%u, r_lock_id=%u, replica_stoped=%u\n", curr_dt(dt), cid, r_info->uuid,
                                r_info->waiting_count_lock, r_info->r_lock_id, r_info->replica_stoped);
                        print_waiting = false;
                    }
                    usleep(1000);
                }

                if (!print_waiting) {
                    fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Done release %s, waiting_count_lock=%u, r_lock_id_before=%u, replica_stoped=%u\n", curr_dt(dt), cid, r_info->uuid,
                            r_info->waiting_count_lock, r_info->r_lock_id_before, r_info->replica_stoped);
                }
            }
            while (r_info->idx_resizer != RESIZE_IDX_DONE && r_info->idx_resizer != RESIZE_IDX_RESIZED)
                usleep(100000);
            {
                bool print_waiting = true;
                int loops = (int)(cli_sock_timeout * 200);

                if (loops < 200)
                    loops = 200;

                for (int k = 0; k < loops; k++) {
                    int wr = atomic_load(&r_info->waiting_count_reconnect);

                    if (wr == 0)
                        break;

                    if (print_waiting) {
                        fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Cmd %d, waiting release %s, r_info->waiting_count_reconnect=%d\n", curr_dt(dt), cid, r_cmd, r_info->uuid, wr);
                        print_waiting = false;
                    }
                    usleep(10000);
                }

                if (!print_waiting)
                    fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Finish waiting release %s, r_info->waiting_count_reconnect=%d\n", curr_dt(dt), cid, r_info->uuid,
                            r_info->waiting_count_reconnect);
            }

            if (r_cmd == STATUS_STOP_REGION) {
                flush_write_4K_buffer(r_info, cid);
                r_info->stop = true;

                if (log_level > 0)
                    fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Region %s has stopped\n", curr_dt(dt), cid, r_info->uuid);

                while (r_info->waiting_count_lock != 0 && r_info->r_lock_id != 0 && r_info->replica_stoped != true)
                    usleep(10000);
                relicas_unassign_cmd(r_info, h);
            } else {
                if (r_info->bad_data == false) {
                    bool same_instance[2] = {false, false};
                    invalidate_replicas_idle_commit(r_info);

                    if ((r_replica1 == NULL) != (r_info->replica->replica1 == NULL)) {
                        result = false;
                        goto start_region_result;
                    }

                    if (r_info->replica->r_info1 != NULL)
                        r_info->replica->r_info1->has_parent_replica = false;
                    r_info->replica->r_info1 = NULL; // TODO Fix leak
                    fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Starting handle region=%s\n", curr_dt(dt), cid, r_info->r_fname);

                    if (r_replica1 != NULL && (strcmp(r_replica1, r_info->replica->replica1) != 0 || r_replica1_port != r_info->replica->replica1_port)) {
                        fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] New address to replica1 %s:%d\n", curr_dt(dt), cid, r_replica1, r_replica1_port);
                        {
                            int fd_local = atomic_load(&r_info->replica->replica1_sock_fd);

                            if (fd_local > 0)
                                close_socket(r_info, 1, fd_local);
                        }

                        if (r_replica1_port == client_port) {
                            set_replica_local(r_info, t_info, r_replica1, r_uuid, 1, h, &same_instance[0]);
                        }

                        if (r_info->replica->r_info1 == NULL) {
                            fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Set address to replica1 %s:%d\n", curr_dt(dt), cid, r_replica1, r_replica1_port);
                            pthread_mutex_lock(&r_info->control_mutex);
                            result = update_rs_replica_name(&r_info->replica->replica1, r_replica1);

                            if (result)
                                r_info->replica->replica1_port = r_replica1_port;
                            pthread_mutex_unlock(&r_info->control_mutex);

                            if (!result)
                                goto start_region_result;
                            result = replica_refresh_region_map(r_info, 1, true, h);
                        }
                    }

                    if (r_info->replica->r_info2 != NULL)
                        r_info->replica->r_info2->has_parent_replica = false;
                    r_info->replica->r_info2 = NULL; // TODO Fix leak

                    if (result && r_replica2 != NULL && r_info->replica->replica2 != NULL) {
                        if (strcmp(r_replica2, r_info->replica->replica2) != 0 || r_replica2_port != r_info->replica->replica2_port) {
                            fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] New address to replica2 %s:%d\n", curr_dt(dt), cid, r_replica2, r_replica2_port);
                            {
                                int fd_local = atomic_load(&r_info->replica->replica2_sock_fd);

                                if (fd_local > 0)
                                    close_socket(r_info, 2, fd_local);
                            }

                            if (r_replica2_port == client_port) {
                                set_replica_local(r_info, t_info, r_replica2, r_uuid, 2, h, &same_instance[1]);
                            }

                            if (r_info->replica->r_info2 == NULL) {
                                fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Set address to replica2 %s:%d\n", curr_dt(dt), cid, r_replica2, r_replica2_port);
                                pthread_mutex_lock(&r_info->control_mutex);
                                result = update_rs_replica_name(&r_info->replica->replica2, r_replica2);

                                if (result)
                                    r_info->replica->replica2_port = r_replica2_port;
                                pthread_mutex_unlock(&r_info->control_mutex);

                                if (!result)
                                    goto start_region_result;
                                result = replica_refresh_region_map(r_info, 2, true, h);
                                r_info->replica->replica_control = 4;
                            }
                        }
                    }
                start_region_result:
                    if (result) {
                        fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Starting region %s\n", curr_dt(dt), cid, r_info->r_fname);
                        r_info->stop = false;
                    } else {
                        r_info->stop = true;
                        fprintf(stderr, "%s WARN [IDX_BUILDER=%015lu] Cannot start region %s\n", curr_dt(dt), cid, r_info->r_fname);
                    }
                } else {
                    fprintf(stderr, "%s WARN [IDX_BUILDER=%015lu] Cannot start region %s, data corruption detected\n", curr_dt(dt), cid, r_info->r_fname);
                }
            }

            if (log_level > 1) {
                char fname[MAX_FN_SIZE];
                snprintf(fname, MAX_FN_SIZE, "%s/000.data", r_info->r_fname);
                uint64_t file_size = get_file_size(fname);
                fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Check region %s size = %lu after start/stop\n", curr_dt(dt), cid, fname, file_size);
                snprintf(fname, MAX_FN_SIZE, "%s/lob.data", r_info->r_fname);
                file_size = get_file_size(fname);
                fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Check region %s size = %lu after start/stop\n", curr_dt(dt), cid, fname, file_size);
            }
            size_t len = strlen(r_uuid);
            char uuid[len + 1];
            memcpy(uuid, r_uuid, len + 1);
            admin_region_cleanup(r_info, flags_set, simple_lock, 113, h);
            t_info->need_lock = false;
            t_info = NULL;
            send_answer_java(req, result, buf, ss, t_info, cid, h);

            if (r_cmd == STATUS_START_REGION && result && atomic_load(&r_info->r_need_data_chk) != 0) {
                h = create_t_handler(0);

                if (!h) {
                    fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Deferred START data check: no handler for %s\n", curr_dt(dt), cid, uuid);
                    return NULL;
                }
                h->cid = cid;
                get_r_lock(r_info, DATA_CHK_REGION_LOCK, true, false, h);

                if (r_info->is_freed == false && r_info->stop == false && r_info->bad_data == false && atomic_load(&r_info->r_need_data_chk) != 0) {
                    if (strcmp(uuid, r_info->uuid) == 0) {
                        if (!procces_replicas_data_chk(r_info, h)) {
                            r_info->stop = true;
                            r_info->bad_data = true;
                            fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Unsuccessful data check %s, region stopped\n", curr_dt(dt), cid, r_info->uuid);
                        } else {
                            drop_region_recovery_info(r_info, cid);

                            if (atomic_load(&r_info->r_need_data_chk) != 0)
                                result = false;
                        }
                    }
                }
                do_r_unlock(r_info, 121, true, h);
                free_t_handler(h);
            }
            return NULL;
        }

        if (r_cmd == STATUS_ASSIGN_REGION) {
            if (r_pos > 0) {
                fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Cannot assign region %s:%s-%s again\n", curr_dt(dt), cid, ns, table, r_uuid);
                send_answer_java(req, 1, buf, ss, t_info, cid, h);
                return NULL;
            }

            if (t_pos < 0) {
                t_info = build_new_table(ns, table, cid);

                if (!t_info) {
                    send_answer_java(req, 0, buf, ss, t_info, cid, h);
                    return NULL;
                }
            }
            table_handler_t *followup = create_t_handler(0);

            if (!followup) {
                send_answer_java(req, 0, buf, ss, t_info, cid, h);
                return NULL;
            }
            followup->cid = cid;
            region_info_t *r_info = NULL;

            while (atomic_load(&is_shutting_down))
                sleep(10);

            if (r_pos < 0) {
                r_info = build_new_region(t_info, r_uuid, r_fname, r_cmd, r_key_beg, r_key_end, r_replica1, r_replica1_port, r_replica2, r_replica2_port, r_compress, h);
                t_info = NULL;
            }

            if (!r_info) {
                free_t_handler(followup);
                send_answer_java(req, 0, buf, ss, t_info, cid, h);
                return NULL;
            }
            send_answer_java(req, 1, buf, ss, t_info, cid, h);
            h = followup;
            bool doubles_ok = finish_index_recovery(r_info, true, h);

            for (int bs = 0; bs < BLOCK_MAX_SIZE_M - 1; bs++) {
                free_double_keys_keeper(r_info, bs);
            }
            free(r_info->k_dbl);
            r_info->k_dbl = NULL;

            if (!doubles_ok) {
                do_r_unlock(r_info, 118, true, h);
                free_t_handler(h);
                return NULL;
            }

            if (r_info->is_leader) {
            }

            if (data_check_enabled == 1) {
                if (r_info->is_leader && r_info->replica->replica_num > 0) {
                    if (r_info->r_need_data_chk == 0) {
                        if (replicas_chk_delay_sec != 0) {
                            pthread_mutex_lock(&r_info_chk_mutex);
                            bool add_new_i_info = true;
                            int replica_count = replica_need_data_chk_count;

                            if (replica_count < 0 || (uint32_t)replica_count > replica_need_data_chk_capacity || (replica_count > 0 && r_info_replicas_chk == NULL)) {
                                fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Invalid replicas data check array state count=%d, capacity=%u, array=%p\n", curr_dt(dt), cid,
                                        replica_count, replica_need_data_chk_capacity, (void *)r_info_replicas_chk);
                                add_new_i_info = false;
                            }

                            for (int i = 0; add_new_i_info && i < replica_count; i++) {
                                if (r_info_replicas_chk[i] == r_info)
                                    add_new_i_info = false;
                            }

                            if (add_new_i_info && (uint32_t)replica_count == replica_need_data_chk_capacity) {
                                size_t new_capacity = 8;

                                if (replica_need_data_chk_capacity > UINT32_MAX / 2 || replica_need_data_chk_capacity > SIZE_MAX / 2) {
                                    fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Cannot grow replicas data check array count=%d, capacity=%u\n", curr_dt(dt), cid, replica_count,
                                            replica_need_data_chk_capacity);
                                    add_new_i_info = false;
                                } else if (replica_need_data_chk_capacity != 0) {
                                    new_capacity = (size_t)replica_need_data_chk_capacity * 2;
                                }

                                if (add_new_i_info && new_capacity > SIZE_MAX / sizeof(*r_info_replicas_chk)) {
                                    fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Cannot allocate replicas data check array capacity=%zu\n", curr_dt(dt), cid, new_capacity);
                                    add_new_i_info = false;
                                }
                                struct region_info_t **new_ptr = NULL;

                                if (add_new_i_info)
                                    new_ptr = realloc(r_info_replicas_chk, new_capacity * sizeof(*r_info_replicas_chk));

                                if (!new_ptr) {
                                    if (add_new_i_info) {
                                        fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Realloc region_info_t failed, replicas %s will not checked\n", curr_dt(dt), cid, r_info->uuid);
                                    }
                                    add_new_i_info = false;
                                } else {
                                    r_info_replicas_chk = new_ptr;
                                    replica_need_data_chk_capacity = (uint32_t)new_capacity;
                                }
                            }

                            if (add_new_i_info) {
                                r_info_replicas_chk[replica_need_data_chk_count] = r_info;
                                replica_need_data_chk_count++;
                            }
                            pthread_mutex_unlock(&r_info_chk_mutex);
                        }
                    } else {
                        if (procces_replicas_data_chk(r_info, h) == false)
                            finish_thread(r_info, 76, true, h);
                        drop_region_recovery_info(r_info, cid);
                    }
                }
            }
            do_r_unlock(r_info, 106, true, h);
            free_t_handler(h);
            return NULL;
        }

        if (r_cmd == STATUS_UNASSIGN_REGION) {
            if (t_pos < 0) {
                h->r_pos = -2;

                if (log_level > 1)
                    fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Has tried to unassign region %s from table %s:%s that not belong to this core\n", curr_dt(dt), cid, r_uuid, ns,
                            table);
                send_answer_java(req, 1, buf, ss, t_info, cid, h);
                return NULL;
            }

            if (r_pos < 0) {
                if (log_level > 1)
                    fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Has tried to unassign region %s that not belong to this core\n", curr_dt(dt), cid, r_uuid);
                h->r_pos = -2;
                send_answer_java(req, 1, buf, ss, t_info, cid, h);
                return NULL;
            }
            region_info_t *r_info = NULL;

            if (r_pos < t_info->region_count_curr) {
                r_info = t_info->region_info[r_pos];
                r_info->stop;
            } else {
                fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] r_pos=%d < t_info->region_count_curr=%d, t_info->t_lock_id=%d, r_info->r_lock_id=%d\n", curr_dt(dt), cid, r_pos,
                        t_info->region_count_curr, t_info->t_lock_id, r_info ? (int)atomic_load(&r_info->r_lock_id) : -1);
                send_answer_java(req, 0, buf, ss, t_info, cid, h);
                return NULL;
            }
            uint32_t srv_tbl_version_bkp = t_info->srv_tbl_version;
            do_t_unlock(t_info, 26, true, cid);
            bool simple_lock = false;
            bool flags_set = false;

            for (int i = 0; i < 100; i++) {
                if (pthread_mutex_trylock(&r_info->rlock) == 0) {
                    r_info->r_lock_id = TRY_REGION_ADMIN_UNASSIGN_LOCK;
                    r_info->curr_cid_lock = cid;
                    simple_lock = true;
                    fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Unassign simple locked region %s\n", curr_dt(dt), cid, r_info->uuid);
                    break;
                }
                usleep(rand() % 1000 + 100);
            }

            if (!simple_lock) {
                atomic_store(&r_info->waiting_count_admin, 1);

                if (r_info->replica != NULL) {
                    int fd1 = atomic_load(&r_info->replica->replica1_sock_fd);
                    int fd2 = atomic_load(&r_info->replica->replica2_sock_fd);

                    if (fd1 > 0)
                        (void)shutdown(fd1, SHUT_RDWR);

                    if (fd2 > 0)
                        (void)shutdown(fd2, SHUT_RDWR);
                }
                {
                    int loops = (int)(cli_sock_timeout * 200);

                    if (loops < 200)
                        loops = 200;

                    for (int k = 0; k < loops; k++) {
                        bool fd1_ok = true, fd2_ok = true;

                        if (r_info->replica != NULL) {
                            if (r_info->replica->replica1_port > 0) {
                                fd1_ok = (atomic_load(&r_info->replica->replica1_sock_fd) < 0);
                            }

                            if (r_info->replica->replica2_port > 0) {
                                fd2_ok = (atomic_load(&r_info->replica->replica2_sock_fd) < 0);
                            }
                        }

                        if ((fd1_ok && fd2_ok) || r_info->replica_stoped == true)
                            break;
                        usleep(rand() % 1000 + 100);
                    }
                }

                if (r_info->replica != NULL) {
                    pthread_mutex_lock(&r_info->control_mutex);
                    r_info->replica->replica1_sock_waiting_conn = true;
                    r_info->replica->replica2_sock_waiting_conn = true;
                    pthread_mutex_unlock(&r_info->control_mutex);
                    flags_set = true;
                }
                get_r_lock(r_info, REGION_ADMIN_UNASSIGN_LOCK, true, true, h);
                bool print_waiting = true;

                while (r_info->waiting_count_lock != 0 && r_info->r_lock_id != 0 && r_info->replica_stoped != true) {
                    if (print_waiting) {
                        fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Waiting release %s to unassign, waiting_count_lock=%u, r_lock_id_before=%u, replica_stoped=%u\n", curr_dt(dt),
                                cid, r_info->uuid, r_info->waiting_count_lock, r_info->r_lock_id_before, r_info->replica_stoped);
                        print_waiting = false;
                    }
                    usleep(1000);
                }

                if (!print_waiting) {
                    fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Done release %s to unassign, waiting_count_lock=%u, r_lock_id=%u, replica_stoped=%u\n", curr_dt(dt), cid,
                            r_info->uuid, r_info->waiting_count_lock, r_info->r_lock_id, r_info->replica_stoped);
                }
            }
            get_t_lock(t_info, 26, true, cid);

            if (r_info->stop == false) {
                r_info->stop = true; // TODO WARN
                usleep(10000);
            }
            bool print_waiting = true;

            while (r_info->waiting_count_reconnect != 0) {
                if (print_waiting) {
                    fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Waiting release %s, waiting_count_reconnect=%d\n", curr_dt(dt), cid, r_info->uuid, r_info->waiting_count_reconnect);
                    print_waiting = false;
                }
                usleep(rand() % 1000 + 100);
            }
            r_info->waiting_count_admin = 1;

            if (!print_waiting)
                fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Finish waiting release %s, waiting_count_reconnect=%d\n", curr_dt(dt), cid, r_info->uuid,
                        r_info->waiting_count_reconnect);

            if (srv_tbl_version_bkp != t_info->srv_tbl_version) {
                r_info->waiting_count_admin = 0;
                fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Region %s, srv_tbl_version=%d changed (%d) while take lock. Need to repeat\n", curr_dt(dt), cid, r_info->r_fname,
                        t_info->srv_tbl_version, srv_tbl_version_bkp);
                admin_region_cleanup(r_info, flags_set, simple_lock, 114, h);
                send_answer_java(req, 0, buf, ss, t_info, cid, h);
                return NULL;
            }

            if (r_info->is_freed) {
                r_info->waiting_count_admin = 0;
                fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Region %s was freed, (srv_tbl_version=%d) while take lock. Need to repeat\n", curr_dt(dt), cid, r_info->r_fname,
                        t_info->srv_tbl_version);
                admin_region_cleanup(r_info, flags_set, simple_lock, 115, h);
                send_answer_java(req, 0, buf, ss, t_info, cid, h);
                return NULL;
            }

            if (strcmp(r_uuid, r_info->uuid) != 0) {
                r_info->waiting_count_admin = 0;
                fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Region %s UUID was moved to %s (srv_tbl_version=%d) while take lock. Need to repeat\n", curr_dt(dt), cid, r_info->uuid,
                        r_uuid, srv_tbl_version_bkp);
                admin_region_cleanup(r_info, flags_set, simple_lock, 116, h);
                send_answer_java(req, 0, buf, ss, t_info, cid, h);
                return NULL;
            }
            while (r_info->idx_resizer != RESIZE_IDX_DONE)
                usleep(100000);
            flush_write_4K_buffer(r_info, cid);

            while (atomic_load(&is_shutting_down))
                sleep(10);
            char fname[MAX_FN_SIZE];
            snprintf(fname, MAX_FN_SIZE, "%s/000.data", r_info->r_fname);
            uint64_t file_size = get_file_size(fname);
            fprintf(stderr, "%s DEBUG [IDX_BUILDER=%015lu] Check region %s size = %lu before unassign\n", curr_dt(dt), cid, r_info->r_fname, file_size);
            t_info->srv_tbl_version++;
            fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Change table version to %d for region %s\n", curr_dt(dt), cid, t_info->srv_tbl_version, r_info->r_fname);

            if (r_info->is_freed == true) {
                fprintf(stderr, "%s ERROR [REGION_UTILS=%015lu] Error code 29. Region %s is freed, r_info->r_lock_id=%d\n", curr_dt(dt), cid, r_info->r_fname, r_info->r_lock_id);
                r_info->stop = true;
                r_info->bad_data = true;
                admin_region_cleanup(r_info, flags_set, simple_lock, 114, h);
                send_answer_java(req, 1, buf, ss, t_info, cid, h);
                return NULL;
            }
            r_info->num = r_pos;
            fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] r_info->num=%d\n", curr_dt(dt), cid, r_info->num);
            r_info->status = STATUS_UNASSIGN_REGION;
            invalidate_replicas_idle_commit(r_info);
            t_info->region_count_curr_assigned--;
            del_r_info_chk_element(r_info);

            for (int b = 0; b < BUCKETS; b++) {
                for (int bs = 0; bs < r_info->max_idx_b; bs++) {
                    close_data_file(r_info, bs, b, cid);
                }
            }

            if (t_info->region_count_curr_assigned == 0) {
                usleep(rand() % 1000 + 100);

                for (int i = 0; i < t_info->count_cli_fd; i++) {
                    if (t_info->cli_fd[i] > 0)
                        shutdown(t_info->cli_fd[i], SHUT_RDWR);
                }

                if (t_info->count_cli_fd > 0) {
                    free(t_info->cli_fd);
                    t_info->cli_fd = NULL;
                    t_info->count_cli_fd = 0;
                }
            }
            atomic_store(&r_info->recovery_path_ready, 0);
            atomic_store(&r_info->shutdown_region_state, SHUTDOWN_REGION_IDLE);
            r_info->is_freed = true;
            r_info->stop = false;
            r_info->has_parent_replica = false;

            if (r_info->replica->r_info1 != NULL)
                r_info->replica->r_info1->has_parent_replica = false;

            if (r_info->replica->r_info2 != NULL)
                r_info->replica->r_info2->has_parent_replica = false;
            r_info->replica->r_info1 = NULL; // TODO FREE
            r_info->replica->r_info2 = NULL; // TODO FREE
            set_replica_pointer(t_info, r_info, NULL, cid);
            usleep(rand() % 10000 + 1000);
            free_region(r_info, cid); // TODO Не все чистим, проверить что не юзается больше и зачищать

            if (t_info->region_count_curr_assigned == 0)
                request_malloc_trim();

            if (t_info->region_count_curr_assigned == 0)
                retire_empty_table(t_info, cid);
            fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] Unassign done. Table %s->region_count_curr=%d\n", curr_dt(dt), cid, t_info->fullname, t_info->region_count_curr);
            r_info->waiting_count_reconnect = 0;
            admin_region_cleanup(r_info, flags_set, simple_lock, 117, h);
            send_answer_java(req, 1, buf, ss, t_info, cid, h);
            return NULL;
        }
    }
    fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Recv = %d (rs_cmd_processing = %d)\n", curr_dt(dt), cid, rsize, rs_cmd_processing);
    perror("recv: ");
    transport_close(&req->transport);
    free(req);
    free(buf);
    rs_cmd_processing--;
    return NULL;
}

void *add_new_region(void *arg) {
    void *result;
    pthread_cleanup_push(rs_shutdown_admin_leave, NULL);
    result = region_cmd(arg);
    pthread_cleanup_pop(1);
    return result;
}


static uint16_t highest_bit(uint64_t v) {
    uint16_t out = 0;

    while (v > 0) {
        v >>= 1;
        ++out;
    }
    return out;
}


uint64_t get_new_cid() {
    uint64_t res;
    pthread_mutex_lock(&uuid_lock);
    proc_id++;
    res = proc_id;
    pthread_mutex_unlock(&uuid_lock);
    return res;
}
