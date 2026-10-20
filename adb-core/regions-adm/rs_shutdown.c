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
#include "../utils/voids_service.h"
#include "../handle/storage.h"
#include "../index/f_index.h"
_Atomic int rs_shutdown_state = RS_SHUTDOWN_RUNNING;
static pthread_mutex_t shutdown_admission_lock = PTHREAD_MUTEX_INITIALIZER;
static uint32_t leader_admin_commands;
static __thread bool leader_admin_admitted;
static _Atomic uint64_t replica_request_generation;
static _Atomic uint64_t replica_requests_active;
static _Atomic uint64_t quiet_generation;
void rs_shutdown_replica_request_begin(void) {
    atomic_fetch_add(&replica_requests_active, 1);
    atomic_fetch_add(&replica_request_generation, 1);
}


void rs_shutdown_replica_request_end(void *unused) {
    (void)unused;
    atomic_fetch_sub(&replica_requests_active, 1);
}

static bool shutdown_monotonic_ms(uint64_t *value) {
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
        return false;
    *value = (uint64_t)ts.tv_sec * 1000U + (uint64_t)ts.tv_nsec / 1000000U;
    return true;
}


static bool wait_for_replica_quiet(uint64_t cid, bool reuse_previous_window) {
    uint64_t seen = atomic_load(&replica_request_generation);

    if (reuse_previous_window && seen == atomic_load(&quiet_generation) && atomic_load(&replica_requests_active) == 0 && seen == atomic_load(&replica_request_generation) &&
        !atomic_load(&is_shutting_down))
        return true;
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s INFO [RS_SHUTDOWN=%015lu] Waiting for 30 seconds without replica requests; reserve replicas remain online\n", curr_dt(dt), cid);
    bool idle = false;
    uint64_t quiet_since = 0;

    while (!atomic_load(&is_shutting_down)) {
        uint64_t generation = atomic_load(&replica_request_generation);
        uint64_t active = atomic_load(&replica_requests_active);
        uint64_t now;

        if (!shutdown_monotonic_ms(&now)) {
            fprintf(stderr, "%s ERROR [RS_SHUTDOWN=%015lu] Cannot read monotonic clock while waiting for replica requests\n", curr_dt(dt), cid);
            return false;
        }

        if (active == 0 && generation == atomic_load(&replica_request_generation)) {
            if (!idle || generation != seen) {
                quiet_since = now;
                idle = true;
            }

            if (now >= quiet_since && now - quiet_since >= 30000U && atomic_load(&replica_requests_active) == 0 && generation == atomic_load(&replica_request_generation)) {
                atomic_store(&quiet_generation, generation);
                return true;
            }
        } else {
            idle = false;
        }
        seen = generation;
        usleep(100000);
    }
    return false;
}


bool is_rs_shutdown_command(const char *command) {
    return command != NULL && strcmp(command, "::::::4") == 0;
}

bool rs_shutdown_admin_enter(const char *uuid) {
    size_t length = uuid == NULL ? 0 : strlen(uuid);

    if (length == 0 || uuid[length - 1] != '0')
        return true;
    pthread_mutex_lock(&shutdown_admission_lock);
    bool allowed = atomic_load(&rs_shutdown_state) == RS_SHUTDOWN_RUNNING;

    if (allowed && !leader_admin_admitted) {
        leader_admin_commands++;
        leader_admin_admitted = true;
    }
    pthread_mutex_unlock(&shutdown_admission_lock);
    return allowed;
}


void rs_shutdown_admin_leave(void *unused) {
    (void)unused;
    pthread_mutex_lock(&shutdown_admission_lock);

    if (leader_admin_admitted) {
        leader_admin_commands--;
        leader_admin_admitted = false;
    }
    pthread_mutex_unlock(&shutdown_admission_lock);
}


static void shutdown_coordinator_exit(void *unused) {
    (void)unused;
    int state = atomic_load(&rs_shutdown_state);

    if (state != RS_SHUTDOWN_READY && state != RS_SHUTDOWN_FAILED)
        atomic_store(&rs_shutdown_state, RS_SHUTDOWN_FAILED);
}

static void flush_shutdown_leader(region_info_t *region, uint64_t cid) {
    flush_write_4K_buffer(region, cid);

    if (region->replica != NULL) {
        region_info_t *first = region->replica->r_info1;
        region_info_t *second = region->replica->r_info2;

        if (first != NULL && first != region && atomic_load(&first->has_parent_replica))
            flush_write_4K_buffer(first, cid);

        if (second != NULL && second != region && second != first && atomic_load(&second->has_parent_replica))
            flush_write_4K_buffer(second, cid);
    }
}

static int shutdown_leaders_pass(tables_snapshot_t *snapshot, table_handler_t *handler, bool compacting) {
    if (!take_tables_snapshot(snapshot, handler->cid))
        return -1;
    bool pending = false;

    for (int t = 0; t < snapshot->count; t++) {
        table_info_t *table = snapshot->tables[t];

        if (table == NULL)
            continue;

        for (int r = 0;; r++) {
            if (pthread_mutex_trylock(&table->tlock) != 0) {
                pending = true;
                break;
            }

            if (table->t_lock_id != 0) {
                pthread_mutex_unlock(&table->tlock);
                pending = true;
                break;
            }
            table->t_lock_id = 31;

            if (table->retired || r >= table->region_count_curr) {
                do_t_unlock(table, 33, false, handler->cid);
                break;
            }
            region_info_t *region = table->region_info[r];

            if (region == NULL || region->is_freed || !region->is_leader) {
                do_t_unlock(table, 33, false, handler->cid);
                continue;
            }

            if (pthread_mutex_trylock(&region->rlock) != 0) {
                do_t_unlock(table, 33, false, handler->cid);
                pending = true;
                continue;
            }
            pthread_mutex_lock(&region->control_mutex);

            if (region->r_lock_id != 0 || region->waiting_count_lock != 0 || region->waiting_count_admin != 0) {
                pthread_mutex_unlock(&region->control_mutex);
                pthread_mutex_unlock(&region->rlock);
                do_t_unlock(table, 33, false, handler->cid);
                pending = true;
                continue;
            }
            region->r_lock_id = TRY_REGION_ADMIN_START_STOP_LOCK;
            region->curr_cid_lock = handler->cid;
            pthread_mutex_unlock(&region->control_mutex);
            do_t_unlock(table, 33, false, handler->cid);
            int result = 0;

            if (region->bad_data || atomic_load(&region->r_need_data_chk) != 0) {
                result = -1;
            } else if (!region->stop) {
                if (region->idx_resizer != RESIZE_IDX_DONE || region->bulk_mode != BUIK_RELICA_OFF) {
                    result = 1;
                } else if (!compacting) {
                    flush_shutdown_leader(region, handler->cid);
                } else {
                    if (APPEND_COMPACTION_ENABLED)
                        result = request_append_compaction_shutdown_drain(region);

                    if (result > 0 && append_compact_thread_count <= 0)
                        result = -1;

                    if (result == 0) {
                        flush_shutdown_leader(region, handler->cid);
                        region->stop = true;

                        if (!atomic_load(&region->replicas_idle_committed) && !replicas_shutdown_cmd(region, handler)) {
                            result = -1;
                        } else {
                            char dt[DT_LOG_SIZE];
                            fprintf(stderr, "%s INFO [RS_SHUTDOWN=%015lu] Leader drained and stopped, replica sessions confirmed: %s\n", curr_dt(dt), handler->cid, region->uuid);
                        }
                    }
                }
            }

            if (result < 0) {
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s ERROR [RS_SHUTDOWN=%015lu] Cannot drain leader %s: stop=%u bad_data=%u recovery=%lu dirty=%u truncate_pending=%u workers=%u\n", curr_dt(dt),
                        handler->cid, region->uuid, (unsigned)region->stop, (unsigned)region->bad_data, (unsigned long)atomic_load(&region->r_need_data_chk),
                        atomic_load(&region->append_compact_dirty_segments), (unsigned)atomic_load(&region->append_compact_truncate_pending), (unsigned)append_compact_thread_count);
            }
            do_r_unlock(region, 135, false, handler);

            if (result < 0)
                return -1;
            pending |= result != 0;
        }
    }
    return pending ? 1 : 0;
}


bool process_rs_shutdown(table_handler_t *handler) {
    if (handler == NULL)
        return false;
    pthread_mutex_lock(&shutdown_admission_lock);
    bool owner = atomic_load(&rs_shutdown_state) == RS_SHUTDOWN_RUNNING;

    if (owner)
        atomic_store(&rs_shutdown_state, RS_SHUTDOWN_QUIESCING);
    pthread_mutex_unlock(&shutdown_admission_lock);

    if (!owner) {
        while (atomic_load(&rs_shutdown_state) < RS_SHUTDOWN_READY && !atomic_load(&is_shutting_down))
            usleep(10000);
        return atomic_load(&rs_shutdown_state) == RS_SHUTDOWN_READY && wait_for_replica_quiet(handler->cid, true);
    }
    bool success = false;
    tables_snapshot_t snapshot = {0};
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s INFO [RS_SHUTDOWN=%015lu] Quiescing leaders; reserve replicas remain online\n", curr_dt(dt), handler->cid);
    pthread_cleanup_push(shutdown_coordinator_exit, NULL);
    pthread_cleanup_push(free_tables_snapshot, &snapshot);

    for (;;) {
        pthread_mutex_lock(&shutdown_admission_lock);
        uint32_t active = leader_admin_commands;
        pthread_mutex_unlock(&shutdown_admission_lock);

        if (active == 0 || atomic_load(&is_shutting_down))
            break;
        usleep(10000);
    }
    int result = 1;

    while (!atomic_load(&is_shutting_down) && result > 0) {
        result = shutdown_leaders_pass(&snapshot, handler, false);

        if (result > 0)
            usleep(10000);
    }

    if (result == 0 && !atomic_load(&is_shutting_down)) {
        if (APPEND_COMPACTION_ENABLED)
            atomic_store(&append_compact_ops_limit, 1000000);
        atomic_store(&rs_shutdown_state, RS_SHUTDOWN_COMPACTING);
        fprintf(stderr, "%s INFO [RS_SHUTDOWN=%015lu] Draining compaction and deferred truncate\n", curr_dt(dt), handler->cid);

        do {
            result = shutdown_leaders_pass(&snapshot, handler, true);

            if (result > 0)
                usleep(10000);
        } while (result > 0 && !atomic_load(&is_shutting_down));
        success = result == 0 && !atomic_load(&is_shutting_down);
    }

    if (success) {
        atomic_store(&rs_shutdown_state, RS_SHUTDOWN_WAITING_RPS);
        success = wait_for_replica_quiet(handler->cid, false);
    }
    atomic_store(&rs_shutdown_state, success ? RS_SHUTDOWN_READY : RS_SHUTDOWN_FAILED);
    fprintf(stderr, "%s %s [RS_SHUTDOWN=%015lu] %s; reserve replicas remain online\n", curr_dt(dt), success ? "INFO" : "ERROR", handler->cid,
            success ? "Leaders stopped, ready for RS kill" : "Drain failed, leaders remain admission-blocked");
    pthread_cleanup_pop(1);
    pthread_cleanup_pop(1);
    return success;
}
