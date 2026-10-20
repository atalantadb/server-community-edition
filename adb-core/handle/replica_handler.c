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
#include "replica_handler.h"
#include "req_proc.h"
#include "block_handler.h"
#include "../utils/voids_service.h"
#define h_addr h_addr_list[0]
static uint64_t replica_metric_monotonic_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000U + (uint64_t)ts.tv_nsec / 1000U;
}


void close_socket(struct region_info_t *r_info, uint8_t replica_num, int socket_fd) {
    char dt[DT_LOG_SIZE];

    if (r_info == NULL || r_info->replica == NULL)
        return;

    if (socket_fd <= 0)
        return;
    invalidate_replicas_idle_commit(r_info);
    pthread_mutex_lock(&slock);
    invalidate_record_gc(r_info);
    (void)shutdown(socket_fd, SHUT_RDWR);
    (void)close(socket_fd);

    if (replica_num == 1) {
        int expected = socket_fd;
        (void)atomic_compare_exchange_strong(&r_info->replica->replica1_sock_fd, &expected, -1);
    } else if (replica_num == 2) {
        int expected = socket_fd;
        (void)atomic_compare_exchange_strong(&r_info->replica->replica2_sock_fd, &expected, -1);
    }
    usleep(10000);
    pthread_mutex_unlock(&slock);
}


bool auth_msg_send(int socket_fd, char *uuid, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    char msg_auth[FIX_MAX_MSG_SIZE];
    memset(msg_auth, 0, FIX_MAX_MSG_SIZE);
    msg_auth[MSG_SIZE_OFFSET] = MSG_SIZE_AUTH_RESP;
    uint32_t msg_auth_len = MSG_SIZE_AUTH_RESP;
    msg_auth_len = INIT_MSG_HDR;
    msg_auth[MSG_SIZE_OFFSET] = INIT_MSG_HDR;
    msg_auth[MSG_TYPE_OFFSET] = MSG_NO_AUTH;
    ssize_t res = write(socket_fd, msg_auth, msg_auth_len);

    if (res < 0) {
        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Auth %s result=%ld, write failed errno=%d(%s)\n", curr_dt(dt), cid, uuid, res, errno, strerror(errno));
        return false;
    }

    if (res != msg_auth_len) {
        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Auth %s send msg result is %ld != %d\n", curr_dt(dt), cid, uuid, res, msg_auth_len);
        return false;
    }
    ssize_t rsize = read(socket_fd, msg_auth, FIX_MAX_MSG_SIZE);

    if (rsize < 0) {
        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Failed read auth %s response=%ld, errno=%d(%s)\n", curr_dt(dt), cid, uuid, rsize, errno, strerror(errno));
        return false;
    } else {
        if (rsize < MSG_SIZE_AUTH_RESP) {
            fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Auth %s response msg size=%ld too small\n", curr_dt(dt), cid, uuid, rsize);
            return false;
        }

        if (msg_auth[MSG_TYPE_OFFSET] != MSG_AUTH) {
            fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Auth %s msg (size=%ld) response FAIL (%d != %d)\n", curr_dt(dt), cid, uuid, rsize, MSG_AUTH,
                    msg_auth[MSG_TYPE_OFFSET]);
            return false;
        }
        uint32_t off_result = offsetof(struct msg_rs_result_auth_t, result);

        if (msg_auth[off_result] != MSG_AUTH_RESULT_OK) {
            fprintf(stderr, "%s WARN [REPLICA_HANDLER=%015lu] Replicas %s permission not granted\n", curr_dt(dt), cid, uuid);
            return false;
        }
    }
    fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Auth %s OK\n", curr_dt(dt), cid, uuid);
    return true;
}

void finish_thread(region_info_t *r_info, int call_id, bool do_unlock, table_handler_t *t_handler) {
    if (t_handler == NULL)
        pthread_exit(NULL);
    char dt[DT_LOG_SIZE];
    const uint64_t cid = t_handler->cid;
    const bool is_client = atomic_load(&t_handler->is_client);
    connection_t *conn = t_handler->conn_father;

    if (conn != NULL)
        atomic_store(&conn->state, STATE_FREED);

    if (do_unlock && r_info != NULL) {
        invalidate_record_gc(r_info);

        if ((t_handler->record_mutation != NULL && t_handler->record_mutation->changed) || r_info->record_versions.pending != 0) {
            r_info->stop = true;
            r_info->bad_data = true;
        }

        if (!r_info->already_finishing) {
            r_info->already_finishing = true;

            if (r_info->bad_data)
                set_region_recovery_info(r_info, 1000005l, t_handler);
        }
        fprintf(stderr, "%s INFO [REPLICA_HANDLER=%015lu] Finishing region request (call_id=%d)\n", curr_dt(dt), cid, call_id);

        if (r_info->r_lock_id != 0)
            do_r_unlock(r_info, 121, true, t_handler);
    } else {
        fprintf(stderr, "%s INFO [REPLICA_HANDLER=%015lu] Finishing request without region unlock (call_id=%d)\n", curr_dt(dt), cid, call_id);
    }

    if (r_info != NULL)
        r_info->waiting_count_reconnect = 0;
    sleep(3);

    if (conn != NULL) {
        const bool blocking = conn->mode == BLOCKING_MODE;
        close_conn(conn);

        if (blocking)
            free(conn);
    } else {
        while (atomic_load(&t_handler->resizing) > 0)
            sleep(1);
        close_handler_transport(t_handler);
        free_t_handler(t_handler);

        if (is_client)
            atomic_fetch_sub(&total_clients, 1);
        else
            rs_cmd_processing--;
    }
    fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Handler memory freed\n", curr_dt(dt), cid);
    pthread_exit(NULL);
}

static inline void set_replica_waiting_conn(region_info_t *r, int replica_num, bool v) {
    if (!r || !r->replica)
        return;
    pthread_mutex_lock(&r->control_mutex);

    if (v == false && atomic_load(&r->waiting_count_admin) != 0) {
        pthread_mutex_unlock(&r->control_mutex);
        return;
    }

    if (replica_num == 1)
        r->replica->replica1_sock_waiting_conn = v;

    if (replica_num == 2)
        r->replica->replica2_sock_waiting_conn = v;
    pthread_mutex_unlock(&r->control_mutex);
}

static bool replicas_unassign_cmd_internal(region_info_t *r_info, table_handler_t *t_handler, bool force_all_replicas) {
    uint64_t cid = t_handler->cid;
    bool unassign_replicas_ok = true;
    bool unassign_replicas_found = false;
    char dt[DT_LOG_SIZE];

    if (r_info->is_leader && r_info->replica != NULL) {
        uint8_t unassign_msg[MSG_RSC_BUFF_OFFSET];
        memset(unassign_msg, 0, sizeof(unassign_msg));
        uint32_t unassign_msg_size = sizeof(unassign_msg);
        r_info->req_count_chk++;

        if (r_info->req_count_chk > MAX_REPLICA_COUNTER_MASK)
            r_info->req_count_chk = 1;
        uint32_t unassign_req_count = r_info->req_count_chk;
        memcpy(unassign_msg + MSG_SIZE_OFFSET, &unassign_msg_size, sizeof(unassign_msg_size));
        unassign_msg[MSG_TYPE_OFFSET] = MSG_UNASSIGN;
        memcpy(unassign_msg + offsetof(struct msg_rsc_t, req_count_chk), &unassign_req_count, sizeof(unassign_req_count));
        memcpy(unassign_msg + offsetof(struct msg_rsc_t, uuid), r_info->uuid, UUID_SIZE);

        for (uint8_t replica_num = 1; replica_num <= 2; replica_num++) {
            bool remote_replica;
            int replica_port;
            int replica_fd;
            uint32_t replica_region_pos;

            if (!force_all_replicas && r_info->req_put_counter_sum == 0 && r_info->req_del_counter_sum == 0)
                continue;

            if (replica_num == 1) {
                remote_replica = r_info->replica->r_info1 == NULL && r_info->replica->replica1_port > 0;
                replica_port = r_info->replica->replica1_port;
                replica_fd = atomic_load(&r_info->replica->replica1_sock_fd);
                replica_region_pos = r_info->replica->replica1_region_pos;
            } else {
                remote_replica = r_info->replica->r_info2 == NULL && r_info->replica->replica2_port > 0;
                replica_port = r_info->replica->replica2_port;
                replica_fd = atomic_load(&r_info->replica->replica2_sock_fd);
                replica_region_pos = r_info->replica->replica2_region_pos;
            }

            if (!remote_replica)
                continue;

            if (replica_fd <= 0)
                continue;
            unassign_replicas_found = true;
            memcpy(unassign_msg + offsetof(struct msg_rsc_t, region_pos), &replica_region_pos, sizeof(replica_region_pos));
            unassign_msg[offsetof(struct msg_rsc_t, uuid) + UUID_SIZE - 1] = (replica_num == 1) ? '1' : '2';

            if ((force_all_replicas || r_info->req_counter > 0) && replica_fd <= 0) {
                fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Cannot send MSG_UNASSIGN for %s to replica%d: connection is not available, fd=%d, port=%d\n", curr_dt(dt), cid,
                        r_info->uuid, replica_num, replica_fd, replica_port);
                unassign_replicas_ok = false;
                continue;
            }
            connection_transport_t transport;
            transport_init(&transport, replica_fd);
            ssize_t write_result = transport_write(&transport, unassign_msg, unassign_msg_size);

            if (write_result != (ssize_t)unassign_msg_size) {
                fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] Failed send MSG_UNASSIGN for %s to replica%d: result=%ld, expected=%u, fd=%d\n", curr_dt(dt), cid, r_info->uuid,
                        replica_num, write_result, unassign_msg_size, replica_fd);
                unassign_replicas_ok = false;
                continue;
            }
            uint8_t answer[MSG_SIZE_RESERVE_RS_ANSWER];
            size_t answer_size = 0;

            while (answer_size < sizeof(answer)) {
                int read_result = (int)transport_read(&transport, answer + answer_size, sizeof(answer) - answer_size, cid);

                if (read_result <= 0)
                    break;
                answer_size += (size_t)read_result;
            }
            uint32_t answer_msg_size = 0;
            uint32_t answer_req_count = 0;

            if (answer_size == sizeof(answer)) {
                memcpy(&answer_msg_size, answer + MSG_SIZE_OFFSET, sizeof(answer_msg_size));
                memcpy(&answer_req_count, answer + MSG_REPLICA_COUNTER_OFFSET, MSG_REPLICA_COUNTER_SIZE);
                answer_req_count &= MAX_REPLICA_COUNTER_MASK;
            }

            if (answer_size != sizeof(answer) || answer_msg_size != MSG_SIZE_RESERVE_RS_ANSWER || answer[MSG_TYPE_OFFSET] != MSG_RESP_OK ||
                answer_req_count != unassign_req_count) {
                fprintf(stderr,
                        "%s WARN [REGION_HANDLER=%015lu] Invalid MSG_UNASSIGN answer for %s from replica%d: read=%lu, size=%u, type=%u, counter=%u, expected_counter=%u, fd=%d\n",
                        curr_dt(dt), cid, r_info->uuid, replica_num, answer_size, answer_msg_size, answer_size > MSG_TYPE_OFFSET ? answer[MSG_TYPE_OFFSET] : 0, answer_req_count,
                        unassign_req_count, replica_fd);
                unassign_replicas_ok = false;
                continue;
            }
            fprintf(stderr, "%s INFO [REGION_HANDLER=%015lu] MSG_UNASSIGN confirmed for %s by replica%d, counter=%u\n", curr_dt(dt), cid, r_info->uuid, replica_num,
                    unassign_req_count);
        }
    }

    if (unassign_replicas_found && !unassign_replicas_ok) {
        fprintf(stderr, "%s WARN [REGION_HANDLER=%015lu] MSG_UNASSIGN was not confirmed by all replicas for %s, creating recovery marker\n", curr_dt(dt), cid, r_info->uuid);
        set_region_recovery_info(r_info, 1L, t_handler);
    }
    return unassign_replicas_ok;
}

void relicas_unassign_cmd(region_info_t *r_info, table_handler_t *t_handler) {
    (void)replicas_unassign_cmd_internal(r_info, t_handler, false);
}


bool replicas_shutdown_cmd(region_info_t *r_info, table_handler_t *t_handler) {
    return replicas_unassign_cmd_internal(r_info, t_handler, true);
}
#define REPLICAS_IDLE_EXCHANGE_MS 1000U
#define REPLICAS_IDLE_RETRY_MS 1000U
typedef struct idle_replica_target_t {
    region_info_t *local;
    int port;
    int fd;
    uint32_t position;
    bool waiting;
} idle_replica_target_t;
static int64_t replica_idle_now_ms(void) {
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
        return -1;
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static bool replica_idle_transfer(idle_replica_target_t *target, uint8_t *bytes, size_t size, bool sending, int64_t deadline) {
    size_t used = 0;

    while (used < size) {
        int64_t now = replica_idle_now_ms();

        if (now < 0 || now >= deadline || atomic_load(&is_shutting_down) || atomic_load(&rs_shutdown_state) != RS_SHUTDOWN_RUNNING) {
            errno = ETIMEDOUT;
            return false;
        }
        short events = sending ? POLLOUT : POLLIN;
        int result;
        {
            result = sending ? (int)send(target->fd, bytes + used, size - used, MSG_DONTWAIT | MSG_NOSIGNAL) : (int)recv(target->fd, bytes + used, size - used, MSG_DONTWAIT);

            if (result == 0)
                return false;

            if (result < 0) {
                if (errno == EINTR)
                    continue;

                if (errno != EAGAIN && errno != EWOULDBLOCK)
                    return false;
            }
        }

        if (result > 0) {
            used += (size_t)result;
            continue;
        }
        now = replica_idle_now_ms();

        if (now < 0 || now >= deadline)
            return false;
        struct pollfd pfd = {.fd = target->fd, .events = events};
        int polled = poll(&pfd, 1, (int)(deadline - now));

        if (polled < 0 && errno == EINTR)
            continue;

        if (polled <= 0 || (pfd.revents & (POLLERR | POLLNVAL)) != 0 || ((pfd.revents & POLLHUP) != 0 && (pfd.revents & events) == 0))
            return false;
    }
    return true;
}

static idle_replica_target_t replica_idle_target(region_info_t *region, unsigned n, bool tls) {
    replica_info_t *replica = region->replica;
    idle_replica_target_t target = {0};
    target.local = n == 0 ? replica->r_info1 : replica->r_info2;
    target.port = n == 0 ? replica->replica1_port : replica->replica2_port;
    (void)tls;
    target.fd = n == 0 ? replica->replica1_sock_fd : replica->replica2_sock_fd;
    target.position = (uint32_t)(n == 0 ? replica->replica1_region_pos : replica->replica2_region_pos);
    target.waiting = n == 0 ? replica->replica1_sock_waiting_conn : replica->replica2_sock_waiting_conn;
    return target;
}


static bool replica_idle_same_target(const idle_replica_target_t *a, const idle_replica_target_t *b) {
    return a->local == b->local && a->port == b->port && a->fd == b->fd && a->position == b->position && a->waiting == b->waiting;
}


static bool replica_idle_try_table(table_info_t *table) {
    if (pthread_mutex_trylock(&table->tlock) != 0)
        return false;

    if (table->retired || table->t_lock_id != 0) {
        pthread_mutex_unlock(&table->tlock);
        return false;
    }
    return true;
}

static bool replica_idle_lock_children(region_info_t *region, idle_replica_target_t targets[2], bool held[2]) {
    for (unsigned n = 0; n < 2; n++) {
        region_info_t *child = targets[n].local;

        if (child == NULL)
            continue;

        if (child == region || (n == 1 && child == targets[0].local) || pthread_mutex_trylock(&child->rlock) != 0)
            return false;
        held[n] = true;

        if (!child->has_parent_replica || child->is_leader || child->is_freed || child->stop || child->bad_data || child->r_need_data_chk != 0 || child->waiting_count_admin != 0 ||
            child->waiting_count_lock != 0 || child->is_write || child->is_replicate || child->idx_resizer != RESIZE_IDX_DONE || child->status != STATUS_ASSIGN_REGION ||
            child->uuid == NULL || strnlen(child->uuid, UUID_SIZE + 1U) != UUID_SIZE || memcmp(region->uuid, child->uuid, UUID_SIZE - 1U) != 0 ||
            child->uuid[UUID_SIZE - 1U] != (char)('1' + n))
            return false;
    }
    return true;
}

static void replica_idle_unlock_children(idle_replica_target_t targets[2], bool held[2]) {
    for (unsigned n = 0; n < 2; n++)
        if (held[n])
            pthread_mutex_unlock(&targets[n].local->rlock);
}

static void replica_idle_discard(region_info_t *region, unsigned n, idle_replica_target_t *target) {
    shutdown(target->fd, SHUT_RDWR);
    { close_socket(region, (uint8_t)(n + 1), target->fd); }
}


bool try_replicas_idle_commit(table_info_t *table, region_info_t *region, table_handler_t *handler) {
    if (table == NULL || handler == NULL || !replicas_idle_commit_load_ready() || !replicas_idle_commit_ready(region) || atomic_load(&region->replicas_idle_committed))
        return false;
    int64_t now = replica_idle_now_ms();

    if (now < 0 || (uint64_t)now < atomic_load(&region->replicas_idle_retry_after_ms))
        return false;
    atomic_store(&region->replicas_idle_retry_after_ms, (uint64_t)now + REPLICAS_IDLE_RETRY_MS);

    if (!replica_idle_try_table(table))
        return false;
    bool tls = false;
    idle_replica_target_t targets[2] = {replica_idle_target(region, 0, tls), replica_idle_target(region, 1, tls)};
    bool children_held[2] = {false, false};
    int version = table->srv_tbl_version;
    unsigned configured = 0;
    bool ready = region->uuid != NULL && strnlen(region->uuid, UUID_SIZE + 1U) == UUID_SIZE;

    for (unsigned n = 0; n < 2; n++) {
        if (targets[n].local != NULL || targets[n].port > 0)
            configured++;
        bool connected = targets[n].fd > 0;

        if (targets[n].waiting || (targets[n].local == NULL && targets[n].port > 0 && !connected))
            ready = false;
    }
    ready = ready && configured == (unsigned)region->replica->replica_num;

    if (ready)
        ready = replica_idle_lock_children(region, targets, children_held);

    if (ready) {
        flush_write_4K_buffer(region, handler->cid);

        for (unsigned n = 0; n < 2; n++)
            if (targets[n].local != NULL)
                flush_write_4K_buffer(targets[n].local, handler->cid);
    }
    replica_idle_unlock_children(targets, children_held);
    pthread_mutex_unlock(&table->tlock);

    if (!ready)
        return false;
    uint32_t counter = (atomic_load(&region->req_count_chk) + 1U) & MAX_REPLICA_COUNTER_MASK;

    if (counter == 0)
        counter = 1;
    atomic_store(&region->req_count_chk, counter);
    uint8_t command[MSG_RSC_BUFF_OFFSET] = {0};
    uint32_t command_size = sizeof(command);
    memcpy(command, &command_size, sizeof(command_size));
    command[MSG_TYPE_OFFSET] = MSG_UNASSIGN;
    memcpy(command + offsetof(msg_rsc_t, req_count_chk), &counter, sizeof(counter));
    memcpy(command + offsetof(msg_rsc_t, uuid), region->uuid, UUID_SIZE);
    now = replica_idle_now_ms();

    if (now < 0)
        return false;
    int64_t deadline = now + REPLICAS_IDLE_EXCHANGE_MS;

    for (unsigned n = 0; n < 2; n++) {
        if (targets[n].local != NULL || targets[n].port <= 0)
            continue;
        idle_replica_target_t *target = &targets[n];
        command[offsetof(msg_rsc_t, uuid) + UUID_SIZE - 1U] = (uint8_t)('1' + n);
        memcpy(command + offsetof(msg_rsc_t, region_pos), &target->position, sizeof(target->position));
        int flags = fcntl(target->fd, F_GETFL);
        bool ok = flags >= 0 && ((flags & O_NONBLOCK) != 0 || fcntl(target->fd, F_SETFL, flags | O_NONBLOCK) == 0);
        uint8_t reply[MSG_SIZE_RESERVE_RS_ANSWER] = {0};
        uint32_t size = 0, echoed = 0;

        if (ok)
            ok = replica_idle_transfer(target, command, sizeof(command), true, deadline) && replica_idle_transfer(target, reply, sizeof(size), false, deadline);

        if (ok) {
            memcpy(&size, reply, sizeof(size));
            ok = size == sizeof(reply) && replica_idle_transfer(target, reply + sizeof(size), sizeof(reply) - sizeof(size), false, deadline);
        }

        if (ok) {
            memcpy(&echoed, reply + MSG_REPLICA_COUNTER_OFFSET, MSG_REPLICA_COUNTER_SIZE);
            ok = reply[MSG_TYPE_OFFSET] == MSG_RESP_OK && echoed == counter;
        }

        if (ok && (flags & O_NONBLOCK) == 0)
            ok = fcntl(target->fd, F_SETFL, flags) == 0;

        if (!ok) {
            replica_idle_discard(region, n, target);
            now = replica_idle_now_ms();

            if (now >= 0)
                atomic_store(&region->replicas_idle_retry_after_ms, (uint64_t)now + REPLICAS_IDLE_RETRY_MS);
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s WARN [REPLICA_HANDLER=%015lu] Idle MSG_UNASSIGN failed for %s replica%u; session closed, confirmation deferred\n", curr_dt(dt), handler->cid,
                    region->uuid, n + 1U);
            return false;
        }
    }

    if (!replica_idle_try_table(table))
        return false;
    ready = table->srv_tbl_version == version && replicas_idle_commit_load_ready() && replicas_idle_commit_ready(region);

    for (unsigned n = 0; ready && n < 2; n++) {
        idle_replica_target_t current = replica_idle_target(region, n, tls);
        ready = replica_idle_same_target(&targets[n], &current);
    }
    children_held[0] = children_held[1] = false;

    if (ready)
        ready = replica_idle_lock_children(region, targets, children_held);

    if (ready)
        atomic_store(&region->replicas_idle_committed, true);
    replica_idle_unlock_children(targets, children_held);
    pthread_mutex_unlock(&table->tlock);
    return ready;
}

void waiting_connection(struct region_info_t *r_info, uint8_t replica_num, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];

    if (r_info == NULL || r_info->replica == NULL)
        return;

    if (r_info->is_freed)
        finish_thread(r_info, 11, true, t_handler);
    char curr_uuid[UUID_SIZE + 1];
    strcpy(curr_uuid, r_info->uuid);

    if (replica_num == 1) {
        int fd_local = atomic_load(&r_info->replica->replica1_sock_fd);

        if (fd_local > 0)
            close_socket(r_info, 1, fd_local);
        set_replica_waiting_conn(r_info, 1, true);
        fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Reset replica%d and waiting the new connection=%d\n", curr_dt(dt), t_handler->cid, replica_num,
                atomic_load(&r_info->replica->replica1_sock_fd));

        if (r_info->replica->r_info1 == NULL) {
            for (int j = 10; j > 0; j--) {
                sleep(1);

                if (r_info->is_freed)
                    finish_thread(r_info, 12, true, t_handler);
                fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Waiting replica%d the new connection=%d (%d)\n", curr_dt(dt), t_handler->cid, replica_num,
                        atomic_load(&r_info->replica->replica1_sock_fd), j);
            }
        }
        set_replica_waiting_conn(r_info, 1, false);
        return;
    }

    if (replica_num == 2) {
        int fd_local = atomic_load(&r_info->replica->replica2_sock_fd);

        if (fd_local > 0)
            close_socket(r_info, 2, fd_local);
        set_replica_waiting_conn(r_info, 2, true);
        fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Reset replica%d and waiting the new connection=%d\n", curr_dt(dt), t_handler->cid, replica_num,
                atomic_load(&r_info->replica->replica2_sock_fd));

        if (r_info->replica->r_info2 == NULL) {
            for (int j = 10; j > 0; j--) {
                sleep(1);

                if (r_info->is_freed)
                    finish_thread(r_info, 13, true, t_handler);
                fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Waiting replica%d the new connection=%d (%d)\n", curr_dt(dt), t_handler->cid, replica_num,
                        atomic_load(&r_info->replica->replica2_sock_fd), j);
            }
        }
        set_replica_waiting_conn(r_info, 2, false);
        return;
    }
}

int init_replica_conn(region_info_t *r_info, char *curr_uuid, uint8_t replica_num, bool is_stoppable, table_handler_t *t_handler) {
    invalidate_replicas_idle_commit(r_info);
    invalidate_record_gc(r_info);
    char dt[DT_LOG_SIZE];
    int socket_fd;
    struct hostent_t *server_host;
    struct sockaddr_in server_address;
    char server_name[1024];

    for (int i = 0; i <= max_retry_num; i++) {
        if (atomic_load(&is_shutting_down)) {
            do_r_unlock(r_info, 141, true, t_handler);

            while (atomic_load(&is_shutting_down)) {
                sleep(10);
            }
        }
        r_info->waiting_count_reconnect = 0;
        bool print_waiting = true;

        while (atomic_load(&r_info->waiting_count_admin) != 0 && is_stoppable == false) {
            if (atomic_load(&is_shutting_down)) {
                do_r_unlock(r_info, 142, true, t_handler);

                while (atomic_load(&is_shutting_down)) {
                    sleep(10);
                }
            }
            r_info->replica_stoped = true;

            if (print_waiting) {
                fprintf(stderr, "%s INFO [REPLICA_HANDLER=%015lu] Init %d replica%d waiting for admin operation to complete, r_lock_id=%d\n", curr_dt(dt), t_handler->cid, i,
                        replica_num, r_info->r_lock_id);
                print_waiting = false;
            }
            usleep(100000);
        }

        if (!print_waiting)
            fprintf(stderr, "%s INFO [REPLICA_HANDLER=%015lu] Finish waiting for admin operation to complete, r_lock_id=%d\n", curr_dt(dt), t_handler->cid, r_info->r_lock_id);
        r_info->replica_stoped = false;
        int server_port = -1;

        if (r_info->replica == NULL)
            finish_thread(r_info, 14, false, t_handler); // TODO А так бывает?

        if (r_info->is_freed)                            // TODO А так бывает?
            finish_thread(r_info, 15, true, t_handler);  // TODO 20250122 Атавизм, пробуй удалить

        if (replica_num == 1) {
            pthread_mutex_lock(&r_info->control_mutex);

            while (atomic_load(&r_info->waiting_count_admin) != 0 && is_stoppable == false) {
                pthread_mutex_unlock(&r_info->control_mutex);

                if (atomic_load(&is_shutting_down)) {
                    do_r_unlock(r_info, 142, true, t_handler);

                    while (atomic_load(&is_shutting_down)) {
                        sleep(10);
                    }
                }
                usleep(100000);
                pthread_mutex_lock(&r_info->control_mutex);
            }
            strncpy(server_name, r_info->replica->replica1, sizeof(server_name) - 1);
            server_name[sizeof(server_name) - 1] = '\0';
            server_port = r_info->replica->replica1_port;
            pthread_mutex_unlock(&r_info->control_mutex);
        }

        if (replica_num == 2) {
            pthread_mutex_lock(&r_info->control_mutex);

            while (atomic_load(&r_info->waiting_count_admin) != 0 && is_stoppable == false) {
                pthread_mutex_unlock(&r_info->control_mutex);

                if (atomic_load(&is_shutting_down)) {
                    do_r_unlock(r_info, 142, true, t_handler);

                    while (atomic_load(&is_shutting_down)) {
                        sleep(10);
                    }
                }
                usleep(100000);
                pthread_mutex_lock(&r_info->control_mutex);
            }
            strncpy(server_name, r_info->replica->replica2, sizeof(server_name) - 1);
            server_name[sizeof(server_name) - 1] = '\0';
            server_port = r_info->replica->replica2_port;
            pthread_mutex_unlock(&r_info->control_mutex);
        }
        r_info->waiting_count_reconnect = 1;

        if (blocking_socket_mode == 0)
            server_port++;
        server_host = thread_safe_gethostname(server_name);

        if (server_host == NULL) {
            fprintf(stderr, "%s WARN [REPLICA_HANDLER=%015lu] Could not resolve hostname %s\n", curr_dt(dt), t_handler->cid, server_name);
            r_info->waiting_count_reconnect = 0;
            return -1;
        }
        memset(&server_address, 0, sizeof server_address);
        server_address.sin_family = AF_INET;
        server_address.sin_port = htons(server_port);
        memcpy(&server_address.sin_addr.s_addr, server_host->hp->h_addr, server_host->hp->h_length);

        if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
            perror("socket init_replica_conn");
            graceful_exit(-4);
        }

        if (server_port <= 0) {
            socket_fd = -1;
            break;
        }
        struct timeval tv;
        tv.tv_sec = t_handler->client_timeout_ms / 1000;
        tv.tv_usec = 0;
        setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv));
        setsockopt(socket_fd, SOL_SOCKET, SO_SNDTIMEO, (const char *)&tv, sizeof(tv));

        if (set_tcp_nodelay) {
            int nd = 1;

            if (setsockopt(socket_fd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd)) == -1) {
                perror("setsockopt TCP_NODELAY");
            }
        }

        if (connect(socket_fd, (struct sockaddr *)&server_address, sizeof server_address) == -1) {
            fprintf(stderr, "%s WARN [REPLICA_HANDLER=%015lu] Replica region %s:%d is not ready. Try count = %d\n", curr_dt(dt), t_handler->cid, server_name, server_port, i);
            close_socket(r_info, replica_num, socket_fd);
            sleep(1);
        } else {
            if (log_level > 1) {
                void *bt[1024];
                int bt_size = backtrace(bt, sizeof(bt) / sizeof(bt[0]));
                backtrace_symbols_fd(bt, bt_size, STDERR_FILENO);
            }

            if (r_info->is_freed) {
                finish_thread(r_info, 16, true, t_handler);
                return -1;
            }
            fprintf(stderr, "%s INFO [REPLICA_HANDLER=%015lu] Auth try %d (connection=%d)\n", curr_dt(dt), t_handler->cid, i, socket_fd);

            if (auth_msg_send(socket_fd, curr_uuid, t_handler->cid) == true)
                break;
            else {
                fprintf(stderr, "%s WARN [REPLICA_HANDLER=%015lu] Auth failed for %s:%d (close connection=%d)\n", curr_dt(dt), t_handler->cid, server_name, server_port, socket_fd);
                close_socket(r_info, replica_num, socket_fd);
            }
        }

        if (r_info->is_freed) {
            finish_thread(r_info, 17, true, t_handler);
            return -1;
        }

        if (i == max_retry_num) {
            fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Replica region %s:%d is not found. Break the connection\n", curr_dt(dt), t_handler->cid, server_name, server_port);

            if (is_stoppable)
                return -1;
            r_info->waiting_count_reconnect = 0;
            fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Init replica connection for %s r_info->waiting_count_reconnect=%d\n", curr_dt(dt), t_handler->cid, curr_uuid,
                    r_info->waiting_count_reconnect);
            usleep(20000);

            while (r_info->waiting_count_admin != 0 && is_stoppable == false) {
                if (atomic_load(&is_shutting_down)) {
                    do_r_unlock(r_info, 142, true, t_handler);

                    while (atomic_load(&is_shutting_down)) {
                        sleep(10);
                    }
                }
                r_info->replica_stoped = true;
                fprintf(stderr, "%s INFO [REPLICA_HANDLER=%015lu] Waiting for admin operation to complete, r_lock_id=%d\n", curr_dt(dt), t_handler->cid, r_info->r_lock_id);
                usleep(100000);
            }
            r_info->replica_stoped = false;
            r_info->waiting_count_reconnect = 1;
            fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Init replica connection for %s r_info->waiting_count_reconnect=%d\n", curr_dt(dt), t_handler->cid, curr_uuid,
                    r_info->waiting_count_reconnect);

            if (replica_num == 1) {
                set_replica_waiting_conn(r_info, 1, true);

                if (r_info->replica->r_info1 == NULL && !r_info->is_freed) {
                    for (int j = 0; j < 10; j++)
                        sleep(1);
                    i--;
                    r_info->waiting_count_reconnect = 0;
                    set_replica_waiting_conn(r_info, 1, false);
                    continue;
                }

                if (r_info->replica->r_info1 == NULL)
                    socket_fd = r_info->replica->replica1_sock_fd;
                else
                    socket_fd = -1;
                r_info->waiting_count_reconnect = 0;
                set_replica_waiting_conn(r_info, 1, false);
            }

            if (replica_num == 2) {
                set_replica_waiting_conn(r_info, 2, true);

                if (r_info->replica->r_info2 == NULL && !r_info->is_freed) {
                    for (int j = 0; j < 10; j++)
                        sleep(1);
                    i--;
                    r_info->waiting_count_reconnect = 0;
                    set_replica_waiting_conn(r_info, 2, false);
                    continue;
                }

                if (r_info->replica->r_info2 == NULL)
                    socket_fd = r_info->replica->replica2_sock_fd;
                else
                    socket_fd = -1;
                r_info->waiting_count_reconnect = 0;
                set_replica_waiting_conn(r_info, 2, false);
            }
            break;
        }
    }
    free(server_host->hostbuf);
    free(server_host->tmphstbuf);
    free(server_host);
    return socket_fd;
}

bool create_reserve_region_map(char *cbuff, region_info_t *r_info, uint8_t rep, table_handler_t *t_handler) {
    union i2b_u region_pos;
    uint64_t cid = t_handler->cid;
    uint32_t msg_offset = INIT_MSG_HDR;
    memcpy(region_pos.byte, cbuff + msg_offset, INIT_MSG_SIZE); // TODO calc
    char dt[DT_LOG_SIZE];

    if (region_pos.integer_val == -1) {
        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Error code 101. Request region %s map position return -1\n", curr_dt(dt), cid, r_info->uuid);
        r_info->stop = true;
        finish_thread(r_info, 18, true, t_handler);
    }
    fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Region %s replica%d receive map position %d\n", curr_dt(dt), cid, r_info->uuid, rep, region_pos.integer_val);

    if (rep == 1)
        r_info->replica->replica1_region_pos = region_pos.integer_val;

    if (rep == 2)
        r_info->replica->replica2_region_pos = region_pos.integer_val;
}

bool replica_data_check(struct region_info_t *r_info, uint8_t replica_num, char *buff_reserve, uint32_t msg_size, table_handler_t *t_handler) {
    invalidate_record_gc(r_info);
    char dt[DT_LOG_SIZE];
    bool result = false;
    fprintf(stderr, "%s INFO [REPLICA_HANDLER=%015lu] Replica%d %s requested data check\n", curr_dt(dt), t_handler->cid, replica_num, r_info->uuid);
    uint8_t *zbuff_tmp = malloc(r_info->zbuff_size);

    if (zbuff_tmp == NULL) {
        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Replica%d %s cannot malloc %lu for data check, sleep 10s\n", curr_dt(dt), t_handler->cid, replica_num, r_info->uuid,
                r_info->zbuff_size);
        sleep(10);
        return false;
    }
    memcpy(zbuff_tmp, r_info->zbuff, r_info->zbuff_size);
    msg_files_list_t *files_list = get_files_list(r_info, true, t_handler);

    if (files_list != NULL) {
        print_files_list(files_list, r_info->uuid, t_handler->cid);

        if (buff_reserve == NULL) {
            result = check_remote_replica(r_info, replica_num, files_list, t_handler);
        } else {
            msg_files_list_t *files_list_replica = deserialize_files_list(buff_reserve, msg_size, t_handler->cid);

            if (files_list_replica != NULL) {
                result = compare_fix_files_lists(r_info, replica_num, files_list, files_list_replica, t_handler);
                free_files_list(files_list_replica);
            }
        }
        free_files_list(files_list);
    }
    memcpy(r_info->zbuff, zbuff_tmp, r_info->zbuff_size);
    free(zbuff_tmp);
    return result;
}

bool replica_refresh_region_map(struct region_info_t *r_info, uint8_t replica_num, bool is_stoppable, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];

    if (r_info == NULL) {
        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Cant refresh region map, region == NULL\n", curr_dt(dt), t_handler->cid);
        finish_thread(r_info, 19, true, t_handler);
        return false;
    }

    if (r_info->is_freed) {
        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Cant refresh region map, region has freed\n", curr_dt(dt), t_handler->cid);
        finish_thread(r_info, 20, true, t_handler);
        return false;
    }

    if (r_info->replica == NULL) {
        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Cant refresh region map, replica == NULL\n", curr_dt(dt), t_handler->cid);
        finish_thread(r_info, 21, true, t_handler);
        return false;
    }
    char curr_uuid[UUID_SIZE + 1];
    strcpy(curr_uuid, r_info->uuid);
    uint32_t curr_max_msg_size = FIX_MAX_MSG_SIZE;
    char *buff_reserve = (char *)malloc(curr_max_msg_size);

    if (buff_reserve == NULL) {
        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] OOM alloc buff_reserve\n", curr_dt(dt), t_handler->cid);
        return false;
    }
    struct timeval tv;
    tv.tv_sec = cli_sock_timeout;
    tv.tv_usec = 0;
    uint32_t req_sz = r_info->replica->request_params_size;
    uint8_t *req_local = NULL;

    if (req_sz > 0 && r_info->replica->request_params != NULL) {
        req_local = (uint8_t *)malloc(req_sz);

        if (!req_local) {
            fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] OOM alloc req_local\n", curr_dt(dt), t_handler->cid);
            free(buff_reserve);
            return false;
        }
        memcpy(req_local, r_info->replica->request_params, req_sz);
        memcpy(req_local + INIT_MSG_HDR, &t_handler->client_timeout_ms, sizeof(t_handler->client_timeout_ms));
    } else {
        free(buff_reserve);
        return false;
    }
    bool overall_ok = true;

    if (replica_num == 1) {
        uint8_t try_connect_count = 1;
        bool create_connection_failed;

        do {
            create_connection_failed = false;

            if (is_stoppable && try_connect_count == TRY_REFRESH_REGION_MAP_LIMIT) {
                r_info->waiting_count_reconnect = 0;
                overall_ok = false;
                break;
            }
            int fd_shared = atomic_load(&r_info->replica->replica1_sock_fd);

            if (fd_shared < 0 && r_info->replica->replica1_port > 0) {
                fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Replica1 waiting start init connection for %s r_info->waiting_count_reconnect=0\n", curr_dt(dt), t_handler->cid,
                        r_info->uuid);
                r_info->waiting_count_reconnect = 0;
                usleep(1000);
                bool print_waiting = true;

                while (r_info->waiting_count_admin != 0 && is_stoppable == false) {
                    if (atomic_load(&is_shutting_down)) {
                        do_r_unlock(r_info, 142, true, t_handler);

                        while (atomic_load(&is_shutting_down)) {
                            sleep(10);
                        }
                    }
                    r_info->replica_stoped = true;

                    if (print_waiting) {
                        fprintf(stderr, "%s INFO [REPLICA_HANDLER=%015lu] Replica1 waiting for admin operation to complete, r_lock_id=%d\n", curr_dt(dt), t_handler->cid,
                                r_info->r_lock_id);
                        print_waiting = false;
                    }
                    usleep(10000);
                }

                if (!print_waiting)
                    fprintf(stderr, "%s INFO [REPLICA_HANDLER=%015lu] Finish waiting for admin operation to complete, r_lock_id=%d\n", curr_dt(dt), t_handler->cid,
                            r_info->r_lock_id);
                r_info->replica_stoped = false;

                if (r_info->is_freed) {
                    free(req_local);
                    free(buff_reserve);
                    finish_thread(r_info, 22, true, t_handler);
                }
                r_info->waiting_count_reconnect = 1;
                fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Replica1 ready try %d start init connection for %s r_info->waiting_count_reconnect=%d\n", curr_dt(dt),
                        t_handler->cid, try_connect_count, curr_uuid, r_info->waiting_count_reconnect);
                int fd_new = init_replica_conn(r_info, curr_uuid, 1, is_stoppable, t_handler);

                if (fd_new < 0) {
                    r_info->waiting_count_reconnect = 0;
                    set_replica_waiting_conn(r_info, 1, false);
                    free(req_local);
                    free(buff_reserve);
                    return false;
                }

                if (r_info->is_freed) {
                    close_socket(r_info, 1, fd_new);
                    free(req_local);
                    free(buff_reserve);
                    finish_thread(r_info, 23, true, t_handler);
                }
                int expected = fd_shared;
                bool published = false;

                while (true) {
                    expected = atomic_load(&r_info->replica->replica1_sock_fd);

                    if (expected >= 0) {
                        break;
                    }

                    if (atomic_compare_exchange_strong(&r_info->replica->replica1_sock_fd, &expected, fd_new)) {
                        published = true;
                        break;
                    }
                }

                if (!published) {
                    close_socket(r_info, 1, fd_new);
                }
                int fd_local = atomic_load(&r_info->replica->replica1_sock_fd);

                if (fd_local < 0) {
                    create_connection_failed = true;
                    sleep(1);
                    try_connect_count++;
                    continue;
                }
                set_replica_waiting_conn(r_info, 1, false);
                fprintf(stderr, "%s INFO [REPLICA_HANDLER=%015lu] Create to %s-%s replica1=%s:%d connection=%d\n", curr_dt(dt), t_handler->cid, r_info->r_fname, curr_uuid,
                        r_info->replica->replica1, r_info->replica->replica1_port, fd_local);

                if (req_sz >= 2)
                    req_local[req_sz - 2] = '1';
                int reserve_msg_size = 0;
                uint32_t to_write = req_sz;

                while ((uint32_t)reserve_msg_size < to_write) {
                    ssize_t wn = write(fd_local, req_local + reserve_msg_size, to_write - reserve_msg_size);

                    if (wn <= 0) {
                        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Replica1 write failed (wn=%zd, errno=%d) connection=%d. Reconnect\n", curr_dt(dt), t_handler->cid, wn,
                                errno, fd_local);
                        create_connection_failed = true;
                        close_socket(r_info, 1, fd_local);
                        sleep(1);
                        break;
                    }
                    reserve_msg_size += (int)wn;
                }

                if (create_connection_failed) {
                    try_connect_count++;
                    continue;
                }
                fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Sent %d bytes to connection=%d\n", curr_dt(dt), t_handler->cid, reserve_msg_size, fd_local);
                fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Waiting reply from connection=%d\n", curr_dt(dt), t_handler->cid, fd_local);
                int got = 0;

                while (got < (int)INIT_MSG_SIZE) {
                    ssize_t rn = read(fd_local, buff_reserve + got, INIT_MSG_SIZE - got);

                    if (rn <= 0) {
                        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Replica1 read header failed (rn=%zd, errno=%d) connection=%d. Reconnect\n", curr_dt(dt), t_handler->cid,
                                rn, errno, fd_local);
                        create_connection_failed = true;
                        close_socket(r_info, 1, fd_local);
                        sleep(1);
                        break;
                    }
                    got += (int)rn;
                }

                if (create_connection_failed) {
                    try_connect_count++;
                    continue;
                }
                uint32_t msize;
                memcpy(&msize, buff_reserve, sizeof(msize));
                fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Recv %d bytes from connection=%d. Full packet size = %u\n", curr_dt(dt), t_handler->cid, got, fd_local, msize);

                if ((uint32_t)got < INIT_MSG_SIZE || msize < INIT_MSG_SIZE) {
                    fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Replica1 recv header %d bytes or bad msize=%u from connection=%d. Reconnect\n", curr_dt(dt), t_handler->cid,
                            got, msize, fd_local);
                    create_connection_failed = true;
                    close_socket(r_info, 1, fd_local);
                    sleep(1);
                    try_connect_count++;
                    continue;
                }

                if (msize > curr_max_msg_size) {
                    if (msize > FIX_MAX_MSG_SIZE * 10) {
                        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Replica1 - got too big msg size=%u. The limit=%u\n", curr_dt(dt), t_handler->cid, msize,
                                FIX_MAX_MSG_SIZE * 100);
                        create_connection_failed = true;
                        close_socket(r_info, 1, fd_local);
                        sleep(1);
                        try_connect_count++;
                        continue;
                    }
                    uint32_t new_cap = (uint32_t)(ceil((float)msize / FIX_MAX_MSG_SIZE) * FIX_MAX_MSG_SIZE);
                    fprintf(stderr, "%s INFO [REPLICA_HANDLER=%015lu] Recv replica1 msg size %u. Realloc handler buff up to %u\n", curr_dt(dt), t_handler->cid, msize, new_cap);
                    char *tmp = (char *)realloc(buff_reserve, msize);

                    if (!tmp) {
                        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Replica1 realloc failed for msize=%u\n", curr_dt(dt), t_handler->cid, msize);
                        create_connection_failed = true;
                        close_socket(r_info, 1, fd_local);
                        sleep(1);
                        try_connect_count++;
                        continue;
                    }
                    buff_reserve = tmp;
                    curr_max_msg_size = new_cap;
                    memset(buff_reserve + INIT_MSG_SIZE, 0, msize - INIT_MSG_SIZE);
                }
                setsockopt(fd_local, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv));
                int total = got;

                while ((uint32_t)total < msize) {
                    int remaining = (int)(msize - (uint32_t)total);
                    fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Replica1 ready to recv rest %d bytes from connection=%d\n", curr_dt(dt), t_handler->cid, remaining,
                            fd_local);
                    ssize_t n = recv(fd_local, buff_reserve + total, remaining, 0);

                    if (n <= 0) {
                        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Replica1 recv body failed (n=%zd, errno=%d) connection=%d. Reconnect\n", curr_dt(dt), t_handler->cid, n,
                                errno, fd_local);
                        create_connection_failed = true;
                        close_socket(r_info, 1, fd_local);
                        sleep(1);
                        break;
                    }
                    total += (int)n;
                }

                if (create_connection_failed) {
                    try_connect_count++;
                    continue;
                }
                fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Recv %d bytes from connection=%d\n", curr_dt(dt), t_handler->cid, total, fd_local);

                if ((uint32_t)total == MSG_SIZE_NEW_REGION_ORDER) {
                    waiting_connection(r_info, 1, t_handler);
                    create_connection_failed = true;
                    r_info->replica->replica1_region_pos = -1;
                } else {
                    create_reserve_region_map(buff_reserve, r_info, 1, t_handler);
                }

                if (data_check_enabled && (uint32_t)total > MSG_SIZE_NEW_REGION_ORDER) {
                    if ((uint8_t)buff_reserve[MSG_TYPE_OFFSET] == MSG_DATA_CHK) {
                        fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Refresh region map %s recieve data check request\n", curr_dt(dt), t_handler->cid, curr_uuid);
                        replica_data_check(r_info, 1, buff_reserve, (uint32_t)total, t_handler);
                    }
                }
                try_connect_count++;
            }
        } while (create_connection_failed);

        if (!overall_ok) {
            free(req_local);
            free(buff_reserve);
            return false;
        }
    }

    if (replica_num == 2) {
        uint8_t try_connect_count = 1;
        bool create_connection_failed;

        do {
            create_connection_failed = false;

            if (is_stoppable && try_connect_count == TRY_REFRESH_REGION_MAP_LIMIT) {
                r_info->waiting_count_reconnect = 0;
                overall_ok = false;
                break;
            }
            int fd_shared = atomic_load(&r_info->replica->replica2_sock_fd);

            if (fd_shared < 0 && r_info->replica->replica2_port > 0) {
                fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Replica2 waiting start init connection for %s r_info->waiting_count_reconnect=0\n", curr_dt(dt), t_handler->cid,
                        curr_uuid);
                r_info->waiting_count_reconnect = 0;
                usleep(1000);
                bool print_waiting = true;

                while (r_info->waiting_count_admin != 0 && is_stoppable == false) {
                    if (atomic_load(&is_shutting_down)) {
                        do_r_unlock(r_info, 142, true, t_handler);

                        while (atomic_load(&is_shutting_down)) {
                            sleep(10);
                        }
                    }
                    r_info->replica_stoped = true;

                    if (print_waiting) {
                        fprintf(stderr, "%s INFO [REPLICA_HANDLER=%015lu] Replica2 waiting for admin operation to complete, r_lock_id=%d\n", curr_dt(dt), t_handler->cid,
                                r_info->r_lock_id);
                        print_waiting = false;
                    }
                    usleep(10000);
                }

                if (!print_waiting)
                    fprintf(stderr, "%s INFO [REPLICA_HANDLER=%015lu] Finish waiting for admin operation to complete, r_lock_id=%d\n", curr_dt(dt), t_handler->cid,
                            r_info->r_lock_id);
                r_info->replica_stoped = false;

                if (r_info->is_freed) {
                    free(req_local);
                    free(buff_reserve);
                    finish_thread(r_info, 24, true, t_handler);
                }
                r_info->waiting_count_reconnect = 1;
                fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Replica2 ready try %d start init connection for %s r_info->waiting_count_reconnect=%d\n", curr_dt(dt),
                        t_handler->cid, try_connect_count, curr_uuid, r_info->waiting_count_reconnect);
                int fd_new = init_replica_conn(r_info, curr_uuid, 2, is_stoppable, t_handler);

                if (fd_new < 0) {
                    r_info->waiting_count_reconnect = 0;
                    set_replica_waiting_conn(r_info, 2, false);
                    free(req_local);
                    free(buff_reserve);
                    return false;
                }

                if (r_info->is_freed) {
                    close_socket(r_info, 2, fd_new);
                    free(req_local);
                    free(buff_reserve);
                    finish_thread(r_info, 25, true, t_handler);
                }
                bool published = false;

                while (true) {
                    int expected = atomic_load(&r_info->replica->replica2_sock_fd);

                    if (expected >= 0)
                        break;

                    if (atomic_compare_exchange_strong(&r_info->replica->replica2_sock_fd, &expected, fd_new)) {
                        published = true;
                        break;
                    }
                }

                if (!published) {
                    close_socket(r_info, 2, fd_new);
                }
                int fd_local = atomic_load(&r_info->replica->replica2_sock_fd);

                if (fd_local < 0) {
                    create_connection_failed = true;
                    sleep(1);
                    try_connect_count++;
                    continue;
                }
                set_replica_waiting_conn(r_info, 2, false);
                fprintf(stderr, "%s INFO [REPLICA_HANDLER=%015lu] Create to %s-%s replica2=%s:%d connection=%d\n", curr_dt(dt), t_handler->cid, r_info->r_fname, curr_uuid,
                        r_info->replica->replica2, r_info->replica->replica2_port, fd_local);

                if (req_sz >= 2)
                    req_local[req_sz - 2] = '2';
                int reserve_msg_size = 0;

                while ((uint32_t)reserve_msg_size < req_sz) {
                    ssize_t wn = write(fd_local, req_local + reserve_msg_size, req_sz - (uint32_t)reserve_msg_size);

                    if (wn <= 0) {
                        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Replica2 write failed (wn=%zd, errno=%d) connection=%d. Reconnect\n", curr_dt(dt), t_handler->cid, wn,
                                errno, fd_local);
                        create_connection_failed = true;
                        close_socket(r_info, 2, fd_local);
                        sleep(1);
                        break;
                    }
                    reserve_msg_size += (int)wn;
                }

                if (create_connection_failed) {
                    try_connect_count++;
                    continue;
                }
                fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Sent %d bytes to connection=%d\n", curr_dt(dt), t_handler->cid, reserve_msg_size, fd_local);
                fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Waiting reply from connection=%d\n", curr_dt(dt), t_handler->cid, fd_local);
                int got = 0;

                while (got < (int)INIT_MSG_SIZE) {
                    ssize_t rn = read(fd_local, buff_reserve + got, INIT_MSG_SIZE - got);

                    if (rn <= 0) {
                        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Replica2 read header failed (rn=%zd, errno=%d) connection=%d. Reconnect\n", curr_dt(dt), t_handler->cid,
                                rn, errno, fd_local);
                        create_connection_failed = true;
                        close_socket(r_info, 2, fd_local);
                        sleep(1);
                        break;
                    }
                    got += (int)rn;
                }

                if (create_connection_failed) {
                    try_connect_count++;
                    continue;
                }
                uint32_t msize;
                memcpy(&msize, buff_reserve, sizeof(msize));
                fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Recv %d bytes from connection=%d. Full packet size = %u\n", curr_dt(dt), t_handler->cid, got, fd_local, msize);

                if ((uint32_t)got < INIT_MSG_SIZE || msize < INIT_MSG_SIZE) {
                    fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Replica2 recv header %d bytes or bad msize=%u from connection=%d. Reconnect\n", curr_dt(dt), t_handler->cid,
                            got, msize, fd_local);
                    create_connection_failed = true;
                    close_socket(r_info, 2, fd_local);
                    sleep(1);
                    try_connect_count++;
                    continue;
                }

                if (msize > curr_max_msg_size) {
                    if (msize > FIX_MAX_MSG_SIZE * 10) {
                        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Replica2 - got too big msg size=%u. The limit=%u\n", curr_dt(dt), t_handler->cid, msize,
                                FIX_MAX_MSG_SIZE * 100);
                        create_connection_failed = true;
                        close_socket(r_info, 2, fd_local);
                        sleep(1);
                        try_connect_count++;
                        continue;
                    }
                    uint32_t new_cap = (uint32_t)(ceil((float)msize / FIX_MAX_MSG_SIZE) * FIX_MAX_MSG_SIZE);
                    fprintf(stderr, "%s INFO [REPLICA_HANDLER=%015lu] Recv replica2 msg size %u. Realloc handler buff up to %u\n", curr_dt(dt), t_handler->cid, msize, new_cap);
                    char *tmp = (char *)realloc(buff_reserve, msize);

                    if (!tmp) {
                        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Replica2 realloc failed for msize=%u\n", curr_dt(dt), t_handler->cid, msize);
                        create_connection_failed = true;
                        close_socket(r_info, 2, fd_local);
                        sleep(1);
                        try_connect_count++;
                        continue;
                    }
                    buff_reserve = tmp;
                    curr_max_msg_size = new_cap;
                    memset(buff_reserve + INIT_MSG_SIZE, 0, msize - INIT_MSG_SIZE);
                }
                setsockopt(fd_local, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv));
                int total = got;

                while ((uint32_t)total < msize) {
                    int remaining = (int)(msize - (uint32_t)total);
                    fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Replica2 ready to recv rest %d bytes from connection=%d\n", curr_dt(dt), t_handler->cid, remaining,
                            fd_local);
                    ssize_t n = recv(fd_local, buff_reserve + total, remaining, 0);

                    if (n <= 0) {
                        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Replica2 recv body failed (n=%zd, errno=%d) connection=%d. Reconnect\n", curr_dt(dt), t_handler->cid, n,
                                errno, fd_local);
                        create_connection_failed = true;
                        close_socket(r_info, 2, fd_local);
                        sleep(1);
                        break;
                    }
                    total += (int)n;
                }

                if (create_connection_failed) {
                    try_connect_count++;
                    continue;
                }
                fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Recv %d bytes from connection=%d\n", curr_dt(dt), t_handler->cid, total, fd_local);

                if ((uint32_t)total == MSG_SIZE_NEW_REGION_ORDER) {
                    waiting_connection(r_info, 2, t_handler);
                    create_connection_failed = true;
                    r_info->replica->replica2_region_pos = -1;
                } else {
                    create_reserve_region_map(buff_reserve, r_info, 2, t_handler);
                }

                if (data_check_enabled && (uint32_t)total > MSG_SIZE_NEW_REGION_ORDER) {
                    if ((uint8_t)buff_reserve[MSG_TYPE_OFFSET] == MSG_DATA_CHK) {
                        fprintf(stderr, "%s DEBUG [REPLICA_HANDLER=%015lu] Refresh region map %s recieve data check request\n", curr_dt(dt), t_handler->cid, curr_uuid);
                        replica_data_check(r_info, 2, buff_reserve, (uint32_t)total, t_handler);
                    }
                }
                try_connect_count++;
            }
        } while (create_connection_failed);

        if (!overall_ok) {
            free(req_local);
            free(buff_reserve);
            return false;
        }
    }
    free(req_local);
    free(buff_reserve);

    if (r_info->is_freed)
        finish_thread(r_info, 26, true, t_handler);
    r_info->waiting_count_reconnect = 0;
    return true;
}

static inline ssize_t write_full(int fd, const void *buf, size_t len) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t sent = 0;

    while (sent < len) {
        ssize_t n = write(fd, p + sent, len - sent);

        if (n > 0) {
            sent += (size_t)n;
            continue;
        }

        if (n < 0) {
            if (errno == EINTR)
                continue;

            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return -1;
            return -1;
        }
        return -1;
    }
    return (ssize_t)sent;
}


static bool write_compact_fail(region_info_t *r_info, table_handler_t *t_handler, const char *reason) {
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] MSG_DIR_WRITE_AND_COMPACT failed for %s: %s; recovery marker preserved\n", curr_dt(dt),
            t_handler != NULL ? t_handler->cid : 0L, r_info != NULL ? r_info->uuid : "NULL", reason);

    if (r_info != NULL) {
        if (t_handler != NULL && set_region_recovery_info(r_info, 1L, t_handler))
            atomic_store(&r_info->r_need_data_chk, 1L);
        r_info->stop = true;
        r_info->bad_data = true;
    }
    return false;
}

static bool get_write_compact_v2_target(region_info_t *r_info, const msg_write_compact_v2_t *header, table_handler_t *t_handler, uint8_t *bs, int *fd) {
    if (r_info == NULL || header == NULL || t_handler == NULL || bs == NULL || fd == NULL || r_info->status != STATUS_ASSIGN_REGION || r_info->is_freed || r_info->r_idx_b == NULL)
        return false;
    int8_t compact_bs_pos = get_bs_map_pos(r_info, (uint8_t)(header->compact_bs - 1U), true, t_handler);
    int8_t write_bs_pos = get_bs_map_pos(r_info, (uint8_t)(header->write_bs - 1U), true, t_handler);

    if (compact_bs_pos < 0 || write_bs_pos < 0 || compact_bs_pos != write_bs_pos)
        return false;
    *bs = (uint8_t)compact_bs_pos;

    if (*bs >= r_info->max_idx_b || r_info->r_idx_b[*bs] == NULL || header->compact_b != FBP || header->write_b != FBP || r_info->r_idx_b[*bs]->pos != header->compact_bs ||
        r_info->r_idx_b[*bs]->bsize_factor != (uint32_t)header->compact_bs * FIX_BSIZE_FACTOR)
        return false;
    *fd = r_info->r_idx_b[*bs]->fd[header->compact_b];
    return *fd >= 0 && *fd == r_info->r_idx_b[*bs]->fd[header->write_b] && *fd == r_info->data_000_fd;
}


typedef enum write_compact_v2_state_result_t {
    WRITE_COMPACT_V2_STATE_ERROR = -1,
    WRITE_COMPACT_V2_STATE_MISMATCH = 0,
    WRITE_COMPACT_V2_STATE_MATCH = 1
} write_compact_v2_state_result_t;
static bool write_compact_v2_final_state_matches(region_info_t *r_info, const msg_write_compact_v2_t *header, const uint8_t *write_buffer, append_compaction_context_t *context,
                                                 table_handler_t *t_handler, uint8_t target_bs, int target_fd, uint64_t physical_file_size) {
    atomic_fetch_add(&append_compact_metric_v2_final_probe_calls, 1);

    if (r_info == NULL || header == NULL || write_buffer == NULL || context == NULL || context->source_buffer == NULL || t_handler == NULL || target_bs >= r_info->max_idx_b ||
        r_info->r_idx_b[target_bs] == NULL || target_fd < 0)
        return false;

    if (physical_file_size != header->expected_final_file_size)
        return false;
    uint64_t compact_offset = (uint64_t)header->first_block * FIX_BLOCK_SIZE;
    uint64_t compact_size64 = (uint64_t)header->source_blocks * FIX_BLOCK_SIZE;
    uint64_t retained_end = (header->flags & MSG_WRITE_COMPACT_FLAG_TRUNCATE) != 0 && header->compact_new_file_size < compact_offset + compact_size64
                                ? header->compact_new_file_size
                                : compact_offset + compact_size64;

    if (retained_end < compact_offset || retained_end - compact_offset > UINT32_MAX)
        return false;
    uint32_t retained_compact_size = (uint32_t)(retained_end - compact_offset);
    memset(context->source_buffer, 0, APPEND_COMPACTION_SEGMENT_SIZE);

    if (retained_compact_size != 0 && read_file(target_fd, context->source_buffer, compact_offset, retained_compact_size, r_info, t_handler->cid) != (int)retained_compact_size)
        return false;
    uint8_t compact_hash[MSG_WRITE_COMPACT_HASH_SIZE];

    if (msg_write_compact_hash128(context->source_buffer, retained_compact_size, compact_hash) != MSG_WRITE_COMPACT_OK)
        return false;
    uint32_t alignment = r_info->r_idx_b[target_bs]->bsize_factor;
    uint64_t aligned_write_size64 = ((uint64_t)header->write_size + alignment - 1U) / alignment * alignment;

    if (aligned_write_size64 > UINT32_MAX)
        return false;
    uint32_t aligned_write_size = (uint32_t)aligned_write_size64;
    uint8_t *disk_write = malloc(aligned_write_size);

    if (disk_write == NULL)
        return false;
    bool result = read_file(target_fd, disk_write, header->write_offset, aligned_write_size, r_info, t_handler->cid) == (int)aligned_write_size;
    uint8_t write_hash[MSG_WRITE_COMPACT_HASH_SIZE];
    uint8_t final_hash[MSG_WRITE_COMPACT_HASH_SIZE];

    if (result)
        result = msg_write_compact_hash128(disk_write, aligned_write_size, write_hash) == MSG_WRITE_COMPACT_OK &&
                 msg_write_compact_v2_final_hash(header, retained_compact_size, compact_hash, aligned_write_size, write_hash, final_hash) == MSG_WRITE_COMPACT_OK &&
                 memcmp(final_hash, header->final_hash, sizeof(final_hash)) == 0;
    free(disk_write);
    return result;
}

static bool write_compact_v2_expected_final_hash_matches(const msg_write_compact_v2_t *header, const uint8_t *result_buffer, const uint8_t *write_buffer, uint32_t alignment) {
    if (header == NULL || result_buffer == NULL || write_buffer == NULL || alignment == 0)
        return false;
    uint64_t compact_offset = (uint64_t)header->first_block * FIX_BLOCK_SIZE;
    uint64_t compact_end = compact_offset + (uint64_t)header->source_blocks * FIX_BLOCK_SIZE;
    uint64_t retained_end = (header->flags & MSG_WRITE_COMPACT_FLAG_TRUNCATE) != 0 && header->compact_new_file_size < compact_end ? header->compact_new_file_size : compact_end;
    uint64_t aligned_write_size64 = ((uint64_t)header->write_size + alignment - 1U) / alignment * alignment;

    if (retained_end < compact_offset || retained_end - compact_offset > UINT32_MAX || aligned_write_size64 > UINT32_MAX)
        return false;
    uint32_t retained_compact_size = (uint32_t)(retained_end - compact_offset);
    uint32_t aligned_write_size = (uint32_t)aligned_write_size64;
    uint8_t compact_hash[MSG_WRITE_COMPACT_HASH_SIZE];
    uint8_t write_hash[MSG_WRITE_COMPACT_HASH_SIZE];
    uint8_t final_hash[MSG_WRITE_COMPACT_HASH_SIZE];
    uint32_t full_compact_size = header->source_blocks * FIX_BLOCK_SIZE;
    int compact_hash_result = MSG_WRITE_COMPACT_OK;

    if (retained_compact_size == full_compact_size)
        memcpy(compact_hash, header->result_hash, sizeof(compact_hash));
    else
        compact_hash_result = msg_write_compact_hash128(result_buffer, retained_compact_size, compact_hash);
    return compact_hash_result == MSG_WRITE_COMPACT_OK &&
           msg_write_compact_hash128_padded(write_buffer, header->write_size, aligned_write_size, write_hash) == MSG_WRITE_COMPACT_OK &&
           msg_write_compact_v2_final_hash(header, retained_compact_size, compact_hash, aligned_write_size, write_hash, final_hash) == MSG_WRITE_COMPACT_OK &&
           memcmp(final_hash, header->final_hash, sizeof(final_hash)) == 0;
}


static uint8_t *prepare_write_compact_v2_write_copy(append_compaction_context_t *context, const uint8_t *write_buffer, uint32_t write_size, bool *heap_allocated) {
    if (context == NULL || context->source_buffer == NULL || write_buffer == NULL || heap_allocated == NULL)
        return NULL;
    uint8_t *write_copy;

    if (write_size <= APPEND_COMPACTION_SEGMENT_SIZE - MAX_BSIZE_FACTOR) {
        write_copy = context->source_buffer;
        *heap_allocated = false;
    } else {
        write_copy = malloc((size_t)write_size + MAX_BSIZE_FACTOR);
        *heap_allocated = true;
    }

    if (write_copy != NULL)
        memcpy(write_copy, write_buffer, write_size);
    return write_copy;
}

static void free_write_compact_v2_write_copy(uint8_t *write_copy, bool heap_allocated) {
    if (heap_allocated)
        free(write_copy);
}

static write_compact_v2_state_result_t check_write_compact_v2_source_state(region_info_t *r_info, const msg_write_compact_v2_t *header, const uint8_t *block_meta,
                                                                           const uint8_t *live_bitmap, append_compaction_context_t *context, append_compaction_plan_t *plan,
                                                                           table_handler_t *t_handler, uint8_t compact_bs, uint64_t physical_file_size) {
    if (r_info == NULL || header == NULL || block_meta == NULL || live_bitmap == NULL || context == NULL || plan == NULL || t_handler == NULL)
        return WRITE_COMPACT_V2_STATE_ERROR;

    if (physical_file_size != header->compact_old_file_size)
        return WRITE_COMPACT_V2_STATE_MISMATCH;
    int verify_result = verify_append_compaction_replica_plan(r_info, header->compact_b, compact_bs, header, block_meta, live_bitmap, context, plan, t_handler);
    return verify_result == APPEND_COMPACTION_ANALYZE_OK ? WRITE_COMPACT_V2_STATE_MATCH : WRITE_COMPACT_V2_STATE_MISMATCH;
}

static bool write_compact_memory_ranges_overlap(const void *first, size_t first_size, const void *second, size_t second_size) {
    if (first == NULL || second == NULL || first_size == 0 || second_size == 0)
        return false;
    uintptr_t first_begin = (uintptr_t)first;
    uintptr_t second_begin = (uintptr_t)second;

    if (first_size > UINTPTR_MAX - first_begin || second_size > UINTPTR_MAX - second_begin)
        return true;
    uintptr_t first_end = first_begin + first_size;
    uintptr_t second_end = second_begin + second_size;
    return first_begin < second_end && second_begin < first_end;
}

static bool init_write_compact_v2_context(append_compaction_context_t *context, const uint8_t *payload, uint32_t payload_size, table_handler_t *t_handler) {
    size_t workspace_size = 2U * (size_t)APPEND_COMPACTION_SEGMENT_SIZE;

    if (context == NULL || t_handler == NULL)
        return false;

    if (t_handler->big_buff != NULL && !write_compact_memory_ranges_overlap(t_handler->big_buff, t_handler->big_buff_size, payload, payload_size)) {
        if (t_handler->big_buff_size < workspace_size) {
            uint8_t *workspace = realloc(t_handler->big_buff, workspace_size);

            if (workspace != NULL) {
                t_handler->big_buff = workspace;
                t_handler->big_buff_size = (uint32_t)workspace_size;
            } else {
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Failed realloc workspace=%lu\n", curr_dt(dt), t_handler->cid, workspace_size);
                return false;
            }
        }

        if (t_handler->big_buff_size >= workspace_size)
            return init_append_compaction_context_with_workspace(context, t_handler->big_buff, workspace_size);
    }
    return init_append_compaction_context(context);
}

static void free_write_compact_v2_context_profiled(append_compaction_context_t *context) {
    uint64_t started_us = replica_metric_monotonic_us();
    free_append_compaction_context(context);
    atomic_fetch_add(&append_compact_metric_v2_cleanup_us, replica_metric_monotonic_us() - started_us);
}

static bool write_compact_v2_fail_profiled(region_info_t *r_info, table_handler_t *t_handler, const char *reason) {
    uint64_t started_us = replica_metric_monotonic_us();
    bool result = write_compact_fail(r_info, t_handler, reason);
    atomic_fetch_add(&append_compact_metric_v2_fail_handler_us, replica_metric_monotonic_us() - started_us);
    return result;
}


static bool apply_write_compact_v2_transaction_internal(region_info_t *r_info, const uint8_t *payload, uint32_t payload_size, table_handler_t *t_handler) {
    uint64_t setup_started_us = replica_metric_monotonic_us();
    uint64_t phase_started_us;
    append_compaction_context_t context;
    append_compaction_plan_t plan;
    msg_write_compact_v2_t header;
    const uint8_t *write_buffer = NULL;
    memset(&context, 0, sizeof(context));
    memset(&plan, 0, sizeof(plan));
    phase_started_us = replica_metric_monotonic_us();
    bool context_initialized = init_write_compact_v2_context(&context, payload, payload_size, t_handler);
    atomic_fetch_add(&append_compact_metric_v2_context_init_us, replica_metric_monotonic_us() - phase_started_us);

    if (!context_initialized)
        return write_compact_v2_fail_profiled(r_info, t_handler, "cannot allocate v2 compaction context");

    if (context.owns_work_buffers)
        atomic_fetch_add(&append_compact_metric_v2_heap_contexts, 1);
    else
        atomic_fetch_add(&append_compact_metric_v2_workspace_contexts, 1);
    const uint8_t *block_meta = NULL;
    const uint8_t *live_bitmap = NULL;
    phase_started_us = replica_metric_monotonic_us();
    int validation_result = validate_msg_write_compact_v2(payload, payload_size, &header, &block_meta, &live_bitmap, &write_buffer);
    atomic_fetch_add(&append_compact_metric_v2_validate_us, replica_metric_monotonic_us() - phase_started_us);

    if (validation_result != MSG_WRITE_COMPACT_OK) {
        free_write_compact_v2_context_profiled(&context);
        return write_compact_v2_fail_profiled(r_info, t_handler, msg_write_compact_result_string(validation_result));
    }

    if (log_level > 2) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr,
                "%s TRACE [COMPACT_PROBE=%015lu] stage=replica_received uuid=%s tx=%lu segment=%u "
                "compact_range=[%lu,%lu) write_range=[%lu,%lu) old_file=%lu new_file=%lu final_file=%lu\n",
                curr_dt(dt), t_handler->cid, r_info == NULL ? "(null)" : r_info->uuid, header.transaction_id, header.segment, (uint64_t)header.first_block * FIX_BLOCK_SIZE,
                ((uint64_t)header.first_block + header.source_blocks) * FIX_BLOCK_SIZE, header.write_offset, header.write_offset + header.write_size, header.compact_old_file_size,
                header.compact_new_file_size, header.expected_final_file_size);
    }
    phase_started_us = replica_metric_monotonic_us();

    if (r_info == NULL || t_handler == NULL || r_info->status != STATUS_ASSIGN_REGION || r_info->is_freed || r_info->r_idx_b == NULL) {
        atomic_fetch_add(&append_compact_metric_v2_region_check_us, replica_metric_monotonic_us() - phase_started_us);
        atomic_fetch_add(&append_compact_metric_v2_state_failures, 1);
        free_write_compact_v2_context_profiled(&context);
        return write_compact_v2_fail_profiled(r_info, t_handler, "replica region is not assigned");
    }
    atomic_fetch_add(&append_compact_metric_v2_region_check_us, replica_metric_monotonic_us() - phase_started_us);
    uint8_t compact_bs = 0;
    int compact_fd = -1;
    phase_started_us = replica_metric_monotonic_us();

    if (!get_write_compact_v2_target(r_info, &header, t_handler, &compact_bs, &compact_fd)) {
        atomic_fetch_add(&append_compact_metric_v2_target_us, replica_metric_monotonic_us() - phase_started_us);
        atomic_fetch_add(&append_compact_metric_v2_state_failures, 1);
        free_write_compact_v2_context_profiled(&context);
        return write_compact_v2_fail_profiled(r_info, t_handler, "cannot map v2 b/bs target");
    }
    atomic_fetch_add(&append_compact_metric_v2_target_us, replica_metric_monotonic_us() - phase_started_us);
    phase_started_us = replica_metric_monotonic_us();
    flush_write_4K_buffer(r_info, t_handler->cid);
    atomic_fetch_add(&append_compact_metric_v2_preflush_us, replica_metric_monotonic_us() - phase_started_us);
    struct stat file_stat;
    uint64_t fstat_started_us = replica_metric_monotonic_us();

    if (fstat(compact_fd, &file_stat) != 0 || file_stat.st_size < 0) {
        uint64_t fstat_elapsed_us = replica_metric_monotonic_us() - fstat_started_us;
        atomic_fetch_add(&append_compact_metric_v2_fstat_us, fstat_elapsed_us);
        atomic_fetch_add(&append_compact_metric_v2_initial_fstat_us, fstat_elapsed_us);
        atomic_fetch_add(&append_compact_metric_v2_state_failures, 1);
        free_write_compact_v2_context_profiled(&context);
        return write_compact_v2_fail_profiled(r_info, t_handler, "cannot read v2 physical file size");
    }
    uint64_t fstat_elapsed_us = replica_metric_monotonic_us() - fstat_started_us;
    atomic_fetch_add(&append_compact_metric_v2_fstat_us, fstat_elapsed_us);
    atomic_fetch_add(&append_compact_metric_v2_initial_fstat_us, fstat_elapsed_us);
    atomic_fetch_add(&append_compact_metric_v2_setup_us, replica_metric_monotonic_us() - setup_started_us);
    uint64_t physical_file_size = (uint64_t)file_stat.st_size;
    uint64_t source_verify_started_us = replica_metric_monotonic_us();
    write_compact_v2_state_result_t source_state =
        check_write_compact_v2_source_state(r_info, &header, block_meta, live_bitmap, &context, &plan, t_handler, compact_bs, physical_file_size);
    atomic_fetch_add(&append_compact_metric_v2_source_verify_us, replica_metric_monotonic_us() - source_verify_started_us);

    if (source_state == WRITE_COMPACT_V2_STATE_ERROR) {
        atomic_fetch_add(&append_compact_metric_v2_state_failures, 1);
        free_write_compact_v2_context_profiled(&context);
        return write_compact_v2_fail_profiled(r_info, t_handler, "invalid v2 source verification target");
    }

    if (source_state == WRITE_COMPACT_V2_STATE_MISMATCH) {
        atomic_fetch_add(&append_compact_metric_v2_source_mismatches, 1);
        uint64_t final_probe_started_us = replica_metric_monotonic_us();
        bool final_state_matches = write_compact_v2_final_state_matches(r_info, &header, write_buffer, &context, t_handler, compact_bs, compact_fd, physical_file_size);
        atomic_fetch_add(&append_compact_metric_v2_final_probe_us, replica_metric_monotonic_us() - final_probe_started_us);

        if (!final_state_matches) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr,
                    "%s ERROR [COMPACT_PROBE=%015lu] stage=replica_state_mismatch uuid=%s tx=%lu segment=%u "
                    "fd=%d bs=%u flags=%u physical_file=%lu old_file=%lu new_file=%lu final_file=%lu "
                    "source_check=%s final_check=%s compact_range=[%lu,%lu) write_range=[%lu,%lu) "
                    "original_b_offset=%u new_b_offset=%u\n",
                    curr_dt(dt), t_handler->cid, r_info->uuid, header.transaction_id, header.segment, compact_fd, compact_bs, header.flags, physical_file_size,
                    header.compact_old_file_size, header.compact_new_file_size, header.expected_final_file_size,
                    physical_file_size != header.compact_old_file_size ? "file_size_mismatch" : "plan_verification_failed",
                    physical_file_size != header.expected_final_file_size ? "file_size_mismatch" : "probe_failed", (uint64_t)header.first_block * FIX_BLOCK_SIZE,
                    ((uint64_t)header.first_block + header.source_blocks) * FIX_BLOCK_SIZE, header.write_offset, header.write_offset + header.write_size, header.original_b_offset,
                    header.new_b_offset);
            atomic_fetch_add(&append_compact_metric_v2_state_failures, 1);
            free_write_compact_v2_context_profiled(&context);
            return write_compact_v2_fail_profiled(r_info, t_handler, "v2 state matches neither source nor final state");
        }
        atomic_fetch_add(&append_compact_metric_v2_replay_matches, 1);
        bool write_copy_heap_allocated = false;
        phase_started_us = replica_metric_monotonic_us();
        uint8_t *write_copy = prepare_write_compact_v2_write_copy(&context, write_buffer, header.write_size, &write_copy_heap_allocated);
        atomic_fetch_add(&append_compact_metric_v2_write_copy_us, replica_metric_monotonic_us() - phase_started_us);

        if (write_copy == NULL) {
            free_write_compact_v2_context_profiled(&context);
            return write_compact_v2_fail_profiled(r_info, t_handler, "cannot allocate repeated v2 ordinary write buffer");
        }

        if (write_copy_heap_allocated)
            atomic_fetch_add(&append_compact_metric_v2_heap_write_copies, 1);
        uint64_t ordinary_write_started_us = replica_metric_monotonic_us();
        phase_started_us = replica_metric_monotonic_us();
        write_file(compact_fd, header.write_offset, write_copy, header.write_size, r_info->r_idx_b[compact_bs]->bsize_factor, r_info, t_handler->cid);
        atomic_fetch_add(&append_compact_metric_v2_ordinary_write_call_us, replica_metric_monotonic_us() - phase_started_us);
        atomic_fetch_add(&append_compact_metric_v2_ordinary_write_calls, 1);
        atomic_fetch_add(&append_compact_metric_v2_ordinary_write_bytes, header.write_size);
        phase_started_us = replica_metric_monotonic_us();
        flush_write_4K_buffer(r_info, t_handler->cid);
        atomic_fetch_add(&append_compact_metric_v2_postflush_us, replica_metric_monotonic_us() - phase_started_us);
        atomic_fetch_add(&append_compact_metric_v2_ordinary_write_us, replica_metric_monotonic_us() - ordinary_write_started_us);
        free_write_compact_v2_write_copy(write_copy, write_copy_heap_allocated);
        fstat_started_us = replica_metric_monotonic_us();

        if (fstat(compact_fd, &file_stat) != 0 || file_stat.st_size < 0) {
            uint64_t replay_fstat_elapsed_us = replica_metric_monotonic_us() - fstat_started_us;
            atomic_fetch_add(&append_compact_metric_v2_fstat_us, replay_fstat_elapsed_us);
            atomic_fetch_add(&append_compact_metric_v2_replay_fstat_us, replay_fstat_elapsed_us);
            atomic_fetch_add(&append_compact_metric_v2_state_failures, 1);
            free_write_compact_v2_context_profiled(&context);
            return write_compact_v2_fail_profiled(r_info, t_handler, "cannot read repeated v2 physical file size");
        }
        uint64_t replay_fstat_elapsed_us = replica_metric_monotonic_us() - fstat_started_us;
        atomic_fetch_add(&append_compact_metric_v2_fstat_us, replay_fstat_elapsed_us);
        atomic_fetch_add(&append_compact_metric_v2_replay_fstat_us, replay_fstat_elapsed_us);
        free_write_compact_v2_context_profiled(&context);

        if ((uint64_t)file_stat.st_size != header.expected_final_file_size) {
            atomic_fetch_add(&append_compact_metric_v2_state_failures, 1);
            return write_compact_v2_fail_profiled(r_info, t_handler, "repeated v2 final file size mismatch");
        }
        return true;
    }
    atomic_fetch_add(&append_compact_metric_v2_source_matches, 1);
    log_append_compaction_result_probe(r_info, &context, &plan, "replica_built", t_handler->cid);
    uint64_t expected_hash_started_us = replica_metric_monotonic_us();
    bool expected_hash_matches = write_compact_v2_expected_final_hash_matches(&header, context.result_buffer, write_buffer, r_info->r_idx_b[plan.bs]->bsize_factor);
    atomic_fetch_add(&append_compact_metric_v2_expected_hash_us, replica_metric_monotonic_us() - expected_hash_started_us);

    if (!expected_hash_matches) {
        free_write_compact_v2_context_profiled(&context);
        return write_compact_v2_fail_profiled(r_info, t_handler, "v2 expected final hash differs before apply");
    }
    bool write_copy_heap_allocated = false;
    phase_started_us = replica_metric_monotonic_us();
    uint8_t *write_copy = prepare_write_compact_v2_write_copy(&context, write_buffer, header.write_size, &write_copy_heap_allocated);
    atomic_fetch_add(&append_compact_metric_v2_write_copy_us, replica_metric_monotonic_us() - phase_started_us);

    if (write_copy == NULL) {
        free_write_compact_v2_context_profiled(&context);
        return write_compact_v2_fail_profiled(r_info, t_handler, "cannot allocate v2 ordinary write buffer");
    }

    if (write_copy_heap_allocated)
        atomic_fetch_add(&append_compact_metric_v2_heap_write_copies, 1);
    log_append_compaction_result_probe(r_info, &context, &plan, "replica_prewrite", t_handler->cid);
    uint64_t compact_write_started_us = replica_metric_monotonic_us();
    write_file(compact_fd, plan.result_write_offset, context.result_buffer, plan.result_write_size, r_info->r_idx_b[plan.bs]->bsize_factor, r_info, t_handler->cid);
    atomic_fetch_add(&append_compact_metric_v2_compact_write_us, replica_metric_monotonic_us() - compact_write_started_us);
    atomic_fetch_add(&append_compact_metric_v2_compact_write_calls, 1);
    atomic_fetch_add(&append_compact_metric_v2_compact_write_bytes, plan.result_write_size);
    log_append_compaction_disk_probe(r_info, &plan, compact_fd, "replica_postwrite", t_handler->cid);

    if ((header.flags & MSG_WRITE_COMPACT_FLAG_TRUNCATE) != 0) {
        uint64_t truncate_started_us = replica_metric_monotonic_us();
        int truncate_result = truncate_file(r_info, compact_fd, header.compact_new_file_size, t_handler->cid);
        atomic_fetch_add(&append_compact_metric_v2_truncate_us, replica_metric_monotonic_us() - truncate_started_us);

        if (truncate_result != 0) {
            free_write_compact_v2_write_copy(write_copy, write_copy_heap_allocated);
            free_write_compact_v2_context_profiled(&context);
            return write_compact_v2_fail_profiled(r_info, t_handler, "v2 truncate failed");
        }
    }
    uint64_t ordinary_write_started_us = replica_metric_monotonic_us();
    phase_started_us = replica_metric_monotonic_us();
    write_file(compact_fd, header.write_offset, write_copy, header.write_size, r_info->r_idx_b[plan.bs]->bsize_factor, r_info, t_handler->cid);
    atomic_fetch_add(&append_compact_metric_v2_ordinary_write_call_us, replica_metric_monotonic_us() - phase_started_us);
    atomic_fetch_add(&append_compact_metric_v2_ordinary_write_calls, 1);
    atomic_fetch_add(&append_compact_metric_v2_ordinary_write_bytes, header.write_size);
    phase_started_us = replica_metric_monotonic_us();
    flush_write_4K_buffer(r_info, t_handler->cid);
    atomic_fetch_add(&append_compact_metric_v2_postflush_us, replica_metric_monotonic_us() - phase_started_us);
    atomic_fetch_add(&append_compact_metric_v2_ordinary_write_us, replica_metric_monotonic_us() - ordinary_write_started_us);
    free_write_compact_v2_write_copy(write_copy, write_copy_heap_allocated);
    fstat_started_us = replica_metric_monotonic_us();

    if (fstat(compact_fd, &file_stat) != 0 || file_stat.st_size < 0) {
        uint64_t final_fstat_elapsed_us = replica_metric_monotonic_us() - fstat_started_us;
        atomic_fetch_add(&append_compact_metric_v2_fstat_us, final_fstat_elapsed_us);
        atomic_fetch_add(&append_compact_metric_v2_final_fstat_us, final_fstat_elapsed_us);
        atomic_fetch_add(&append_compact_metric_v2_state_failures, 1);
        free_write_compact_v2_context_profiled(&context);
        return write_compact_v2_fail_profiled(r_info, t_handler, "cannot read applied v2 physical file size");
    }
    uint64_t final_fstat_elapsed_us = replica_metric_monotonic_us() - fstat_started_us;
    atomic_fetch_add(&append_compact_metric_v2_fstat_us, final_fstat_elapsed_us);
    atomic_fetch_add(&append_compact_metric_v2_final_fstat_us, final_fstat_elapsed_us);
    free_write_compact_v2_context_profiled(&context);

    if ((uint64_t)file_stat.st_size != header.expected_final_file_size) {
        atomic_fetch_add(&append_compact_metric_v2_state_failures, 1);
        return write_compact_v2_fail_profiled(r_info, t_handler, "v2 final file size mismatch");
    }
    return true;
}

static bool apply_write_compact_v2_transaction(region_info_t *r_info, const uint8_t *payload, uint32_t payload_size, table_handler_t *t_handler) {
    uint64_t total_started_us = replica_metric_monotonic_us();
    atomic_fetch_add(&append_compact_metric_v2_commands, 1);
    bool result = apply_write_compact_v2_transaction_internal(r_info, payload, payload_size, t_handler);
    atomic_fetch_add(&append_compact_metric_v2_total_us, replica_metric_monotonic_us() - total_started_us);
    return result;
}

bool apply_write_compact_transaction(region_info_t *r_info, const uint8_t *payload, uint32_t payload_size, table_handler_t *t_handler) {
    if (payload == NULL || payload_size == 0)
        return write_compact_fail(r_info, t_handler, "empty compact transaction");

    if (payload[0] == MSG_WRITE_COMPACT_VERSION_BITMAP)
        return apply_write_compact_v2_transaction(r_info, payload, payload_size, t_handler);

    if (payload[0] != MSG_WRITE_COMPACT_VERSION)
        return write_compact_fail(r_info, t_handler, "unsupported compact transaction version");
    msg_write_compact_t header;
    const uint8_t *compact_buffer = NULL;
    const uint8_t *write_buffer = NULL;
    int validation_result = validate_msg_write_compact(payload, payload_size, &header, &compact_buffer, &write_buffer);

    if (r_info == NULL || t_handler == NULL || validation_result != MSG_WRITE_COMPACT_OK)
        return write_compact_fail(r_info, t_handler, validation_result == MSG_WRITE_COMPACT_OK ? "invalid target" : msg_write_compact_result_string(validation_result));

    if (r_info->status != STATUS_ASSIGN_REGION || r_info->is_freed || r_info->r_idx_b == NULL)
        return write_compact_fail(r_info, t_handler, "replica region is not assigned");
    int8_t compact_bs_pos = get_bs_map_pos(r_info, (uint8_t)(header.compact_bs - 1U), true, t_handler);
    int8_t write_bs_pos = get_bs_map_pos(r_info, (uint8_t)(header.write_bs - 1U), true, t_handler);

    if (compact_bs_pos < 0 || write_bs_pos < 0)
        return write_compact_fail(r_info, t_handler, "cannot map b/bs target");
    uint8_t compact_bs = (uint8_t)compact_bs_pos;
    uint8_t write_bs = (uint8_t)write_bs_pos;

    if (compact_bs >= r_info->max_idx_b || write_bs >= r_info->max_idx_b || compact_bs != write_bs || r_info->r_idx_b[compact_bs] == NULL || r_info->r_idx_b[write_bs] == NULL ||
        header.compact_b != FBP || header.write_b != FBP)
        return write_compact_fail(r_info, t_handler, "cannot map b/bs target");
    int compact_fd = r_info->r_idx_b[compact_bs]->fd[header.compact_b];
    int write_fd = r_info->r_idx_b[write_bs]->fd[header.write_b];

    if (compact_fd < 0 || write_fd < 0 || compact_fd != write_fd || compact_fd != r_info->data_000_fd)
        return write_compact_fail(r_info, t_handler, "target file is not open");
    bool recovery_marker_present = atomic_load(&r_info->r_need_data_chk) != 0;
    flush_write_4K_buffer(r_info, t_handler->cid);
    struct stat file_stat;

    if (fstat(compact_fd, &file_stat) != 0 || file_stat.st_size < 0)
        return write_compact_fail(r_info, t_handler, "cannot read physical file size");
    uint64_t physical_size = (uint64_t)file_stat.st_size;

    if (physical_size != header.compact_old_file_size && physical_size != header.expected_final_file_size) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu], Wrong physical_size=%lu, compact_old_file_size=%lu, expected_final_file_size=%lu for %d\n", curr_dt(dt), t_handler->cid,
                physical_size, header.compact_old_file_size, header.expected_final_file_size, compact_fd);
        return write_compact_fail(r_info, t_handler, "unexpected physical file size");
    }
    uint8_t *write_copy = malloc((size_t)header.write_size + MAX_BSIZE_FACTOR);

    if (write_copy == NULL)
        return write_compact_fail(r_info, t_handler, "cannot allocate ordinary write buffer");
    memcpy(write_copy, write_buffer, header.write_size);
    write_file(compact_fd, header.compact_offset, (uint8_t *)compact_buffer, header.compact_size, r_info->r_idx_b[compact_bs]->bsize_factor, r_info, t_handler->cid);

    if ((header.flags & MSG_WRITE_COMPACT_FLAG_TRUNCATE) != 0 && truncate_file(r_info, compact_fd, header.compact_new_file_size, t_handler->cid) != 0) {
        free(write_copy);
        return write_compact_fail(r_info, t_handler, "truncate failed");
    }
    write_file(write_fd, header.write_offset, write_copy, header.write_size, r_info->r_idx_b[write_bs]->bsize_factor, r_info, t_handler->cid);
    flush_write_4K_buffer(r_info, t_handler->cid);
    free(write_copy);

    if (fstat(write_fd, &file_stat) != 0 || file_stat.st_size < 0 || (uint64_t)file_stat.st_size != header.expected_final_file_size)
        return write_compact_fail(r_info, t_handler, "final physical file size mismatch");

    if (recovery_marker_present) {
        drop_region_recovery_info(r_info, t_handler->cid);

        if (atomic_load(&r_info->r_need_data_chk) != 0)
            return write_compact_fail(r_info, t_handler, "cannot remove pre-existing recovery marker");
    }
    return true;
}


void inline send_to_replicas(struct region_info_t *r_info, char *new_buff, uint8_t b, uint64_t rw_off, uint32_t new_rsize, bool do_truncate, uint8_t bs, uint8_t replica_msg_type,
                             table_handler_t *t_handler) {
    uint32_t rsc_msg_size = MSG_RSC_BUFF_OFFSET + new_rsize;
    memcpy(r_info->rsc_msg, &rsc_msg_size, sizeof(rsc_msg_size));

    if (replica_msg_type == MSG_DIR_WRITE_AND_COMPACT)
        r_info->rsc_msg[MSG_TYPE_OFFSET] = MSG_DIR_WRITE_AND_COMPACT;
    else if (do_truncate)
        r_info->rsc_msg[MSG_TYPE_OFFSET] = MSG_DIR_WRITE_TUNC;
    else
        r_info->rsc_msg[MSG_TYPE_OFFSET] = MSG_DIR_WRITE;
    uint32_t int_converter = r_info->replica->replica1_region_pos;
    memcpy(r_info->rsc_msg + offsetof(struct msg_rsc_t, region_pos), &int_converter, sizeof(int_converter));
    r_info->req_count_chk++;

    if (r_info->req_count_chk > MAX_REPLICA_COUNTER_MASK)
        r_info->req_count_chk = 1;
    memcpy(r_info->rsc_msg + offsetof(struct msg_rsc_t, req_count_chk), &r_info->req_count_chk, sizeof(r_info->req_count_chk));
    memcpy(r_info->rsc_msg + offsetof(struct msg_rsc_t, uuid), r_info->uuid, UUID_SIZE);
    r_info->rsc_msg[offsetof(struct msg_rsc_t, uuid) + UUID_SIZE - 1] = '1';
    memcpy(r_info->rsc_msg + MSG_RSC_OFF_OFFSET, &rw_off, sizeof(rw_off));
    r_info->rsc_msg[MSG_RSC_BS_OFFSET] = (bs != BS_LOB) ? r_info->r_idx_b[bs]->pos : 0;

    if (new_rsize != 0)
        memmove(r_info->rsc_msg + MSG_RSC_BUFF_OFFSET, new_buff, new_rsize);

    if (r_info->replica->r_info1 != NULL) {
        uint8_t bs2;
        bs2 = get_bs_map_pos(r_info->replica->r_info1, r_info->r_idx_b[bs]->pos - 1, true, t_handler);

        if (log_level > 2) {
            region_info_t *target = r_info->replica->r_info1;
            pid_t tid = syscall(SYS_gettid);
            char dt[DT_LOG_SIZE];
            fprintf(stderr,
                    "%s TRACE [REPLICA_HANDLER=%015lu] Local replica write source_uuid=%s, target_uuid=%s, source_r_info=%p, target_r_info=%p, bs=%u, bs2=%u, b=%u, target_fd=%d, "
                    "target_r_lock_id=%u, target_is_freed=%u, target_stop=%u, target_status=%u, write_4K_fd=%d, write_4K_buff_used=%u, write_4K_file_offset=%lu, TID=%u\n",
                    curr_dt(dt), t_handler->cid, r_info->uuid, target->uuid, (void *)r_info, (void *)target, bs, bs2, b, target->r_idx_b[bs2]->fd[b], target->r_lock_id,
                    target->is_freed, target->stop, target->status, target->write_4K_fd, target->write_4K_buff_used, target->write_4K_file_offset, (unsigned int)tid);
        }

        if (replica_msg_type == MSG_DIR_WRITE_AND_COMPACT) {
            if (!apply_write_compact_transaction(r_info->replica->r_info1, (uint8_t *)new_buff, new_rsize, t_handler))
                finish_thread(r_info, 137, true, t_handler);
        } else if (do_truncate)
            truncate_file(r_info->replica->r_info1, r_info->replica->r_info1->r_idx_b[bs2]->fd[b], rw_off, t_handler->cid);
        else {
            write_file(r_info->replica->r_info1->r_idx_b[bs2]->fd[b], rw_off, new_buff, new_rsize, r_info->replica->r_info1->r_idx_b[bs2]->bsize_factor, r_info->replica->r_info1,
                       t_handler->cid);
        }
    } else if (r_info->replica->replica1_port > 0 && (r_info->replica->replica_control & 1) == 1) {
        if (r_info->bulk_mode) {
            if (ensure_bulk_buffer_capacity(&r_info->bulk_buff1, &r_info->bulk_buff_size1, r_info->bulk_buff_offset1, rsc_msg_size, t_handler->cid, "bulk_buff1") != 0) {
                finish_thread(r_info, 132, true, t_handler);
                return;
            }
            memcpy(r_info->bulk_buff1 + r_info->bulk_buff_offset1, r_info->rsc_msg, rsc_msg_size);
            r_info->bulk_buff_offset1 += rsc_msg_size;
            r_info->replica->replica_control &= 2;
        } else {
            ssize_t res_r1 = write_full(r_info->replica->replica1_sock_fd, r_info->rsc_msg, rsc_msg_size);

            if (res_r1 < 0) {
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s WARN [REPLICA_HANDLER=%015lu] Write failed to replica1 fd=%d (size=%u, errno=%d)\n", curr_dt(dt), t_handler->cid,
                        r_info->replica->replica1_sock_fd, rsc_msg_size, errno);
                usleep(100000);
            }
        }
    }

    if (r_info->is_freed)
        return;
    int_converter = r_info->replica->replica2_region_pos;
    memcpy(r_info->rsc_msg + offsetof(struct msg_rsc_t, region_pos), &int_converter, sizeof(int_converter));
    r_info->rsc_msg[offsetof(struct msg_rsc_t, uuid) + UUID_SIZE - 1] = '2';

    if (r_info->replica->r_info2 != NULL) {
        uint8_t bs2;
        bs2 = get_bs_map_pos(r_info->replica->r_info2, r_info->r_idx_b[bs]->pos - 1, true, t_handler);

        if (log_level > 2) {
            region_info_t *target = r_info->replica->r_info2;
            pid_t tid = syscall(SYS_gettid);
            char dt[DT_LOG_SIZE];
            fprintf(stderr,
                    "%s TRACE [REPLICA_HANDLER=%015lu] Local replica write source_uuid=%s, target_uuid=%s, source_r_info=%p, target_r_info=%p, bs=%u, bs2=%u, b=%u, target_fd=%d, "
                    "target_r_lock_id=%u, target_is_freed=%u, target_stop=%u, target_status=%u, write_4K_fd=%d, write_4K_buff_used=%u, write_4K_file_offset=%lu, TID=%u\n",
                    curr_dt(dt), t_handler->cid, r_info->uuid, target->uuid, (void *)r_info, (void *)target, bs, bs2, b, target->r_idx_b[bs2]->fd[b], target->r_lock_id,
                    target->is_freed, target->stop, target->status, target->write_4K_fd, target->write_4K_buff_used, target->write_4K_file_offset, (unsigned int)tid);
        }

        if (replica_msg_type == MSG_DIR_WRITE_AND_COMPACT) {
            if (!apply_write_compact_transaction(r_info->replica->r_info2, (uint8_t *)new_buff, new_rsize, t_handler))
                finish_thread(r_info, 138, true, t_handler);
        } else if (do_truncate)
            truncate_file(r_info->replica->r_info2, r_info->replica->r_info2->r_idx_b[bs2]->fd[b], rw_off, t_handler->cid);
        else {
            write_file(r_info->replica->r_info2->r_idx_b[bs2]->fd[b], rw_off, new_buff, new_rsize, r_info->replica->r_info2->r_idx_b[bs2]->bsize_factor, r_info->replica->r_info2,
                       t_handler->cid);
        }
    } else if (r_info->replica->replica2_port > 0 && (r_info->replica->replica_control & 2) == 2) {
        if (r_info->bulk_mode) {
            // TODO Shrink mem usage and calc r2 from r1
            if (ensure_bulk_buffer_capacity(&r_info->bulk_buff2, &r_info->bulk_buff_size2, r_info->bulk_buff_offset2, rsc_msg_size, t_handler->cid, "bulk_buff2") != 0) {
                finish_thread(r_info, 133, true, t_handler);
                return;
            }
            memcpy(r_info->bulk_buff2 + r_info->bulk_buff_offset2, r_info->rsc_msg, rsc_msg_size);
            r_info->bulk_buff_offset2 += rsc_msg_size;
            r_info->replica->replica_control &= 1;
        } else {
            ssize_t res_r2 = write_full(r_info->replica->replica2_sock_fd, r_info->rsc_msg, rsc_msg_size);

            if (res_r2 < 0) {
                char dt[DT_LOG_SIZE];
                fprintf(stderr, "%s WARN [REPLICA_HANDLER=%015lu] Write failed to replica2 fd=%d (size=%u, errno=%d)\n", curr_dt(dt), t_handler->cid,
                        r_info->replica->replica2_sock_fd, rsc_msg_size, errno);
                usleep(100000);
            }
        }
    }
}

static inline uint64_t process_replica(region_info_t *r_info, int replica_index, bool do_retry, table_handler_t *t_handler) {
    int sock_fd = -1, port = 0;
    bool read_timed_out = false;

    if (r_info == NULL || r_info->replica == NULL)
        return 0;

    if (replica_index == 1) {
        if (r_info->replica->r_info1 != NULL) {
            r_info->replica->replica_control &= 2;
            return 0;
        }
        port = r_info->replica->replica1_port;
        sock_fd = atomic_load(&r_info->replica->replica1_sock_fd);

        if (port <= 0 || ((r_info->replica->replica_control & 1) != 1)) {
            return 0;
        }
    } else if (replica_index == 2) {
        if (r_info->replica->r_info2 != NULL) {
            r_info->replica->replica_control &= 1;
            return 0;
        }
        port = r_info->replica->replica2_port;
        sock_fd = atomic_load(&r_info->replica->replica2_sock_fd);

        if (port <= 0 || ((r_info->replica->replica_control & 2) != 2)) {
            return 0;
        }
    } else {
        return 0;
    }

    if (sock_fd <= 0) {
        goto process_error;
    }

    if (!apply_replica_reply_timeout(r_info, (uint8_t)replica_index, sock_fd, t_handler->cid))
        goto process_error;
    uint64_t resp_data = 0;
    {
        uint8_t *p = (uint8_t *)&resp_data;
        int need = (int)sizeof(resp_data);
        int got = 0;

        while (got < need) {
            ssize_t n = read(sock_fd, p + got, (size_t)(need - got));

            if (n <= 0) {
                read_timed_out = n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK);
                goto process_error;
            }
            got += (int)n;
        }
    }
    int32_t rsize = (int32_t)sizeof(resp_data);

    if ((uint32_t)(resp_data & 0xFFFFFFFFu) == (uint32_t)MSG_SIZE_RESERVE_RS_ANSWER && (uint8_t)((resp_data >> 32) & 0xFFu) == (uint8_t)MSG_RESP_OK) {
        return (uint32_t)((resp_data >> 40) & MAX_REPLICA_COUNTER_MASK);
    }
    uint32_t total_size = (uint32_t)(resp_data & 0xFFFFFFFFu);

    if (total_size > (uint32_t)MSG_SIZE_RESERVE_RS_ANSWER) {
        if (total_size >= (uint32_t)BLOCK_MAX_SIZE) {
            if (((resp_data << 32) >> 44) != MSG_DATA_CHK_RESULT) {
                goto process_error;
            }
        }
        uint8_t *buffer = (uint8_t *)malloc(total_size);

        if (!buffer) {
            goto process_error;
        }
        memcpy(buffer, &resp_data, (size_t)MSG_SIZE_RESERVE_RS_ANSWER);
        int bytes_read = (int)MSG_SIZE_RESERVE_RS_ANSWER;
        int remaining = (int)total_size - (int)MSG_SIZE_RESERVE_RS_ANSWER;

        while (remaining > 0) {
            ssize_t n = read(sock_fd, buffer + bytes_read, (size_t)remaining);

            if (n <= 0) {
                read_timed_out = n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK);
                free(buffer);
                goto process_error;
            }
            bytes_read += (int)n;
            remaining -= (int)n;
        }
        rsize = bytes_read;

        if (r_info->bulk_mode && buffer[MSG_TYPE_OFFSET] == MSG_RES_BULK_GET_PUT) {
            if (total_size < 5 || ((total_size - 5) % 8) != 0) {
                char dt2[DT_LOG_SIZE];
                fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Bulk-ACK bad total_size=%u\n", curr_dt(dt2), t_handler->cid, total_size);
                free(buffer);
                goto process_error;
            }
            uint32_t n_acks = (total_size - 5) / 8;
            uint32_t last_req_cnt = 0;
            bool have_valid = false;

            for (uint32_t k = 0; k < n_acks; ++k) {
                size_t off = 5 + (size_t)k * 8;
                uint32_t ack_size;
                memcpy(&ack_size, buffer + off, sizeof(uint32_t));
                uint8_t ack_type = buffer[off + 4];

                if (ack_size == (uint32_t)MSG_SIZE_RESERVE_RS_ANSWER && ack_type == (uint8_t)MSG_RESP_OK) {
                    uint32_t cnt = 0;
                    memcpy(&cnt, buffer + off + MSG_REPLICA_COUNTER_OFFSET, MSG_REPLICA_COUNTER_SIZE);
                    last_req_cnt = (cnt & MAX_REPLICA_COUNTER_MASK);
                    have_valid = true;
                } else {
                    char dt2[DT_LOG_SIZE];
                    fprintf(stderr, "%s WARN  [REPLICA_HANDLER=%015lu] Bulk-ACK submsg invalid: size=%u type=%u (k=%u/%u)\n", curr_dt(dt2), t_handler->cid, ack_size, ack_type,
                            k + 1, n_acks);
                    free(buffer);
                    goto process_error;
                }
            }

            if (!have_valid) {
                free(buffer);
                goto process_error;
            }
            free(buffer);
            return last_req_cnt;
        }

        if (data_check_enabled && total_size > (uint32_t)MSG_SIZE_NEW_REGION_ORDER) {
            if (buffer[MSG_TYPE_OFFSET] == (uint8_t)MSG_DATA_CHK_RESULT) {
                free(buffer);
                return REPLICA_REPLY_CHECK_REQUIRED;
            } else {
                char dt2[DT_LOG_SIZE];
                fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Recv %d bytes incorrect msg type %u for msg size %u from replica%d connection=%d\n", curr_dt(dt2),
                        t_handler->cid, rsize, buffer[MSG_TYPE_OFFSET], total_size, replica_index, sock_fd);
            }
        }
        free(buffer);
    }
    {
        char dt2[DT_LOG_SIZE];
        fprintf(stderr, "%s WARN [REPLICA_HANDLER=%015lu] Read %d bytes from %s replica%d connection=%d, replica_control=%d\n", curr_dt(dt2), t_handler->cid, rsize,
                r_info->r_fname, replica_index, sock_fd, r_info->replica->replica_control);
    }
process_error:
    if (do_retry) {
        if (read_timed_out)
            grow_replica_reply_timeout(r_info, (uint8_t)replica_index, sock_fd, t_handler->cid);

        if (sock_fd > 0)
            close_socket(r_info, (uint8_t)replica_index, sock_fd);
    }
    return REPLICA_REPLY_ERROR;
}


static void finish_replica_reply(region_info_t *r_info, uint8_t replica_index, uint64_t reply, bool do_retry, bool use_ssl, table_handler_t *t_handler) {
    if (reply == REPLICA_REPLY_CHECK_REQUIRED) {
        if (replica_data_check(r_info, replica_index, NULL, 0, t_handler))
            return;
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s WARN [REPLICA_HANDLER=%015lu] Replica%d %s data check is not complete, reconnect and repeat, req_count_chk=%u\n", curr_dt(dt), t_handler->cid,
                replica_index, r_info->uuid, r_info->req_count_chk);

        if (do_retry) {
            {
                int fd = replica_index == 1 ? atomic_load(&r_info->replica->replica1_sock_fd) : atomic_load(&r_info->replica->replica2_sock_fd);

                if (fd > 0)
                    close_socket(r_info, replica_index, fd);
            }
        }
    }

    if (do_retry) {
        if (atomic_load(&is_shutting_down)) {
            do_r_unlock(r_info, 142, true, t_handler);

            while (atomic_load(&is_shutting_down)) {
                sleep(10);
            }
        }
        replica_refresh_region_map(r_info, replica_index, false, t_handler);
    }

    if (r_info->replica->replica_control > 0) {
        while (r_info->stop == true) {
            if (atomic_load(&is_shutting_down)) {
                do_r_unlock(r_info, 140, true, t_handler);

                while (atomic_load(&is_shutting_down)) {
                    sleep(10);
                }
            }
            char dt2[DT_LOG_SIZE];
            fprintf(stderr, "%s WARN [REPLICA_HANDLER=%015lu] Region %s stopped and cannot finish the job\n", curr_dt(dt2), t_handler->cid, r_info->r_fname);

            if ((replica_index == 1 && r_info->replica->r_info1 == NULL) || (replica_index == 2 && r_info->replica->r_info2 == NULL)) {
                waiting_connection(r_info, replica_index, t_handler);
            }
            r_info->replica_stoped = true;
            sleep(1);
            r_info->replica_stoped = false;
        }
    }
}


static void recv_from_replicas_internal(region_info_t *r_info, bool do_retry, uint32_t req_count_chk1, uint32_t req_count_chk2, bool use_ssl, table_handler_t *t_handler) {
    const uint32_t expected[2] = {req_count_chk1, req_count_chk2};
    uint64_t replies[2];

    for (uint8_t i = 0; i < 2; ++i) {
        do {
            (void)use_ssl;
            replies[i] = process_replica(r_info, i + 1, do_retry, t_handler);
        } while (replies[i] != 0 && replies[i] <= MAX_REPLICA_COUNTER_MASK && replies[i] != expected[i]);

        if (replies[i] == expected[i]) {
            if (replies[i] != 0)
                reset_replica_reply_timeout(r_info, i + 1, t_handler->cid);
            r_info->replica->replica_control &= i == 0 ? 2 : 1;
        }
    }
    bool can_check = do_retry || (replies[0] != REPLICA_REPLY_ERROR && replies[1] != REPLICA_REPLY_ERROR);

    for (uint8_t i = 0; i < 2; ++i) {
        if (replies[i] == REPLICA_REPLY_ERROR || (replies[i] == REPLICA_REPLY_CHECK_REQUIRED && can_check))
            finish_replica_reply(r_info, i + 1, replies[i], do_retry, use_ssl, t_handler);
    }
}


void recv_from_replicas(region_info_t *r_info, bool do_retry, uint32_t req_count_chk1, uint32_t req_count_chk2, table_handler_t *t_handler) {
    recv_from_replicas_internal(r_info, do_retry, req_count_chk1, req_count_chk2, false, t_handler);
}

static bool get_bulk_last_req_count(const char *bulk_buff, uint32_t bulk_size, uint32_t *req_count_chk) {
    if (bulk_buff == NULL || req_count_chk == NULL || bulk_size < MSG_TYPE_OFFSET + 1)
        return false;
    uint8_t msg_type = (uint8_t)bulk_buff[MSG_TYPE_OFFSET];
    uint32_t bulk_header_size;

    if (msg_type == MSG_BULK_PUT)
        bulk_header_size = 9;
    else if (msg_type == MSG_BULK_PUT_NIO)
        bulk_header_size = 13;
    else
        return false;
    uint32_t declared_size;
    memcpy(&declared_size, bulk_buff, sizeof(declared_size));

    if (declared_size != bulk_size || bulk_size <= bulk_header_size)
        return false;
    uint32_t offset = bulk_header_size;
    uint32_t last_req_count = 0;

    while (offset < bulk_size) {
        if (bulk_size - offset < MSG_RSC_BUFF_OFFSET)
            return false;
        uint32_t msg_size;
        memcpy(&msg_size, bulk_buff + offset, sizeof(msg_size));

        if (msg_size < MSG_RSC_BUFF_OFFSET || msg_size > bulk_size - offset)
            return false;
        memcpy(&last_req_count, bulk_buff + offset + offsetof(struct msg_rsc_t, req_count_chk), sizeof(last_req_count));
        last_req_count &= MAX_REPLICA_COUNTER_MASK;
        offset += msg_size;
    }

    if (offset != bulk_size || last_req_count == 0)
        return false;
    *req_count_chk = last_req_count;
    return true;
}

static uint32_t get_replica_bulk_header_size(const char *bulk_buff, uint32_t bulk_size) {
    if (bulk_buff == NULL || bulk_size < MSG_TYPE_OFFSET + 1)
        return 0;

    if ((uint8_t)bulk_buff[MSG_TYPE_OFFSET] == MSG_BULK_PUT)
        return 9;

    if ((uint8_t)bulk_buff[MSG_TYPE_OFFSET] == MSG_BULK_PUT_NIO)
        return 13;
    return 0;
}


void bulk_replicas_writer(region_info_t *r_info, table_handler_t *t_handler) {
    invalidate_replicas_idle_commit(r_info);
    uint32_t req_count_chk1 = 0;
    uint32_t req_count_chk2 = 0;
    uint32_t bulk_header_size1 = get_replica_bulk_header_size(r_info->bulk_buff1, r_info->bulk_buff_offset1);
    uint32_t bulk_header_size2 = get_replica_bulk_header_size(r_info->bulk_buff2, r_info->bulk_buff_offset2);
    bool replica1_has_bulk = r_info->replica->replica1_port > 0 && r_info->replica->r_info1 == NULL && r_info->bulk_buff_offset1 > bulk_header_size1;
    bool replica2_has_bulk = r_info->replica->replica2_port > 0 && r_info->replica->r_info2 == NULL && r_info->bulk_buff_offset2 > bulk_header_size2;

    if ((r_info->replica->replica1_port > 0 && r_info->replica->r_info1 == NULL &&
         (bulk_header_size1 == 0 || (replica1_has_bulk && !get_bulk_last_req_count(r_info->bulk_buff1, r_info->bulk_buff_offset1, &req_count_chk1)))) ||
        (r_info->replica->replica2_port > 0 && r_info->replica->r_info2 == NULL &&
         (bulk_header_size2 == 0 || (replica2_has_bulk && !get_bulk_last_req_count(r_info->bulk_buff2, r_info->bulk_buff_offset2, &req_count_chk2))))) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Invalid replica bulk container for %s: size1=%u, size2=%u\n", curr_dt(dt), t_handler->cid, r_info->r_fname,
                r_info->bulk_buff_offset1, r_info->bulk_buff_offset2);
        r_info->stop = true;
        r_info->bad_data = true;
        finish_thread(r_info, 134, true, t_handler);
    }
    r_info->replica->replica_control = rf_ctrl;

    if (r_info->replica->replica1_port > 0 && r_info->replica->r_info1 == NULL && !replica1_has_bulk)
        r_info->replica->replica_control &= 2;

    if (r_info->replica->replica2_port > 0 && r_info->replica->r_info2 == NULL && !replica2_has_bulk)
        r_info->replica->replica_control &= 1;
    r_info->is_replicate = 1;

    do {
        if (r_info->is_freed) {
            finish_thread(r_info, 27, true, t_handler);
        }

        if (atomic_load(&is_shutting_down)) {
            do_r_unlock(r_info, 140, true, t_handler);

            while (atomic_load(&is_shutting_down)) {
                sleep(10);
            }
        }

        if (r_info->replica->replica1_port > 0 || r_info->replica->replica2_port > 0) {
            if (r_info->replica->replica1_port > 0 && (r_info->replica->replica_control & 1) == 1) {
                if (r_info->bulk_buff_offset1 > 0) {
                    memcpy(r_info->bulk_buff1 + offsetof(struct msg_rsc_t, region_pos), &r_info->replica->replica1_region_pos, sizeof(r_info->replica->replica1_region_pos));
                    {
                        if (write_full(r_info->replica->replica1_sock_fd, r_info->bulk_buff1, r_info->bulk_buff_offset1) < 0) {
                            char dt[DT_LOG_SIZE];
                            fprintf(stderr, "%s WARN [REPLICA_HANDLER=%015lu] Bulk write failed to replica1 fd=%d (size=%u, errno=%d)\n", curr_dt(dt), t_handler->cid,
                                    r_info->replica->replica1_sock_fd, r_info->bulk_buff_offset1, errno);
                        }
                    }
                }
            }

            if (r_info->replica->replica2_port > 0 && (r_info->replica->replica_control & 2) == 2) {
                if (r_info->bulk_buff_offset2 > 0) {
                    memcpy(r_info->bulk_buff2 + offsetof(struct msg_rsc_t, region_pos), &r_info->replica->replica2_region_pos, sizeof(r_info->replica->replica2_region_pos));
                    {
                        if (write_full(r_info->replica->replica2_sock_fd, r_info->bulk_buff2, r_info->bulk_buff_offset2) < 0) {
                            char dt[DT_LOG_SIZE];
                            fprintf(stderr, "%s WARN [REPLICA_HANDLER=%015lu] Bulk write failed to replica2 fd=%d (size=%u, errno=%d)\n", curr_dt(dt), t_handler->cid,
                                    r_info->replica->replica2_sock_fd, r_info->bulk_buff_offset2, errno);
                        }
                    }
                }
            }
        } else
            r_info->replica->replica_control = 0;

        if (r_info->is_freed) {
            set_region_recovery_info(r_info, 1000003L, t_handler);
            finish_thread(r_info, 15, true, t_handler);
        }

        if (atomic_load(&is_shutting_down)) {
            do_r_unlock(r_info, 140, true, t_handler);

            while (atomic_load(&is_shutting_down)) {
                sleep(10);
            }
        }

        if (r_info->replica->replica1_port > 0 || r_info->replica->replica2_port > 0) {
            recv_from_replicas(r_info, true, req_count_chk1, req_count_chk2, t_handler);
        }

        if (r_info->is_freed) {
            if (r_info->replica->replica_control != 0)
                set_region_recovery_info(r_info, 1000003L, t_handler);
            finish_thread(r_info, 16, true, t_handler);
        }

        if (r_info->replica->replica_control == 4)
            r_info->replica->replica_control = 3;
    } while (r_info->replica->replica_control != 0);
    r_info->is_replicate = 0;
    complete_append_compaction_piggyback(r_info, t_handler);

    if (!record_version_commit_pending(&r_info->record_versions)) {
        stop_region_after_key_error(r_info, KEY_EXTRACT_INVALID_STATE, t_handler);
        finish_thread(r_info, 134, true, t_handler);
    }
}

static _Atomic uint64_t write_compact_transaction_id = 1;
static bool build_write_compact_payload(region_info_t *r_info, char *write_buffer, uint8_t b, uint64_t write_offset, uint32_t write_size, bool do_truncate, uint8_t bs,
                                        table_handler_t *t_handler, uint8_t **payload, uint32_t *payload_size) {
    append_compaction_piggyback_t *piggyback = t_handler->append_compaction_piggyback;

    if (do_truncate || write_buffer == NULL || write_size == 0 || piggyback == NULL || !piggyback->active || !piggyback->local_applied || piggyback->attached_to_write ||
        piggyback->r_info != r_info || piggyback->plan.b != b || piggyback->plan.bs != bs || r_info->r_idx_b[bs]->fd[b] != r_info->data_000_fd)
        return false;
    uint32_t bitmap_size = piggyback->plan.record_decision_count / 8U + ((piggyback->plan.record_decision_count & 7U) != 0);
    uint64_t meta_size = (uint64_t)piggyback->plan.block_meta_count * sizeof(msg_write_compact_block_meta_t);
    uint64_t total_size = (uint64_t)MSG_WRITE_COMPACT_V2_HEADER_SIZE + meta_size + bitmap_size + write_size;

    if (total_size > RSC_MSG_MAX_PAYLOAD || total_size > UINT32_MAX)
        return false;
    msg_write_compact_v2_t header;
    memset(&header, 0, sizeof(header));
    header.version = MSG_WRITE_COMPACT_VERSION_BITMAP;
    header.flags = piggyback->truncate_applied ? MSG_WRITE_COMPACT_FLAG_TRUNCATE : 0;
    header.compact_b = piggyback->plan.b;
    header.compact_bs = r_info->r_idx_b[piggyback->plan.bs]->pos;
    header.write_b = b;
    header.write_bs = r_info->r_idx_b[bs]->pos;
    header.segment = piggyback->plan.segment;
    header.first_block = piggyback->plan.first_block;
    header.source_blocks = piggyback->plan.source_blocks;
    header.tail_range_first_block = piggyback->plan.tail_range_first_block;
    header.tail_first_block = piggyback->plan.tail_first_block;
    header.tail_blocks = piggyback->plan.tail_blocks;
    header.original_b_offset = piggyback->plan.original_b_offset;
    header.new_b_offset = piggyback->plan.new_b_offset;
    header.source_record_count = piggyback->plan.source_record_count;
    header.record_count = piggyback->plan.record_decision_count;
    header.bitmap_size = bitmap_size;
    header.block_meta_count = piggyback->plan.block_meta_count;
    header.write_size = write_size;
    header.compact_old_file_size = piggyback->plan.original_file_size;
    header.compact_new_file_size = piggyback->plan.new_file_size;
    header.write_offset = write_offset;
    header.transaction_id = atomic_fetch_add(&write_compact_transaction_id, 1);

    if (header.transaction_id == 0)
        header.transaction_id = atomic_fetch_add(&write_compact_transaction_id, 1);
    memcpy(header.result_hash, piggyback->plan.result_hash, sizeof(header.result_hash));
    log_append_compaction_result_probe(r_info, &piggyback->context, &piggyback->plan, "leader_wire", t_handler->cid);
    log_append_compaction_disk_probe(r_info, &piggyback->plan, r_info->r_idx_b[piggyback->plan.bs]->fd[piggyback->plan.b], "leader_pre_ordinary_disk", t_handler->cid);
    uint64_t compact_offset = (uint64_t)header.first_block * FIX_BLOCK_SIZE;
    uint64_t compact_size = (uint64_t)header.source_blocks * FIX_BLOCK_SIZE;
    uint64_t compact_end = compact_offset + compact_size;
    uint32_t write_alignment = (uint32_t)header.write_bs * FIX_BSIZE_FACTOR;
    uint64_t aligned_write_size = ((uint64_t)write_size + write_alignment - 1U) / write_alignment * write_alignment;
    uint64_t write_end = write_offset + aligned_write_size;
    uint64_t size_after_compact =
        piggyback->truncate_applied ? header.compact_new_file_size : (header.compact_old_file_size > compact_end ? header.compact_old_file_size : compact_end);
    header.expected_final_file_size = size_after_compact > write_end ? size_after_compact : write_end;
    uint64_t retained_end = piggyback->truncate_applied && header.compact_new_file_size < compact_end ? header.compact_new_file_size : compact_end;

    if (retained_end < compact_offset || retained_end - compact_offset > UINT32_MAX || aligned_write_size > UINT32_MAX)
        return false;
    uint32_t retained_compact_size = (uint32_t)(retained_end - compact_offset);
    uint8_t compact_hash[MSG_WRITE_COMPACT_HASH_SIZE];
    uint8_t write_hash[MSG_WRITE_COMPACT_HASH_SIZE];

    if (msg_write_compact_v2_source_hash(piggyback->plan.block_meta, piggyback->plan.block_meta_count, header.source_hash) != MSG_WRITE_COMPACT_OK)
        return false;

    if (msg_write_compact_hash128(piggyback->context.result_buffer, retained_compact_size, compact_hash) != MSG_WRITE_COMPACT_OK ||
        msg_write_compact_hash128_padded((const uint8_t *)write_buffer, write_size, (uint32_t)aligned_write_size, write_hash) != MSG_WRITE_COMPACT_OK ||
        msg_write_compact_v2_final_hash(&header, retained_compact_size, compact_hash, (uint32_t)aligned_write_size, write_hash, header.final_hash) != MSG_WRITE_COMPACT_OK)
        return false;

    if (log_level > 2) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr,
                "%s TRACE [COMPACT_PROBE=%015lu] stage=leader_container uuid=%s tx=%lu segment=%u "
                "compact_range=[%lu,%lu) write_range=[%lu,%lu) retained=%u old_file=%lu new_file=%lu final_file=%lu\n",
                curr_dt(dt), t_handler->cid, r_info->uuid, header.transaction_id, header.segment, compact_offset, compact_end, header.write_offset, write_end,
                retained_compact_size, header.compact_old_file_size, header.compact_new_file_size, header.expected_final_file_size);
    }
    uint8_t *result = malloc((size_t)total_size);

    if (result == NULL)
        return false;
    uint32_t serialized_size = 0;
    int serialize_result =
        serialize_msg_write_compact_v2(result, (uint32_t)total_size, &header, piggyback->plan.block_meta, piggyback->plan.live_bitmap, (uint8_t *)write_buffer, &serialized_size);

    if (serialize_result != MSG_WRITE_COMPACT_OK) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s WARN [REPLICA_HANDLER=%015lu] Cannot build MSG_DIR_WRITE_AND_COMPACT for %s: %s; use separate replica messages\n", curr_dt(dt), t_handler->cid,
                r_info->uuid, msg_write_compact_result_string(serialize_result));
        free(result);
        return false;
    }
    *payload = result;
    *payload_size = serialized_size;
    return true;
}


void replicas_writer(region_info_t *r_info, char *rbuff, uint8_t b, uint64_t write_off, uint32_t new_rsize, bool do_truncate, uint8_t bs, bool do_retry,
                     table_handler_t *t_handler) {
#if !ENTERPRISE
    if (bs == BS_LOB) {
        stop_region_after_key_error(r_info, KEY_EXTRACT_INVALID_STATE, t_handler);
        finish_thread(r_info, 136, true, t_handler);
        return;
    }
#endif
    if (t_handler->record_mutation != NULL)
        t_handler->record_mutation->changed = true;
    invalidate_replicas_idle_commit(r_info);

    if (new_rsize > RSC_MSG_MAX_PAYLOAD || (new_rsize != 0 && rbuff == NULL)) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Replica message payload is invalid for %s: size=%u, max=%u, buff=%p\n", curr_dt(dt), t_handler->cid, r_info->uuid,
                new_rsize, RSC_MSG_MAX_PAYLOAD, (void *)rbuff);
        r_info->stop = true;
        r_info->bad_data = true;
        finish_thread(r_info, 136, true, t_handler);
        return;
    }
    bool excluder = false;
    bool complete_piggyback_after_write = false;
    uint8_t *combined_payload = NULL;
    uint32_t combined_payload_size = 0;
    uint32_t replica_payload_size = new_rsize;
    char *replica_payload = rbuff;
    uint8_t replica_msg_type = do_truncate ? MSG_DIR_WRITE_TUNC : MSG_DIR_WRITE;
    append_compaction_piggyback_t *piggyback = t_handler->append_compaction_piggyback;
    bool pending_piggyback = piggyback != NULL && piggyback->active && piggyback->local_applied && !piggyback->attached_to_write && piggyback->r_info == r_info;

    if (pending_piggyback && build_write_compact_payload(r_info, rbuff, b, write_off, new_rsize, do_truncate, bs, t_handler, &combined_payload, &combined_payload_size)) {
        replica_payload = (char *)combined_payload;
        replica_payload_size = combined_payload_size;
        replica_msg_type = MSG_DIR_WRITE_AND_COMPACT;
        piggyback->attached_to_write = true;
        complete_piggyback_after_write = true;
    } else if (pending_piggyback) {
        piggyback->attached_to_write = true;
        replicas_writer(r_info, (char *)piggyback->context.result_buffer, piggyback->plan.b, piggyback->plan.result_write_offset, piggyback->plan.result_write_size, false,
                        piggyback->plan.bs, do_retry, t_handler);

        if (piggyback->truncate_applied)
            replicas_writer(r_info, NULL, piggyback->plan.b, piggyback->plan.new_file_size, 0, true, piggyback->plan.bs, do_retry, t_handler);
        replica_payload = rbuff;
        replica_payload_size = new_rsize;
        replica_msg_type = do_truncate ? MSG_DIR_WRITE_TUNC : MSG_DIR_WRITE;
        complete_piggyback_after_write = true;
    }

    if (replica_msg_type != MSG_DIR_WRITE_AND_COMPACT &&
        (replica_payload != rbuff || replica_payload_size != new_rsize || replica_msg_type != (do_truncate ? MSG_DIR_WRITE_TUNC : MSG_DIR_WRITE))) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [REPLICA_HANDLER=%015lu] Ordinary replica payload invariant failed for %s: payload=%p/%p, size=%u/%u, type=%u/%u\n", curr_dt(dt), t_handler->cid,
                r_info->uuid, (void *)replica_payload, (void *)rbuff, replica_payload_size, new_rsize, replica_msg_type, do_truncate ? MSG_DIR_WRITE_TUNC : MSG_DIR_WRITE);
        r_info->stop = true;
        r_info->bad_data = true;
        finish_thread(r_info, 139, true, t_handler);
        return;
    }
    bool compact_metric = r_info->r_lock_id == APPEND_COMPACTION_LOCK;
    uint32_t compact_attempt = 0;

    if (compact_metric)
        atomic_fetch_add(&append_compact_metric_replica_bytes, new_rsize);
    r_info->replica->replica_control = rf_ctrl;
    uint32_t bsz = r_info->r_idx_b[bs]->bsize_factor;
    r_info->is_replicate = 1;

    do {
        if (compact_metric && compact_attempt++ != 0)
            atomic_fetch_add(&append_compact_metric_replica_retries, 1);

        if (r_info->is_freed) {
            if (excluder)
                set_region_recovery_info(r_info, 1000001L, t_handler);
            finish_thread(r_info, 14, true, t_handler);
        }

        if (atomic_load(&is_shutting_down)) {
            do_r_unlock(r_info, 140, true, t_handler);

            while (atomic_load(&is_shutting_down)) {
                sleep(10);
            }
        }
        uint64_t metric_started_us = compact_metric ? replica_metric_monotonic_us() : 0;

        if (r_info->replica->replica1_port > 0 || r_info->replica->replica2_port > 0 || r_info->replica->r_info1 != NULL || r_info->replica->r_info2 != NULL) {
            send_to_replicas(r_info, replica_payload, b, write_off, replica_payload_size, do_truncate, bs, replica_msg_type, t_handler);
        } else {
            r_info->replica->replica_control = 0;
        }

        if (compact_metric)
            atomic_fetch_add(&append_compact_metric_replica_send_us, replica_metric_monotonic_us() - metric_started_us);

        if (r_info->is_freed) {
            set_region_recovery_info(r_info, 1000003L, t_handler);
            finish_thread(r_info, 15, true, t_handler);
        }

        if (atomic_load(&is_shutting_down)) {
            do_r_unlock(r_info, 140, true, t_handler);

            while (atomic_load(&is_shutting_down)) {
                sleep(10);
            }
        }

        if (!excluder) {
            metric_started_us = compact_metric ? replica_metric_monotonic_us() : 0;

            if (do_truncate)
                truncate_file(r_info, r_info->r_idx_b[bs]->fd[b], write_off, t_handler->cid);
            else
                write_file(r_info->r_idx_b[bs]->fd[b], write_off, rbuff, new_rsize, bsz, r_info, t_handler->cid);

            if (replica_msg_type == MSG_DIR_WRITE_AND_COMPACT && piggyback != NULL)
                log_append_compaction_disk_probe(r_info, &piggyback->plan, r_info->r_idx_b[piggyback->plan.bs]->fd[piggyback->plan.b], "leader_post_ordinary_write",
                                                 t_handler->cid);

            if (compact_metric)
                atomic_fetch_add(&append_compact_metric_replica_local_write_us, replica_metric_monotonic_us() - metric_started_us);
            excluder = true;
        }
        metric_started_us = compact_metric ? replica_metric_monotonic_us() : 0;

        if (r_info->replica->replica1_port > 0 || r_info->replica->replica2_port > 0 || r_info->replica->r_info1 != NULL || r_info->replica->r_info2 != NULL) {
            recv_from_replicas(r_info, do_retry, r_info->req_count_chk, r_info->req_count_chk, t_handler);
        }

        if (compact_metric)
            atomic_fetch_add(&append_compact_metric_replica_recv_us, replica_metric_monotonic_us() - metric_started_us);

        if ((r_info->replica->replica1_port < 0 && r_info->replica->r_info1 == NULL) || (r_info->replica->replica2_port < 0 && r_info->replica->r_info2 == NULL)) {
            bool print_waiting = true;

            while (r_info->waiting_count_admin || r_info->stop) {
                if (atomic_load(&is_shutting_down)) {
                    do_r_unlock(r_info, 140, true, t_handler);

                    while (atomic_load(&is_shutting_down)) {
                        sleep(10);
                    }
                }
                r_info->replica_stoped = true;

                if (print_waiting) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s INFO [REPLICA_HANDLER=%015lu] Waiting admin release %s waiting_count_lock=%u, r_lock_id=%u, replica stopped\n", curr_dt(dt), t_handler->cid,
                            r_info->uuid, r_info->waiting_count_admin, r_info->r_lock_id);
                    print_waiting = false;
                }
                sleep(1);
            }
            r_info->replica_stoped = false;
        }

        if (r_info->is_freed) {
            set_region_recovery_info(r_info, 1000003L, t_handler);
            finish_thread(r_info, 15, true, t_handler);
        }
        // TODO Удалить
        if (r_info->replica->replica_control == 4)
            r_info->replica->replica_control = 3;
    } while (r_info->replica->replica_control != 0 && do_retry == true);

    if (r_info->bulk_mode == BUIK_RELICA_OFF)
        r_info->is_replicate = 0;

    if (complete_piggyback_after_write && r_info->bulk_mode == BUIK_RELICA_OFF && r_info->replica->replica_control == 0)
        complete_append_compaction_piggyback(r_info, t_handler);
    free(combined_payload);
}


int hostname_to_ip(char *hostname, char *ip) {
    struct hostent *he;
    struct in_addr **addr_list;
    int i;

    if ((he = gethostbyname(hostname)) == NULL) {
        herror("gethostbyname");
        return 1;
    }
    addr_list = (struct in_addr **)he->h_addr_list;

    for (i = 0; addr_list[i] != NULL; i++) {
        strcpy(ip, inet_ntoa(*addr_list[i]));
        return 0;
    }
    return 1;
}

bool get_all_local_ip() {
    struct ifaddrs *ifAddrStruct = NULL;
    struct ifaddrs *ifa = NULL;
    void *tmpAddrPtr = NULL;
    int i = 0;
    long host_name_max = sysconf(_SC_HOST_NAME_MAX);

    if (host_name_max < 0) {
        host_name_max = 1024;
    }
    hostname = malloc((size_t)host_name_max + 1);

    if (hostname == NULL) {
        perror("malloc");
        return false;
    }

    if (gethostname(hostname, (size_t)host_name_max + 1) != 0) {
        perror("gethostname");
        free(hostname);
        return false;
    }
    hostname[host_name_max] = '\0';
    size_t hostname_len = strlen(hostname);
    char *tmp = realloc(hostname, hostname_len + 1);

    if (tmp != NULL) {
        hostname = tmp;
    } else {
        fprintf(stderr, "FATAL [MAIN] Failed realloc hostname=%lu\n", hostname_len + 1);
        exit(-1);
    }
    fprintf(stderr, "hostname: %s\n", hostname);
    char ip[100];
    hostname_to_ip(hostname, ip);
    fprintf(stderr, "%s resolved to %s\n", hostname, ip);
    strcpy(local_ip[i], ip);
    local_ip_num = ++i;

    if (i == MAX_LOCAL_IP_NUM)
        return false;
    getifaddrs(&ifAddrStruct);

    for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr) {
            continue;
        }

        if (ifa->ifa_addr->sa_family == AF_INET) {
            tmpAddrPtr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
            inet_ntop(AF_INET, tmpAddrPtr, local_ip[i], INET_ADDRSTRLEN);
            fprintf(stderr, "%s IP Address=%s\n", ifa->ifa_name, local_ip[i]);
            local_ip_num = ++i;

            if (i == MAX_LOCAL_IP_NUM)
                break;
        }
    }

    if (ifAddrStruct != NULL)
        freeifaddrs(ifAddrStruct);
    return true;
}
