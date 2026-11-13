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
#include "../all_headers.h"
#include "connection_transport.h"
static int64_t transport_monotonic_ms(void) {
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
        return -1;
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}


static int transport_wait_fd(int fd, short events, int64_t deadline_ms) {
    struct pollfd pfd = {.fd = fd, .events = events, .revents = 0};

    while (true) {
        int64_t now_ms = transport_monotonic_ms();

        if (now_ms < 0 || now_ms >= deadline_ms)
            return 0;
        int64_t left_ms = deadline_ms - now_ms;
        int timeout_ms = left_ms > INT_MAX ? INT_MAX : (int)left_ms;
        int result = poll(&pfd, 1, timeout_ms);

        if (result > 0) {
            if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL))
                return -1;

            if (pfd.revents & events)
                return 1;
            continue;
        }

        if (result == 0)
            return 0;

        if (errno != EINTR)
            return -1;
    }
}


void transport_init(connection_transport_t *transport, int fd) {
    if (transport == NULL)
        return;
    transport->fd = fd;
}


int transport_fd(const connection_transport_t *transport) {
    return transport == NULL ? -1 : transport->fd;
}

bool transport_is_tls(const connection_transport_t *transport) {
    (void)transport;
    return false;
}


bool transport_prepare_server(connection_transport_t *transport) {
    if (transport == NULL || transport->fd < 0)
        return false;
    return true;
}


bool transport_accept_server(connection_transport_t *transport, uint64_t cid) {
    (void)cid;
    return transport != NULL && transport->fd >= 0;
}


ssize_t transport_read(connection_transport_t *transport, void *buffer, size_t size, uint64_t cid) {
    if (transport == NULL || transport->fd < 0 || buffer == NULL || size > INT_MAX) {
        errno = EINVAL;
        return -1;
    }

    if (size == 0)
        return 0;
    int64_t now_ms = transport_monotonic_ms();

    if (now_ms < 0)
        return -1;
    int64_t deadline_ms = now_ms + 10000;

    while (true) {
        ssize_t result = recv(transport->fd, buffer, size, 0);

        if (result >= 0)
            return result;

        if (errno == EINTR)
            continue;

        if (errno != EAGAIN && errno != EWOULDBLOCK)
            return -1;
        int wait_result = transport_wait_fd(transport->fd, POLLIN, deadline_ms);

        if (wait_result > 0)
            continue;

        if (wait_result == 0) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s WARN [REQUEST_HANDLER=%015lu] Poll timeout, no data for 10000 ms\n", curr_dt(dt), cid);
        }
        return wait_result;
    }
}


ssize_t transport_write(connection_transport_t *transport, const void *buffer, size_t size) {
    if (transport == NULL || transport->fd < 0 || buffer == NULL || size > SSIZE_MAX) {
        errno = EINVAL;
        return -1;
    }
    const uint8_t *bytes = buffer;
    size_t total = 0;
    int64_t deadline_ms = 0;

    while (total < size) {
        size_t left = size - total;
        size_t chunk = left > 64000U ? 64000U : left;
        ssize_t result = send(transport->fd, bytes + total, chunk, MSG_NOSIGNAL | MSG_DONTWAIT);

        if (result > 0) {
            total += (size_t)result;
            continue;
        }

        if (result == 0) {
            errno = EPIPE;
            return -1;
        }

        if (errno == EINTR)
            continue;

        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != ENOBUFS)
            return -1;

        if (deadline_ms == 0) {
            int64_t now_ms = transport_monotonic_ms();

            if (now_ms < 0)
                return -1;
            deadline_ms = now_ms + block_epoll_worker_timeout_ms;
        }
        int wait_result = transport_wait_fd(transport->fd, POLLOUT, deadline_ms);

        if (wait_result > 0)
            continue;

        if (wait_result == 0)
            errno = ETIMEDOUT;
        return -1;
    }
    return (ssize_t)total;
}

void transport_close(connection_transport_t *transport) {
    if (transport == NULL)
        return;

    if (transport->fd >= 0) {
        (void)close(transport->fd);
        transport->fd = -1;
    }
}
