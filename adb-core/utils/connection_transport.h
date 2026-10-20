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
#ifndef ADB_CONNECTION_TRANSPORT_H
#define ADB_CONNECTION_TRANSPORT_H
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
typedef struct connection_transport_t {
    int fd;
} connection_transport_t;
void transport_init(connection_transport_t *transport, int fd);
int transport_fd(const connection_transport_t *transport);
bool transport_is_tls(const connection_transport_t *transport);
bool transport_prepare_server(connection_transport_t *transport);
bool transport_accept_server(connection_transport_t *transport, uint64_t cid);
ssize_t transport_read(connection_transport_t *transport, void *buffer, size_t size, uint64_t cid);
ssize_t transport_write(connection_transport_t *transport, const void *buffer, size_t size);
void transport_close(connection_transport_t *transport);
#endif
