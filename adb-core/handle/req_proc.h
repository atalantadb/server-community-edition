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
#include "../utils/stat_utils.h"
typedef struct pthread_arg_t {
    connection_transport_t *transport;
    struct sockaddr_in client_address;
    char ip[256];
    char port[32];
} pthread_arg_t;
#define BLOCKING_MODE 0
#define NONBLOCKING_MODE 1
#define ALWAYS_BLOCKING_MODE 2
#define WRITE_CHUNK_SIZE 16384
void close_conn(connection_t *conn);
void close_handler_transport(table_handler_t *t_handler);
table_handler_t *init_connection(void *arg);
int process_authentication(struct table_handler_t *t_handler);
int recv_client_requests(connection_t *conn);
int migrate_connection_to_nonblocking(connection_t *conn);
