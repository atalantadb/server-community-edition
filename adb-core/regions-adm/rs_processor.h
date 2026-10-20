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
#ifndef ADB_RS_PROCESSOR_H
#define ADB_RS_PROCESSOR_H
#define BACKLOG 128
#include <unistd.h>
int rs_socket_fd;
_Atomic char is_shutting_down;
void rs_signal_handler(int signal_number);
_Noreturn void *rs_connection(void *unused);
#endif
