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
#include "rs_processor.h"
#include "../all_headers.h"
#include "../utils/stat_utils.h"
#include "../utils/health.h"
_Atomic char exit_code;
static void segv_handler(int signo, siginfo_t *info, void *context) {
    (void)context;
    time_t t = time(NULL);
    char tbuf[64];
    struct tm tm;

    if (localtime_r(&t, &tm) != NULL) {
        strftime(tbuf, sizeof(tbuf), "%Y-%m-%d %H:%M:%S", &tm);
    } else {
        snprintf(tbuf, sizeof(tbuf), "unknown time");
    }
    pid_t tid = syscall(SYS_gettid);
    dprintf(STDERR_FILENO,
            "%s FATAL: Caught signal %d (SIGSEGV)\n"
            "    Fault address: %p\n"
            "    Kernel TID=%u\n",
            tbuf, signo, info->si_addr, tid);
    void *bt[1024];
    int bt_size = backtrace(bt, sizeof(bt) / sizeof(bt[0]));
    backtrace_symbols_fd(bt, bt_size, STDERR_FILENO);
    _exit(EXIT_FAILURE);
}


static void hup_handler(int signo) {
    (void)signo;
    need_reopen = 1;
}


static void setup_segv_handler(void) {
    struct sigaction sa;
    sa.sa_sigaction = segv_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_SIGINFO | SA_RESTART;

    if (sigaction(SIGSEGV, &sa, NULL) == -1) {
        perror("sigaction(SIGSEGV)");
        exit(EXIT_FAILURE);
    }

    if (log_path != NULL) {
        struct sigaction sa_hup;
        memset(&sa_hup, 0, sizeof(sa_hup));
        sa_hup.sa_handler = hup_handler;
        sigemptyset(&sa_hup.sa_mask);
        sa_hup.sa_flags = SA_RESTART;

        if (sigaction(SIGHUP, &sa_hup, NULL) == -1) {
            perror("sigaction(SIGHUP)");
            exit(EXIT_FAILURE);
        }
    }
}

void *rs_connection(void *unused) {
    char dt[DT_LOG_SIZE];
    pthread_t rs_pthread;
    pthread_attr_t pthread_attr;
    int new_socket_fd;
    struct sockaddr_in address;
    struct sockaddr_in client_address;
    socklen_t client_address_len;
    memset(&address, 0, sizeof address);
    address.sin_family = AF_INET;
    address.sin_port = htons(rs_port);
    address.sin_addr.s_addr = INADDR_ANY;

    if ((rs_socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("RS socket");
        fprintf(stderr, "%s FATAL [RS_PROCESSOR] Exit code -2\n", curr_dt(dt));
        exit(-2);
    }
    // TODO Это норм для отладки, но в ПРОМ отключить!
    int reuse = 1;

    if (setsockopt(rs_socket_fd, SOL_SOCKET, SO_REUSEPORT, (const char *)&reuse, sizeof(reuse)) < 0)
        perror("setsockopt(SO_REUSEPORT) failed");

    for (int i = 15; i >= 0; i--) {
        int res_bind = bind(rs_socket_fd, (struct sockaddr *)&address, sizeof address);

        if (res_bind != -1)
            break;

        if (res_bind == -1 && i == 0) {
            fprintf(stderr, "Exit. Try to change the port %u in settings or kill the process which using it\n", rs_port);
            fprintf(stderr, "%s FATAL [RS_PROCESSOR] Exit code -2\n", curr_dt(dt));
            exit(-2);
        }
        fprintf(stderr, "%s WARN [RS_PROCESSOR] Port %u already in use. Sleep 5 sec and try %d\n", curr_dt(dt), rs_port, i);
        sleep(5);
    }

    if (listen(rs_socket_fd, BACKLOG) == -1) {
        perror("RS listen");
        fprintf(stderr, "%s FATAL [RS_PROCESSOR] Exit code -2\n", curr_dt(dt));
        exit(-2);
    }

    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        perror("RS signal");
        fprintf(stderr, "%s FATAL [RS_PROCESSOR] Exit code -2\n", curr_dt(dt));
        exit(-2);
    }

    if (signal(SIGTERM, rs_signal_handler) == SIG_ERR) {
        perror("RS signal");
        fprintf(stderr, "%s FATAL [RS_PROCESSOR] Exit code -2\n", curr_dt(dt));
        exit(-2);
    }

    if (signal(SIGINT, rs_signal_handler) == SIG_ERR) {
        perror("RS signal");
        fprintf(stderr, "%s FATAL [RS_PROCESSOR] Exit code -2\n", curr_dt(dt));
        exit(-2);
    }

    if (signal(SIGSEGV, rs_signal_handler) == SIG_ERR) {
        perror("RS signal");
        fprintf(stderr, "%s FATAL [RS_PROCESSOR] Exit code -2\n", curr_dt(dt));
        exit(-2);
    }
    setup_segv_handler();

    if (pthread_attr_init(&pthread_attr) != 0) {
        perror("RS pthread_attr_init");
        fprintf(stderr, "%s FATAL [RS_PROCESSOR] Exit code -2\n", curr_dt(dt));
        exit(-2);
    }

    if (pthread_attr_setdetachstate(&pthread_attr, PTHREAD_CREATE_DETACHED) != 0) {
        perror("RS pthread_attr_setdetachstate");
        fprintf(stderr, "%s FATAL [RS_PROCESSOR] Exit code -2\n", curr_dt(dt));
        exit(-2);
    }
    is_shutting_down = false;
    rs_cmd_processing = 0;

    while (1) {
        pthread_arg_rs_t *pthread_arg_rs = (pthread_arg_rs_t *)malloc(sizeof *pthread_arg_rs);

        if (!pthread_arg_rs) {
            perror("malloc pthread_arg_rs");
            fprintf(stderr, "%s FATAL [RS_PROCESSOR] Exit code -2\n", curr_dt(dt));
            exit(-2);
        }
        memset(pthread_arg_rs, 0, sizeof(*pthread_arg_rs));
        transport_init(&pthread_arg_rs->transport, -1);
        client_address_len = sizeof pthread_arg_rs->client_address;
        new_socket_fd = accept(rs_socket_fd, (struct sockaddr *)&pthread_arg_rs->client_address, &client_address_len);

        if (new_socket_fd == -1) {
            if (!is_shutting_down)
                perror("rs_pthread accept ");
            free(pthread_arg_rs);
            continue;
        }
        transport_init(&pthread_arg_rs->transport, new_socket_fd);

        if (!transport_prepare_server(&pthread_arg_rs->transport)) {
            fprintf(stderr, "%s ERROR [RS_PROCESSOR=%015lu] TLS transport setup failed, fd=%d\n", curr_dt(dt), 0UL, new_socket_fd);
            transport_close(&pthread_arg_rs->transport);
            free(pthread_arg_rs);
            continue;
        }
        pthread_arg_rs->cid = get_new_cid();
        rs_cmd_processing++;
        fprintf(stderr, "%s INFO [RS_PROCESSOR=%015lu] rs_cmd_processing = %d (max_rs_cmd_num = %d)\n", curr_dt(dt), pthread_arg_rs->cid, rs_cmd_processing, max_rs_cmd_num);

        while (atomic_load(&is_shutting_down)) {
            sleep(1);
            fprintf(stderr, "%s INFO [RS_PROCESSOR=%015lu] Stop processing new RS cmd\n", curr_dt(dt), pthread_arg_rs->cid);
        }
        int rc = pthread_create(&rs_pthread, &pthread_attr, add_new_region, pthread_arg_rs);

        if (rc != 0) {
            errno = rc;

            if (!is_shutting_down) {
                fprintf(stderr, "%s ERROR [RS_PROCESSOR=%015lu] Cannot create the new thread, %s\n", curr_dt(dt), pthread_arg_rs->cid, strerror(errno));
            }
            transport_close(&pthread_arg_rs->transport);
            free(pthread_arg_rs);
            continue;
        }
        while (rs_cmd_processing >= max_rs_cmd_num) {
            sleep(1);
            fprintf(stderr, "%s INFO [RS_PROCESSOR] rs_cmd_processing=%d >= max_rs_cmd_num=%d. Sleep 1 sec.\n", curr_dt(dt), rs_cmd_processing, max_rs_cmd_num);
        }
    }
}

void rs_signal_handler(int signal_number) {
    char expected = 0;

    if (atomic_compare_exchange_strong(&is_shutting_down, &expected, 1)) {
        atomic_fetch_add(&exit_code, signal_number);
    }
}
