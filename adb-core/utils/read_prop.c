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
#include "read_prop.h"
#include "msg_utils.h"
#include <limits.h>
char *replace_char(char *str, char find, char replace) {
    char *current_pos = strchr(str, find);

    while (current_pos) {
        *current_pos = replace;
        current_pos = strchr(current_pos, find);
    }
    return str;
}

static char *xstrdup_or_null(const char *s) {
    if (!s || !*s)
        return NULL;
    size_t n = strlen(s);
    char *p = (char *)malloc(n + 1);

    if (!p)
        return NULL;
    memcpy(p, s, n + 1);
    return p;
}


static void rstrip(char *s) {
    if (!s)
        return;
    size_t n = strlen(s);

    while (n && (s[n - 1] == '\n' || s[n - 1] == '\r')) {
        s[--n] = 0;
    }
    while (n && isspace((unsigned char)s[n - 1])) {
        s[--n] = 0;
    }
}


static char *lstrip_inplace(char *s) {
    if (!s)
        return s;

    while (*s && isspace((unsigned char)*s))
        s++;
    return s;
}

static void trim_inplace(char **ps) {
    if (!ps || !*ps)
        return;
    char *s = *ps;
    s = lstrip_inplace(s);
    size_t n = strlen(s);

    while (n && isspace((unsigned char)s[n - 1]))
        s[--n] = 0;
    *ps = s;
}


static int parse_kv(char *line, char **out_k, char **out_v) {
    char *s = line;
    rstrip(s);
    s = lstrip_inplace(s);

    if (!*s)
        return 0;

    if (*s == '#' || *s == ';')
        return 0;
    char *eq = strchr(s, '=');

    if (!eq)
        return -1;
    *eq = 0;
    char *k = s;
    char *v = eq + 1;
    trim_inplace(&k);
    trim_inplace(&v);

    if (!*k)
        return -1;
    *out_k = k;
    *out_v = v;
    return 1;
}


static int parse_int(const char *v, int *out) {
    if (!v || !*v)
        return 0;
    char *end = NULL;
    long x = strtol(v, &end, 10);

    if (end == v || (end && *end))
        return 0;
    *out = (int)x;
    return 1;
}

static int parse_bool(const char *v, int *out) {
    if (!v)
        return 0;

    if (!*v)
        return 0;

    if (!strcasecmp(v, "true") || !strcmp(v, "1") || !strcasecmp(v, "yes") || !strcasecmp(v, "on")) {
        *out = 1;
        return 1;
    }

    if (!strcasecmp(v, "false") || !strcmp(v, "0") || !strcasecmp(v, "no") || !strcasecmp(v, "off")) {
        *out = 0;
        return 1;
    }
    return parse_int(v, out);
}


static void log_change(const char *key, const char *from, const char *to) {
    char dt[64];
    curr_dt(dt);
    int secret = (strstr(key, "password") != NULL);

    if (secret) {
        fprintf(stderr, "%s %s: ****** -> ******\n", dt, key);
    } else {
        fprintf(stderr, "%s %s: %s -> %s\n", dt, key, from ? from : "NULL", to ? to : "NULL");
    }
}


static void log_change_int(const char *key, long from, long to) {
    char dt[64];
    curr_dt(dt);
    fprintf(stderr, "%s %s: %ld -> %ld\n", dt, key, from, to);
}

static int str_eq(const char *a, const char *b) {
    if (a == b)
        return 1;

    if (!a || !b)
        return 0;
    return strcmp(a, b) == 0;
}

static int can_hot_change(const char *k) {
    const char *allowed[] = {"max_rs_cmd_num",
                             "log_level",
                             "max_retry_num",
                             "drive_free_mb_limit",
                             "scan_wait_client_msec",
                             "cli_sock_timeout",
                             "replica_sock_timeout",
                             "max_record_size",
                             "unpack_lob_to_client",
                             "old_ts",
                             "rewrite_dict_flag",
                             "allow_scan_lob",
                             "new_scan_result",
                             "mem_free_limit_percentage",
                             "create_idx_resize_backup",
                             "region_border_exclusive",
                             "hbase_scan_filter",
                             "log_rotation_seconds",
                             "replicas_chk_delay_sec",
                             "worker_idle_timeout",
                             "rtt_fresh_idle_limit_ms",
                             "rtt_client_get_stat_count",
                             "rtt_replica_get_stat_count",
                             "enable_write_4k_cache",
                             "block_epoll_worker_timeout_ms",
                             "set_socket_send_buffer_kb",
                             "set_socket_receive_buffer_kb",
                             "append_compact_start_percent",
                             "append_compact_client_stop_percent",
                             "append_compact_max_segment_per_visit",
                             "append_compact_pending_delete_limit",
                             "append_compact_ops_limit",
                             "append_compact_sleeping_ops_limit",
                             "append_compact_sleeping_thread_count",
                             "append_compact_piggyback_enabled",
                             "append_compact_min_percent",
                             "append_compact_thread_count",
                             "set_tcp_nodelay",
                             NULL};

    for (int i = 0; allowed[i]; ++i) {
        if (strcmp(k, allowed[i]) == 0)
            return 1;
    }
    return 0;
}
#if !ENTERPRISE
static bool parameter_requires_enterprise(const char *key) {
    static const char *const enterprise_parameters[] = {"auth",
                                                        "audit",
                                                        "replicas_secret",
                                                        "enable_jwt_ts",
                                                        "auth_count_try_limit",
                                                        "srv_ca_pem",
                                                        "srv_cert_pem",
                                                        "srv_key_pem",
                                                        "srv_pem_password",
                                                        "cli_ca_pem",
                                                        "cli_cert_pem",
                                                        "cli_key_pem",
                                                        "cli_pem_password",
                                                        NULL};

    for (size_t i = 0; enterprise_parameters[i] != NULL; ++i) {
        if (strcmp(key, enterprise_parameters[i]) == 0)
            return true;
    }
    return false;
}
#endif
static void log_or_fatal(int init, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, (init ? "ERROR: " : "WARN: "));
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);

    if (init)
        exit(EXIT_FAILURE);
}

static void set_int_param(const char *key, const char *v, volatile int init, int allow_hot, int *target, int *p_err) {
    int nv;

    if (!parse_int(v, &nv)) {
        (*p_err)++;
        log_or_fatal(init, "Bad integer for %s: '%s'", key, v ? v : "");
        return;
    }

    if (init) {
        *target = nv;
        return;
    }

    if (!allow_hot) {
        fprintf(stderr, "INFO: attempt to change '%s' after start is ignored.\n", key);
        return;
    }

    if (*target != nv) {
        log_change_int(key, *target, nv);
        *target = nv;
    }
}

static void set_int_param_atomic(const char *key, const char *v, int init, int allow_hot, _Atomic int *target, int *p_err) {
    int nv;

    if (!parse_int(v, &nv)) {
        (*p_err)++;
        log_or_fatal(init, "Bad integer for %s: '%s'", key, v ? v : "");
        return;
    }

    if (init) {
        atomic_store_explicit(target, nv, memory_order_relaxed);
        return;
    }

    if (!allow_hot) {
        fprintf(stderr, "INFO: attempt to change '%s' after start is ignored.\n", key);
        return;
    }
    int old = atomic_load_explicit(target, memory_order_relaxed);

    if (old != nv) {
        log_change_int(key, old, nv);
        atomic_store_explicit(target, nv, memory_order_relaxed);
    }
}

static void set_bounded_int_param_atomic(const char *key, const char *v, int init, int allow_hot, int min_value, int max_value, _Atomic int *target, int *p_err) {
    int nv;

    if (!parse_int(v, &nv) || nv < min_value || nv > max_value) {
        (*p_err)++;
        log_or_fatal(init, "Bad integer for %s: '%s' (expected %d..%d)", key, v ? v : "", min_value, max_value);
        return;
    }

    if (init) {
        atomic_store_explicit(target, nv, memory_order_relaxed);
        return;
    }

    if (!allow_hot) {
        fprintf(stderr, "INFO: attempt to change '%s' after start is ignored.\n", key);
        return;
    }
    int old = atomic_load_explicit(target, memory_order_relaxed);

    if (old != nv) {
        log_change_int(key, old, nv);
        atomic_store_explicit(target, nv, memory_order_relaxed);
    }
}

static void set_percent_param_atomic(const char *key, const char *v, int init, int allow_hot, _Atomic int *target, int *p_err) {
    int nv;

    if (!parse_int(v, &nv) || nv < 0 || nv > 99) {
        (*p_err)++;
        log_or_fatal(init, "Bad percentage for %s: '%s' (expected 0..99)", key, v ? v : "");
        return;
    }

    if (init) {
        atomic_store_explicit(target, nv, memory_order_relaxed);
        return;
    }

    if (!allow_hot) {
        fprintf(stderr, "INFO: attempt to change '%s' after start is ignored.\n", key);
        return;
    }
    int old = atomic_load_explicit(target, memory_order_relaxed);

    if (old != nv) {
        log_change_int(key, old, nv);
        atomic_store_explicit(target, nv, memory_order_relaxed);
    }
}

static void set_bool_param(const char *key, const char *v, int init, int allow_hot, int *target, int *p_err) {
    int nv;

    if (!parse_bool(v, &nv)) {
        (*p_err)++;
        log_or_fatal(init, "Bad boolean for %s: '%s'", key, v ? v : "");
        return;
    }

    if (init) {
        *target = nv;
        return;
    }

    if (!allow_hot) {
        fprintf(stderr, "INFO: attempt to change '%s' after start is ignored.\n", key);
        return;
    }

    if (*target != nv) {
        log_change_int(key, *target, nv);
        *target = nv;
    }
}


static void set_str_param(const char *key, const char *v, int init, int allow_hot, char **target, int *p_err) {
    const char *nv_raw = (v && *v) ? v : NULL;
    char *nv = xstrdup_or_null(nv_raw);

    if (nv_raw && !nv) {
        (*p_err)++;
        log_or_fatal(init, "OOM for %s", key);
        return;
    }

    if (init) {
        *target = nv;
    }
}

bool has_digit_lower_upper_special(const char *str) {
    bool has_digit = false;
    bool has_lower = false;
    bool has_upper = false;
    bool has_special = false;

    if (str == NULL) {
        return false;
    }

    for (int i = 0; str[i] != '\0'; i++) {
        unsigned char c = str[i];

        if (isdigit(c)) {
            has_digit = true;
        } else if (islower(c)) {
            has_lower = true;
        } else if (isupper(c)) {
            has_upper = true;
        } else if (ispunct(c)) {
            has_special = true;
        }

        if (has_digit && has_lower && has_upper && has_special) {
            return true;
        }
    }
    return (has_digit && has_lower && has_upper && has_special);
}

void read_file_prop(const char *fn, bool init) {
    FILE *file = fopen(fn, "r");

    if (!file) {
        log_or_fatal(init, "Cannot open config '%s': %s", fn, strerror(errno));
        return;
    }

    if (init) {
        enable_compression = log_level = enable_dict = merge_keys = 0;
        rf = check_double_keys = rebuild_index = data_check_enabled = -1;
        max_retry_num = 60;
        max_file_size = 4096 * 2;
        initial_idx_coeff = 8 * 2;
        rf_ctrl = 1;
        max_rs_cmd_num = 10000;
        drive_free_mb_limit = 1024;
        scan_wait_client_msec = 60 * 60 * 1000;
        cli_sock_timeout = 60;
        replica_sock_timeout = 120;
        replicas_chk_delay_sec = 60;
        replicas_last_change_time_check_diff_ms = 100000;
        meta_crc_enable = 0;
        old_ts = 0;
        num_workers = 0;
        max_workers = 0;
        max_record_size = 1024 * 1024 * 1024;
        unpack_lob_to_client = 0;
        rewrite_dict_flag = 0;
        enable_block_compression = BLOCK_COMPRESSION_NONE;
        mem_free_limit_percentage = 3;
        create_idx_resize_backup = 0;
        need_reopen = 0;
        region_border_exclusive = 1;
        hbase_scan_filter = 1;
        stop_resize_worker_pool = 0;
        allow_scan_lob = 1;
        log_rotation_seconds = 7 * 24 * 60 * 60;
        new_scan_result = 2;
        blocking_socket_mode = 0;
        worker_client_idle_timeout = 60;
        worker_replica_idle_timeout = 1200;
        idx_resize_max_num_chunk = 256;
        rtt_fresh_idle_limit_ms = 100;
        rtt_client_get_stat_count = 100000;
        rtt_replica_get_stat_count = 100000;
        rtt_client_print_stat_count = 1000000;
        rtt_replica_print_stat_count = 1000000;
        set_tcp_nodelay = 1;
        enable_write_4k_cache = 1;
        block_epoll_worker_timeout_ms = 1000;
        set_socket_send_buffer_kb = 512;
        set_socket_receive_buffer_kb = 512;
        append_compact_start_percent = 50;
        append_compact_client_stop_percent = 50;
        append_compact_max_segment_per_visit = 16;
        append_compact_pending_delete_limit = 100000;
        append_compact_ops_limit = 100;
        append_compact_sleeping_ops_limit = 100000;
        append_compact_sleeping_thread_count = 2;
        append_compact_piggyback_enabled = 1;
        append_compact_min_percent = 0;
        append_compact_thread_count = 64;
        append_compact_low_ops_streak = 0;
        append_compact_sleeping_ops_streak = 0;
        append_compact_truncate_allowed = 1;
        health_file_name = NULL;
        debug_file_name = "debug.info";
        exit_file_name = NULL;
        recovery_info_fn = "recovery.info";
    }
    char *line = NULL;
    size_t cap = 0;
    ssize_t len;
    int parse_errors = 0;

    while ((len = getline(&line, &cap, file)) != -1) {
        if (init) {
            if (strstr(line, "password") == NULL && strstr(line, "replicas_secret") == NULL) {
                fprintf(stderr, "INFO [MAIN] Configuration: %s", line);

                if (len > 0 && line[len - 1] != '\n')
                    fprintf(stderr, "\n");
            } else {
                fprintf(stderr, "INFO [MAIN] Configuration: skip password line\n");
            }
        }
        line[strcspn(line, " \t\r\n")] = '\0';
        char *k = NULL, *v = NULL;
        int kv = parse_kv(line, &k, &v);

        if (kv == 0)
            continue;

        if (kv < 0) {
            parse_errors++;
            log_or_fatal(init, "Bad line in config: '%s'", line);

            if (!init) {
                free(line);
                fclose(file);
                return;
            }
            continue;
        }
#if !ENTERPRISE
        if (parameter_requires_enterprise(k)) {
            if (strcmp(v, "none") != 0) {
                fprintf(stderr, "FATAL: parameter '%s' requires ENTERPRISE build\n", k);
                free(line);
                fclose(file);
                exit(EXIT_FAILURE);
            }
        }
#endif
        int allow_hot = can_hot_change(k);

        if (!allow_hot && init) {
            if (!strcmp(k, "client_port")) {
                set_int_param(k, v, init, 0, &client_port, &parse_errors);
                continue;
            }

            if (!strcmp(k, "replication_port")) {
                set_int_param(k, v, init, 0, &replication_port, &parse_errors);
                continue;
            }

            if (!strcmp(k, "rs_port")) {
                set_int_param(k, v, init, 0, &rs_port, &parse_errors);
                continue;
            }

            if (!strcmp(k, "rf")) {
                set_int_param(k, v, init, 0, &rf, &parse_errors);
                rf_ctrl = 2 * rf - 3;
                continue;
            }

            if (!strcmp(k, "enable_compression")) {
                set_bool_param(k, v, init, 0, &enable_compression, &parse_errors);
                continue;
            }

            if (!strcmp(k, "enable_dict")) {
                set_bool_param(k, v, init, 0, &enable_dict, &parse_errors);
                continue;
            }

            if (!strcmp(k, "merge_keys")) {
                set_bool_param(k, v, init, 0, &merge_keys, &parse_errors);
                continue;
            }

            if (!strcmp(k, "check_double_keys")) {
                set_int_param(k, v, init, 0, &check_double_keys, &parse_errors);
                continue;
            }

            if (!strcmp(k, "num_workers")) {
                set_int_param(k, v, init, 0, &num_workers, &parse_errors);
                continue;
            }

            if (!strcmp(k, "max_workers")) {
                set_int_param(k, v, init, 0, &max_workers, &parse_errors);
                continue;
            }

            if (!strcmp(k, "rebuild_index")) {
                set_int_param(k, v, init, 0, &rebuild_index, &parse_errors);
                continue;
            }

            if (!strcmp(k, "health_file_name")) {
                set_str_param(k, v, init, 0, &health_file_name, &parse_errors);
                continue;
            }

            if (!strcmp(k, "debug_file_name")) {
                set_str_param(k, v, init, 0, &debug_file_name, &parse_errors);
                continue;
            }

            if (!strcmp(k, "exit_file_name")) {
                set_str_param(k, v, init, 0, &exit_file_name, &parse_errors);
                continue;
            }

            if (!strcmp(k, "blocking_socket_mode")) {
                set_int_param(k, v, init, 0, &blocking_socket_mode, &parse_errors);
                continue;
            }

            if (!strcmp(k, "enable_block_compression")) {
                if (!v || !*v) {
                    parse_errors++;
                    log_or_fatal(init, "Bad value for %s", k);
                    continue;
                }

                if (!strcmp(v, "lz"))
                    enable_block_compression = BLOCK_COMPRESSION_LZ;
                else if (!strcmp(v, "snappy"))
                    enable_block_compression = BLOCK_COMPRESSION_SNAPPY;
                else if (!strcmp(v, "none"))
                    enable_block_compression = BLOCK_COMPRESSION_NONE;
                else {
                    parse_errors++;
                    log_or_fatal(init, "Bad value for %s: %s", k, v);
                }
                continue;
            }

            if (!strcmp(k, "data_check_enabled")) {
                set_int_param(k, v, init, 0, &data_check_enabled, &parse_errors);
                continue;
            }

            if (!strcmp(k, "meta_crc_enable")) {
                set_int_param(k, v, init, 0, &meta_crc_enable, &parse_errors);
                continue;
            }

            if (!strcmp(k, "replicas_last_change_time_check_diff_ms")) {
                set_int_param(k, v, init, 0, &replicas_last_change_time_check_diff_ms, &parse_errors);
                continue;
            }
        }

        if (!strcmp(k, "max_rs_cmd_num")) {
            set_int_param_atomic(k, v, init, 1, &max_rs_cmd_num, &parse_errors);
            continue;
        }

        if (!strcmp(k, "log_level")) {
            set_int_param_atomic(k, v, init, 1, &log_level, &parse_errors);
            continue;
        }

        if (!strcmp(k, "max_retry_num")) {
            set_int_param_atomic(k, v, init, 1, &max_retry_num, &parse_errors);
            continue;
        }

        if (!strcmp(k, "drive_free_mb_limit")) {
            set_int_param_atomic(k, v, init, 1, &drive_free_mb_limit, &parse_errors);
            continue;
        }

        if (!strcmp(k, "scan_wait_client_msec")) {
            set_int_param_atomic(k, v, init, 1, &scan_wait_client_msec, &parse_errors);
            continue;
        }

        if (!strcmp(k, "cli_sock_timeout")) {
            set_int_param_atomic(k, v, init, 1, &cli_sock_timeout, &parse_errors);
            continue;
        }

        if (!strcmp(k, "replica_sock_timeout")) {
            set_int_param_atomic(k, v, init, 1, &replica_sock_timeout, &parse_errors);
            continue;
        }

        if (!strcmp(k, "max_record_size")) {
            set_int_param_atomic(k, v, init, 1, &max_record_size, &parse_errors);
            continue;
        }

        if (!strcmp(k, "unpack_lob_to_client")) {
            set_int_param_atomic(k, v, init, 1, &unpack_lob_to_client, &parse_errors);
            continue;
        }

        if (!strcmp(k, "old_ts")) {
            set_int_param_atomic(k, v, init, 1, &old_ts, &parse_errors);
            continue;
        }

        if (!strcmp(k, "rewrite_dict_flag")) {
            set_int_param_atomic(k, v, init, 1, &rewrite_dict_flag, &parse_errors);
            continue;
        }

        if (!strcmp(k, "allow_scan_lob")) {
            set_int_param_atomic(k, v, init, 1, &allow_scan_lob, &parse_errors);
            continue;
        }

        if (!strcmp(k, "new_scan_result")) {
            set_int_param_atomic(k, v, init, 1, &new_scan_result, &parse_errors);
            continue;
        }

        if (!strcmp(k, "mem_free_limit_percentage")) {
            set_int_param_atomic(k, v, init, 1, &mem_free_limit_percentage, &parse_errors);
            continue;
        }

        if (!strcmp(k, "create_idx_resize_backup")) {
            set_int_param_atomic(k, v, init, 1, &create_idx_resize_backup, &parse_errors);
            continue;
        }

        if (!strcmp(k, "region_border_exclusive")) {
            set_int_param_atomic(k, v, init, 1, &region_border_exclusive, &parse_errors);
            continue;
        }

        if (!strcmp(k, "hbase_scan_filter")) {
            set_int_param_atomic(k, v, init, 1, &hbase_scan_filter, &parse_errors);
            continue;
        }

        if (!strcmp(k, "log_rotation_seconds")) {
            set_int_param_atomic(k, v, init, 1, &log_rotation_seconds, &parse_errors);
            continue;
        }

        if (!strcmp(k, "replicas_chk_delay_sec")) {
            set_int_param_atomic(k, v, init, 1, &replicas_chk_delay_sec, &parse_errors);
            continue;
        }

        if (!strcmp(k, "worker_client_idle_timeout")) {
            set_int_param_atomic(k, v, init, 1, &worker_client_idle_timeout, &parse_errors);
            continue;
        }

        if (!strcmp(k, "worker_replica_idle_timeout")) {
            set_int_param_atomic(k, v, init, 1, &worker_replica_idle_timeout, &parse_errors);
            continue;
        }

        if (!strcmp(k, "idx_resize_max_num_chunk")) {
            set_int_param_atomic(k, v, init, 1, &idx_resize_max_num_chunk, &parse_errors);
            continue;
        }

        if (!strcmp(k, "rtt_fresh_idle_limit_ms")) {
            set_int_param_atomic(k, v, init, 1, &rtt_fresh_idle_limit_ms, &parse_errors);
            continue;
        }

        if (!strcmp(k, "rtt_client_get_stat_count")) {
            set_int_param_atomic(k, v, init, 1, &rtt_client_get_stat_count, &parse_errors);
            continue;
        }

        if (!strcmp(k, "rtt_replica_get_stat_count")) {
            set_int_param_atomic(k, v, init, 1, &rtt_replica_get_stat_count, &parse_errors);
            continue;
        }

        if (!strcmp(k, "rtt_client_print_stat_count")) {
            set_int_param_atomic(k, v, init, 1, &rtt_client_print_stat_count, &parse_errors);
            continue;
        }

        if (!strcmp(k, "rtt_replica_print_stat_count")) {
            set_int_param_atomic(k, v, init, 1, &rtt_replica_print_stat_count, &parse_errors);
            continue;
        }

        if (!strcmp(k, "enable_write_4k_cache")) {
            set_int_param_atomic(k, v, init, 1, &enable_write_4k_cache, &parse_errors);
            continue;
        }

        if (!strcmp(k, "block_epoll_worker_timeout_ms")) {
            set_int_param_atomic(k, v, init, 1, &block_epoll_worker_timeout_ms, &parse_errors);
            continue;
        }

        if (!strcmp(k, "set_socket_send_buffer_kb")) {
            set_int_param_atomic(k, v, init, 1, &set_socket_send_buffer_kb, &parse_errors);
            continue;
        }

        if (!strcmp(k, "set_socket_receive_buffer_kb")) {
            set_int_param_atomic(k, v, init, 1, &set_socket_receive_buffer_kb, &parse_errors);
            continue;
        }

        if (!strcmp(k, "append_compact_start_percent")) {
            set_int_param_atomic(k, v, init, 1, &append_compact_start_percent, &parse_errors);
            continue;
        }

        if (!strcmp(k, "append_compact_client_stop_percent")) {
            set_int_param_atomic(k, v, init, 1, &append_compact_client_stop_percent, &parse_errors);
            continue;
        }

        if (!strcmp(k, "append_compact_max_segment_per_visit")) {
            set_int_param_atomic(k, v, init, 1, &append_compact_max_segment_per_visit, &parse_errors);
            continue;
        }

        if (!strcmp(k, "append_compact_pending_delete_limit")) {
            set_bounded_int_param_atomic(k, v, init, 1, 0, INT_MAX, &append_compact_pending_delete_limit, &parse_errors);
            continue;
        }

        if (!strcmp(k, "append_compact_ops_limit")) {
            set_int_param_atomic(k, v, init, 1, &append_compact_ops_limit, &parse_errors);
            continue;
        }

        if (!strcmp(k, "append_compact_sleeping_ops_limit")) {
            set_bounded_int_param_atomic(k, v, init, 1, 0, INT_MAX, &append_compact_sleeping_ops_limit, &parse_errors);
            continue;
        }

        if (!strcmp(k, "append_compact_sleeping_thread_count")) {
            set_bounded_int_param_atomic(k, v, init, 1, 0, (int)APPEND_COMPACTION_SLEEPING_MAX_THREADS, &append_compact_sleeping_thread_count, &parse_errors);
            continue;
        }

        if (!strcmp(k, "append_compact_piggyback_enabled")) {
            set_int_param_atomic(k, v, init, 1, &append_compact_piggyback_enabled, &parse_errors);
            continue;
        }

        if (!strcmp(k, "append_compact_min_percent")) {
            set_int_param_atomic(k, v, init, 1, &append_compact_min_percent, &parse_errors);
            continue;
        }

        if (!strcmp(k, "append_compact_thread_count")) {
            set_int_param_atomic(k, v, init, 1, &append_compact_thread_count, &parse_errors);
            continue;
        }

        if (!strcmp(k, "set_tcp_nodelay")) {
            set_int_param_atomic(k, v, init, 1, &set_tcp_nodelay, &parse_errors);
            continue;
        }
        parse_errors++;

        if (init)
            log_or_fatal(false, "Unknown parameter '%s'", k);
    }
    free(line);
    fclose(file);

    if (init) {
        if (enable_block_compression) {
            enable_compression = 0;
        }

        if (rf == -1) {
            rf = 1;
            rf_ctrl = 2 * rf - 3;
            fprintf(stderr, "INFO [MAIN] Configuration: rf=%d (default)\n", rf);
        }

        if (check_double_keys == -1) {
            check_double_keys = 1;
            fprintf(stderr, "INFO [MAIN] Configuration: check_double_keys=%d (default)\n", check_double_keys);
        }

        if (rebuild_index == -1) {
            rebuild_index = 1000;
            fprintf(stderr, "INFO [MAIN] Configuration: rebuild_index=%d (default)\n", rebuild_index);
        }
        fprintf(stderr, "\n");

        if (rf == 1) {
            fprintf(stderr, "WARNING: rf=1 (Replication Factor). Data restoration from the reserve replica is impossible.\n");
            fprintf(stderr, "WARNING: Recommended to increase rf=2 or rf=3 (important: rf for Masters and RegionsServers must be the same)\n");
        }

        if (data_check_enabled == -1) {
            if (rf > 1) {
                data_check_enabled = 1;
                fprintf(stderr, "INFO [MAIN] Configuration: data_check_enabled=%d (default)\n", data_check_enabled);
            } else {
                data_check_enabled = 0;
            }
        }

        if (data_check_enabled == true && health_file_name == NULL) {
            fprintf(stderr, "FATAL [MAIN] Configuration: parameter 'health_file_name' must be set unique name!\n");
            fprintf(stderr, "FATAL [MAIN] Configuration: It is crucial to ensure data integrity and consistency!\n");
            fprintf(stderr,
                    "FATAL [MAIN] Configuration: Specify a unique file name and ensure that there are no other DB instances running with that file in the same directory!\n");
            exit(EXIT_FAILURE);
        }

        if (data_check_enabled == 0 && rf > 1) {
            fprintf(stderr, "WARNING: data_check_enabled=0 (disabled). This prevents data restoration from the reserve replica. Strongly recommended set data_check_enabled=1\n");
        }

        if (check_double_keys == 0) {
            fprintf(stderr, "WARNING: check_double_keys=0 (disabled). This prevents merging multiple records into one, which may result in data loss in some cases. Strongly "
                            "recommended to set check_double_keys=1\n");
        }

        if (replicas_chk_delay_sec == 0 && rf > 1) {
            fprintf(stderr, "WARNING: replicas_chk_delay_sec=%d. It means leader will not initiate request to reserve replica for data check\n", replicas_chk_delay_sec);
        }

        if (data_check_enabled == 1 && rf > 1 && replicas_last_change_time_check_diff_ms == 0) {
            fprintf(stderr, "WARNING: replicas_last_change_time_check_diff_ms=0. This prevents replicas data check after hard reset, recommended value is 3000\n");
        }

        if (exit_file_name == NULL)
            exit_file_name = "EXIT.cmd";
        fprintf(stderr, "\n");
    }
}


void check_log_prop(char *fn) {
    FILE *file = fopen(fn, "r");
    char currentline[10240];
    char *param;
    log_path = NULL;
    assert(file != NULL);

    while (fgets(currentline, sizeof(currentline), file) != NULL) {
        param = strtok(currentline, "=");

        if (strcmp(param, "log_path") == 0) {
            param = strtok(NULL, "=");
            replace_char(param, '\n', 0);
            log_path = malloc(strlen(param) + 1);
            memcpy(log_path, param, strlen(param) + 1);
            fprintf(stderr, "AtalantaDB is running, log_path=%s\n", log_path);
        }
    }
    fclose(file);
}
