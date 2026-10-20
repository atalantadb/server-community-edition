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
#ifndef ADB_REGION_UTILS_H
#define ADB_REGION_UTILS_H
#include "../all_headers.h"
#include "../utils/string_builder.h"
#include "../crypto/base64.h"
#include "../utils/stat_utils.h"
#include "../utils/request_context.h"
#include "../utils/health.h"
#include "../utils/record_version.h"
#include "../utils/index_memory.h"
#define BUCKETS 1 // TODO Сейчас бакет это прототип CF, но сейчас предполагается что данных в бакете примерно поровну, тогда как когда будет CF то нужно разнести f_idx_size и т.д.
#define FBP 0
#define META_OFFSET 0
#define K0_POS 0
#define K1_POS 1
#define K2_POS 2
#define K3_POS 3
#define BUCKET_POS 5
#define C_IDX_POS 8
#define INS_IDX_OK 0
#define INS_COLL_OK 1
#define INS_IDX_NO 2
#define INS_IDX_UPDATE 3
#define FIX_BSIZE_FACTOR 16
#define MAX_BSIZE_FACTOR 256
#define BULK_MSG_SIZE_LIMIT (100U * 1024U * 1024U)
#define RECORD_SIZE_LIMIT (1U * 1024U * 1024U * 1024U)
#define FIX_BLOCK_SIZE 4096
#define BLOCK_MAX_SIZE_M 16
#define BLOCK_ARRAY_FIRST_POS 0
#define BLOCK_ARRAY_LAST_POS 15
#define BS_LOB 16
#define LOB_CHUNK_SIZE 16384
#define BLOCK_MAX_SIZE 65536
#define FIX_USEFUL_BLOCK_SIZE (4096 - FIX_BSIZE_FACTOR)
#define MSG_RSC_HEADER_SIZE (22 + UUID_SIZE)
#define FIX_MAX_MSG_SIZE (FIX_BLOCK_SIZE + MSG_RSC_HEADER_SIZE)
#define CURR_MAX_MSG_SIZE FIX_MAX_MSG_SIZE * 16 + MAX_BSIZE_FACTOR
#define REAL_MAX_MSG_SIZE (FIX_USEFUL_BLOCK_SIZE * 15 - MSG_RSC_HEADER_SIZE - MAX_BSIZE_FACTOR) // TODO Удалить FIX_MAX_MSG_SIZE
#define RSC_MSG_MAX_PAYLOAD (BLOCK_MAX_SIZE * 2U)
#define RSC_MSG_BUFFER_SIZE (MSG_RSC_HEADER_SIZE + RSC_MSG_MAX_PAYLOAD)
#define SNAPPY_BLOCK_SIZE_POW2 15
#define INIT_BLOCK_FACTOR 8
#define LIMIT_BLOCK_TRY 3
#define BLOCK_COMPRESSION_NONE 0
#define BLOCK_COMPRESSION_LZ 1
#define BLOCK_COMPRESSION_SNAPPY 2
#define APPEND_COMPACTION_ENABLED 1
#define APPEND_COMPACTION_SKIP_TRUNCATE_POC 1
#define APPEND_COMPACTION_SEGMENT_SIZE (64 * 1024U)
#define APPEND_COMPACTION_BLOCKS_PER_SEGMENT (APPEND_COMPACTION_SEGMENT_SIZE / FIX_BLOCK_SIZE)
#define APPEND_COMPACTION_GARBAGE_THRESHOLD (128ULL * 1024ULL * 1024ULL)
#define APPEND_COMPACTION_SLEEPING_MAX_THREADS 32U
#define APPEND_COMPACTION_IDLE_STREAK_REQUIRED 2U
#define APPEND_LOGICAL_EOF_MARKER UINT32_MAX
#define APPEND_LOGICAL_EOF_MARKER_SIZE FIX_BSIZE_FACTOR
static inline bool is_append_logical_eof_marker(const uint8_t *data, size_t available) {
    uint32_t marker = 0;

    if (data == NULL || available < sizeof(marker))
        return false;
    memcpy(&marker, data, sizeof(marker));
    return marker == APPEND_LOGICAL_EOF_MARKER;
}

static inline void write_append_logical_eof_marker(uint8_t *data) {
    uint32_t marker = APPEND_LOGICAL_EOF_MARKER;
    memset(data, 0, APPEND_LOGICAL_EOF_MARKER_SIZE);
    memcpy(data, &marker, sizeof(marker));
}
#define BLOCK_SIZE_Z 255
#define ZIP_OVERHEAD_M 10
#define VOIDS 256
#define MAX_NS_SIZE 1024
#define MAX_TBL_SIZE 1024
#define UUID_SIZE 18
#define MAX_FN_SIZE 4096
#define MAX_TABLES_LIMIT 1 // TODO Решить проблему при ресайзе
#define MAX_REGIONS_LIMIT 4
#define MAX_REGION_WAIT_LOCK_M 100
#define MSG_SIZE_AUTH_RESP 6
#define SCAN_DATA_BLOCK_TRY_NUM_SIZE 8
#define MSG_SIZE_SCAN_DATA_POS 12
#define SCAN_RES_POS 16
#define SCAN_NONE_RESP 5
#define MSG_SIZE_SCAN_NONE_RESP_OLD 9
#define MSG_SIZE_SCAN_NONE_RESP 17
#define INIT_COLL_KEEPER 1
#define OFFSET_COLL_KEEPER_SIZE 3
#define DIGEST_SIZE_SMALL 16
#define DIGEST_SIZE_BIG 20
#define COLL_IDX_SIZE 4
#define DIGEST_SIZE_FULL (DIGEST_SIZE_SMALL + DIGEST_SIZE_BIG)
#define COLL_KEEPER_SIZE (DIGEST_SIZE_FULL + OFFSET_COLL_KEEPER_SIZE + COLL_IDX_SIZE)
#define STATUS_UNASSIGN_REGION 0
#define STATUS_ASSIGN_REGION 1
#define STATUS_STOP_REGION 2
#define STATUS_START_REGION 3
#define STATUS_SHUTDOWN 4
typedef enum rs_shutdown_state_t {
    RS_SHUTDOWN_RUNNING = 0,
    RS_SHUTDOWN_QUIESCING,
    RS_SHUTDOWN_COMPACTING,
    RS_SHUTDOWN_WAITING_RPS,
    RS_SHUTDOWN_READY,
    RS_SHUTDOWN_FAILED
} rs_shutdown_state_t;
extern _Atomic int rs_shutdown_state;
typedef enum shutdown_region_state_t {
    SHUTDOWN_REGION_IDLE = 0,
    SHUTDOWN_REGION_RETRY_LOCK,
    SHUTDOWN_REGION_NEED_MARK,
    SHUTDOWN_REGION_CLOSED,
    SHUTDOWN_REGION_MARKED
} shutdown_region_state_t;
#define MSG_SIZE_NEW_REGION_ORDER 8
#define MSG_SIZE_RESERVE_RS_ANSWER 8
#define MAX_SERVICE_REST_MSG_SIZE MSG_SIZE_NEW_REGION_ORDER
#define REGION_BYTE_RANGE 8
#define LARGE_RECORD_LIMIT (REAL_MAX_MSG_SIZE - MAX_BSIZE_FACTOR)
#if !ENTERPRISE && LARGE_RECORD_LIMIT > (REAL_MAX_MSG_SIZE - MAX_BSIZE_FACTOR)
#error CE record limit must leave room for the ordinary-record allocation margin
#endif
#define RECORD_TOO_LARGE_ERROR 100
#define RECORD_TOO_LARGE_RESULT (-20)
#define INVALID_REGION_MAP 11
#define CURRENT_REGION_MAP 12
#define REGION_IS_DISABLED 13
#define MSG_T_STAT 14
#define LOCK_ID_FIND_REGION 12
#define FLUSH_TABLE_LOCK 29
#define FLUSH_REGION_LOCK 114
#define MIN_REGION_MAP_TOKEN_COUNT 7
#define MAX_PORT_NUM 5
#define MAX_VOID_ELEMENTS 32
#define VOID_ELEMENTS_TRY_COUNT 3
#define MSG_USAGE_STAT_HEADER 13
#define INIT_MSG_SIZE 4
#define INIT_MSG_TYPE 1
#define INIT_MSG_HDR (INIT_MSG_SIZE + INIT_MSG_TYPE) // TODO Переделать на структуру
#define INIT_MSG_CLIENT_NS_POS 0
#define INIT_MSG_CLIENT_TABLE_POS 1
#define INIT_MSG_CLIENT_UUIDS_POS 2
#define MAX_RS_MSG_SIZE 1024 * 16
#define LIMIT (1000000)
#define TRY_REFRESH_REGION_MAP_LIMIT 3
#define RETRY_JOIN_REPLICA_SLEEP 10
#define MAX_RETRY_JOIN_REPLICA_NUM 100
#define DATA_BLOCK_SIZE_STR "datablocksize-"
#define COMRESSION_STR "compress-"
#define STATIC_ASSERT(cond, msg) typedef char static_assertion_##msg[(cond) ? 1 : -1]
STATIC_ASSERT(BLOCK_MAX_SIZE < CURR_MAX_MSG_SIZE, block_size_check_failed);
typedef struct void_info_t {
    _Atomic uint32_t void_count_tot;
    _Atomic uint32_t void_count_curr;
    uint32_t void_arr[MAX_VOID_ELEMENTS];
} void_info_t;
typedef struct replica_info_t {
    _Atomic int8_t replica_num;
    char *replica1;
    _Atomic int replica1_port;
    char *replica2;
    _Atomic int replica2_port;
    _Atomic char replica1_sock_waiting_conn;
    _Atomic char replica2_sock_waiting_conn;
    _Atomic int replica1_sock_fd;
    _Atomic int replica2_sock_fd;
    _Atomic int replica1_region_pos;
    _Atomic int replica2_region_pos;
    _Atomic uint32_t request_params_size;
    uint8_t *request_params;
    struct region_info_t *r_info1;
    struct region_info_t *r_info2;
    _Atomic uint8_t replica_control;
    uint32_t reply_retry_timeout[2];
} replica_info_t;
typedef struct region_idx_helper_t {
    uint8_t b;
    uint32_t k_idx;
    uint32_t k_idx_orig;
    uint32_t c_idx;
    uint8_t k3;
    uint8_t k2;
    uint8_t k1;
} region_idx_helper_t;
#define J_INDEX_SIZE 7
#define S_INDEX_SIZE 12
#define IDX_OVERHEAD_MAX 4096
#define CHUNK_GUARD (IDX_OVERHEAD_MAX * 2u)
#define PURE_IDX_MIN_SIZE 16384
#define MAX_HASH_SIZE 64
#define O_IDX_MAX_BIT 24
#define O_IDX_MAX_SIZE 16777216
#define CRC_COUNT_LEN 25
typedef struct idx_t {
    unsigned int s_index : S_INDEX_SIZE;
    unsigned int j_index : J_INDEX_SIZE;
    unsigned int o_index : O_IDX_MAX_BIT;
#ifdef COLL_LOB_TEST
    unsigned int c_index : 8;
#else
    unsigned int c_index : 24;
#endif
    unsigned int z_index : 8;
    unsigned int k3_index : 6;
} __attribute__((packed)) idx_t;
union i2b_u {
    uint32_t integer_val;
    uint8_t byte[4];
};
union l2b_u {
    uint64_t long_val;
    uint8_t byte[8];
};
struct dic_info {
    uint16_t size;
    char qfl_dic[0];
};
typedef struct lob_map_t {
    uint64_t l_offset;
    uint32_t l_size;
    uint8_t align_size;
    uint64_t lob_id;
} __attribute__((packed)) lob_map_t;
typedef struct index_memory_sizes_t {
    size_t idx[BUCKETS], idx_x2[BUCKETS];
    size_t k1_index[BUCKETS], k1_index_x2[BUCKETS];
    size_t k2_index[BUCKETS], k2_index_x2[BUCKETS];
    size_t coll_keeper[BUCKETS], coll_keeper_x2[BUCKETS], key_keeper[BUCKETS];
    size_t idx_ptrs, idx_x2_ptrs, k1_index_ptrs, k1_index_x2_ptrs;
    size_t k2_index_ptrs, k2_index_x2_ptrs;
    size_t coll_keeper_ptrs, coll_keeper_x2_ptrs, key_keeper_ptrs;
    size_t idx_r_help, idx_r_help_x2;
    size_t resize_buff, resize_args;
} index_memory_sizes_t;
typedef struct idx_region_info_t {
    index_memory_sizes_t memory;
    uint8_t pos;
    char *volatile dir_name;
    char *volatile fname;
    int volatile fd[BUCKETS];
    _Atomic uint64_t f_idx_size[BUCKETS];
    uint32_t volatile idx_mask[BUCKETS];
    _Atomic uint64_t f_idx_curr_size[BUCKETS];
    uint32_t volatile b_offset[BUCKETS];
    struct idx_t **idx;
    uint8_t **k2_index;
    uint8_t **k1_index;
    struct idx_t **idx_x2;
    uint8_t **k2_index_x2;
    uint8_t **k1_index_x2;
    uint8_t **volatile coll_keeper;
    _Atomic int coll_count_curr[BUCKETS];
    _Atomic int coll_count_tot[BUCKETS];
    uint8_t **volatile coll_keeper_x2;
    _Atomic int coll_count_curr_x2[BUCKETS];
    _Atomic int coll_count_tot_x2[BUCKETS];
    uint8_t **volatile key_keeper;
    _Atomic int key_count_curr[BUCKETS];
    _Atomic int key_count_tot[BUCKETS];
    void_info_t void_info[BUCKETS][VOIDS];
    uint8_t volatile max_void_pos;
    uint8_t volatile max_void_count;
    struct region_idx_helper_t *idx_r_help;
    uint32_t volatile count_max_idx_shift;
    uint32_t volatile count_max_idx_jump;
    struct region_idx_helper_t *idx_r_help_x2;
    _Atomic uint32_t count_max_idx_shift_x2;
    _Atomic uint32_t count_max_idx_jump_x2;
    uint16_t bsize_factor;
    uint32_t block_size;
    uint32_t block_useful_size;
    uint32_t block_size_zip;
    bool moved_coll;
} idx_region_info_t;
typedef struct keys_storage_t {
    char *keys;
    char *key_sizes;
    uint32_t *block_num;
    uint64_t keys_mem_usage_limit;
    uint64_t max_key_count;
    uint64_t k_count;
} __attribute__((packed)) keys_storage_t;
typedef struct keys_double_t {
    char *keys;
    char *key_sizes;
    uint32_t *block_del;
    uint64_t k_count;
    uint64_t max_key_count;
    uint64_t keys_mem_limit;
    uint64_t keys_offset;
    uint64_t wal_count;
} __attribute__((packed)) keys_double_t;
typedef struct data_keeper_t {
    uint32_t file_name_size;
    uint8_t *file_name;
    uint32_t file_pos;
    uint32_t buffer_key_size;
    uint8_t *buffer_key;
    uint32_t buffer_block_data_size;
    uint8_t *buffer_block_data;
} data_keeper_t;
#define XXH128_SIZE 16
typedef struct msg_hsh_info_t {
    uint64_t position;
    uint8_t data_state;
    uint8_t *xxh128;
} msg_hsh_info_t;
typedef struct msg_file_info_t {
    uint32_t file_name_size;
    uint8_t *file_name;
    uint64_t file_size;
    uint64_t file_ts;
    uint64_t num_blocks;
    msg_hsh_info_t **msg_hsh_info;
} msg_file_info_t;
typedef struct msg_files_list_t {
    uint32_t msg_size;
    uint8_t type;
    uint8_t req_count_chk1;
    uint8_t req_count_chk2;
    uint8_t req_count_chk3;
    uint32_t files_count;
    uint8_t close_state;
    msg_file_info_t *fi;
} msg_files_list_t;
typedef struct online_debug_info_t {
    char *namespace;
    char *table_name;
    char *uuid;
    uint8_t log_level;
} online_debug_info_t;
#define MAX_RECORDS 5000
#define QFL_INFO_CAP 4096
typedef struct region_info_t {
    _Atomic uint8_t r_lock_id;
    _Atomic uint8_t r_lock_id_before;
    _Atomic uint8_t r_call_id_unlock;
    _Atomic uint64_t curr_cid_lock;
    _Atomic uint32_t num;
    pthread_mutex_t rlock;
    pthread_mutex_t control_mutex;
    _Atomic uint64_t freed;
    _Atomic uint8_t status;
    char *uuid;
    char *r_fname;
    replica_info_t *replica;
    _Atomic bool has_parent_replica;
    _Atomic int waiting_count_lock;
    _Atomic int waiting_count_admin;
    _Atomic int waiting_count_reconnect;
    _Atomic bool replica_stoped;
    _Atomic char is_freed;
    _Atomic char dont_free_now;
    uint8_t *k_beg;
    uint8_t *k_end;
    struct idx_region_info_t **r_idx_b;
    _Atomic uint64_t index_memory_bytes;
    _Atomic uint64_t auxiliary_memory_bytes;
    uint64_t auxiliary_memory_sample_ms;
    size_t index_object_bytes[BLOCK_MAX_SIZE_M];
    _Atomic char max_idx_b;
    uint8_t bs_map[BLOCK_MAX_SIZE_M];
    struct lob_map_t *lob_map;
    _Atomic long lob_count_total;
    uint64_t lob_count_curr;
    bool hash_idx;
    uint32_t max_file_bsize;
    _Atomic long req_counter;
    _Atomic long req_ins_counter;
    _Atomic long req_upd_counter;
    _Atomic long req_get_counter;
    _Atomic long req_del_counter;
    _Atomic long req_scn_counter;
    _Atomic long req_rep_counter;
    _Atomic long req_put_counter_sum;
    _Atomic long req_get_counter_sum;
    _Atomic long req_del_counter_sum;
    _Atomic long req_scn_counter_sum;
    record_version_state_t record_versions;
    _Atomic uint64_t pending_delete;
    record_gc_state_t record_gc;
    _Atomic uint64_t record_gc_generation;
    _Atomic uint64_t record_gc_next_visit_ms;
    _Atomic bool record_gc_forced;
    uint8_t *rbuff;
    uint64_t rbuff_size;
    uint8_t *zbuff;
    uint64_t zbuff_size;
    uint8_t *bbuff;
    uint64_t bbuff_size;
    uint8_t *write_4K_buff;
    uint32_t write_4K_buff_used;
    _Atomic uint64_t write_4K_buffered_since_ms;
    uint64_t write_4K_file_offset;
    bool write_4K_cache_ok;
    uint64_t write_4K_cache_block_offset;
    int write_4K_fd;
    uint8_t *append_compact_dirty_map;
    uint32_t append_compact_dirty_map_size;
    _Atomic uint32_t append_compact_dirty_segments;
    uint64_t append_compact_garbage_bytes;
    uint64_t append_compact_occupied_bytes;
    uint32_t append_compact_cursor_segment;
    _Atomic bool append_compaction_requested;
    _Atomic uint64_t append_compaction_requested_since_ms;
    _Atomic uint64_t append_compaction_retry_after_ms;
    _Atomic bool append_compact_truncate_pending;
    _Atomic bool append_compaction_lock_held;
    _Atomic uint64_t append_compact_last_activity_sum;
    _Atomic uint32_t append_compact_idle_streak;
    uint64_t append_compact_diag_next_ms[2];
    uint64_t append_compact_diag_attempts;
    uint64_t append_compact_diag_applied;
    uint32_t append_compact_diag_segment;
    int append_compact_diag_result;
    int data_000_fd;
    uint8_t *block_comp_ins_buff;
    uint32_t block_comp_ins_buff_size;
    uint8_t *block_comp_buff;
    uint32_t block_comp_buff_size;
    uint8_t *scan_key;
    uint32_t scan_key_size;
    uint8_t **resize_buff;
    uint32_t *resize_buff_size;
    uint8_t *lob_buff;
    uint8_t *wal_lob_buff;
    uint32_t lob_in_bbuff;
    uint8_t *rsc_msg;
    struct msg_cli_t *msg_get;
    struct msg_cli_t *msg_get_put;
    struct msg_cli_t *msg_get_put_link;
    uint32_t msg_get_put_allocated_key_size;
    uint32_t msg_get_put_allocated_cf_size;
    uint32_t msg_get_put_allocated_qfl_size;
    struct msg_cli_t *msg_get_link;
    struct msg_cli_t *msg_get_rotate;
    bool is_leader;
    _Atomic bool stop;
    _Atomic bool bad_data;
    _Atomic bool already_finishing;
    _Atomic uint8_t idx_resizer;
    _Atomic uint8_t recovery_path_ready;
    _Atomic uint8_t shutdown_region_state;
    bool msg_cli_dict_done;
    bool msg_get_dict_done;
    char **qfl_dic;
    uint16_t *qfl_dic_size;
    int qfl_dic_lens;
    int qfl_dic_total_size;
    uint16_t max_dict_qfl_len;
    uint8_t digest[DIGEST_SIZE_FULL];
    bool has_digest_calc;
    _Atomic int scan_keys_list_count_curr;
    _Atomic int scan_keys_list_count_tot;
    struct scan_keys_list_t **scan_keys_list;
    struct scan_vars_t *scan_vars;
    keys_double_t **k_dbl;
    bool drop_lob;
    _Atomic uint64_t r_need_data_chk;
    _Atomic uint32_t req_count_chk;
    msg_files_list_t *files_list;
    uint32_t max_lob_id;
    bool bulk_mode;
    uint8_t *bulk_buff1;
    uint32_t bulk_buff_size1;
    uint32_t bulk_buff_offset1;
    uint8_t *bulk_buff2;
    uint32_t bulk_buff_size2;
    uint32_t bulk_buff_offset2;
    uint8_t z_stat_size;
    uint8_t **z_stat;
    float avg_zip_rate;
    uint8_t enable_block_compression;
    void *snappy_work_buff;
    uint8_t enable_record_compression;
    uint8_t enable_dict;
    _Atomic char is_write;
    _Atomic char is_replicate;
    _Atomic bool replicas_idle_committed;
    _Atomic uint64_t replicas_idle_retry_after_ms;
    _Atomic uint8_t log_level;
    char *qfl_info;
    uint64_t record_target_count;
    pthread_mutex_t idx_resize_coll_lock;
    pthread_mutex_t idx_resize_help_lock;
} region_info_t;
static inline void invalidate_record_gc(region_info_t *r_info) {
    if (r_info != NULL)
        atomic_fetch_add(&r_info->record_gc_generation, 1);
}


static inline void reset_record_gc(region_info_t *r_info) {
    r_info->record_gc = (record_gc_state_t){0};
    atomic_store(&r_info->record_gc_next_visit_ms, 0);
    atomic_store(&r_info->record_gc_forced, false);
    invalidate_record_gc(r_info);
}

static inline void invalidate_replicas_idle_commit(region_info_t *r_info) {
    if (r_info != NULL)
        atomic_store_explicit(&r_info->replicas_idle_committed, false, memory_order_relaxed);
}

enum { ADD_CLI_FD_OK = 0, ADD_CLI_FD_ERR_EXISTS = -1, ADD_CLI_FD_ERR_REALLOC = -2, ADD_CLI_FD_ERR_BADARG = -3 };
_Atomic char STOP_THE_WORLD;
_Atomic int worker_count;
typedef struct table_info_t {
    _Atomic char need_lock;
    _Atomic char t_lock_id;
    _Atomic uint8_t t_lock_id_before;
    _Atomic uint8_t t_call_id_unlock;
    pthread_mutex_t tlock;
    bool retired;
    char *ns;
    char *table;
    char *fullname;
    _Atomic int srv_tbl_version;
    region_info_t **volatile region_info;
    _Atomic int region_count_curr;
    _Atomic int region_count_total;
    _Atomic int region_count_curr_assigned;
    int *cli_fd;
    size_t count_cli_fd;
} table_info_t;
typedef struct tables_snapshot_t {
    table_info_t **tables;
    size_t capacity;
    int count;
} tables_snapshot_t;
struct connection_t;
struct table_handler_t;
typedef enum { WR_CLIENT = 0, WR_REPLICA = 1 } worker_role_t;
typedef struct {
    _Atomic int epoll_fd;
    struct connection_t **clients;
    _Atomic int num_clients;
    atomic_uint active_tasks;
    atomic_ulong last_progress_ns;
    pthread_mutex_t clients_lock;
    atomic_bool is_quarantined;
    atomic_bool ready_to_migrate;
    atomic_bool ready_to_epoll;
    _Atomic worker_role_t role;
    atomic_ulong os_tid;
    atomic_ulong gen;
    atomic_bool running;
    atomic_bool migrate_parked;
    _Atomic pthread_t self;
} worker_t;
static worker_t *workers_lower = NULL;
static worker_t *workers_upper = NULL;
static pthread_t *worker_threads_lower = NULL;
static pthread_t *worker_threads_upper = NULL;
static _Atomic int worker_count_lower = 0;
static _Atomic int worker_count_upper = 0;
static unsigned int max_workers_lower = 0;
static unsigned int max_workers_upper = 0;
static pthread_mutex_t worker_mutex_lower = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t worker_mutex_upper = PTHREAD_MUTEX_INITIALIZER;
typedef enum { STATE_AUTH_PENDING, STATE_AUTHENTICATED, STATE_WORKING, STATE_FREED } conn_state_t;
typedef struct connection_t {
    connection_transport_t transport;
    struct table_handler_t *t_handler;
    uint8_t *buff;
    _Atomic int state;
    int mode;
    _Atomic int in_processing;
    _Atomic int owner_wi;
    _Atomic int owner_idx;
    _Atomic int owner_gen;
    bool has_table;
    _Atomic int migrate_count;
    worker_t *worker;
} connection_t;
typedef struct table_handler_t {
    uint32_t t_pos;
    int32_t r_pos;
    uint32_t cli_tbl_version;
    uint64_t cid;
    uint64_t cli_req_count;
    uint64_t cli_void_count;
    _Atomic bool is_client;
    _Atomic bool is_replica_client;
    char *recovery_info_path;
    _Atomic bool leader_commit;
    _Atomic long all_req_count;
    _Atomic long read_req_count;
    _Atomic long mutation_req_count;
    struct table_info_t **t_info;
    uint32_t *cli_tbl_versions;
    uint32_t t_info_size;
    uint32_t t_info_curr;
    struct msg_srv_t *msg_srv;
    struct msg_cli_t *msg_cli;
    uint32_t msg_cli_allocated_key_size;
    connection_transport_t *transport;
    char *ip;
    char *port;
    _Atomic int resizing;
    uint8_t *buff;
    uint32_t buff_size;
    bool need_regions_map;
    bool realloc_buff;
    uint8_t *big_buff;
    uint32_t big_buff_size;
    bool realloc_big_buff;
    bool always_blocking_mode;
    struct connection_t *conn_father;
    double d_avg_send_msg_size;
    double d_avg_recv_msg_size;
    uint32_t client_timeout_ms;
    double rtt_us_avg;
    uint64_t rtt_us_count;
    _Atomic long recv_ts;
    struct append_compaction_piggyback_t *append_compaction_piggyback;
    struct region_info_t *admin_data_check_region;
    record_mutation_t *record_mutation;
} table_handler_t;
_Static_assert(sizeof(table_handler_t) == 280, "table_handler_t size mismatch");
typedef struct pthread_arg_rs_t {
    connection_transport_t transport;
    struct sockaddr_in client_address;
    uint64_t cid;
} pthread_arg_rs_t;
struct table_info_t **table_info;
pthread_mutex_t tables_lock;
_Atomic bool tables_resize_pending;
_Atomic uint32_t tables_readers;
_Atomic int table_count_curr;
volatile uint32_t table_count_total;
pthread_mutex_t uuid_lock;
void init_table_region_slots();
pthread_mutex_t slock;
volatile int sockets_count;
#define MAX_SOCKETS 1024 * 1204
int sockets_store[MAX_SOCKETS]; // TODO Unlim
void close_socket(struct region_info_t *r_info, uint8_t replica_num, int socket_fd);
int offset4d(int x, int y, int z, int t, struct region_info_t *r_info);
bool try_get_t_lock(struct table_info_t *t_info, uint32_t new_t_lock_id);
bool get_t_lock(struct table_info_t *t_info, uint32_t new_t_lock_id, bool do_log, uint64_t cid);
void do_t_unlock(struct table_info_t *t_info, uint32_t call_id, bool do_log, uint64_t cid);
void begin_tables_read(void);
void end_tables_read(void);
void begin_tables_resize(void);
void end_tables_resize(void);
bool take_tables_snapshot(tables_snapshot_t *snapshot, uint64_t cid);
void free_tables_snapshot(void *snapshot);
bool get_r_lock(struct region_info_t *r_info, uint32_t new_r_lock_id, bool do_log, bool force_lock, table_handler_t *t_handler);
bool do_r_unlock(struct region_info_t *r_info, uint32_t r_call_id, bool do_log, table_handler_t *t_handler);
bool mutex_creator(pthread_mutex_t *lock);
int del_cli_fd(int fd, table_info_t *t_info);
bool delete_all_key_doubles(struct region_info_t *r_info, keys_double_t *kd, uint8_t bs_del, table_handler_t *t_handler);
bool finish_index_recovery(region_info_t *r_info, bool recover_lob_wal, table_handler_t *t_handler);
void free_double_keys_keeper(struct region_info_t *r_info, uint8_t bs);
bool create_double_keys_keeper(struct region_info_t *r_info, uint8_t bs);
uint32_t idx_builder(struct region_info_t *r_info, uint8_t bs, uint32_t block_size, table_handler_t *t_handler);
bool load_dict(region_info_t *r_info, table_handler_t *t_handler);
void *add_new_region(void *arg);
bool is_rs_shutdown_command(const char *command);
bool rs_shutdown_admin_enter(const char *uuid);
void rs_shutdown_admin_leave(void *unused);
bool process_rs_shutdown(table_handler_t *t_handler);
void rs_shutdown_replica_request_begin(void);
void rs_shutdown_replica_request_end(void *unused);
void request_malloc_trim(void);
void *pthread_malloc_trim(void *arg);
region_info_t *build_new_region(struct table_info_t *t_info, char *r_uuid, char *r_fname, uint32_t r_cmd, char *r_key_beg, char *r_key_end, char *r_replica1, int32_t r_replica1_port,
                                char *r_replica2, int32_t r_replica2_port, char *r_compress, table_handler_t *t_handler);
struct table_info_t *find_region_t_info(char *ns, char *table, char *region_uuid, table_handler_t *t_handler, struct table_info_t *t_info);
struct region_info_t *find_region_r_info(char *ns, char *table, char *region_uuid, table_handler_t *t_handler, struct table_info_t *t_info);
bool resize_regions(struct table_info_t *t_info, uint64_t cid);
bool resize_tables(uint64_t cid);
void relicas_unassign_cmd(region_info_t *r_info, table_handler_t *t_handler);
bool replicas_shutdown_cmd(region_info_t *r_info, table_handler_t *t_handler);
bool set_replica_local(struct region_info_t *r_info, struct table_info_t *t_info, char *r_replica, char *r_uuid, uint8_t r_num, table_handler_t *t_handler, bool *same_instance);
table_handler_t *create_t_handler(int new_socket_fd);
void free_t_handler(table_handler_t *t_handler);
uint64_t get_new_cid();
uint64_t proc_id;
int parent_pid;
char *prop_file_name;
int client_port, replication_port, max_file_size, initial_idx_coeff, rf, blocking_socket_mode;
int enable_compression, enable_dict, enable_block_compression;
_Atomic int log_level, max_rs_cmd_num;
_Atomic int drive_free_mb_limit, scan_wait_client_msec, cli_sock_timeout, replica_sock_timeout;
int check_double_keys, merge_keys, rebuild_index, data_check_enabled;
int meta_crc_enable;
uint8_t h_count;
int replicas_last_change_time_check_diff_ms;
_Atomic int old_ts, region_border_exclusive;
_Atomic int stop_resize_worker_pool, replicas_chk_delay_sec;
_Atomic int mem_free_limit_percentage, create_idx_resize_backup;
int num_workers, max_workers;
_Atomic int worker_client_idle_timeout, worker_replica_idle_timeout, idx_resize_max_num_chunk;
_Atomic int max_record_size, unpack_lob_to_client, hbase_scan_filter, allow_scan_lob, new_scan_result;
_Atomic int rtt_fresh_idle_limit_ms, rtt_client_get_stat_count, rtt_replica_get_stat_count, rtt_client_print_stat_count, rtt_replica_print_stat_count;
_Atomic int rewrite_dict_flag;
char *health_file_name;
char *exit_file_name;
char *recovery_info_fn;
char *debug_file_name;
_Atomic int online_debug_tables_count;
struct online_debug_info_t **online_debug_info;
char *log_path;
_Atomic int block_epoll_worker_timeout_ms;
_Atomic int log_rotation_seconds;
_Atomic char need_reopen;
_Atomic int set_socket_send_buffer_kb;
_Atomic int set_socket_receive_buffer_kb;
_Atomic int enable_write_4k_cache;
_Atomic uint64_t ops;
_Atomic int append_compact_start_percent, append_compact_client_stop_percent, append_compact_max_segment_per_visit, append_compact_ops_limit, append_compact_piggyback_enabled;
_Atomic int append_compact_pending_delete_limit;
_Atomic int append_compact_sleeping_ops_limit, append_compact_sleeping_thread_count;
_Atomic int append_compact_min_percent, append_compact_thread_count, set_tcp_nodelay;
_Atomic uint32_t append_compact_low_ops_streak;
_Atomic uint32_t append_compact_sleeping_ops_streak;
_Atomic bool append_compact_truncate_allowed;
_Atomic uint64_t append_compact_metric_garbage_generated_bytes, append_compact_metric_garbage_removed_bytes;
_Atomic uint64_t append_compact_metric_lock_attempts, append_compact_metric_lock_misses, append_compact_metric_lock_acquired;
_Atomic uint64_t append_compact_metric_request_wait_ms, append_compact_metric_request_wait_max_ms, append_compact_metric_request_wait_samples, append_compact_metric_lock_hold_us;
_Atomic uint64_t append_compact_metric_build_us, append_compact_metric_apply_us, append_compact_metric_segments, append_compact_metric_client_acquires_waiting;
_Atomic uint64_t append_compact_metric_changed_blocks, append_compact_metric_sent_blocks;
_Atomic uint64_t append_compact_metric_build_prepare_us, append_compact_metric_build_source_read_us, append_compact_metric_build_source_lookup_us,
    append_compact_metric_build_source_pack_us;
_Atomic uint64_t append_compact_metric_build_tail_read_us, append_compact_metric_build_tail_lookup_us, append_compact_metric_build_tail_pack_us, append_compact_metric_build_verify_us,
    append_compact_metric_build_hash_us;
_Atomic uint64_t append_compact_metric_build_source_bytes, append_compact_metric_build_tail_bytes, append_compact_metric_build_records, append_compact_metric_build_index_lookups;
_Atomic uint64_t append_compact_metric_apply_flush_us, append_compact_metric_apply_validate_us, append_compact_metric_apply_replica_us, append_compact_metric_apply_index_us,
    append_compact_metric_apply_scans_us, append_compact_metric_apply_zindex_us, append_compact_metric_apply_truncate_us, append_compact_metric_apply_accounting_us;
_Atomic uint64_t append_compact_metric_replica_send_us, append_compact_metric_replica_local_write_us, append_compact_metric_replica_recv_us, append_compact_metric_replica_retries,
    append_compact_metric_replica_bytes;
_Atomic uint64_t append_compact_metric_client_lock_attempts, append_compact_metric_client_lock_contended, append_compact_metric_client_lock_contended_by_compactor;
_Atomic uint64_t append_compact_metric_client_lock_wait_us, append_compact_metric_client_lock_wait_compactor_us, append_compact_metric_client_lock_wait_max_us,
    append_compact_metric_client_lock_wait_hist[8];
_Atomic uint64_t append_compact_metric_client_lock_hold_us, append_compact_metric_client_lock_hold_max_us, append_compact_metric_client_lock_hold_samples;
_Atomic uint64_t append_compact_metric_client_block_maker_us, append_compact_metric_client_bulk_process_us, append_compact_metric_client_bulk_replica_us;
_Atomic uint64_t append_compact_metric_slow_log_last_ms;
_Atomic uint64_t append_compact_metric_v2_source_matches, append_compact_metric_v2_source_mismatches, append_compact_metric_v2_replay_matches;
_Atomic uint64_t append_compact_metric_v2_state_failures, append_compact_metric_v2_final_probe_calls;
_Atomic uint64_t append_compact_metric_v2_commands, append_compact_metric_v2_total_us, append_compact_metric_v2_setup_us, append_compact_metric_v2_source_verify_us;
_Atomic uint64_t append_compact_metric_v2_raw_read_us, append_compact_metric_v2_raw_read_calls, append_compact_metric_v2_raw_read_bytes, append_compact_metric_v2_block_verify_us;
_Atomic uint64_t append_compact_metric_v2_rebuild_us, append_compact_metric_v2_result_hash_us, append_compact_metric_v2_expected_hash_us, append_compact_metric_v2_final_probe_us;
_Atomic uint64_t append_compact_metric_v2_compact_write_us, append_compact_metric_v2_truncate_us, append_compact_metric_v2_ordinary_write_us, append_compact_metric_v2_fstat_us;
_Atomic uint64_t append_compact_metric_v2_context_init_us, append_compact_metric_v2_validate_us, append_compact_metric_v2_region_check_us, append_compact_metric_v2_target_us;
_Atomic uint64_t append_compact_metric_v2_preflush_us, append_compact_metric_v2_write_copy_us, append_compact_metric_v2_ordinary_write_call_us, append_compact_metric_v2_postflush_us;
_Atomic uint64_t append_compact_metric_v2_initial_fstat_us, append_compact_metric_v2_replay_fstat_us, append_compact_metric_v2_final_fstat_us, append_compact_metric_v2_cleanup_us,
    append_compact_metric_v2_fail_handler_us;
_Atomic uint64_t append_compact_metric_v2_workspace_contexts, append_compact_metric_v2_heap_contexts, append_compact_metric_v2_heap_write_copies;
_Atomic uint64_t append_compact_metric_v2_compact_write_calls, append_compact_metric_v2_compact_write_bytes, append_compact_metric_v2_ordinary_write_calls,
    append_compact_metric_v2_ordinary_write_bytes;
uint8_t rf_ctrl;
_Atomic int max_retry_num;
char *auth_mode;
int rs_port;
_Atomic int rs_cmd_processing;
_Atomic int stop_write;
_Atomic int stop_any_client_requests;
_Atomic int stop_new_client_requests;
#define MAX_LOCAL_IP_NUM 100
char local_ip[MAX_LOCAL_IP_NUM][INET_ADDRSTRLEN];
int local_ip_num;
char *hostname;
#endif
