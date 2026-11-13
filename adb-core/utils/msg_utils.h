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
#include "../crypto/xxhash.h"
#if !ENTERPRISE
bool ce_region_has_no_lob(const char *directory, uint64_t cid);
#endif
#define X_POS 0
#define Y_POS 1
#define Z_POS 2
#define T_POS 3
#define MIN_IDX_DIM 2
#define MIN_REC_SIZE 28 // TODO recalc
#define MSG_NIL 0
#define MSG_GET 1
#define MSG_PUT 2
#define MSG_DEL 6
#define MSG_DIR_WRITE 4
#define MSG_DIR_WRITE_TUNC 5
#define MSG_RES 7
#define MSG_AUTH 8
#define MSG_NO_AUTH 9
#define GET_REGION_MAP 10
#define MSG_STAT 14
#define MSG_SCN 15
#define MSG_RS 16
#define MSG_DENY 17
#define MSG_DEL_DOUBLE 20
#define MSG_CNT 21
#define MSG_DATA_CHK 22
#define MSG_GET_DOUBLE 23
#define MSG_DATA_FIX 24
#define MSG_DATA_GET 25
#define MSG_DATA_GET_RESULT 26
#define MSG_DEL_ALL 27
#define MSG_CHK_DOUBLE 23
#define MSG_RESP_OK 28
#define MSG_DATA_CHK_RESULT 29
#define MSG_BULK_GET 30
#define MSG_BULK_PUT 31
#define MSG_PUT_AND_GET 32
#define GET_REGION_MAP_TIMEOUT 33
#define MSG_UNASSIGN 34
#define MSG_DIR_WRITE_AND_COMPACT 35
#define MSG_BULK_GET_NIO 40
#define MSG_BULK_PUT_NIO 41
#define MSG_BULK_PUT_AND_GET_NIO 42
#define MSG_SCN_NIO 43
#define MSG_RES_BULK_GET_PUT 70
#define MSG_IDX_RESIZE 80
#define AUTH_READ_OK 80
#define AUTH_READ_FAILED 81
#define AUTH_MUTATION_OK 82
#define AUTH_MUTATION_FAILED 83
#define LONG_MAX_VALUE 9223372036854775807
#define MAX_REPLICA_COUNTER_MASK 0xFFFFFF
#define MSG_REPLICA_COUNTER_SIZE 3
#define MSG_REPLICA_COUNTER_OFFSET 5
#define MSG_SHORT_ANSWER_SIZE 5
#define MSG_PUT_OK 0x0200000005
#define MSG_AUTH_RESULT_OK 1
#define MSG_AUTH_RESULT_FAIL 0
#define MSG_SIZE_OFFSET 0
#define MSG_TYPE_OFFSET 4
#define INT_SIZE 4
#define LONG_SIZE 8
#define SHORT_SIZE 2
#define USED_META 8
#define LAST_CRC16 2
#define SEND_MSG_WINDOW_SIZE 15
#define RECV_MSG_WINDOW_SIZE 15
#define BUIK_RELICA_ON 1
#define BUIK_RELICA_OFF 0
#define MSG_REGION_POS_OFFSET 8
#define MSG_KEY_SIZE_OFFSET 12
#define MSG_VAL_SIZE_OFFSET 16
#define MSG_KEY_OFFSET 20
typedef struct msg_stat_t {
    uint32_t msg_size;
    uint8_t type;
    uint64_t ts_stat;
    char *full_stat;
} __attribute__((packed)) msg_stat_t;
typedef struct msg_auth_t {
    uint32_t msg_size;
    uint8_t type;
    uint32_t jwt_size;
    uint8_t *jwt;
    uint32_t ns_size;
    uint8_t *ns;
    uint32_t table_size;
    uint8_t *table;
} __attribute__((packed)) msg_auth_t;
typedef struct msg_rs_result_auth_t {
    uint32_t msg_size;
    uint8_t type;
    uint8_t result;
    uint64_t ts_expiration;
    uint8_t access_r;
    uint8_t access_w;
} __attribute__((packed)) msg_rs_result_auth_t;
typedef struct msg_client_result_auth_t {
    uint32_t msg_size;
    uint8_t type;
    uint8_t result;
} __attribute__((packed)) msg_client_result_auth_t;
typedef struct msg_rs_answer_t {
    uint32_t msg_size;
    uint8_t type;
    uint8_t result;
} __attribute__((packed)) msg_rs_answer_t;
#define MAX_DELETION_ENTRIES 128
typedef struct {
    uint32_t qfl_size;
    uint8_t *qfl_ptr;
} deletion_entry_t;
// TODO Do not write the whole message
struct msg_qv_t {
    uint64_t ts;
    uint32_t qfl_size;
    uint32_t val_size;
    uint8_t *qfl;
    uint8_t *val;
} __attribute__((packed)) msg_qv_t;
#define QV_SIZE_RECORD_OFFSET 24
#define ZIP_INFO_RECORD_OFFSET 14
#define NEW_VERSION_RECORD_OFFSET 15
#define KEY_SIZE_RECORD_OFFSET 16
#define CF_SIZE_RECORD_OFFSET 20
#define QV_SIZE_RECORD_OFFSET 24
#define KEY_RECORD_OFFSET 12
#define QV_QFL_RECORD_OFFSET 16
#define DICT_SIZE 2
#define TS_SIZE 8
#define QFL_SIZE_SIZE 4
#define VAL_SIZE_SIZE 4
#define MIN_QFL_DIC_SIZE 4
typedef struct msg_kv_t {
    uint32_t key_size;
    uint32_t cf_size;
    uint32_t qv_size;
    uint8_t *key;
    uint8_t *cf;
    struct msg_qv_t *qv;
} __attribute__((packed)) msg_kv_t;
typedef struct msg_cli_t {
    uint32_t msg_size;
    uint8_t type;
    uint32_t region_pos;
    uint32_t rec_crc;
    uint8_t lob_info;
    uint8_t zip_info;
    uint8_t new_version;
    struct msg_kv_t *kv;
} __attribute__((packed)) msg_cli_t;
typedef char record_version_layout_check[offsetof(msg_cli_t, region_pos) == RECORD_VERSION_OFFSET && offsetof(msg_cli_t, rec_crc) == RECORD_VERSION_OFFSET + 4U &&
                                                 offsetof(msg_cli_t, lob_info) == 13U && offsetof(msg_cli_t, zip_info) == 14U && offsetof(msg_cli_t, new_version) == 15U &&
                                                 offsetof(msg_cli_t, kv) == 16U && offsetof(msg_kv_t, key) == 12U && MIN_REC_SIZE == DISK_RECORD_HEADER_SIZE &&
                                                 KEY_SIZE_RECORD_OFFSET == 16U && CF_SIZE_RECORD_OFFSET == 20U && QV_SIZE_RECORD_OFFSET == 24U
                                             ? 1
                                             : -1];
typedef struct scan_vars_t {
    uint32_t unzip_msg_size_sum;
    uint32_t unzip_msg_size;
    uint32_t record_size_round_up;
    uint32_t record_size_round_up_unzip;
    uint32_t last_record_size_unzip;
    uint32_t tech_offset;
    uint32_t curr_record_offset;
    uint32_t last_record_size;
} __attribute__((packed)) scan_vars_t;
#define MSG_CLI_HBASE_SCAN_FILTER_SIZE 17
#define MSG_CLI_SCAN_FILTER_SIZE 33
#pragma pack(push, 1)
typedef struct msg_cli_scan_wire_hdr_t {
    uint32_t msg_size;
    uint8_t type;
    uint32_t region_pos;
    uint32_t block_num;
    uint16_t try_num;
    uint16_t reserve;
    uint32_t start_key_len;
    uint32_t end_key_len;
} msg_cli_scan_wire_hdr_t;
#pragma pack(pop)
#pragma pack(push, 1)
typedef struct msg_cli_scan_hbase_t {
    uint32_t msg_size;
    uint8_t type;
    uint32_t region_pos;
    uint32_t block_num;
    uint16_t try_num;
    uint16_t reserve;
    uint32_t start_key_len;
    const uint8_t *start_key;
    uint32_t end_key_len;
    const uint8_t *end_key;
} msg_cli_scan_hbase_t;
#pragma pack(pop)
typedef struct msg_cli_scan_t {
    uint32_t msg_size;
    uint8_t type;
    uint32_t region_pos;
    uint32_t block_num;
    uint16_t try_num;
    uint16_t reserve;
    uint8_t *start_key;
    uint8_t *end_key;
} __attribute__((packed)) msg_cli_scan_t;
typedef struct msg_cli_count_t {
    uint32_t msg_size;
    uint8_t type;
    uint32_t region_pos;
    uint64_t count;
} __attribute__((packed)) msg_cli_count_t;
typedef struct msg_info_t {
    uint32_t qv_size;
    uint32_t qfl_count;
    uint64_t max_ts;
} __attribute__((packed)) msg_info_t;
#pragma pack(push, 1)
typedef struct msg_rsc_t { // TODO Автоматический расчет
    uint32_t msg_size;
    uint8_t type;
    uint32_t region_pos;
    uint32_t req_count_chk;
    uint8_t uuid[UUID_SIZE];
    uint64_t offset;
    uint8_t bs;
    uint8_t buff[];
} __attribute__((packed)) msg_rsc_t;
#pragma pack(pop)
#define MSG_RSC_UUID_OFFSET offsetof(msg_rsc_t, uuid)
#define MSG_RSC_OFF_OFFSET offsetof(msg_rsc_t, offset)
#define MSG_RSC_BS_OFFSET offsetof(msg_rsc_t, bs)
#define MSG_RSC_BUFF_OFFSET offsetof(msg_rsc_t, buff)
#define MSG_WRITE_COMPACT_VERSION 1U
#define MSG_WRITE_COMPACT_VERSION_BITMAP 2U
#define MSG_WRITE_COMPACT_FLAG_TRUNCATE 0x01U
#define MSG_WRITE_COMPACT_KNOWN_FLAGS MSG_WRITE_COMPACT_FLAG_TRUNCATE
#define MSG_WRITE_COMPACT_HASH_SIZE 16U
typedef struct msg_write_compact_t {
    uint8_t version;
    uint8_t flags;
    uint8_t compact_b;
    uint8_t compact_bs;
    uint8_t write_b;
    uint8_t write_bs;
    uint32_t compact_size;
    uint32_t write_size;
    uint64_t compact_offset;
    uint64_t compact_old_file_size;
    uint64_t compact_new_file_size;
    uint64_t expected_final_file_size;
    uint64_t write_offset;
    uint8_t data[];
} __attribute__((packed)) msg_write_compact_t;
#define MSG_WRITE_COMPACT_HEADER_SIZE offsetof(msg_write_compact_t, data)
typedef struct msg_write_compact_block_meta_t {
    uint32_t block;
    uint16_t used_bytes;
    uint16_t record_count;
    uint8_t input_hash[MSG_WRITE_COMPACT_HASH_SIZE];
} __attribute__((packed)) msg_write_compact_block_meta_t;
typedef struct msg_write_compact_v2_t {
    uint8_t version;
    uint8_t flags;
    uint8_t compact_b;
    uint8_t compact_bs;
    uint8_t write_b;
    uint8_t write_bs;
    uint32_t segment;
    uint32_t first_block;
    uint32_t source_blocks;
    uint32_t tail_range_first_block;
    uint32_t tail_first_block;
    uint32_t tail_blocks;
    uint32_t original_b_offset;
    uint32_t new_b_offset;
    uint32_t source_record_count;
    uint32_t record_count;
    uint32_t bitmap_size;
    uint32_t block_meta_count;
    uint32_t write_size;
    uint64_t compact_old_file_size;
    uint64_t compact_new_file_size;
    uint64_t expected_final_file_size;
    uint64_t write_offset;
    uint64_t transaction_id;
    uint8_t source_hash[MSG_WRITE_COMPACT_HASH_SIZE];
    uint8_t result_hash[MSG_WRITE_COMPACT_HASH_SIZE];
    uint8_t final_hash[MSG_WRITE_COMPACT_HASH_SIZE];
    uint8_t data[];
} __attribute__((packed)) msg_write_compact_v2_t;
#define MSG_WRITE_COMPACT_V2_HEADER_SIZE offsetof(msg_write_compact_v2_t, data)
typedef enum msg_write_compact_result_t {
    MSG_WRITE_COMPACT_OK = 0,
    MSG_WRITE_COMPACT_INVALID_ARGUMENT = -1,
    MSG_WRITE_COMPACT_INVALID_VERSION = -2,
    MSG_WRITE_COMPACT_INVALID_FLAGS = -3,
    MSG_WRITE_COMPACT_SIZE_OVERFLOW = -4,
    MSG_WRITE_COMPACT_SIZE_MISMATCH = -5,
    MSG_WRITE_COMPACT_PAYLOAD_TOO_LARGE = -6,
    MSG_WRITE_COMPACT_INVALID_COMPACT_SIZE = -7,
    MSG_WRITE_COMPACT_INVALID_TARGET = -8,
    MSG_WRITE_COMPACT_INVALID_OFFSET = -9,
    MSG_WRITE_COMPACT_INVALID_FILE_SIZE = -10,
    MSG_WRITE_COMPACT_INVALID_FINAL_SIZE = -11,
    MSG_WRITE_COMPACT_INVALID_ORDER = -12,
    MSG_WRITE_COMPACT_INVALID_BLOCK_COUNT = -13,
    MSG_WRITE_COMPACT_INVALID_BLOCK_META = -14,
    MSG_WRITE_COMPACT_INVALID_BITMAP = -15
} msg_write_compact_result_t;
int validate_msg_write_compact(const uint8_t *payload, uint32_t payload_size, msg_write_compact_t *header, const uint8_t **compact_buffer, const uint8_t **write_buffer);
int serialize_msg_write_compact(uint8_t *payload, uint32_t payload_capacity, const msg_write_compact_t *header, const uint8_t *compact_buffer, const uint8_t *write_buffer,
                                uint32_t *payload_size);
int validate_msg_write_compact_v2(const uint8_t *payload, uint32_t payload_size, msg_write_compact_v2_t *header, const uint8_t **block_meta, const uint8_t **live_bitmap,
                                  const uint8_t **write_buffer);
int serialize_msg_write_compact_v2(uint8_t *payload, uint32_t payload_capacity, const msg_write_compact_v2_t *header, const msg_write_compact_block_meta_t *block_meta,
                                   const uint8_t *live_bitmap, const uint8_t *write_buffer, uint32_t *payload_size);
int msg_write_compact_hash128(const uint8_t *data, uint32_t size, uint8_t hash[MSG_WRITE_COMPACT_HASH_SIZE]);
int msg_write_compact_hash128_padded(const uint8_t *data, uint32_t size, uint32_t padded_size, uint8_t hash[MSG_WRITE_COMPACT_HASH_SIZE]);
int msg_write_compact_v2_source_hash(const msg_write_compact_block_meta_t *block_meta, uint32_t block_meta_count, uint8_t hash[MSG_WRITE_COMPACT_HASH_SIZE]);
int msg_write_compact_v2_final_hash(const msg_write_compact_v2_t *header, uint32_t retained_compact_size, const uint8_t compact_hash[MSG_WRITE_COMPACT_HASH_SIZE],
                                    uint32_t aligned_write_size, const uint8_t write_hash[MSG_WRITE_COMPACT_HASH_SIZE], uint8_t hash[MSG_WRITE_COMPACT_HASH_SIZE]);
const char *msg_write_compact_result_string(int result);
typedef struct msg_chk_t {
    uint32_t msg_size;
    uint8_t type;
    uint32_t region_pos;
    uint32_t req_count_chk;
    uint8_t *uuid;
} __attribute__((packed)) msg_chk_t;
#define STATIC_ASSERT(cond, msg) typedef char static_assertion_##msg[(cond) ? 1 : -1]
STATIC_ASSERT(sizeof(msg_chk_t) < sizeof(msg_rsc_t), size_check_failed);
STATIC_ASSERT(MSG_RSC_BUFF_OFFSET == MSG_RSC_HEADER_SIZE, rsc_header_size_mismatch);
STATIC_ASSERT(APPEND_COMPACTION_SEGMENT_SIZE <= RSC_MSG_MAX_PAYLOAD, append_compaction_segment_exceeds_rsc_payload);
STATIC_ASSERT(MSG_WRITE_COMPACT_HEADER_SIZE == 54U, write_compact_header_size_mismatch);
typedef struct msg_map_req_t {
    uint32_t msg_size;
    uint8_t type;
    uint8_t *namespace;
    uint8_t *table;
    uint8_t *UUIDs;
} __attribute__((packed)) msg_map_req_t;
typedef struct hostent_t {
    struct hostent *hostbuf;
    struct hostent *hp;
    char *tmphstbuf;
} hostent_t;
uint64_t buff_to_field(const uint8_t *buff, uint32_t *curr, uint32_t size);
char *extract_key(uint8_t *buff);
int realloc_buff_or_bbuff(table_handler_t *t_handler, uint8_t **buff_io, uint32_t required);
bool calc_msg_buff_size(uint64_t required, uint32_t *result, uint64_t cid);
int realloc_resp_buff(msg_srv_t *msg_srv, size_t required_free, uint64_t cid);
void data_tools_checkers(int argc, char *argv[]);
int ensure_bulk_buffer_capacity(uint8_t **buffer, uint32_t *capacity, uint32_t used, uint32_t additional, uint64_t cid, const char *buffer_name);
uint16_t bytes_to_msg_cont(msg_cli_t *msg_any, uint8_t *buff, uint32_t curr, region_info_t *r_info, uint64_t cid);
uint16_t get_put_bytes_to_msg_cont(msg_cli_t *msg_any, uint8_t *buff, uint32_t curr, region_info_t *r_info, uint32_t *allocated_qfl_size, uint64_t cid);
int16_t get_key_from_msg_bytes(msg_cli_t *msg_cli, uint8_t *buff, uint64_t cid);
int16_t get_put_key_from_msg_bytes(msg_cli_t *msg_cli, uint8_t *buff, uint32_t *max_key_size, uint32_t *max_cf_size, uint64_t cid);
int16_t get_key_from_msg_bytes_safety(msg_cli_t *msg_cli, uint8_t *buff);
#define KEY_EXTRACT_NOMEM (-7)
#define KEY_EXTRACT_INVALID_STATE (-8)
int16_t get_put_key_from_msg_bytes_checked(msg_cli_t *msg, const uint8_t *buff, uint32_t *max_key_size, uint32_t *max_cf_size, uint64_t cid);
int16_t get_key_from_msg_bytes_checked(msg_cli_t *msg, const uint8_t *buff, uint64_t cid);
int16_t get_key_from_msg_bytes_safety_checked(msg_cli_t *msg, const uint8_t *buff);
void clear_extracted_key_fields(msg_cli_t *msg);
uint32_t bytes_to_msg(msg_cli_t *msg_cli, uint8_t *buff, uint32_t available, table_handler_t *t_handler);
void free_get_msg(msg_cli_t *msg_cli);
void free_cli_msg(msg_cli_t *msg_cli);
void free_msg(msg_cli_t *msg_cli);
void stat_msg_to_bytes(msg_stat_t *msg_stat, uint8_t *buff);
void qv_to_bytes(struct msg_qv_t *qv, char *dbuff, char *sbuff);
hostent_t *thread_safe_gethostname(char *host);
int decompress_record(struct region_info_t *r_info, char *zip_buff, int zip_buff_size, char *zbuff, uint64_t dst_capacity, uint64_t cid);
bool check_endianness();
int format_validation_check_init_msg(uint8_t *buff, int rsize);
enum { DECOMPRESS_LZMA_NOMEM = 4, DECOMPRESS_LZMA_REALLOC_FAILED = 5, DECOMPRESS_LZMA_INVALID_SIZE = 7 };
uint64_t decompress_lzma(msg_srv_t *msg_srv, uint64_t data_size, uint64_t input_size, region_info_t *r_info, uint64_t *decompressed_size, uint64_t cid);
bool compress_lzma(const uint8_t *input_data, uint64_t input_size, region_info_t *r_info, uint64_t *compressed_size);
size_t align_up(size_t x, size_t align);
void rotate_block_messages(region_info_t *r_info, uint32_t total_size);
int compress_block(const uint8_t *input_data, uint64_t input_size, region_info_t *r_info, uint64_t *compressed_size, uint8_t snappy_block_factor, uint16_t records_count);
uint64_t decompress_block(uint8_t *sbuff, size_t input_size, uint32_t b_offset, uint8_t bs, region_info_t *r_info, uint64_t *decompressed_size, uint32_t result_offset,
                          table_handler_t *t_handler);
void print_full_request_infomation(region_info_t *r_info, char *msg_text, char *key_hex, msg_cli_t *msg_cli, uint8_t bs, table_handler_t *t_handler);
#define CRC16_POLYNOMIAL 0x1021
#define CRC16_TABLE_SIZE 256
uint16_t crc16(char *input_str, size_t num_bytes);
void generate_crc16_table();
uint16_t crc16_table[CRC16_TABLE_SIZE];
inline static uint32_t compute_crc32(const uint8_t *buff, uint32_t msg_size) {
    uint32_t crc32 = 0xFFFFFFFF;
    buff += offsetof(struct msg_cli_t, kv);
    msg_size -= offsetof(struct msg_cli_t, kv);
    uint32_t step = (msg_size + CRC_COUNT_LEN - 1) / CRC_COUNT_LEN;
    uint32_t start2 = step / 2;
    uint32_t count1 = (msg_size - 1) / step + 1;
    uint32_t count2 = (msg_size > start2) ? ((msg_size - start2 - 1) / step + 1) : 0;
    uint32_t k;

    for (k = 0; k < count2; k++) {
        uint32_t idx1 = k * step;
        uint32_t idx2 = start2 + k * step;
        uint16_t crc1 = (uint16_t)(crc32);
        uint8_t index1 = (uint8_t)((crc1 >> 8) ^ buff[idx1]);
        crc1 = (crc1 << 8) ^ crc16_table[index1];
        crc32 = (crc32 & 0xFFFF0000) | crc1;
        uint16_t crc2 = (uint16_t)(crc32 >> 16);
        uint8_t index2 = (uint8_t)((crc2 >> 8) ^ buff[idx2]);
        crc2 = (crc2 << 8) ^ crc16_table[index2];
        crc32 = (crc32 & 0x0000FFFF) | ((uint32_t)crc2 << 16);
    }

    if (count1 > count2) {
        uint32_t idx1 = k * step;
        uint16_t crc1 = (uint16_t)(crc32);
        uint8_t index = (uint8_t)((crc1 >> 8) ^ buff[idx1]);
        crc1 = (crc1 << 8) ^ crc16_table[index];
        crc32 = (crc32 & 0xFFFF0000) | crc1;
    }
    buff -= offsetof(struct msg_cli_t, kv);
    return crc32;
}


inline static void add_checksum_rec(uint8_t *buff, uint32_t msg_size) {
    uint32_t crc32 = compute_crc32(buff, msg_size);
    memcpy(buff + offsetof(struct msg_cli_t, rec_crc), &crc32, sizeof(crc32));
}


inline static bool chk_checksum_rec(uint8_t *buff, uint32_t msg_size) {
    uint32_t *file_crc32 = (uint32_t *)(buff + offsetof(struct msg_cli_t, rec_crc));

    if (*file_crc32 != compute_crc32(buff, msg_size))
        return false;
    return true;
}


inline static uint16_t get_checksum_rec(uint8_t *buff, uint32_t msg_size) {
    return compute_crc32(buff, msg_size);
}

inline static void add_checksum_rec(uint8_t *buff, uint32_t msg_size);
inline static bool chk_checksum_rec(uint8_t *buff, uint32_t msg_size);
inline static uint32_t compute_crc32(const uint8_t *buff, uint32_t msg_size);
