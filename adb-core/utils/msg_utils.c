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
#include "msg_utils.h"
#include "../handle/storage.h"
#if !ENTERPRISE
bool ce_region_has_no_lob(const char *directory, uint64_t cid) {
    char path[MAX_FN_SIZE], dt[DT_LOG_SIZE];
    int count = snprintf(path, sizeof(path), "%s/lob.data", directory);

    if (count < 0 || (size_t)count >= sizeof(path))
        return false;
    int fd = open(path, O_RDONLY | O_CLOEXEC);

    if (fd < 0 && errno == ENOENT)
        return true;
    bool supported = false;

    if (fd >= 0) {
        struct stat st;

        if (fstat(fd, &st) == 0 && st.st_size >= 0 && st.st_size <= LOB_CHUNK_SIZE) {
            uint8_t header[LOB_CHUNK_SIZE];
            size_t used = 0, size = (size_t)st.st_size;
            supported = true;

            while (used < size) {
                ssize_t got = pread(fd, header, sizeof(header), (off_t)used);

                if (got < 0 && errno == EINTR)
                    continue;

                if (got <= 0 || (size_t)got > size - used) {
                    supported = false;
                    break;
                }

                for (size_t i = 0; i < (size_t)got; ++i)
                    if (header[i] != 0) {
                        supported = false;
                        break;
                    }

                if (!supported)
                    break;
                used += (size_t)got;
            }
        }
        close(fd);
    }

    if (!supported)
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] CE cannot open %s: LOB data/WAL or unreadable LOB file; use enterprise\n", curr_dt(dt), cid, directory);
    return supported;
}
#endif
// TODO Не везде подходит uint64_t
inline uint64_t buff_to_field(const uint8_t *buff, uint32_t *curr, uint32_t size) {
    uint64_t result = 0;

    for (uint32_t i = 0; i < size; i++) {
        result |= ((uint64_t)buff[*curr + i]) << (8 * i);
    }
    *curr += size;
    return result;
}


static inline uint32_t field_to_buff(uint64_t field, uint8_t *buff, uint32_t curr, uint32_t size) {
    for (uint32_t i = 0; i < size; i++) {
        buff[curr + i] = (uint8_t)(field >> (8 * i));
    }
    return curr + size;
}

int decompress_record(struct region_info_t *r_info, char *zip_buff, int zip_buff_size, char *zbuff, uint64_t dst_capacity, uint64_t cid) {
    if (zip_buff == NULL || zbuff == NULL || zip_buff_size < 0 || dst_capacity == 0)
        return 0;
    int capacity = dst_capacity > INT_MAX ? INT_MAX : (int)dst_capacity;
    int decompressed_size = LZ4_decompress_safe(zip_buff, zbuff, zip_buff_size, capacity);

    if (decompressed_size < 0) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s WARN [MSG_UTILS=%015lu] Result %d from LZ4_decompress_safe indicates a failure trying to decompress the data\n", curr_dt(dt), cid, decompressed_size);
        return 0;
    }
    return decompressed_size;
}


char *extract_key(uint8_t *buff) {
    uint32_t key_size;
    memcpy(&key_size, buff + KEY_SIZE_RECORD_OFFSET, sizeof(key_size));

    if (key_size > BLOCK_MAX_SIZE)
        key_size = 20;

    if (key_size == 0)
        return NULL;
    char *key = malloc(key_size * 4 + 1);

    if (!key)
        return NULL;
    char *ptr = key;

    for (uint32_t i = 0; i < key_size; i++)
        ptr += sprintf(ptr, "\\x%.2X", buff[KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET + i]);
    *ptr = '\0';
    return key;
}

uint16_t bytes_to_msg_cont(msg_cli_t *msg_any, uint8_t *buff, uint32_t curr, region_info_t *r_info, uint64_t cid) {
    uint16_t msg_size_orig = msg_any->msg_size;

    if ((msg_any->zip_info & 0x01) == 1 && r_info->enable_record_compression == 1) {
        uint32_t qv_offset = KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET + msg_any->kv->key_size + msg_any->kv->cf_size;
        memcpy(r_info->zbuff, buff, qv_offset);
        uint64_t dst_capacity = r_info->zbuff_size - qv_offset;
        msg_any->msg_size = decompress_record(r_info, buff + qv_offset, msg_any->msg_size - qv_offset, r_info->zbuff + qv_offset, dst_capacity, cid);

        if (msg_any->msg_size == 0)
            return 0;
        msg_any->msg_size += qv_offset;
        union i2b_u msg_size;
        msg_size.integer_val = msg_any->msg_size;
        memcpy(r_info->zbuff, msg_size.byte, INT_SIZE);
    } else
        memcpy(r_info->zbuff, buff, msg_any->msg_size);

    if (msg_any->lob_info == 1)
        return msg_size_orig;
    memcpy(msg_any->kv->cf, r_info->zbuff + curr, msg_any->kv->cf_size);
    curr += msg_any->kv->cf_size;
    msg_any->kv->qv->ts = buff_to_field(r_info->zbuff, &curr, sizeof(msg_any->kv->qv->ts));
    msg_any->kv->qv->qfl_size = buff_to_field(r_info->zbuff, &curr, sizeof(msg_any->kv->qv->qfl_size));
    msg_any->kv->qv->val_size = buff_to_field(r_info->zbuff, &curr, sizeof(msg_any->kv->qv->val_size));

    if (msg_any->kv->qv->val_size > BLOCK_MAX_SIZE) {
        return 0;
    }

    if (msg_any->kv->qv->qfl_size > BLOCK_MAX_SIZE) {
        if (msg_any->kv->qv->qfl_size > 0x80000000)
            if (msg_any->kv->qv->qfl_size - 0x80000000 > BLOCK_MAX_SIZE)
                return 0;
            else {
                msg_any->kv->qv->qfl = (char *)malloc(msg_any->kv->qv->qfl_size - 0x80000000);

                if (msg_any->kv->qv->qfl == NULL) {
                    char dt[DT_LOG_SIZE];
                    fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed malloc msg_any->kv->qv->qfl=%u for %s\n", curr_dt(dt), cid, msg_any->kv->qv->qfl_size - 0x80000000,
                            r_info->uuid);
                    return 0;
                }
                memcpy(msg_any->kv->qv->qfl, r_info->zbuff + curr, msg_any->kv->qv->qfl_size - 0x80000000);
            }
    } else {
        msg_any->kv->qv->qfl = (char *)malloc(msg_any->kv->qv->qfl_size);
        memcpy(msg_any->kv->qv->qfl, r_info->zbuff + curr, msg_any->kv->qv->qfl_size);
    }
    return msg_size_orig;
}

uint16_t get_put_bytes_to_msg_cont(msg_cli_t *msg_any, uint8_t *buff, uint32_t curr, region_info_t *r_info, uint32_t *allocated_qfl_size, uint64_t cid) {
    uint16_t msg_size_orig = msg_any->msg_size;

    if ((msg_any->zip_info & 0x01) == 1 && r_info->enable_record_compression == 1) {
        uint32_t qv_offset = KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET + msg_any->kv->key_size + msg_any->kv->cf_size;
        memcpy(r_info->zbuff, buff, qv_offset);
        uint64_t dst_capacity = r_info->zbuff_size - qv_offset;
        msg_any->msg_size = decompress_record(r_info, buff + qv_offset, msg_any->msg_size - qv_offset, r_info->zbuff + qv_offset, dst_capacity, cid);

        if (msg_any->msg_size == 0)
            return 0;
        msg_any->msg_size += qv_offset;
        union i2b_u msg_size;
        msg_size.integer_val = msg_any->msg_size;
        memcpy(r_info->zbuff, msg_size.byte, INT_SIZE);
    } else
        memcpy(r_info->zbuff, buff, msg_any->msg_size);

    if (msg_any->lob_info == 1)
        return msg_size_orig;
    memcpy(msg_any->kv->cf, r_info->zbuff + curr, msg_any->kv->cf_size);
    curr += msg_any->kv->cf_size;
    msg_any->kv->qv->ts = buff_to_field(r_info->zbuff, &curr, sizeof(msg_any->kv->qv->ts));
    msg_any->kv->qv->qfl_size = buff_to_field(r_info->zbuff, &curr, sizeof(msg_any->kv->qv->qfl_size));
    msg_any->kv->qv->val_size = buff_to_field(r_info->zbuff, &curr, sizeof(msg_any->kv->qv->val_size));

    if (msg_any->kv->qv->val_size > BLOCK_MAX_SIZE) {
        return 0;
    }
    uint32_t qfl_size = msg_any->kv->qv->qfl_size;

    if (qfl_size > BLOCK_MAX_SIZE) {
        if (qfl_size > 0x80000000)
            if (qfl_size - 0x80000000 > BLOCK_MAX_SIZE)
                return 0;
            else
                qfl_size -= 0x80000000;
    }

    if (qfl_size > *allocated_qfl_size) {
        char *qfl_tmp;

        if (*allocated_qfl_size == 0)
            qfl_tmp = (char *)malloc(qfl_size);
        else
            qfl_tmp = (char *)realloc(msg_any->kv->qv->qfl, qfl_size);

        if (qfl_tmp == NULL) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed malloc qfl=%u for %s\n", curr_dt(dt), cid, qfl_size, r_info->uuid);
            return 0;
        }
        msg_any->kv->qv->qfl = qfl_tmp;
        *allocated_qfl_size = qfl_size;
    }

    if (qfl_size > 0)
        memcpy(msg_any->kv->qv->qfl, r_info->zbuff + curr, qfl_size);
    return msg_size_orig;
}


int16_t get_put_key_from_msg_bytes(msg_cli_t *msg_get, uint8_t *buff, uint32_t *max_key_size, uint32_t *max_cf_size, uint64_t cid) {
    uint32_t curr = offsetof(msg_cli_t, kv);
    memcpy(msg_get, buff, offsetof(msg_cli_t, kv));
    msg_get->kv->qv->qfl_size = 0;

    if (msg_get->msg_size == 0) {
        msg_get->kv->key_size = 0;
        msg_get->kv->cf_size = 0;
        return 0;
    }
    msg_get->kv->key_size = buff_to_field(buff, &curr, sizeof(msg_get->kv->key_size));

    if (msg_get->kv->key_size > FIX_BLOCK_SIZE) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed get_put, wrong msg_get key size = %d\n", curr_dt(dt), cid, msg_get->kv->key_size);
        msg_get->msg_size = 0;
        msg_get->kv->key_size = 0;
        msg_get->kv->cf_size = 0;
        return -1;
    }
    msg_get->kv->cf_size = buff_to_field(buff, &curr, sizeof(msg_get->kv->cf_size));
    msg_get->kv->qv_size = buff_to_field(buff, &curr, sizeof(msg_get->kv->qv_size));

    if (msg_get->kv->key_size > *max_key_size) {
        if (*max_key_size == 0) {
            msg_get->kv->key = malloc(msg_get->kv->key_size);
        } else
            msg_get->kv->key = realloc(msg_get->kv->key, msg_get->kv->key_size);

        if (msg_get->kv->key == NULL) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed malloc msg_get->kv->key=%u\n", curr_dt(dt), cid, msg_get->kv->key_size);
            return -1;
        }
        *max_key_size = msg_get->kv->key_size;
    }

    if (msg_get->kv->cf_size > *max_cf_size) {
        if (*max_cf_size == 0) {
            msg_get->kv->cf = malloc(msg_get->kv->cf_size);
        } else
            msg_get->kv->cf = realloc(msg_get->kv->cf, msg_get->kv->cf_size);

        if (msg_get->kv->cf == NULL) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed malloc msg_get->kv->cf=%u\n", curr_dt(dt), cid, msg_get->kv->cf_size);
            return -1;
        }
        *max_cf_size = msg_get->kv->cf_size;
    }
    memcpy(msg_get->kv->key, buff + curr, msg_get->kv->key_size);
    curr += msg_get->kv->key_size;
    return curr;
}


int16_t get_key_from_msg_bytes(msg_cli_t *msg_cli, uint8_t *buff, uint64_t cid) {
    uint32_t curr = offsetof(msg_cli_t, kv);
    memcpy(msg_cli, buff, offsetof(msg_cli_t, kv));
    msg_cli->kv->qv->qfl_size = 0;

    if (msg_cli->msg_size == 0) {
        msg_cli->kv->key_size = 0;
        msg_cli->kv->cf_size = 0;
        return 0;
    }
    msg_cli->kv->key_size = buff_to_field(buff, &curr, sizeof(msg_cli->kv->key_size));

    if (msg_cli->kv->key_size > FIX_BLOCK_SIZE) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed get, wrong key size = %d\n", curr_dt(dt), cid, msg_cli->kv->key_size);
        msg_cli->msg_size = 0;
        msg_cli->kv->key_size = 0;
        msg_cli->kv->cf_size = 0;
        return -1;
    }
    msg_cli->kv->cf_size = buff_to_field(buff, &curr, sizeof(msg_cli->kv->cf_size));
    msg_cli->kv->qv_size = buff_to_field(buff, &curr, sizeof(msg_cli->kv->qv_size));
    msg_cli->kv->key = (char *)malloc(msg_cli->kv->key_size);

    if (msg_cli->kv->key == NULL) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed malloc msg_cli->kv->key=%u\n", curr_dt(dt), cid, msg_cli->kv->key_size);
        return -1;
    }
    msg_cli->kv->cf = (char *)malloc(msg_cli->kv->cf_size);

    if (msg_cli->kv->cf == NULL) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed malloc msg_cli->kv->cf=%u\n", curr_dt(dt), cid, msg_cli->kv->cf_size);
        return -1;
    }
    memcpy(msg_cli->kv->key, buff + curr, msg_cli->kv->key_size);
    curr += msg_cli->kv->key_size;
    return curr;
}

int16_t get_key_from_msg_bytes_safety(msg_cli_t *msg_cli, uint8_t *buff) {
    uint32_t curr = 0;
    msg_cli->msg_size = buff_to_field(buff, &curr, sizeof(msg_cli->msg_size));

    if (msg_cli->msg_size == 0) {
        msg_cli->kv->key_size = 0;
        msg_cli->kv->cf_size = 0;
        return 0;
    }

    if (msg_cli->msg_size < offsetof(struct msg_cli_t, kv) + offsetof(struct msg_kv_t, key)) {
        msg_cli->kv->key_size = 0;
        return -1;
    }
    msg_cli->type = buff_to_field(buff, &curr, sizeof(msg_cli->type));

    if (msg_cli->type > MSG_RS) {
        msg_cli->msg_size = 0;
        msg_cli->kv->key_size = 0;
        msg_cli->kv->cf_size = 0;
        return -2;
    }
    msg_cli->region_pos = buff_to_field(buff, &curr, sizeof(msg_cli->region_pos));
    msg_cli->rec_crc = buff_to_field(buff, &curr, sizeof(msg_cli->rec_crc));
    msg_cli->lob_info = buff_to_field(buff, &curr, sizeof(msg_cli->lob_info));
    msg_cli->zip_info = buff_to_field(buff, &curr, sizeof(msg_cli->zip_info));

    if (msg_cli->zip_info > 3) {
        msg_cli->msg_size = 0;
        msg_cli->kv->key_size = 0;
        msg_cli->kv->cf_size = 0;
        return -3;
    }
    msg_cli->new_version = buff_to_field(buff, &curr, sizeof(msg_cli->new_version));
    msg_cli->kv->key_size = buff_to_field(buff, &curr, sizeof(msg_cli->kv->key_size));

    if (msg_cli->kv->key_size > FIX_BLOCK_SIZE) {
        msg_cli->kv->key_size = 0;
        msg_cli->msg_size = 0;
        msg_cli->kv->cf_size = 0;
        return -4;
    }
    msg_cli->kv->cf_size = buff_to_field(buff, &curr, sizeof(msg_cli->kv->cf_size));

    if (msg_cli->kv->cf_size > FIX_BLOCK_SIZE) {
        msg_cli->kv->key_size = 0;
        msg_cli->msg_size = 0;
        msg_cli->kv->cf_size = 0;
        return -5;
    }
    msg_cli->kv->qv_size = buff_to_field(buff, &curr, sizeof(msg_cli->kv->qv_size));

    if (msg_cli->kv->qv_size > BLOCK_MAX_SIZE && msg_cli->lob_info == 0) {
        msg_cli->msg_size = 0;
        msg_cli->kv->key_size = 0;
        msg_cli->kv->cf_size = 0;
        return -6;
    }
    msg_cli->kv->key = (char *)malloc(msg_cli->kv->key_size);

    if (msg_cli->kv->key == NULL) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed malloc msg_cli->kv->key=%u\n", curr_dt(dt), 1L, msg_cli->kv->key_size);
        return -1;
    }
    msg_cli->kv->cf = (char *)malloc(msg_cli->kv->cf_size);

    if (msg_cli->kv->cf == NULL) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed malloc msg_cli->kv->cf=%u\n", curr_dt(dt), 1L, msg_cli->kv->cf_size);
        return -1;
    }
    memcpy(msg_cli->kv->key, buff + curr, msg_cli->kv->key_size);
    curr += msg_cli->kv->key_size;
    msg_cli->kv->qv->qfl_size = 0;
    return curr;
}

static int16_t parse_key_fields_checked(msg_cli_t *parsed, const uint8_t *buff, bool safety) {
    uint32_t curr = 0;
    parsed->msg_size = buff_to_field(buff, &curr, sizeof(parsed->msg_size));

    if (parsed->msg_size == 0)
        return 0;

    if (safety && parsed->msg_size < offsetof(msg_cli_t, kv) + offsetof(msg_kv_t, key))
        return -1;
    memcpy(parsed, buff, offsetof(msg_cli_t, kv));

    if (safety && parsed->type > MSG_RS)
        return -2;

    if (safety && parsed->zip_info > 3)
        return -3;
    curr = offsetof(msg_cli_t, kv);
    parsed->kv->key_size = buff_to_field(buff, &curr, sizeof(parsed->kv->key_size));

    if (parsed->kv->key_size > FIX_BLOCK_SIZE)
        return safety ? -4 : -1;
    parsed->kv->cf_size = buff_to_field(buff, &curr, sizeof(parsed->kv->cf_size));

    if (safety && parsed->kv->cf_size > FIX_BLOCK_SIZE)
        return -5;
    parsed->kv->qv_size = buff_to_field(buff, &curr, sizeof(parsed->kv->qv_size));

    if (safety && parsed->kv->qv_size > BLOCK_MAX_SIZE && parsed->lob_info == 0)
        return -6;
    return (int16_t)(curr + parsed->kv->key_size);
}

static bool key_extract_state_valid(const msg_cli_t *msg, const uint8_t *buff) {
    return msg != NULL && msg->kv != NULL && msg->kv->qv != NULL && buff != NULL;
}

static void publish_key_fields_checked(msg_cli_t *msg, const msg_cli_t *parsed, const uint8_t *buff, int16_t result, bool safety) {
    if (result == 0) {
        if (!safety) {
            memcpy(msg, buff, offsetof(msg_cli_t, kv));
            msg->kv->qv->qfl_size = 0;
        } else {
            msg->msg_size = 0;
        }
        msg->kv->key_size = 0;
        msg->kv->cf_size = 0;
        return;
    }
    memcpy(msg, parsed, offsetof(msg_cli_t, kv));
    msg->kv->key_size = parsed->kv->key_size;
    msg->kv->cf_size = parsed->kv->cf_size;
    msg->kv->qv_size = parsed->kv->qv_size;
    msg->kv->qv->qfl_size = 0;

    if (msg->kv->key_size != 0)
        memcpy(msg->kv->key, buff + offsetof(msg_cli_t, kv) + offsetof(msg_kv_t, key), msg->kv->key_size);
}


int16_t get_put_key_from_msg_bytes_checked(msg_cli_t *msg, const uint8_t *buff, uint32_t *max_key_size, uint32_t *max_cf_size, uint64_t cid) {
    (void)cid;

    if (!key_extract_state_valid(msg, buff) || max_key_size == NULL || max_cf_size == NULL || ((*max_key_size == 0) != (msg->kv->key == NULL)) ||
        ((*max_cf_size == 0) != (msg->kv->cf == NULL)))
        return KEY_EXTRACT_INVALID_STATE;
    msg_kv_t fields = {0};
    msg_cli_t parsed = {.kv = &fields};
    int16_t result = parse_key_fields_checked(&parsed, buff, false);

    if (result < 0)
        return result;

    if (fields.key_size > *max_key_size) {
        uint8_t *tmp = *max_key_size == 0 ? malloc(fields.key_size) : realloc(msg->kv->key, fields.key_size);

        if (tmp == NULL)
            return KEY_EXTRACT_NOMEM;
        msg->kv->key = tmp;
        *max_key_size = fields.key_size;
    }

    if (fields.cf_size > *max_cf_size) {
        uint8_t *tmp = *max_cf_size == 0 ? malloc(fields.cf_size) : realloc(msg->kv->cf, fields.cf_size);

        if (tmp == NULL)
            return KEY_EXTRACT_NOMEM;
        msg->kv->cf = tmp;
        *max_cf_size = fields.cf_size;
    }
    publish_key_fields_checked(msg, &parsed, buff, result, false);
    return result;
}


static int16_t get_key_from_msg_bytes_checked_internal(msg_cli_t *msg, const uint8_t *buff, bool safety) {
    if (!key_extract_state_valid(msg, buff) || msg->kv->key != NULL || msg->kv->cf != NULL)
        return KEY_EXTRACT_INVALID_STATE;
    msg_kv_t fields = {0};
    msg_cli_t parsed = {.kv = &fields};
    int16_t result = parse_key_fields_checked(&parsed, buff, safety);

    if (result < 0)
        return result;
    uint8_t *key = fields.key_size == 0 ? NULL : malloc(fields.key_size);

    if (fields.key_size != 0 && key == NULL)
        return KEY_EXTRACT_NOMEM;
    uint8_t *cf = fields.cf_size == 0 ? NULL : malloc(fields.cf_size);

    if (fields.cf_size != 0 && cf == NULL) {
        free(key);
        return KEY_EXTRACT_NOMEM;
    }
    msg->kv->key = key;
    msg->kv->cf = cf;
    publish_key_fields_checked(msg, &parsed, buff, result, safety);
    return result;
}

int16_t get_key_from_msg_bytes_checked(msg_cli_t *msg, const uint8_t *buff, uint64_t cid) {
    (void)cid;
    return get_key_from_msg_bytes_checked_internal(msg, buff, false);
}


int16_t get_key_from_msg_bytes_safety_checked(msg_cli_t *msg, const uint8_t *buff) {
    return get_key_from_msg_bytes_checked_internal(msg, buff, true);
}

void clear_extracted_key_fields(msg_cli_t *msg) {
    if (msg->kv->key_size != 0)
        free(msg->kv->key);

    if (msg->kv->cf_size != 0)
        free(msg->kv->cf);
    msg->kv->key = NULL;
    msg->kv->cf = NULL;
    msg->kv->key_size = 0;
    msg->kv->cf_size = 0;
}


bool calc_msg_buff_size(uint64_t required, uint32_t *result, uint64_t cid) {
    if (result == NULL || required > UINT64_MAX - (FIX_MAX_MSG_SIZE - 1))
        return false;
    uint64_t new_size = ((required + FIX_MAX_MSG_SIZE - 1) / FIX_MAX_MSG_SIZE) * FIX_MAX_MSG_SIZE;

    if (new_size > UINT32_MAX) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s WARN [RS_IDX=%015lu] Required buff size overflow: required=%lu, new_size=%lu\n", curr_dt(dt), cid, required, new_size);
        return false;
    }
    *result = new_size;
    return true;
}

int realloc_buff_or_bbuff(table_handler_t *t_handler, uint8_t **buff_io, uint32_t required) {
    char dt[DT_LOG_SIZE];

    if (t_handler == NULL || buff_io == NULL || *buff_io == NULL) {
        fprintf(stderr, "%s WARN [RS_IDX=%015lu] realloc_buff_or_bbuff invalid args: t_handler=%p, buff_io=%p\n", curr_dt(dt), t_handler ? t_handler->cid : 0, (void *)t_handler,
                (void *)buff_io);
        return -1;
    }
    uint8_t *buff = *buff_io;
    uint8_t **owner = NULL;
    uint32_t *capacity = NULL;

    if (buff == t_handler->big_buff) {
        owner = &t_handler->big_buff;
        capacity = &t_handler->big_buff_size;
    } else if (buff == t_handler->buff) {
        owner = &t_handler->buff;
        capacity = &t_handler->buff_size;
    } else {
        fprintf(stderr, "%s WARN [RS_IDX=%015lu] Realloc of non-owned buff=%p, t_handler->buff=%p, big_buff=%p\n", curr_dt(dt), t_handler->cid, (void *)buff,
                (void *)t_handler->buff, (void *)t_handler->big_buff);
        return -1;
    }

    if (required <= *capacity)
        return 0;
    uint32_t new_capacity;

    if (calc_msg_buff_size(required, &new_capacity, t_handler->cid) == false)
        return -1;
    uint8_t *tmp = realloc(*owner, new_capacity);

    if (!tmp) {
        fprintf(stderr, "%s WARN [RS_IDX=%015lu] Failed realloc buff=%p from %u to %u\n", curr_dt(dt), t_handler->cid, (void *)*owner, *capacity, new_capacity);
        return -1;
    }
    *owner = tmp;
    *capacity = new_capacity;
    *buff_io = tmp;

    if (owner == &t_handler->buff) {
        t_handler->realloc_buff = true;
        t_handler->buff_size = new_capacity;
    } else {
        t_handler->realloc_big_buff = true;
        t_handler->big_buff_size = new_capacity;
    }
    return 0;
}


int realloc_resp_buff(msg_srv_t *msg_srv, size_t required_free, uint64_t cid) {
    if (msg_srv == NULL || msg_srv->resp_buff_base == NULL || msg_srv->resp_buff == NULL || msg_srv->total_max_msg_size == 0 || required_free > RECORD_SIZE_LIMIT ||
        required_free > UINT32_MAX - FIX_BSIZE_FACTOR) {
        return -1;
    }

    if (log_level > 1) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s DEBUG [MSG_UTILS=%015lu] Current size curr_max_msg_size=%u, required_free=%lu\n", curr_dt(dt), cid, msg_srv->curr_max_msg_size, required_free);
    }
    uintptr_t base_addr = (uintptr_t)msg_srv->resp_buff_base;
    uintptr_t tail_addr = (uintptr_t)msg_srv->resp_buff;

    if (tail_addr < base_addr || tail_addr - base_addr > msg_srv->total_max_msg_size)
        return -1;
    uint32_t used = (uint32_t)(tail_addr - base_addr);
    uint32_t total = msg_srv->total_max_msg_size;
    required_free += FIX_BSIZE_FACTOR;

    if (required_free > UINT32_MAX - used)
        return -1;
    uint32_t need_total = used + (uint32_t)required_free;

    if (total >= need_total) {
        msg_srv->curr_max_msg_size = total - used;
        return 0;
    }
    uint32_t new_total = total;

    while (new_total < need_total) {
        if (new_total > UINT32_MAX / 2) {
            new_total = need_total;
            break;
        }
        new_total <<= 1;
    }
    void *nb = realloc(msg_srv->resp_buff_base, new_total);

    if (!nb) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed realloc resp_buff_base=%u\n", curr_dt(dt), cid, new_total);
        return -1;
    }
    msg_srv->resp_buff_base = (char *)nb;
    msg_srv->resp_buff = (char *)nb + used;
    msg_srv->total_max_msg_size = new_total;
    msg_srv->curr_max_msg_size = new_total - used;

    if (log_level > 2) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s DEBUG [MSG_UTILS=%015lu] Success realloc curr_max_msg_size=%u, required_free=%lu\n", curr_dt(dt), cid, msg_srv->curr_max_msg_size, required_free);
    }
    return 0;
}


int ensure_bulk_buffer_capacity(uint8_t **buffer, uint32_t *capacity, uint32_t used, uint32_t additional, uint64_t cid, const char *buffer_name) {
    char dt[DT_LOG_SIZE];
    const char *name = buffer_name != NULL ? buffer_name : "bulk buffer";

    if (buffer == NULL || capacity == NULL || (*buffer == NULL && used != 0) || (*buffer != NULL && used > *capacity) || used > BULK_MSG_SIZE_LIMIT ||
        additional > BULK_MSG_SIZE_LIMIT - used) {
        fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Invalid %s state: buffer=%p, capacity=%u, used=%u, additional=%u\n", curr_dt(dt), cid, name,
                buffer != NULL ? (void *)*buffer : NULL, capacity != NULL ? *capacity : 0, used, additional);
        return -1;
    }
    uint32_t required = used + additional;

    if (*buffer != NULL && *capacity >= required)
        return 0;
    uint32_t new_capacity = *capacity;

    if (new_capacity == 0)
        new_capacity = 1024U;

    while (new_capacity < required) {
        if (new_capacity > BULK_MSG_SIZE_LIMIT / 2U) {
            new_capacity = required;
            break;
        }
        new_capacity *= 2U;
    }

    if (new_capacity > BULK_MSG_SIZE_LIMIT) {
        fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] %s exceeds bulk limit: required=%u, limit=%u\n", curr_dt(dt), cid, name, required, BULK_MSG_SIZE_LIMIT);
        return -1;
    }
    uint8_t *new_buffer = realloc(*buffer, new_capacity);

    if (new_buffer == NULL) {
        fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Cannot grow %s from %u to %u bytes\n", curr_dt(dt), cid, name, *capacity, new_capacity);
        return -1;
    }
    *buffer = new_buffer;
    *capacity = new_capacity;
    return 0;
}

uint32_t bytes_to_msg(msg_cli_t *msg_cli, uint8_t *buff, uint32_t available, table_handler_t *t_handler) {
    uint32_t msg_size_orig;

    if (available < MIN_REC_SIZE)
        return 0;
    memcpy(&msg_size_orig, buff, sizeof(msg_size_orig));

    if (msg_size_orig < MIN_REC_SIZE || msg_size_orig > available)
        return 0;
    uint32_t curr = offsetof(msg_cli_t, kv);
    memcpy(msg_cli, buff, offsetof(msg_cli_t, kv));

    if (msg_cli->msg_size == 0)
        return 0;
    msg_cli->kv->key_size = buff_to_field(buff, &curr, sizeof(msg_cli->kv->key_size));

    if (msg_cli->kv->key_size > FIX_BLOCK_SIZE) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed convert msg, wrong key size = %u\n", curr_dt(dt), t_handler->cid, msg_cli->kv->key_size);
        return 0;
    }
    msg_cli->kv->cf_size = buff_to_field(buff, &curr, sizeof(msg_cli->kv->cf_size));
    msg_cli->kv->qv_size = buff_to_field(buff, &curr, sizeof(msg_cli->kv->qv_size));

    if (msg_cli->kv->key_size > msg_size_orig - curr)
        return 0;

    if (msg_cli->kv->key_size > t_handler->msg_cli_allocated_key_size) {
        char *new_key = realloc(msg_cli->kv->key, msg_cli->kv->key_size);

        if (new_key == NULL) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed resize key buffer from %u to %u bytes: %s\n", curr_dt(dt), t_handler->cid, t_handler->msg_cli_allocated_key_size,
                    msg_cli->kv->key_size, strerror(errno));
            return 0;
        }
        msg_cli->kv->key = new_key;
        t_handler->msg_cli_allocated_key_size = msg_cli->kv->key_size;
    }
    msg_cli->kv->qv->qfl = NULL;
    msg_cli->kv->qv->qfl_size = 0;

    if (msg_cli->kv->key_size != 0)
        memcpy(msg_cli->kv->key, buff + curr, msg_cli->kv->key_size);

    if (msg_cli->lob_info > 1) {
        msg_cli->kv->qv_size = 0;
        memcpy(&msg_cli->kv->key_size, buff + KEY_SIZE_RECORD_OFFSET, sizeof(msg_cli->kv->key_size));
        curr += msg_cli->kv->key_size;

        if (msg_cli->kv->cf_size > msg_size_orig - curr)
            return 0;
        msg_cli->kv->cf = msg_cli->kv->cf_size ? malloc(msg_cli->kv->cf_size) : NULL;

        if (msg_cli->kv->cf_size != 0 && msg_cli->kv->cf == NULL) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed malloc msg_cli->kv->cf=%u\n", curr_dt(dt), t_handler->cid, msg_cli->kv->cf_size);
            return 0;
        }

        if (msg_cli->kv->cf_size != 0)
            memcpy(msg_cli->kv->cf, buff + curr, msg_cli->kv->cf_size);
        return msg_size_orig;
    }
    curr += msg_cli->kv->key_size;

    if (msg_cli->type == MSG_PUT || (msg_cli->type == MSG_DEL && curr != msg_cli->msg_size)) {
        if (msg_cli->kv->cf_size > msg_size_orig - curr || msg_cli->kv->qv_size != msg_size_orig - curr - msg_cli->kv->cf_size ||
            msg_cli->kv->qv_size < TS_SIZE + 2U * sizeof(uint32_t))
            return 0;
        msg_cli->kv->cf = msg_cli->kv->cf_size ? malloc(msg_cli->kv->cf_size) : NULL;

        if (msg_cli->kv->cf_size != 0 && msg_cli->kv->cf == NULL) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed malloc msg_cli->kv->cf=%u\n", curr_dt(dt), t_handler->cid, msg_cli->kv->cf_size);
            return 0;
        }

        if (msg_cli->kv->cf_size != 0)
            memcpy(msg_cli->kv->cf, buff + curr, msg_cli->kv->cf_size);
        curr += msg_cli->kv->cf_size;
        struct timespec tsp;

        while (clock_gettime(CLOCK_REALTIME, &tsp) == -1)
            ;
        uint64_t l2b_ts = 1000 * tsp.tv_sec + tsp.tv_nsec / 1000000;
#ifdef C_TEST
        msg_cli->kv->qv->ts = buff_to_field(buff, &curr, sizeof(msg_cli->kv->qv->ts));

        if ((msg_cli->kv->qv->ts == 0 && old_ts == 1) || msg_cli->kv->qv->ts == LONG_MAX_VALUE)
            memcpy(buff + curr - TS_SIZE, &l2b_ts, 8);
#else
        do {
            if (msg_size_orig - curr < TS_SIZE + 2U * sizeof(uint32_t))
                return 0;
            msg_cli->kv->qv->ts = buff_to_field(buff, &curr, sizeof(msg_cli->kv->qv->ts));

            if ((msg_cli->kv->qv->ts == 0 && old_ts == 1) || msg_cli->kv->qv->ts == LONG_MAX_VALUE)
                memcpy(buff + curr - TS_SIZE, &l2b_ts, 8);
            msg_cli->kv->qv->qfl_size = buff_to_field(buff, &curr, sizeof(msg_cli->kv->qv->qfl_size));
            msg_cli->kv->qv->val_size = buff_to_field(buff, &curr, sizeof(msg_cli->kv->qv->val_size));
            uint32_t qfl_size = msg_cli->kv->qv->qfl_size & 0x7fffffffU;

            if (qfl_size > msg_size_orig - curr)
                return 0;
            curr += qfl_size;

            if (msg_cli->kv->qv->val_size > msg_size_orig - curr)
                return 0;
            curr += msg_cli->kv->qv->val_size;
        } while (curr < msg_size_orig);

        if (curr != msg_size_orig) {
            // TODO Free
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Wrong QV=%u != msg_size=%u. Sleep 60 sec...\n", curr_dt(dt), t_handler->cid, curr, msg_size_orig);
            sleep(60);
            return 0;
        }
#endif
        msg_cli->kv->qv->qfl_size = 2;
        msg_cli->kv->qv->qfl = (char *)malloc(msg_cli->kv->qv->qfl_size + 1);

        if (msg_cli->kv->qv->qfl == NULL) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed malloc bytes_to_msg msg_cli->kv->qv->qfl=%u\n", curr_dt(dt), t_handler->cid, msg_cli->kv->qv->qfl_size + 1);
            return 0;
        }
    }

    if (msg_cli->type == MSG_GET || msg_cli->type == MSG_NIL) {
        msg_cli->kv->cf_size = 0;
        msg_cli->kv->qv_size = 0;
        msg_cli->kv->qv->ts = 0;
        msg_cli->kv->qv->qfl_size = 0;
        msg_cli->kv->qv->val_size = 0;
    }
#ifdef DEBUG
    fprintf(stderr, "msg_cli->msg_size=%d\n", msg_cli->msg_size);
    fprintf(stderr, "msg_cli->type=%d\n", msg_cli->type);
    fprintf(stderr, "msg_cli->region_pos=%d\n", msg_cli->region_pos);
    fprintf(stderr, "msg_cli->chunk_no=%d\n", msg_cli->chunk_no);
    fprintf(stderr, "msg_cli->chunk_total=%d\n", msg_cli->chunk_total);
    fprintf(stderr, "msg_cli->lob_info=%d\n", msg_cli->lob_info);
    fprintf(stderr, "msg_cli->info=%d\n", msg_cli->coll_info);
    fprintf(stderr, "msg_cli->new_version=%d\n", msg_cli->new_version);
    fprintf(stderr, "msg_cli->kv->key_size=%d\n", msg_cli->kv->key_size);
    fprintf(stderr, "msg_cli->kv->cf_size=%d\n", msg_cli->kv->cf_size);
    fprintf(stderr, "msg_cli->kv->qv_size=%d\n", msg_cli->kv->qv_size);
    fprintf(stderr, "msg_cli->kv->qv->ts=%lu\n", msg_cli->kv->qv->ts);
    fprintf(stderr, "msg_cli->kv->qv->qfl_size=%d\n", msg_cli->kv->qv->qfl_size);
    fprintf(stderr, "msg_cli->kv->qv->val_size=%d\n", msg_cli->kv->qv->val_size);

    if (msg_cli->type == MSG_PUT) {
        msg_cli->kv->qv->val[msg_cli->kv->qv->val_size - 1] = 0;
        fprintf(stderr, "msg_cli->kv->qv->val=%s\n", msg_cli->kv->qv->val);
    }
#endif
    return msg_size_orig;
}

void free_cli_msg(msg_cli_t *msg_cli) {
    if (msg_cli->kv->qv->qfl_size > 0)
        free(msg_cli->kv->qv->qfl);

    if (msg_cli->kv->cf_size > 0)
        free(msg_cli->kv->cf);

    if (msg_cli->kv->key_size > 0) {
        free(msg_cli->kv->key);
        msg_cli->kv->key = NULL;
    }
    msg_cli->kv->key_size = 0;
    msg_cli->kv->cf_size = 0;
    msg_cli->kv->qv->qfl_size = 0;
}

void free_get_msg(msg_cli_t *msg_cli) {
    if (msg_cli->kv->qv->qfl_size > 0)
        free(msg_cli->kv->qv->qfl);
    msg_cli->kv->key_size = 0;
    msg_cli->kv->cf_size = 0;
}


void free_msg(msg_cli_t *msg_any) {
    if (msg_any->kv != NULL) {
        if (msg_any->kv->qv->qfl_size > 0)
            free(msg_any->kv->qv->qfl);

        if (msg_any->kv->qv != NULL)
            free(msg_any->kv->qv);

        if (msg_any->kv->cf != NULL)
            free(msg_any->kv->cf);

        if (msg_any->kv->key != NULL)
            free(msg_any->kv->key);
        free(msg_any->kv);
        msg_any->kv = NULL;
    }
}


void print_full_request_infomation(region_info_t *r_info, char *msg_text, char *key_hex, msg_cli_t *msg_cli, uint8_t bs, table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    msg_srv_t *msg_srv = t_handler->msg_srv;
    uint8_t b = r_info->r_idx_b[bs]->idx_r_help->b;
    uint32_t k_idx = r_info->r_idx_b[bs]->idx_r_help->k_idx;
    uint32_t c_idx = r_info->r_idx_b[bs]->idx_r_help->c_idx;
    uint32_t k_idx_orig = r_info->r_idx_b[bs]->idx_r_help->k_idx_orig;
    uint8_t k3 = r_info->r_idx_b[bs]->idx_r_help->k3;
    uint8_t k2 = r_info->r_idx_b[bs]->idx_r_help->k2;
    uint8_t k1 = r_info->r_idx_b[bs]->idx_r_help->k1;
    fprintf(stderr,
            "%s DEBUG [BLOCK_HANDLER=%015lu] %s: %s to %s, "
            "bs=%u, k_idx=%u, c_idx=%u, k_idx_orig=%u, k3=%u, k2=%u, k1=%u, b_offset=%u, "
            "o_index=%d, c_index=%d, j_index=%d, s_index=%d, z_index=%d, lob_info=%u, "
            "r_control=%d, read=%u, write=%u, msg_size=%u\n",
            curr_dt(dt), t_handler->cid, msg_text, key_hex, r_info->r_idx_b[bs]->fname, bs, k_idx, c_idx, k_idx_orig, k3, k2, k1, r_info->r_idx_b[bs]->b_offset[FBP],
            r_info->r_idx_b[bs]->idx[b][k_idx].o_index, r_info->r_idx_b[bs]->idx[b][k_idx].c_index, r_info->r_idx_b[bs]->idx[b][k_idx].j_index,
            r_info->r_idx_b[bs]->idx[b][k_idx].s_index, r_info->r_idx_b[bs]->idx[b][r_info->r_idx_b[bs]->idx[b][k_idx].o_index].z_index, msg_cli->lob_info,
            r_info->replica->replica_control, msg_srv_read_access(msg_srv), msg_srv_write_access(msg_srv), msg_srv->msg_size);
}

void stat_msg_to_bytes(msg_stat_t *msg_stat, uint8_t *buff) {
    uint32_t curr = 0;
    // TODO memcpy?
    curr = field_to_buff(msg_stat->msg_size, buff, curr, sizeof(msg_stat->msg_size));
    curr = field_to_buff(msg_stat->type, buff, curr, sizeof(msg_stat->type));
    curr = field_to_buff(msg_stat->ts_stat, buff, curr, sizeof(msg_stat->ts_stat));
    memcpy(buff + curr, msg_stat->full_stat, strlen(msg_stat->full_stat));
}


void qv_to_bytes(struct msg_qv_t *qv, char *dbuff, char *sbuff) {
    union l2b_u l2b;
    l2b.long_val = qv->ts;
    memcpy(dbuff, l2b.byte, 8);
    union i2b_u i2b;
    i2b.integer_val = DICT_SIZE;
    memcpy(dbuff + 8, i2b.byte, 4);
    i2b.integer_val = qv->val_size;
    memcpy(dbuff + 12, i2b.byte, 4);
    memcpy(dbuff + 16, qv->qfl, 2);
    memcpy(dbuff + 18, sbuff, qv->val_size);
}


hostent_t *thread_safe_gethostname(char *host) {
    struct hostent_t *hostent = calloc(1, sizeof(*hostent));

    if (hostent == NULL)
        return NULL;
    size_t hstbuflen = 1024;
    int res;
    int herr;
    hostent->hostbuf = malloc(sizeof(struct hostent));

    if (hostent->hostbuf == NULL)
        goto fail;
    hostent->tmphstbuf = malloc(hstbuflen);

    if (hostent->tmphstbuf == NULL)
        goto fail;

    while ((res = gethostbyname_r(host, hostent->hostbuf, hostent->tmphstbuf, hstbuflen, &hostent->hp, &herr)) == ERANGE) {
        if (hstbuflen > SIZE_MAX / 2)
            goto fail;
        size_t new_size = hstbuflen * 2;
        char *tmp = realloc(hostent->tmphstbuf, new_size);

        if (tmp == NULL)
            goto fail;
        hostent->tmphstbuf = tmp;
        hstbuflen = new_size;
    }

    if (res || hostent->hp == NULL)
        goto fail;
    return hostent;
fail:
    free(hostent->tmphstbuf);
    free(hostent->hostbuf);
    free(hostent);
    return NULL;
}


bool check_endianness() {
    unsigned int num = 1;
    char *byte = (char *)&num;

    if (*byte) {
        fprintf(stderr, "little-endian detected\n");
        return true;
    }
    fprintf(stderr, "Big-endian detected, work is not possible\n");
    exit(-1);
}


uint16_t crc16(char *input_str, uint64_t num_bytes) {
    static bool table_ready = false;

    if (!table_ready) {
        generate_crc16_table();
        table_ready = true;
    }
    uint16_t crc = 0xFFFFU;

    for (uint64_t i = 0; i < num_bytes; ++i) {
        uint8_t idx = (uint8_t)(((crc >> 8) & 0xFFU) ^ (uint8_t)input_str[i]);
        crc = (uint16_t)((crc << 8) ^ crc16_table[idx]);
    }
    return crc;
}

void generate_crc16_table() {
    for (uint16_t i = 0; i < CRC16_TABLE_SIZE; i++) {
        uint16_t crc = i << 8;

        for (uint8_t j = 0; j < 8; j++) {
            if (crc & 0x8000) {
                crc = (crc << 1) ^ CRC16_POLYNOMIAL;
            } else {
                crc = crc << 1;
            }
        }
        crc16_table[i] = crc;

        if ((i - 1) == 16)
            h_count = crc;
    }
}


static uint32_t read_u32_le_unchecked(const uint8_t *p) {
    return ((uint32_t)p[0]) | ((uint32_t)p[1] << 8u) | ((uint32_t)p[2] << 16u) | ((uint32_t)p[3] << 24u);
}

static bool is_uuid_char(uint8_t c) {
    if (c >= '0' && c <= '9')
        return true;

    if (c >= 'a' && c <= 'f')
        return true;

    if (c == '_')
        return true;
    return false;
}


static bool check_uuid_list(const uint8_t *p, size_t len) {
    size_t pos = 0;

    if (p == NULL)
        return false;

    if (len < UUID_SIZE)
        return false;

    for (;;) {
        if (len - pos < UUID_SIZE)
            return false;

        for (size_t i = 0; i < UUID_SIZE; i++) {
            if (!is_uuid_char(p[pos + i]))
                return false;
        }
        pos += UUID_SIZE;

        if (pos == len)
            return true;

        if (pos >= len)
            return false;

        if (p[pos] != ',')
            return false;
        pos++;

        if (pos == len)
            return false;
    }
}

int format_validation_check_init_msg(uint8_t *buff, int rsize) {
    if (buff == NULL)
        return -1;

    if (rsize < 6 || rsize > REAL_MAX_MSG_SIZE * 1000)
        return -2;
    size_t len = (size_t)rsize;
    uint32_t msg_size_u32 = read_u32_le_unchecked(buff);
    size_t msg_size = (size_t)msg_size_u32;

    if (msg_size != len)
        return -3;
    uint8_t msg_type = buff[4];

    if (msg_type != GET_REGION_MAP && msg_type != GET_REGION_MAP_TIMEOUT)
        return -4;
    size_t hdr_size;

    if (msg_type == GET_REGION_MAP)
        hdr_size = 5u;
    else
        hdr_size = 9u;

    if (len <= hdr_size)
        return -5;
    size_t params_len = len - hdr_size;
    const uint8_t *params = buff + hdr_size;

    if (params_len == 0)
        return -6;

    if (params[params_len - 1] == '\0')
        params_len--;

    if (params_len == 0)
        return -7;

    if (memchr(params, '\0', params_len) != NULL)
        return -8;
    const uint8_t *colon1 = memchr(params, ':', params_len);

    if (colon1 == NULL)
        return -9;
    size_t ns_len = (size_t)(colon1 - params);

    if (ns_len == 0)
        return -10;
    size_t rest1_len = params_len - ns_len - 1u;
    const uint8_t *table_start = colon1 + 1;

    if (rest1_len == 0)
        return -11;
    const uint8_t *colon2 = memchr(table_start, ':', rest1_len);

    if (colon2 == NULL)
        return -12;
    size_t table_len = (size_t)(colon2 - table_start);

    if (table_len == 0)
        return -13;
    size_t uuids_len = rest1_len - table_len - 1u;
    const uint8_t *uuids_start = colon2 + 1;

    if (uuids_len == 0)
        return -14;

    if (memchr(uuids_start, ':', uuids_len) != NULL)
        return -15;

    if (!check_uuid_list(uuids_start, uuids_len))
        return -16;
    return 0;
}


bool compress_lzma(const uint8_t *input_data, uint64_t input_size, region_info_t *r_info, uint64_t *compressed_size) {
    lzma_stream strm = LZMA_STREAM_INIT;
    lzma_options_lzma options;
    lzma_ret ret = lzma_lzma_preset(&options, LZMA_PRESET_DEFAULT);

    if (ret != LZMA_OK) {
        return false;
    }
    ret = lzma_alone_encoder(&strm, &options);

    if (ret != LZMA_OK) {
        return false;
    }
    uint64_t outbuf_size = input_size + input_size / 3 + 128;
    uint8_t *outbuf = malloc(outbuf_size);

    if (outbuf == NULL) {
        lzma_end(&strm);
        return false;
    }
    strm.next_in = input_data;
    strm.avail_in = input_size;
    strm.next_out = outbuf;
    strm.avail_out = outbuf_size;
    ret = lzma_code(&strm, LZMA_FINISH);

    if (ret != LZMA_STREAM_END) {
        free(outbuf);
        lzma_end(&strm);
        return false;
    }
    uint64_t final_compressed_size = outbuf_size - strm.avail_out;

    if (r_info->bbuff_size < final_compressed_size) {
        void *tmp = realloc(r_info->bbuff, final_compressed_size);

        if (tmp == NULL) {
            free(outbuf);
            lzma_end(&strm);
            return false;
        }
        r_info->bbuff = tmp;
        r_info->bbuff_size = final_compressed_size;
    }
    *compressed_size = final_compressed_size;
    memcpy(r_info->bbuff, outbuf, final_compressed_size);
    free(outbuf);

    if (*compressed_size >= 5 + sizeof(uint64_t)) {
        memcpy(r_info->bbuff + 5, &input_size, sizeof(uint64_t));
    } else {
        lzma_end(&strm);
        return false;
    }
    lzma_end(&strm);
    return true;
}

uint64_t decompress_lzma(msg_srv_t *msg_srv, uint64_t data_size, uint64_t input_size, region_info_t *r_info, uint64_t *decompressed_size, uint64_t cid) {
    *decompressed_size = 0;
    uintptr_t base = (uintptr_t)msg_srv->resp_buff_base;
    uintptr_t current = (uintptr_t)msg_srv->resp_buff;

    if (!base || !current || current < base || current - base > msg_srv->total_max_msg_size || input_size > msg_srv->total_max_msg_size - (current - base) ||
        data_size > input_size)
        return 2;
    uint64_t compressed_size = input_size - data_size;

    if (compressed_size < 5 + sizeof(uint64_t))
        return 3;
    lzma_stream strm = LZMA_STREAM_INIT;
    lzma_ret init_result = lzma_auto_decoder(&strm, UINT64_MAX, 0);

    if (init_result != LZMA_OK) {
        lzma_end(&strm);
        return (init_result == LZMA_MEM_ERROR || init_result == LZMA_MEMLIMIT_ERROR) ? DECOMPRESS_LZMA_NOMEM : 1;
    }
    uint8_t *compressed_data = msg_srv->resp_buff + data_size;
    uint64_t output_buffer_size;
    memcpy(&output_buffer_size, compressed_data + 5, sizeof(uint64_t));
    memset(compressed_data + 5, 0xFF, sizeof(uint64_t));

    if (output_buffer_size > max_record_size) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Invalid LZMA output size=%lu (max_record_size=%u)\n", curr_dt(dt), cid, output_buffer_size, max_record_size);
        lzma_end(&strm);
        return DECOMPRESS_LZMA_INVALID_SIZE;
    }
    uint8_t *decompressed_data = malloc(output_buffer_size != 0 ? output_buffer_size : 1);

    if (decompressed_data == NULL) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed malloc for output_buffer_size=%lu\n", curr_dt(dt), cid, output_buffer_size);
        lzma_end(&strm);
        return DECOMPRESS_LZMA_NOMEM;
    }

    if (output_buffer_size > r_info->bbuff_size) {
        void *tmp_bbuff = realloc(r_info->bbuff, output_buffer_size);

        if (tmp_bbuff == NULL) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Failed realloc r_info->bbuff for output_buffer_size=%lu\n", curr_dt(dt), cid, output_buffer_size);
            free(decompressed_data);
            lzma_end(&strm);
            return DECOMPRESS_LZMA_REALLOC_FAILED;
        }
        r_info->bbuff = tmp_bbuff;
        r_info->bbuff_size = output_buffer_size;
    }
    strm.next_in = compressed_data;
    strm.avail_in = compressed_size;
    strm.next_out = decompressed_data;
    strm.avail_out = output_buffer_size;
    lzma_ret ret = lzma_code(&strm, LZMA_FINISH);

    if (ret != LZMA_STREAM_END) {
        free(decompressed_data);
        lzma_end(&strm);
        return (ret == LZMA_MEM_ERROR || ret == LZMA_MEMLIMIT_ERROR) ? DECOMPRESS_LZMA_NOMEM : 6;
    }
    *decompressed_size = output_buffer_size - strm.avail_out;
    memcpy(r_info->bbuff, decompressed_data, *decompressed_size);
    free(decompressed_data);
    lzma_end(&strm);
    return 0;
}

size_t align_up(size_t x, size_t align) {
    size_t r = x % align;
    return r ? (x + (align - r)) : x;
}

void rotate_block_messages(region_info_t *r_info, uint32_t total_size) {
    uint8_t *rb = r_info->rbuff;
    uint8_t *bb = r_info->bbuff;
    uint32_t first_total_size;
    memcpy(&first_total_size, rb, sizeof(first_total_size));
    size_t first_total_aligned = align_up(first_total_size, FIX_BSIZE_FACTOR);

    if (first_total_aligned <= total_size) {
        memcpy(bb, rb, first_total_size);
        size_t tail_size = total_size - first_total_aligned;

        if (tail_size > 0)
            memmove(rb, rb + first_total_aligned, tail_size);
        size_t insert_off = align_up(tail_size, FIX_BSIZE_FACTOR);

        if (insert_off > tail_size)
            memset(rb + tail_size, 0, insert_off - tail_size);
        memcpy(rb + insert_off, bb, first_total_size);

        if (first_total_aligned > first_total_size)
            memset(rb + insert_off + first_total_size, 0, first_total_aligned - first_total_size);
    }
}


int compress_block(const uint8_t *input_data, uint64_t input_size, region_info_t *r_info, uint64_t *compressed_size, uint8_t snappy_block_factor, uint16_t records_count) {
    if (input_size > UINT32_MAX)
        return -1;

    if (input_size == 0) {
        *compressed_size = 0L;
        return 0;
    }
    uint32_t in_len = (uint32_t)input_size;
    uint32_t max_comp = csnappy_max_compressed_length(in_len);

    if (r_info->block_comp_buff_size < max_comp) {
        uint8_t *block_comp_buff_tmp = realloc(r_info->block_comp_buff, max_comp);

        if (!block_comp_buff_tmp)
            return -2;
        r_info->block_comp_buff = block_comp_buff_tmp;
        r_info->block_comp_buff_size = max_comp;
    }
    uint32_t comp_len = max_comp;

    if (r_info->enable_block_compression == BLOCK_COMPRESSION_LZ)
        comp_len = LZ4_compress_default(input_data, r_info->block_comp_buff, in_len, max_comp);

    if (r_info->enable_block_compression == BLOCK_COMPRESSION_SNAPPY)
        csnappy_compress((const char *)input_data, in_len, (char *)r_info->block_comp_buff, &comp_len, r_info->snappy_work_buff, SNAPPY_BLOCK_SIZE_POW2);
    uint32_t total_len = sizeof(uint32_t) + sizeof(uint64_t) + comp_len + sizeof(uint32_t);

    if (r_info->bbuff_size < total_len + MAX_BSIZE_FACTOR * snappy_block_factor) {
        void *nb = realloc(r_info->bbuff, total_len + MAX_BSIZE_FACTOR * snappy_block_factor);

        if (nb == NULL)
            return -2;
        r_info->bbuff = nb;
        r_info->bbuff_size = total_len + MAX_BSIZE_FACTOR * snappy_block_factor;
    }
    uint8_t *p = r_info->bbuff;
    memcpy(p, &total_len, sizeof(uint32_t));
    memcpy(p + sizeof(uint32_t), &input_size, sizeof(uint64_t));
    memcpy(r_info->bbuff + sizeof(uint32_t) + sizeof(uint64_t) - sizeof(uint16_t), &records_count, sizeof(uint16_t));
    memcpy(p + sizeof(uint32_t) + sizeof(uint64_t), r_info->block_comp_buff, comp_len);
    uint32_t z_index = (uint32_t)((total_len - 1) / MAX_BSIZE_FACTOR + 1);
    uint32_t zero_padd = z_index * MAX_BSIZE_FACTOR - total_len;
    memset(r_info->bbuff + total_len - sizeof(uint32_t), 0, zero_padd);
    uint32_t crc_block = crc_32(r_info->bbuff, z_index * MAX_BSIZE_FACTOR - sizeof(uint32_t));
    memcpy(r_info->bbuff + z_index * MAX_BSIZE_FACTOR - sizeof(uint32_t), &crc_block, sizeof(uint32_t));
    *compressed_size = total_len;
    return 0;
}

uint64_t decompress_block(uint8_t *sbuff, size_t input_size, uint32_t b_offset, uint8_t bs, region_info_t *r_info, uint64_t *decompressed_size, uint32_t result_offset,
                          table_handler_t *t_handler) {
    char dt[DT_LOG_SIZE];
    *decompressed_size = 0;

    if (sbuff == NULL || input_size < sizeof(uint32_t) + sizeof(uint64_t))
        return 3;
    uint32_t total_len;
    memcpy(&total_len, sbuff, sizeof(total_len));

    if (total_len < sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t))
        return 3;
    uint64_t aligned_size = ((uint64_t)total_len + MAX_BSIZE_FACTOR - 1) / MAX_BSIZE_FACTOR * MAX_BSIZE_FACTOR;

    if (aligned_size > input_size || aligned_size > UINT32_MAX)
        return 3;
    uint32_t data_size = (uint32_t)aligned_size;
    uint32_t data_crc;
    uint32_t fact_crc = crc_32(sbuff, data_size - sizeof(data_crc));
    memcpy(&data_crc, sbuff + data_size - sizeof(data_crc), sizeof(data_crc));

    if (data_crc != fact_crc) {
        fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] Read wrong CRC %s for block=%u, data_crc=%x fact_crc=%x, data_size=%u\n", curr_dt(dt), t_handler->cid,
                r_info->r_idx_b[bs]->fname, b_offset, data_crc, fact_crc, data_size);
        data_fix(r_info, bs, b_offset, 0, t_handler);
        return 2;
    }
    uint32_t comp_len = total_len - (sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t));
    uint64_t orig_size;
    memcpy(&orig_size, sbuff + sizeof(uint32_t), sizeof(orig_size));
    orig_size = (orig_size << 16) >> 16;

    if (orig_size > max_record_size) {
        return 3;
    }

    if (r_info->bbuff_size < result_offset + orig_size + FIX_BSIZE_FACTOR) {
        void *nb = realloc(r_info->bbuff, result_offset + orig_size + FIX_BSIZE_FACTOR);

        if (!nb) {
            fprintf(stderr, "%s ERROR [MSG_UTILS=%015lu] realloc(bbuff,%lu) failed\n", curr_dt(dt), t_handler->cid, orig_size);
            return 4;
        }
        r_info->bbuff = nb;
        r_info->bbuff_size = result_offset + orig_size + FIX_BSIZE_FACTOR;
    }
    uint8_t *payload = sbuff + sizeof(uint32_t) + sizeof(uint64_t);

    if (r_info->enable_block_compression == BLOCK_COMPRESSION_LZ) {
        if (comp_len > INT_MAX || orig_size > INT_MAX)
            return 5;

        if (LZ4_decompress_safe(payload, r_info->bbuff + result_offset, comp_len, orig_size) != orig_size)
            return 5;
    }

    if (r_info->enable_block_compression == BLOCK_COMPRESSION_SNAPPY) {
        int status = csnappy_decompress((const char *)payload, comp_len, (char *)r_info->bbuff + result_offset, (size_t)orig_size);

        if (status != CSNAPPY_E_OK)
            return 5;
    }
    memset(r_info->bbuff + result_offset + orig_size, 0, align_up(orig_size, FIX_BSIZE_FACTOR) - orig_size);
    *decompressed_size = orig_size;
    return 0;
}
