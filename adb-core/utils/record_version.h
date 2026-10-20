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
#ifndef ADB_RECORD_VERSION_H
#define ADB_RECORD_VERSION_H
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#define RECORD_VERSION_OFFSET 5U
#define RECORD_VERSION_TAG (UINT64_C(1) << 63)
#define RECORD_VERSION_MAX (RECORD_VERSION_TAG - 1U)
#define DISK_RECORD_HEADER_SIZE 28U
typedef struct {
    uint32_t region_pos;
    uint32_t table_pos;
} client_record_route_t;
typedef enum { DISK_RECORD_LEGACY, DISK_RECORD_LIVE, DISK_RECORD_TOMBSTONE } disk_record_kind_t;
typedef struct {
    uint64_t version;
    uint32_t msg_size;
    uint32_t key_size;
    uint32_t cf_size;
    uint32_t qv_size;
    uint32_t qv_offset;
    disk_record_kind_t kind;
} disk_record_header_t;
static inline uint64_t record_header_load_le(const uint8_t *p, unsigned size) {
    uint64_t value = 0;

    for (unsigned i = 0; i < size; ++i)
        value |= (uint64_t)p[i] << (i * 8U);
    return value;
}


static inline void record_header_store_le(uint8_t *p, uint64_t value, unsigned size) {
    for (unsigned i = 0; i < size; ++i)
        p[i] = (uint8_t)(value >> (i * 8U));
}

static inline bool client_record_route_read(const uint8_t *record, size_t available, client_record_route_t *route) {
    if (record == NULL || route == NULL || available < DISK_RECORD_HEADER_SIZE)
        return false;
    route->region_pos = (uint32_t)record_header_load_le(record + RECORD_VERSION_OFFSET, 4);
    route->table_pos = (uint32_t)record_header_load_le(record + RECORD_VERSION_OFFSET + 4, 4);
    return true;
}

static inline bool client_record_route_write(uint8_t *record, size_t available, const client_record_route_t *route) {
    if (record == NULL || route == NULL || available < DISK_RECORD_HEADER_SIZE)
        return false;
    record_header_store_le(record + RECORD_VERSION_OFFSET, route->region_pos, 4);
    record_header_store_le(record + RECORD_VERSION_OFFSET + 4, route->table_pos, 4);
    return true;
}

static inline bool disk_record_version_read(const uint8_t *record, size_t available, uint64_t *version) {
    if (record == NULL || version == NULL || available < DISK_RECORD_HEADER_SIZE)
        return false;
    uint64_t raw = record_header_load_le(record + RECORD_VERSION_OFFSET, 8);
    uint64_t number = (raw & RECORD_VERSION_TAG) ? raw & RECORD_VERSION_MAX : 0;

    if ((raw & RECORD_VERSION_TAG) && number == 0)
        return false;
    *version = number;
    return true;
}

static inline bool disk_record_version_write(uint8_t *record, size_t available, uint64_t version) {
    if (record == NULL || available < DISK_RECORD_HEADER_SIZE || version == 0 || version > RECORD_VERSION_MAX)
        return false;
    record_header_store_le(record + RECORD_VERSION_OFFSET, version | RECORD_VERSION_TAG, 8);
    return true;
}

static inline bool disk_record_decode_header(const uint8_t *record, size_t available, disk_record_header_t *header) {
    disk_record_header_t parsed = {0};

    if (record == NULL || header == NULL || available < DISK_RECORD_HEADER_SIZE)
        return false;
    parsed.msg_size = (uint32_t)record_header_load_le(record, 4);

    if (parsed.msg_size < DISK_RECORD_HEADER_SIZE || parsed.msg_size > available)
        return false;
    parsed.key_size = (uint32_t)record_header_load_le(record + 16, 4);
    parsed.cf_size = (uint32_t)record_header_load_le(record + 20, 4);
    parsed.qv_size = (uint32_t)record_header_load_le(record + 24, 4);

    if (parsed.key_size > parsed.msg_size - DISK_RECORD_HEADER_SIZE || parsed.cf_size > parsed.msg_size - DISK_RECORD_HEADER_SIZE - parsed.key_size)
        return false;
    parsed.qv_offset = DISK_RECORD_HEADER_SIZE + parsed.key_size + parsed.cf_size;

    if (!disk_record_version_read(record, available, &parsed.version))
        return false;
    parsed.kind = parsed.version == 0 ? DISK_RECORD_LEGACY : DISK_RECORD_LIVE;

    if (parsed.version != 0 && parsed.qv_size == 0) {
        if (parsed.msg_size != parsed.qv_offset || record[13] != 0 || record[14] != 0 || record[15] != 0)
            return false;
        parsed.kind = DISK_RECORD_TOMBSTONE;
    }
    *header = parsed;
    return true;
}


typedef struct {
    uint64_t reserved;
    uint64_t confirmed;
    uint64_t pending;
} record_version_state_t;
typedef struct {
    uint64_t version;
    bool changed;
    bool replacing_marker;
} record_mutation_t;
typedef enum { RECORD_GC_IDLE, RECORD_GC_SWEEP, RECORD_GC_FINISH, RECORD_GC_COMPLETE } record_gc_phase_t;
typedef struct {
    uint64_t obsolete, removed, retained;
    uint64_t removed_min, removed_max, retained_min, retained_max;
} record_gc_marker_stats_t;
typedef struct {
    uint64_t segments, applied, stale_records;
    record_gc_marker_stats_t markers;
} record_gc_pass_stats_t;
typedef struct {
    uint64_t generation;
    uint64_t boundary;
    uint64_t witness_version;
    uint64_t safe_revision;
    uint32_t erase_segment;
    uint32_t erase_end_block;
    bool erasing;
    bool erased;
    uint32_t next_segment;
    uint32_t end_block;
    uint32_t expected_last_block;
    int fd;
    uint8_t bs;
    record_gc_phase_t phase;
    record_gc_pass_stats_t history_stats, erase_stats;
    uint64_t pending_begin, erase_rewinds;
    const char *pause_reason;
    bool empty_reported;
} record_gc_state_t;
static inline void record_version_state_init(record_version_state_t *state) {
    state->reserved = 0;
    state->confirmed = 0;
    state->pending = 0;
}

static inline bool record_version_state_valid(const record_version_state_t *state) {
    return state != NULL && state->confirmed <= state->reserved && state->pending <= state->reserved && state->reserved <= RECORD_VERSION_MAX;
}

static inline bool record_version_reserve(record_version_state_t *state, uint64_t *version) {
    if (!record_version_state_valid(state) || version == NULL || state->reserved == RECORD_VERSION_MAX)
        return false;
    *version = ++state->reserved;
    return true;
}


static inline bool record_version_confirm(record_version_state_t *state, uint64_t version) {
    if (!record_version_state_valid(state) || version == 0 || version > state->reserved)
        return false;

    if (version > state->confirmed)
        state->confirmed = version;
    return true;
}

static inline bool record_version_stage(record_version_state_t *state, uint64_t version) {
    if (!record_version_state_valid(state) || version == 0 || version > state->reserved)
        return false;

    if (version > state->pending)
        state->pending = version;
    return true;
}

static inline bool record_version_commit_pending(record_version_state_t *state) {
    if (!record_version_state_valid(state))
        return false;

    if (state->pending > state->confirmed)
        state->confirmed = state->pending;
    state->pending = 0;
    return true;
}

static inline bool record_version_observe(record_version_state_t *state, uint64_t version) {
    if (!record_version_state_valid(state) || version > RECORD_VERSION_MAX)
        return false;

    if (version > state->reserved)
        state->reserved = version;

    if (version > state->confirmed)
        state->confirmed = version;
    return true;
}
#endif
