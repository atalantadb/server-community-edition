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
typedef struct msg_write_compact_final_hash_input_t {
    uint64_t compact_offset;
    uint64_t write_offset;
    uint64_t expected_final_file_size;
    uint32_t retained_compact_size;
    uint32_t aligned_write_size;
    uint8_t compact_hash[MSG_WRITE_COMPACT_HASH_SIZE];
    uint8_t write_hash[MSG_WRITE_COMPACT_HASH_SIZE];
} __attribute__((packed)) msg_write_compact_final_hash_input_t;
static void msg_write_compact_store_hash(XXH128_hash_t value, uint8_t hash[MSG_WRITE_COMPACT_HASH_SIZE]) {
    XXH128_canonical_t canonical;
    XXH128_canonicalFromHash(&canonical, value);
    memcpy(hash, canonical.digest, MSG_WRITE_COMPACT_HASH_SIZE);
}


int msg_write_compact_hash128(const uint8_t *data, uint32_t size, uint8_t hash[MSG_WRITE_COMPACT_HASH_SIZE]) {
    if (hash == NULL || (size != 0 && data == NULL))
        return MSG_WRITE_COMPACT_INVALID_ARGUMENT;
    msg_write_compact_store_hash(XXH3_128bits(data, size), hash);
    return MSG_WRITE_COMPACT_OK;
}


int msg_write_compact_hash128_padded(const uint8_t *data, uint32_t size, uint32_t padded_size, uint8_t hash[MSG_WRITE_COMPACT_HASH_SIZE]) {
    if (hash == NULL || size > padded_size || (size != 0 && data == NULL))
        return MSG_WRITE_COMPACT_INVALID_ARGUMENT;
    XXH3_state_t *state = XXH3_createState();

    if (state == NULL || XXH3_128bits_reset(state) != XXH_OK || (size != 0 && XXH3_128bits_update(state, data, size) != XXH_OK)) {
        XXH3_freeState(state);
        return MSG_WRITE_COMPACT_INVALID_ARGUMENT;
    }
    static const uint8_t zeroes[FIX_BLOCK_SIZE] = {0};
    uint32_t remaining = padded_size - size;

    while (remaining != 0) {
        uint32_t chunk = remaining > sizeof(zeroes) ? sizeof(zeroes) : remaining;

        if (XXH3_128bits_update(state, zeroes, chunk) != XXH_OK) {
            XXH3_freeState(state);
            return MSG_WRITE_COMPACT_INVALID_ARGUMENT;
        }
        remaining -= chunk;
    }
    msg_write_compact_store_hash(XXH3_128bits_digest(state), hash);
    XXH3_freeState(state);
    return MSG_WRITE_COMPACT_OK;
}


int msg_write_compact_v2_source_hash(const msg_write_compact_block_meta_t *block_meta, uint32_t block_meta_count, uint8_t hash[MSG_WRITE_COMPACT_HASH_SIZE]) {
    if (block_meta == NULL || block_meta_count == 0 || block_meta_count > UINT32_MAX / sizeof(*block_meta))
        return MSG_WRITE_COMPACT_INVALID_ARGUMENT;
    return msg_write_compact_hash128((const uint8_t *)block_meta, block_meta_count * (uint32_t)sizeof(*block_meta), hash);
}

int msg_write_compact_v2_final_hash(const msg_write_compact_v2_t *header, uint32_t retained_compact_size, const uint8_t compact_hash[MSG_WRITE_COMPACT_HASH_SIZE],
                                    uint32_t aligned_write_size, const uint8_t write_hash[MSG_WRITE_COMPACT_HASH_SIZE], uint8_t hash[MSG_WRITE_COMPACT_HASH_SIZE]) {
    if (header == NULL || compact_hash == NULL || write_hash == NULL || hash == NULL)
        return MSG_WRITE_COMPACT_INVALID_ARGUMENT;
    msg_write_compact_final_hash_input_t input;
    memset(&input, 0, sizeof(input));
    input.compact_offset = (uint64_t)header->first_block * FIX_BLOCK_SIZE;
    input.write_offset = header->write_offset;
    input.expected_final_file_size = header->expected_final_file_size;
    input.retained_compact_size = retained_compact_size;
    input.aligned_write_size = aligned_write_size;
    memcpy(input.compact_hash, compact_hash, sizeof(input.compact_hash));
    memcpy(input.write_hash, write_hash, sizeof(input.write_hash));
    return msg_write_compact_hash128((const uint8_t *)&input, sizeof(input), hash);
}

static int msg_write_compact_total_size(const msg_write_compact_t *header, uint32_t *total_size) {
    if (header == NULL || total_size == NULL)
        return MSG_WRITE_COMPACT_INVALID_ARGUMENT;
    uint64_t total = (uint64_t)MSG_WRITE_COMPACT_HEADER_SIZE + header->compact_size + header->write_size;

    if (total > UINT32_MAX)
        return MSG_WRITE_COMPACT_SIZE_OVERFLOW;
    *total_size = (uint32_t)total;
    return MSG_WRITE_COMPACT_OK;
}

static int msg_write_compact_v2_total_size(const msg_write_compact_v2_t *header, uint32_t *total_size) {
    if (header == NULL || total_size == NULL)
        return MSG_WRITE_COMPACT_INVALID_ARGUMENT;
    uint64_t meta_size = (uint64_t)header->block_meta_count * sizeof(msg_write_compact_block_meta_t);
    uint64_t total = (uint64_t)MSG_WRITE_COMPACT_V2_HEADER_SIZE + meta_size + header->bitmap_size + header->write_size;

    if (meta_size > UINT32_MAX || total > UINT32_MAX)
        return MSG_WRITE_COMPACT_SIZE_OVERFLOW;
    *total_size = (uint32_t)total;
    return MSG_WRITE_COMPACT_OK;
}

static bool msg_write_compact_ranges_overlap(uint64_t offset1, uint64_t size1, uint64_t offset2, uint64_t size2) {
    return offset1 < offset2 + size2 && offset2 < offset1 + size1;
}


static int validate_msg_write_compact_header(const msg_write_compact_t *header, uint32_t payload_size) {
    if (header == NULL)
        return MSG_WRITE_COMPACT_INVALID_ARGUMENT;

    if (header->version != MSG_WRITE_COMPACT_VERSION)
        return MSG_WRITE_COMPACT_INVALID_VERSION;

    if ((header->flags & (uint8_t)~MSG_WRITE_COMPACT_KNOWN_FLAGS) != 0)
        return MSG_WRITE_COMPACT_INVALID_FLAGS;
    uint32_t calculated_size = 0;
    int result = msg_write_compact_total_size(header, &calculated_size);

    if (result != MSG_WRITE_COMPACT_OK)
        return result;

    if (calculated_size != payload_size)
        return MSG_WRITE_COMPACT_SIZE_MISMATCH;

    if (calculated_size > RSC_MSG_MAX_PAYLOAD)
        return MSG_WRITE_COMPACT_PAYLOAD_TOO_LARGE;

    if (header->compact_size == 0 || header->compact_size % FIX_BLOCK_SIZE != 0 || header->write_size == 0)
        return MSG_WRITE_COMPACT_INVALID_COMPACT_SIZE;

    if (header->compact_b >= BUCKETS || header->write_b >= BUCKETS || header->compact_bs == 0 || header->write_bs == 0 || header->compact_bs > BLOCK_ARRAY_LAST_POS + 1U ||
        header->write_bs > BLOCK_ARRAY_LAST_POS + 1U || header->compact_b != header->write_b || header->compact_bs != header->write_bs)
        return MSG_WRITE_COMPACT_INVALID_TARGET;

    if (header->write_offset < FIX_BLOCK_SIZE || header->compact_offset % FIX_BLOCK_SIZE != 0)
        return MSG_WRITE_COMPACT_INVALID_OFFSET;
    uint32_t write_alignment = (uint32_t)header->write_bs * FIX_BSIZE_FACTOR;
    uint64_t aligned_write_size = ((uint64_t)header->write_size + write_alignment - 1U) / write_alignment * write_alignment;

    if (header->compact_offset > UINT64_MAX - header->compact_size || header->write_offset > UINT64_MAX - aligned_write_size)
        return MSG_WRITE_COMPACT_SIZE_OVERFLOW;
    uint64_t compact_end = header->compact_offset + header->compact_size;
    uint64_t write_end = header->write_offset + aligned_write_size;
    bool do_truncate = (header->flags & MSG_WRITE_COMPACT_FLAG_TRUNCATE) != 0;

    if (header->compact_old_file_size < FIX_BLOCK_SIZE || header->compact_new_file_size < FIX_BLOCK_SIZE || header->compact_new_file_size > header->compact_old_file_size ||
        header->compact_offset > header->compact_new_file_size || header->write_offset > header->compact_new_file_size)
        return MSG_WRITE_COMPACT_INVALID_FILE_SIZE;

    if ((do_truncate && header->compact_new_file_size == header->compact_old_file_size) ||
        (header->compact_new_file_size != header->compact_old_file_size && header->compact_new_file_size % FIX_BLOCK_SIZE != 0))
        return MSG_WRITE_COMPACT_INVALID_FILE_SIZE;

    if (header->compact_old_file_size > UINT64_MAX - (FIX_BLOCK_SIZE - 1U))
        return MSG_WRITE_COMPACT_SIZE_OVERFLOW;
    uint64_t old_file_size_aligned = (header->compact_old_file_size + FIX_BLOCK_SIZE - 1U) & ~((uint64_t)FIX_BLOCK_SIZE - 1U);

    if (compact_end > old_file_size_aligned)
        return MSG_WRITE_COMPACT_INVALID_FILE_SIZE;

    if (msg_write_compact_ranges_overlap(header->compact_offset, header->compact_size, header->write_offset, aligned_write_size) && header->write_offset < header->compact_offset)
        return MSG_WRITE_COMPACT_INVALID_ORDER;
    uint64_t file_size_after_compact_write = header->compact_old_file_size > compact_end ? header->compact_old_file_size : compact_end;
    uint64_t file_size_before_write = do_truncate ? header->compact_new_file_size : file_size_after_compact_write;
    uint64_t expected_final_file_size = file_size_before_write > write_end ? file_size_before_write : write_end;

    if (header->expected_final_file_size != expected_final_file_size)
        return MSG_WRITE_COMPACT_INVALID_FINAL_SIZE;
    return MSG_WRITE_COMPACT_OK;
}

int validate_msg_write_compact(const uint8_t *payload, uint32_t payload_size, msg_write_compact_t *header, const uint8_t **compact_buffer, const uint8_t **write_buffer) {
    if (payload == NULL || payload_size < MSG_WRITE_COMPACT_HEADER_SIZE)
        return MSG_WRITE_COMPACT_INVALID_ARGUMENT;
    msg_write_compact_t decoded;
    memcpy(&decoded, payload, MSG_WRITE_COMPACT_HEADER_SIZE);
    int result = validate_msg_write_compact_header(&decoded, payload_size);

    if (result != MSG_WRITE_COMPACT_OK)
        return result;

    if (header != NULL)
        memcpy(header, &decoded, MSG_WRITE_COMPACT_HEADER_SIZE);

    if (compact_buffer != NULL)
        *compact_buffer = payload + MSG_WRITE_COMPACT_HEADER_SIZE;

    if (write_buffer != NULL)
        *write_buffer = payload + MSG_WRITE_COMPACT_HEADER_SIZE + decoded.compact_size;
    return MSG_WRITE_COMPACT_OK;
}


int serialize_msg_write_compact(uint8_t *payload, uint32_t payload_capacity, const msg_write_compact_t *header, const uint8_t *compact_buffer, const uint8_t *write_buffer,
                                uint32_t *payload_size) {
    if (payload == NULL || header == NULL || compact_buffer == NULL || write_buffer == NULL || payload_size == NULL)
        return MSG_WRITE_COMPACT_INVALID_ARGUMENT;
    uint32_t total_size = 0;
    int result = msg_write_compact_total_size(header, &total_size);

    if (result != MSG_WRITE_COMPACT_OK)
        return result;

    if (total_size > payload_capacity)
        return MSG_WRITE_COMPACT_PAYLOAD_TOO_LARGE;
    result = validate_msg_write_compact_header(header, total_size);

    if (result != MSG_WRITE_COMPACT_OK)
        return result;
    memcpy(payload, header, MSG_WRITE_COMPACT_HEADER_SIZE);
    memcpy(payload + MSG_WRITE_COMPACT_HEADER_SIZE, compact_buffer, header->compact_size);
    memcpy(payload + MSG_WRITE_COMPACT_HEADER_SIZE + header->compact_size, write_buffer, header->write_size);
    result = validate_msg_write_compact(payload, total_size, NULL, NULL, NULL);

    if (result != MSG_WRITE_COMPACT_OK)
        return result;
    *payload_size = total_size;
    return MSG_WRITE_COMPACT_OK;
}

static int validate_msg_write_compact_v2_header(const msg_write_compact_v2_t *header, uint32_t payload_size) {
    if (header == NULL)
        return MSG_WRITE_COMPACT_INVALID_ARGUMENT;

    if (header->version != MSG_WRITE_COMPACT_VERSION_BITMAP)
        return MSG_WRITE_COMPACT_INVALID_VERSION;

    if ((header->flags & (uint8_t)~MSG_WRITE_COMPACT_KNOWN_FLAGS) != 0)
        return MSG_WRITE_COMPACT_INVALID_FLAGS;
    uint32_t calculated_size = 0;
    int result = msg_write_compact_v2_total_size(header, &calculated_size);

    if (result != MSG_WRITE_COMPACT_OK)
        return result;

    if (calculated_size != payload_size)
        return MSG_WRITE_COMPACT_SIZE_MISMATCH;

    if (calculated_size > RSC_MSG_MAX_PAYLOAD)
        return MSG_WRITE_COMPACT_PAYLOAD_TOO_LARGE;

    if (header->source_blocks == 0 || header->source_blocks > APPEND_COMPACTION_BLOCKS_PER_SEGMENT || header->tail_blocks > APPEND_COMPACTION_BLOCKS_PER_SEGMENT ||
        header->block_meta_count != header->source_blocks + header->tail_blocks)
        return MSG_WRITE_COMPACT_INVALID_BLOCK_COUNT;

    if (header->segment > UINT32_MAX / APPEND_COMPACTION_BLOCKS_PER_SEGMENT || header->first_block != header->segment * APPEND_COMPACTION_BLOCKS_PER_SEGMENT)
        return MSG_WRITE_COMPACT_INVALID_BLOCK_COUNT;

    if (header->compact_b >= BUCKETS || header->write_b >= BUCKETS || header->compact_bs == 0 || header->write_bs == 0 || header->compact_bs > BLOCK_ARRAY_LAST_POS + 1U ||
        header->write_bs > BLOCK_ARRAY_LAST_POS + 1U || header->compact_b != header->write_b || header->compact_bs != header->write_bs)
        return MSG_WRITE_COMPACT_INVALID_TARGET;

    if (header->write_size == 0 || header->write_offset < FIX_BLOCK_SIZE || header->transaction_id == 0)
        return MSG_WRITE_COMPACT_INVALID_ARGUMENT;
    uint32_t expected_bitmap_size = header->record_count / 8U + ((header->record_count & 7U) != 0);

    if (header->source_record_count > header->record_count || header->bitmap_size != expected_bitmap_size)
        return MSG_WRITE_COMPACT_INVALID_BITMAP;

    if (header->first_block > UINT32_MAX - (header->source_blocks - 1U))
        return MSG_WRITE_COMPACT_SIZE_OVERFLOW;
    uint32_t source_last_block = header->first_block + header->source_blocks - 1U;

    if (source_last_block > header->original_b_offset || header->new_b_offset > header->original_b_offset)
        return MSG_WRITE_COMPACT_INVALID_FILE_SIZE;

    if (header->tail_blocks == 0) {
        if (header->tail_first_block != 0)
            return MSG_WRITE_COMPACT_INVALID_BLOCK_COUNT;
    } else {
        if (header->tail_first_block <= source_last_block || header->tail_first_block > UINT32_MAX - (header->tail_blocks - 1U))
            return MSG_WRITE_COMPACT_INVALID_BLOCK_COUNT;
        uint32_t tail_last_block = header->tail_first_block + header->tail_blocks - 1U;

        if (tail_last_block > header->original_b_offset || header->tail_range_first_block == 0 || header->tail_range_first_block > header->tail_first_block)
            return MSG_WRITE_COMPACT_INVALID_BLOCK_COUNT;
    }

    if (header->tail_range_first_block > header->original_b_offset && header->tail_range_first_block != 0)
        return MSG_WRITE_COMPACT_INVALID_BLOCK_COUNT;
    uint64_t compact_offset = (uint64_t)header->first_block * FIX_BLOCK_SIZE;
    uint64_t compact_size = (uint64_t)header->source_blocks * FIX_BLOCK_SIZE;

    if (compact_offset > UINT64_MAX - compact_size || header->write_offset > UINT64_MAX - header->write_size)
        return MSG_WRITE_COMPACT_SIZE_OVERFLOW;
    uint64_t compact_end = compact_offset + compact_size;
    uint32_t write_alignment = (uint32_t)header->write_bs * FIX_BSIZE_FACTOR;
    uint64_t aligned_write_size = ((uint64_t)header->write_size + write_alignment - 1U) / write_alignment * write_alignment;

    if (header->write_offset > UINT64_MAX - aligned_write_size)
        return MSG_WRITE_COMPACT_SIZE_OVERFLOW;
    uint64_t write_end = header->write_offset + aligned_write_size;

    if (msg_write_compact_ranges_overlap(compact_offset, compact_size, header->write_offset, aligned_write_size))
        return MSG_WRITE_COMPACT_INVALID_ORDER;
    bool do_truncate = (header->flags & MSG_WRITE_COMPACT_FLAG_TRUNCATE) != 0;

    if (header->compact_old_file_size < FIX_BLOCK_SIZE || header->compact_new_file_size < FIX_BLOCK_SIZE || header->compact_new_file_size > header->compact_old_file_size ||
        header->write_offset > header->compact_new_file_size)
        return MSG_WRITE_COMPACT_INVALID_FILE_SIZE;

    if (header->compact_old_file_size > UINT64_MAX - (FIX_BLOCK_SIZE - 1U))
        return MSG_WRITE_COMPACT_SIZE_OVERFLOW;
    uint64_t old_file_size_aligned = (header->compact_old_file_size + FIX_BLOCK_SIZE - 1U) & ~((uint64_t)FIX_BLOCK_SIZE - 1U);

    if (compact_end > old_file_size_aligned)
        return MSG_WRITE_COMPACT_INVALID_FILE_SIZE;

    if (header->tail_blocks != 0 && ((uint64_t)header->tail_first_block + header->tail_blocks) * FIX_BLOCK_SIZE > old_file_size_aligned)
        return MSG_WRITE_COMPACT_INVALID_FILE_SIZE;

    if (header->new_b_offset == UINT32_MAX || (uint64_t)header->new_b_offset + 1U > UINT64_MAX / FIX_BLOCK_SIZE)
        return MSG_WRITE_COMPACT_SIZE_OVERFLOW;
    uint64_t logical_new_file_size = ((uint64_t)header->new_b_offset + 1U) * FIX_BLOCK_SIZE;

    if ((header->new_b_offset < header->original_b_offset && header->compact_new_file_size != logical_new_file_size) ||
        (header->new_b_offset == header->original_b_offset && header->compact_new_file_size != header->compact_old_file_size &&
         header->compact_new_file_size != logical_new_file_size))
        return MSG_WRITE_COMPACT_INVALID_FILE_SIZE;

    if (do_truncate && header->compact_new_file_size >= header->compact_old_file_size)
        return MSG_WRITE_COMPACT_INVALID_FILE_SIZE;
    uint64_t file_size_after_compact_write = header->compact_old_file_size > compact_end ? header->compact_old_file_size : compact_end;
    uint64_t file_size_before_write = do_truncate ? header->compact_new_file_size : file_size_after_compact_write;
    uint64_t expected_final_file_size = file_size_before_write > write_end ? file_size_before_write : write_end;

    if (header->expected_final_file_size != expected_final_file_size)
        return MSG_WRITE_COMPACT_INVALID_FINAL_SIZE;
    return MSG_WRITE_COMPACT_OK;
}

int validate_msg_write_compact_v2(const uint8_t *payload, uint32_t payload_size, msg_write_compact_v2_t *header, const uint8_t **block_meta, const uint8_t **live_bitmap,
                                  const uint8_t **write_buffer) {
    if (payload == NULL || payload_size < MSG_WRITE_COMPACT_V2_HEADER_SIZE)
        return MSG_WRITE_COMPACT_INVALID_ARGUMENT;
    msg_write_compact_v2_t decoded;
    memcpy(&decoded, payload, MSG_WRITE_COMPACT_V2_HEADER_SIZE);
    int result = validate_msg_write_compact_v2_header(&decoded, payload_size);

    if (result != MSG_WRITE_COMPACT_OK)
        return result;
    const uint8_t *decoded_meta = payload + MSG_WRITE_COMPACT_V2_HEADER_SIZE;
    uint64_t source_records = 0;
    uint64_t records = 0;
    uint32_t alignment = (uint32_t)decoded.compact_bs * FIX_BSIZE_FACTOR;

    for (uint32_t i = 0; i < decoded.block_meta_count; i++) {
        msg_write_compact_block_meta_t meta;
        memcpy(&meta, decoded_meta + (size_t)i * sizeof(meta), sizeof(meta));
        uint32_t expected_block = i < decoded.source_blocks ? decoded.first_block + i : decoded.tail_first_block + (i - decoded.source_blocks);

        if (meta.block != expected_block || meta.used_bytes > FIX_BLOCK_SIZE || meta.used_bytes % alignment != 0 ||
            (meta.block == 0 && (meta.used_bytes != FIX_BLOCK_SIZE || meta.record_count != 0)) || (meta.block != 0 && ((meta.used_bytes == 0) != (meta.record_count == 0))) ||
            (meta.block != 0 && meta.record_count > meta.used_bytes / (KEY_SIZE_RECORD_OFFSET + KEY_RECORD_OFFSET)))
            return MSG_WRITE_COMPACT_INVALID_BLOCK_META;
        records += meta.record_count;

        if (i < decoded.source_blocks)
            source_records += meta.record_count;
    }

    if (records != decoded.record_count || source_records != decoded.source_record_count)
        return MSG_WRITE_COMPACT_INVALID_BLOCK_META;
    uint8_t source_hash[MSG_WRITE_COMPACT_HASH_SIZE];

    if (msg_write_compact_v2_source_hash((const msg_write_compact_block_meta_t *)decoded_meta, decoded.block_meta_count, source_hash) != MSG_WRITE_COMPACT_OK ||
        memcmp(source_hash, decoded.source_hash, sizeof(source_hash)) != 0)
        return MSG_WRITE_COMPACT_INVALID_BLOCK_META;
    size_t meta_size = (size_t)decoded.block_meta_count * sizeof(msg_write_compact_block_meta_t);
    const uint8_t *decoded_bitmap = decoded_meta + meta_size;

    if ((decoded.record_count & 7U) != 0 && (decoded_bitmap[decoded.bitmap_size - 1U] & (uint8_t) ~((1U << (decoded.record_count & 7U)) - 1U)) != 0)
        return MSG_WRITE_COMPACT_INVALID_BITMAP;

    if (header != NULL)
        memcpy(header, &decoded, MSG_WRITE_COMPACT_V2_HEADER_SIZE);

    if (block_meta != NULL)
        *block_meta = decoded_meta;

    if (live_bitmap != NULL)
        *live_bitmap = decoded_bitmap;

    if (write_buffer != NULL)
        *write_buffer = decoded_bitmap + decoded.bitmap_size;
    return MSG_WRITE_COMPACT_OK;
}

int serialize_msg_write_compact_v2(uint8_t *payload, uint32_t payload_capacity, const msg_write_compact_v2_t *header, const msg_write_compact_block_meta_t *block_meta,
                                   const uint8_t *live_bitmap, const uint8_t *write_buffer, uint32_t *payload_size) {
    if (payload == NULL || header == NULL || block_meta == NULL || write_buffer == NULL || payload_size == NULL || (header->bitmap_size != 0 && live_bitmap == NULL))
        return MSG_WRITE_COMPACT_INVALID_ARGUMENT;
    uint32_t total_size = 0;
    int result = msg_write_compact_v2_total_size(header, &total_size);

    if (result != MSG_WRITE_COMPACT_OK)
        return result;

    if (total_size > payload_capacity)
        return MSG_WRITE_COMPACT_PAYLOAD_TOO_LARGE;
    result = validate_msg_write_compact_v2_header(header, total_size);

    if (result != MSG_WRITE_COMPACT_OK)
        return result;
    size_t meta_size = (size_t)header->block_meta_count * sizeof(msg_write_compact_block_meta_t);
    memcpy(payload, header, MSG_WRITE_COMPACT_V2_HEADER_SIZE);
    memcpy(payload + MSG_WRITE_COMPACT_V2_HEADER_SIZE, block_meta, meta_size);

    if (header->bitmap_size != 0)
        memcpy(payload + MSG_WRITE_COMPACT_V2_HEADER_SIZE + meta_size, live_bitmap, header->bitmap_size);
    memcpy(payload + MSG_WRITE_COMPACT_V2_HEADER_SIZE + meta_size + header->bitmap_size, write_buffer, header->write_size);
    result = validate_msg_write_compact_v2(payload, total_size, NULL, NULL, NULL, NULL);

    if (result != MSG_WRITE_COMPACT_OK)
        return result;
    *payload_size = total_size;
    return MSG_WRITE_COMPACT_OK;
}

const char *msg_write_compact_result_string(int result) {
    switch (result) {
    case MSG_WRITE_COMPACT_OK:
        return "ok";
    case MSG_WRITE_COMPACT_INVALID_ARGUMENT:
        return "invalid argument";
    case MSG_WRITE_COMPACT_INVALID_VERSION:
        return "invalid version";
    case MSG_WRITE_COMPACT_INVALID_FLAGS:
        return "invalid flags";
    case MSG_WRITE_COMPACT_SIZE_OVERFLOW:
        return "size overflow";
    case MSG_WRITE_COMPACT_SIZE_MISMATCH:
        return "size mismatch";
    case MSG_WRITE_COMPACT_PAYLOAD_TOO_LARGE:
        return "payload too large";
    case MSG_WRITE_COMPACT_INVALID_COMPACT_SIZE:
        return "invalid compact size";
    case MSG_WRITE_COMPACT_INVALID_TARGET:
        return "invalid b/bs target";
    case MSG_WRITE_COMPACT_INVALID_OFFSET:
        return "invalid offset";
    case MSG_WRITE_COMPACT_INVALID_FILE_SIZE:
        return "invalid file size";
    case MSG_WRITE_COMPACT_INVALID_FINAL_SIZE:
        return "invalid final file size";
    case MSG_WRITE_COMPACT_INVALID_ORDER:
        return "invalid operation order";
    case MSG_WRITE_COMPACT_INVALID_BLOCK_COUNT:
        return "invalid block count";
    case MSG_WRITE_COMPACT_INVALID_BLOCK_META:
        return "invalid block metadata";
    case MSG_WRITE_COMPACT_INVALID_BITMAP:
        return "invalid live bitmap";
    default:
        return "unknown write-compact validation result";
    }
}
