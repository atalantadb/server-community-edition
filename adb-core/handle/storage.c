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
#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE
#include "storage.h"
static void log_write_4K_state(const char *event, const char *reason, region_info_t *r_info, int fd, uint64_t offset, uint32_t write_size, uint32_t total, uint64_t cid) {
    if (log_level <= 2)
        return;
    char dt[DT_LOG_SIZE];
    fprintf(
        stderr,
        "%s TRACE [STORAGE=%015lu] write_4K %s reason=%s, uuid=%s, r_info=%p, input_fd=%d, offset=%lu, write_size=%u, total=%u, stored_fd=%d, buffered=%u, buffered_offset=%lu\n",
        curr_dt(dt), cid, event, reason != NULL ? reason : "none", r_info != NULL ? r_info->uuid : "(null)", (void *)r_info, fd, offset, write_size, total,
        r_info != NULL ? r_info->write_4K_fd : -1, r_info != NULL ? r_info->write_4K_buff_used : 0, r_info != NULL ? r_info->write_4K_file_offset : 0);
}


static uint64_t monotonic_time_ms(void) {
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
        return 0;
    return (uint64_t)ts.tv_sec * 1000U + (uint64_t)ts.tv_nsec / 1000000U;
}


void bind_data_000_fd(region_info_t *r_info, int fd, uint64_t cid) {
    (void)cid;

    if (r_info == NULL || fd < 0)
        return;
    r_info->data_000_fd = fd;
}


void unbind_data_000_fd(region_info_t *r_info, int fd, uint64_t cid) {
    (void)cid;

    if (r_info == NULL || r_info->data_000_fd != fd)
        return;
    r_info->data_000_fd = -1;
}

int open_file(char *fn, uint64_t cid) {
    int fd = 0;

    if ((fd = open(fn, O_RDWR | O_CREAT, 0777)) < 0) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s FATAL [STORAGE=%015lu] Error code 1. Cannot open file %s\n", curr_dt(dt), cid, fn);
        perror("open_file");
        graceful_exit(-1);
    }
    posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    return fd;
}


static void pwrite_all(int fd, const uint8_t *data, uint32_t size, uint64_t offset, uint64_t cid, const char *operation) {
    if (offset > (uint64_t)INT64_MAX || size > (uint64_t)INT64_MAX - offset) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s FATAL [STORAGE=%015lu] %s range overflow, fd=%d, offset=%lu, size=%u\n", curr_dt(dt), cid, operation, fd, offset, size);
        errno = EOVERFLOW;
        graceful_exit(-2);
    }
    uint32_t written = 0;

    while (written < size) {
        ssize_t result = pwrite(fd, data + written, size - written, (off_t)(offset + written));

        if (result < 0 && errno == EINTR)
            continue;

        if (result <= 0) {
            int write_errno = result < 0 ? errno : EIO;
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s FATAL [STORAGE=%015lu] %s failed, fd=%d, offset=%lu, size=%u, written=%u, remaining=%u: %s\n", curr_dt(dt), cid, operation, fd, offset, size,
                    written, size - written, strerror(write_errno));
            errno = write_errno;
            perror(operation);
            graceful_exit(-2);
        }
        written += (uint32_t)result;
    }

    if (log_level > 2) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s TRACE [STORAGE=%015lu] Flush 4K done: written=%u, size=%u, offset=%lu\n", curr_dt(dt), cid, written, size, offset);
    }
}

static bool pwrite_all_on_exit(int fd, const uint8_t *data, uint32_t size, uint64_t offset) {
    if (offset > (uint64_t)INT64_MAX || size > (uint64_t)INT64_MAX - offset)
        return false;
    uint32_t written = 0;

    while (written < size) {
        ssize_t result = pwrite(fd, data + written, size - written, (off_t)(offset + written));

        if (result < 0 && errno == EINTR)
            continue;

        if (result <= 0) {
            return false;
        }
        written += (uint32_t)result;
    }

    if (log_level > 2) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s TRACE [STORAGE=%015lu] Flush on exit 4K done: written=%u, size=%u, offset=%lu\n", curr_dt(dt), 0L, written, size, offset);
    }
    return true;
}

void flush_write_4K_buffer(region_info_t *r_info, uint64_t cid) {
    if (r_info == NULL || r_info->write_4K_buff_used == 0)
        return;

    if (r_info->write_4K_fd < 0) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [STORAGE=%015lu] Wrong fd=%d to flush 4K buff %s, offset=%lu, size=%u\n", curr_dt(dt), cid, r_info->write_4K_fd, r_info->uuid,
                r_info->write_4K_file_offset, r_info->write_4K_buff_used);
    }
    pwrite_all(r_info->write_4K_fd, r_info->write_4K_buff, r_info->write_4K_buff_used, r_info->write_4K_file_offset, cid, "flush_write_4K_buffer");
    r_info->write_4K_buff_used = 0;
    atomic_store(&r_info->write_4K_buffered_since_ms, 0);
    r_info->write_4K_file_offset = 0;
    r_info->write_4K_fd = -1;
    r_info->write_4K_cache_ok = false;
    r_info->write_4K_cache_block_offset = 0;
}

bool flush_write_4K_buffer_on_exit(region_info_t *r_info) {
    if (r_info == NULL || r_info->write_4K_buff_used == 0)
        return true;

    if (!pwrite_all_on_exit(r_info->write_4K_fd, r_info->write_4K_buff, r_info->write_4K_buff_used, r_info->write_4K_file_offset)) {
        return false;
    }
    r_info->write_4K_buff_used = 0;
    atomic_store(&r_info->write_4K_buffered_since_ms, 0);
    r_info->write_4K_file_offset = 0;
    r_info->write_4K_fd = -1;
    r_info->write_4K_cache_ok = false;
    r_info->write_4K_cache_block_offset = 0;
    return true;
}

int truncate_file(region_info_t *r_info, int fd, uint64_t length, uint64_t cid) {
    invalidate_replicas_idle_commit(r_info);
    flush_write_4K_buffer(r_info, cid);

    if (length > (uint64_t)INT64_MAX) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s ERROR [STORAGE=%015lu] ftruncate length overflow, fd=%d, length=%lu\n", curr_dt(dt), cid, fd, length);
        errno = EOVERFLOW;
        return -1;
    }
    int result;

    do {
        result = ftruncate(fd, (off_t)length);
    } while (result != 0 && errno == EINTR);
    return result;
}

static bool write_4K_buffer_position(const region_info_t *r_info, int fd, uint64_t offset, uint32_t size, uint32_t *position) {
    uint32_t used = r_info->write_4K_buff_used;
    uint64_t start = r_info->write_4K_file_offset;

    if (used == 0 || used > FIX_BLOCK_SIZE || size == 0 || size > FIX_BLOCK_SIZE || r_info->write_4K_fd != fd || offset < start)
        return false;
    uint64_t block_offset = offset & ~((uint64_t)FIX_BLOCK_SIZE - 1U);
    uint64_t delta = offset - start;

    if ((start & ~((uint64_t)FIX_BLOCK_SIZE - 1U)) != block_offset || used > FIX_BLOCK_SIZE - (start - block_offset) || delta > used || delta > FIX_BLOCK_SIZE - size ||
        offset - block_offset > FIX_BLOCK_SIZE - size)
        return false;

    if (delta != used) {
        static const uint8_t marker[APPEND_LOGICAL_EOF_MARKER_SIZE] = {0xff, 0xff, 0xff, 0xff};

        if (fd != r_info->data_000_fd || used - delta != APPEND_LOGICAL_EOF_MARKER_SIZE || size < APPEND_LOGICAL_EOF_MARKER_SIZE ||
            memcmp(r_info->write_4K_buff + delta, marker, sizeof(marker)) != 0)
            return false;
    }
    *position = (uint32_t)delta;
    return true;
}


static void buffer_write_4K_segments(int fd, uint64_t offset, const uint8_t *record, uint32_t write_size, uint32_t total, region_info_t *r_info, uint64_t cid) {
    uint32_t consumed = 0;

    while (consumed < total) {
        uint64_t segment_offset = offset + consumed;
        uint64_t block_offset = segment_offset & ~((uint64_t)FIX_BLOCK_SIZE - 1U);
        uint32_t block_room = FIX_BLOCK_SIZE - (uint32_t)(segment_offset - block_offset);
        uint32_t segment_size = total - consumed;

        if (segment_size > block_room)
            segment_size = block_room;
        uint32_t position = 0;
        bool can_append = write_4K_buffer_position(r_info, fd, segment_offset, segment_size, &position);

        if (r_info->write_4K_buff_used != 0 && !can_append) {
            log_write_4K_state("before_flush", "!can_append", r_info, fd, offset, write_size, total, cid);
            flush_write_4K_buffer(r_info, cid);
        }

        if (r_info->write_4K_buff_used == 0) {
            r_info->write_4K_fd = fd;
            r_info->write_4K_file_offset = segment_offset;
            atomic_store(&r_info->write_4K_buffered_since_ms, monotonic_time_ms());
        }
        uint32_t data_size = 0;

        if (consumed < write_size) {
            data_size = write_size - consumed;

            if (data_size > segment_size)
                data_size = segment_size;
            memcpy(r_info->write_4K_buff + position, record + consumed, data_size);
        }

        if (data_size < segment_size) {
            memset(r_info->write_4K_buff + position + data_size, 0, segment_size - data_size);
        }
        r_info->write_4K_buff_used = position + segment_size;
        consumed += segment_size;

        if (r_info->write_4K_buff_used == FIX_BLOCK_SIZE) {
            log_write_4K_state("before_flush", "full", r_info, fd, offset, write_size, total, cid);
            flush_write_4K_buffer(r_info, cid);
        }
    }
}

void write_file(int fd, uint64_t offset, uint8_t *record, uint32_t write_size, uint16_t bsize_factor, region_info_t *r_info, uint64_t cid) {
    invalidate_replicas_idle_commit(r_info);
    uint16_t tail;

    if (write_size % bsize_factor == 0)
        tail = ((write_size / bsize_factor) + 0) * bsize_factor - write_size;
    else
        tail = ((write_size / bsize_factor) + 1) * bsize_factor - write_size;
    memset(record + write_size, 0, tail);

    while (atomic_load(&is_shutting_down))
        sleep(10);
    atomic_fetch_xor(&r_info->is_write, 1);
    uint64_t total_size = (uint64_t)write_size + tail;

    if (total_size > UINT32_MAX || offset > (uint64_t)INT64_MAX || total_size > (uint64_t)INT64_MAX - offset) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s FATAL [STORAGE=%015lu] File write range overflow, fd=%d, offset=%lu, write_size=%u, tail=%hu\n", curr_dt(dt), cid, fd, offset, write_size, tail);
        errno = EOVERFLOW;
        graceful_exit(-2);
    }
    uint32_t total = (uint32_t)total_size;
    log_write_4K_state("begin_write", NULL, r_info, fd, offset, write_size, total, cid);
    bool buffering_enabled = atomic_load(&enable_write_4k_cache) != 0;
    bool segment_rollover_write = false;

    if (buffering_enabled && total != 0 && r_info->write_4K_cache_ok && fd == r_info->data_000_fd && r_info->write_4K_cache_block_offset <= UINT64_MAX - FIX_BLOCK_SIZE) {
        uint64_t rollover_offset = r_info->write_4K_cache_block_offset + FIX_BLOCK_SIZE;
        segment_rollover_write = offset < rollover_offset && offset + total_size > rollover_offset;
    }
    uint64_t block_offset = offset & ~((uint64_t)FIX_BLOCK_SIZE - 1U);
    bool fits_one_block = total != 0 && total <= FIX_BLOCK_SIZE && offset - block_offset <= FIX_BLOCK_SIZE - total;
    uint32_t position = 0;
    bool can_append = buffering_enabled && total < FIX_BLOCK_SIZE && fits_one_block && write_4K_buffer_position(r_info, fd, offset, total, &position);

    if (segment_rollover_write) {
        log_write_4K_state("segment_rollover", "segment_rollover", r_info, fd, offset, write_size, total, cid);
        buffer_write_4K_segments(fd, offset, record, write_size, total, r_info, cid);
    } else {
        if (r_info->write_4K_buff_used != 0 && !can_append) {
            log_write_4K_state("before_flush", "!can_append", r_info, fd, offset, write_size, total, cid);
            flush_write_4K_buffer(r_info, cid);
        }

        if (buffering_enabled && total < FIX_BLOCK_SIZE && fits_one_block) {
            if (r_info->write_4K_buff_used == 0) {
                r_info->write_4K_fd = fd;
                r_info->write_4K_file_offset = offset;
                atomic_store(&r_info->write_4K_buffered_since_ms, monotonic_time_ms());
            }
            memcpy(r_info->write_4K_buff + position, record, write_size);
            memset(r_info->write_4K_buff + position + write_size, 0, tail);
            r_info->write_4K_buff_used = position + total;

            if (r_info->write_4K_buff_used == FIX_BLOCK_SIZE) {
                log_write_4K_state("before_flush", "full", r_info, fd, offset, write_size, total, cid);
                flush_write_4K_buffer(r_info, cid);
            }
        } else if (total != 0) {
            pwrite_all(fd, record, write_size, offset, cid, "write_file");

            if (tail != 0) {
                uint8_t zeroes[FIX_BLOCK_SIZE] = {0};
                uint32_t padding_written = 0;

                while (padding_written < tail) {
                    uint32_t padding_size = tail - padding_written;

                    if (padding_size > FIX_BLOCK_SIZE)
                        padding_size = FIX_BLOCK_SIZE;
                    pwrite_all(fd, zeroes, padding_size, offset + write_size + padding_written, cid, "write_file padding");
                    padding_written += padding_size;
                }
            }
        }
    }

    if (atomic_load(&is_shutting_down) != 0) {
        log_write_4K_state("before_flush", "shutdown", r_info, fd, offset, write_size, total, cid);
        flush_write_4K_buffer(r_info, cid);

        if (fsync(fd) != 0) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Failed fsync fd=%d, Error code: %d, Error message: %s\n", curr_dt(dt), cid, fd, errno, strerror(errno));
            perror("fsync");
        }
    }
    atomic_fetch_xor(&r_info->is_write, 1);
}


static ssize_t overlay_write_4K_buffer(region_info_t *r_info, uint8_t *rbuff, uint64_t offset, uint64_t read_end, ssize_t result, uint64_t buffered_offset, uint64_t buffered_end) {
    uint64_t overlap_offset = offset > buffered_offset ? offset : buffered_offset;
    uint64_t overlap_end = read_end < buffered_end ? read_end : buffered_end;

    if (overlap_offset >= overlap_end)
        return result;
    uint32_t destination_offset = (uint32_t)(overlap_offset - offset);
    uint32_t source_offset = (uint32_t)(overlap_offset - buffered_offset);
    uint32_t overlap_size = (uint32_t)(overlap_end - overlap_offset);

    if (result < (ssize_t)destination_offset) {
        memset(rbuff + result, 0, destination_offset - (uint32_t)result);
    }
    memcpy(rbuff + destination_offset, r_info->write_4K_buff + source_offset, overlap_size);
    uint32_t buffered_result = destination_offset + overlap_size;

    if (result < (ssize_t)buffered_result)
        result = buffered_result;
    return result;
}

int read_file(int fd, uint8_t *rbuff, uint64_t offset, uint32_t read_size, region_info_t *r_info, uint64_t cid) {
    if (read_size == 0)
        return 0;

    if (offset > (uint64_t)INT64_MAX || read_size > (uint64_t)INT64_MAX - offset) {
        char dt[DT_LOG_SIZE];
        fprintf(stderr, "%s FATAL [STORAGE=%015lu] File read range overflow, offset=%lu, read_size=%u, fd=%d\n", curr_dt(dt), cid, offset, read_size, fd);
        graceful_exit(-3);
    }
    uint64_t read_end = offset + read_size;
    bool has_buffered_data = r_info != NULL && r_info->write_4K_fd == fd && r_info->write_4K_buff_used != 0;
    uint64_t buffered_offset = 0;
    uint64_t buffered_end = 0;

    if (has_buffered_data) {
        buffered_offset = r_info->write_4K_file_offset;
        buffered_end = buffered_offset + r_info->write_4K_buff_used;

        if (offset >= buffered_offset && read_end <= buffered_end) {
            memcpy(rbuff, r_info->write_4K_buff + (offset - buffered_offset), read_size);
            return (int)read_size;
        }
    }
    ssize_t result;
    {
        do {
            result = pread(fd, rbuff, read_size, (off_t)offset);
        } while (result < 0 && errno == EINTR);

        if (result < 0) {
            char dt[DT_LOG_SIZE];
            fprintf(stderr, "%s FATAL [STORAGE=%015lu] Error code 3. File read error, offset=%lu, read_size=%u Descriptor %d\n", curr_dt(dt), cid, offset, read_size, fd);
            perror("read_file");
            graceful_exit(-3);
        }
    }

    if (has_buffered_data) {
        result = overlay_write_4K_buffer(r_info, rbuff, offset, read_end, result, buffered_offset, buffered_end);
    }
    return (int)result;
}

uint64_t get_file_size(char *fn) {
    if (access(fn, F_OK) != 0)
        return 0;
    struct stat buffer;
    stat(fn, &buffer);
    return buffer.st_size;
}


uint64_t get_file_mtime(char *fn) {
    if (access(fn, F_OK) != 0)
        return 0;
    struct stat buffer;
    stat(fn, &buffer);
    return buffer.st_mtim.tv_sec;
}

void close_data_file(region_info_t *r_info, uint8_t bs, uint8_t b, uint64_t cid) {
    char dt[DT_LOG_SIZE];
    fprintf(stderr, "%s DEBUG [REGION_HANDLER=%015lu] Closing file fd=%d, bs=%d, region=%s\n", curr_dt(dt), cid, r_info->r_idx_b[bs]->fd[b], bs, r_info->uuid);
    flush_write_4K_buffer(r_info, cid);

    if (fsync(r_info->r_idx_b[bs]->fd[b]) != 0) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Failed fsync(r_info->r_idx_b[%d]->fd[%d] = %d) Error code: %d, Error message: %s\n", curr_dt(dt), cid, bs, b,
                r_info->r_idx_b[bs]->fd[b], errno, strerror(errno));
        perror("fsync");
        // TODO smth
    }

    if (r_info->r_idx_b[bs]->fd[b] < 0) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Wrong r_info->r_idx_b[%d]->fd[%d] = %d\n", curr_dt(dt), cid, bs, b, r_info->r_idx_b[bs]->fd[b]);
        // TODO smth
    }
    char filePath[PATH_MAX];
    sprintf(filePath, "/proc/self/fd/%d", r_info->r_idx_b[bs]->fd[b]);
    struct stat info;

    if (fstat(r_info->r_idx_b[bs]->fd[b], &info) != 0)
        perror("fstat() error");
    else
        fprintf(stderr, "%s DEBUG [RS_IDX=%015lu] FStat %s, bs=%d, fd=%d => inode=%lu, size=%lu\n", curr_dt(dt), cid, r_info->r_fname, bs, r_info->r_idx_b[bs]->fd[b], info.st_ino,
                info.st_size);
    int res_fd_close = close(r_info->r_idx_b[bs]->fd[b]);

    if (res_fd_close != 0) {
        fprintf(stderr, "%s ERROR [REGION_HANDLER=%015lu] Failed close file descriptor (r_info->r_idx_b[%d]->fd[%d]=%d), res_fd_close=%d\n", curr_dt(dt), cid, bs, b,
                r_info->r_idx_b[bs]->fd[b], res_fd_close);
        perror("close");
    } else
        unbind_data_000_fd(r_info, r_info->r_idx_b[bs]->fd[b], cid);
}
