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
#ifndef ADB_INDEX_MEMORY_H
#define ADB_INDEX_MEMORY_H
#include <assert.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
static inline void index_memory_set(_Atomic uint64_t *total, size_t *owned, size_t bytes) {
    if (bytes >= *owned)
        atomic_fetch_add_explicit(total, bytes - *owned, memory_order_relaxed);
    else
        atomic_fetch_sub_explicit(total, *owned - bytes, memory_order_relaxed);
    *owned = bytes;
}


static inline void *index_memory_malloc(_Atomic uint64_t *total, size_t *owned, size_t bytes) {
    assert(*owned == 0);
    void *p = malloc(bytes);

    if (p != NULL)
        index_memory_set(total, owned, bytes);
    return p;
}

static inline void *index_memory_calloc(_Atomic uint64_t *total, size_t *owned, size_t count, size_t size) {
    assert(*owned == 0);
    void *p = calloc(count, size);

    if (p != NULL)
        index_memory_set(total, owned, count * size);
    return p;
}


static inline void *index_memory_realloc(_Atomic uint64_t *total, size_t *owned, void *p, size_t bytes) {
    assert(bytes != 0);
    void *next = realloc(p, bytes);

    if (next != NULL)
        index_memory_set(total, owned, bytes);
    return next;
}

static inline void index_memory_free(_Atomic uint64_t *total, size_t *owned, void *p) {
    free(p);
    index_memory_set(total, owned, 0);
}

static inline void index_memory_transfer(size_t *destination, size_t *source) {
    assert(*destination == 0);
    *destination = *source;
    *source = 0;
}
#endif
