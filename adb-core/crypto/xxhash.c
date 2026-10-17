/*
 * xxHash - Extremely Fast Hash algorithm
 * Copyright (C) 2012-2020 Yann Collet
 *
 * BSD 2-Clause License (https://www.opensource.org/licenses/bsd-license.php)
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following disclaimer
 *      in the documentation and/or other materials provided with the
 *      distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * You can contact the author at:
 *   - xxHash homepage: https://www.xxhash.com
 *   - xxHash source repository: https://github.com/Cyan4973/xxHash
 */


/*
 * xxhash.c instantiates functions defined in xxhash.h
 */

#define XXH_STATIC_LINKING_ONLY   /* access advanced declarations */
#define XXH_IMPLEMENTATION   /* access definitions */

#include "xxhash.h"
#include "ripemd160.h"
#include "../regions-adm/region_utils.h"

#define K3_BIT_MASK 0x3F


void calc_hash(const void* data, size_t len, char* digest) {
    XXH128_hash_t hash = XXH3_128bits(data, len);
    digest[0] = (hash.low64 >> 0) & 0xFF;
    digest[1] = (hash.low64 >> 8) & 0xFF;
    digest[2] = (hash.low64 >> 16) & 0xFF;
    digest[3] = (hash.low64 >> 24) & K3_BIT_MASK;
    digest[4] = (hash.low64 >> 32) & 0xFF;
    digest[5] = (hash.low64 >> 40) & 0xFF;
    digest[6] = (hash.low64 >> 48) & 0xFF;
    digest[7] = (hash.low64 >> 56) & 0xFF;
    digest[8] = (hash.high64 >> 0) & 0xFF;
    digest[9] = (hash.high64 >> 8) & 0xFF;
    digest[10] = (hash.high64 >> 16) & 0xFF;
    digest[11] = (hash.high64 >> 24) & 0xFF;
    digest[12] = (hash.high64 >> 32) & 0xFF;
    digest[13] = (hash.high64 >> 40) & 0xFF;
    digest[14] = (hash.high64 >> 48) & 0xFF;
    digest[15] = (hash.high64 >> 56) & 0xFF;
#ifdef COLL_LOB_TEST
    if (digest[C_IDX_POS] == 0)
        digest[C_IDX_POS] = 0xFF; // 0 is reserved like void place
#else
    if (digest[C_IDX_POS + 0] == 0)
        digest[C_IDX_POS + 0] = 0xFF; // 0 is reserved like void place
    if (digest[C_IDX_POS + 1] == 0)
        digest[C_IDX_POS + 1] = 0xFF; // 0 is reserved like void place
    if (digest[C_IDX_POS + 2] == 0)
        digest[C_IDX_POS + 2] = 0x7F; // 0 is reserved like void place
#endif
   // digest[C_IDX_POS] = 0xCC;

}

void calc_hash_full(const void* data, size_t len, char* digest, uint8_t start) {
    ripemd160(data, len, digest + start);
}