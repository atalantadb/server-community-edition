/*-
 * SSLsplit - transparent SSL/TLS interception
 * https://www.roe.ch/SSLsplit
 *
 * Copyright (c) 2009-2019, Daniel Roethlisberger <daniel@roe.ch>.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER AND CONTRIBUTORS ``AS IS''
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
/*
 * SPDX-License-Identifier: BSD-2-Clause
 * Adapted from SSLsplit 43c3aaf6b381cf9817aa794f8c2861c421690850, base64.h.
 * ADB API names and standalone standard-C declarations; no attrib.h required.
 */
#ifndef ADB_BASE64_H
#define ADB_BASE64_H

#include <stddef.h>

/*
 * Results are malloc-owned: release with free().
 * output_length excludes the extra NUL byte and is zero on failure.
 * Decode rejects empty input, whitespace, invalid alphabet/padding and lengths
 * not divisible by four. Nonzero unused pad bits remain accepted for compatibility.
 * Encode accepts empty input (data may be NULL only for an empty input).
 * For nonempty input, data must address input_length readable bytes.
 */
char *base64_encode(const unsigned char *data, size_t input_length,
                    size_t *output_length);
unsigned char *base64_decode(const char *data, size_t input_length,
                             size_t *output_length);

/* Compatibility no-ops; safe to call repeatedly and concurrently. */
void base64_init(void);
void build_decoding_table(void);
void base64_cleanup(void);

#endif /* ADB_BASE64_H */
