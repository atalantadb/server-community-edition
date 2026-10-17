/*
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * This file is part of csnappy.
 * Upstream: https://github.com/zeevt/csnappy
 *
 * See licenses/BSD-3-Clause-csnappy.txt in the AtalantaDB project root
 * for the copyright notice, license conditions and disclaimer.
 */

#ifndef CSNAPPY_COMPAT_H

/* This file was added to Sereal to attempt some MSVC compatibility,
 * but is at best a band-aid. And done without a lot of experience
 * in whatever subset of C99 MSVC supports.
 */

#ifndef INLINE
#   if defined(_MSC_VER)
#     define INLINE __inline
#   else
#     define INLINE inline
#   endif
#endif

#endif
