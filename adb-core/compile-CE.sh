#!/bin/bash

rm -f *.gch
rm -f */*.gch
rm -f ./adb

gcc -O2 main.c crypto/crc32.c crypto/crc32.h handle/block_handler.c handle/block_handler.h handle/scan_handler.c index/f_index.c utils/connection_transport.c utils/connection_transport.h utils/request_context.h utils/msg_utils.c utils/msg_utils.h utils/write_compact_msg.c handle/req_proc.c utils/stat_utils.c \
utils/stat_utils.h handle/storage.c handle/storage.h utils/time_utils.c utils/time_utils.h all_headers.h index/f_index.h crypto/checksum.h crypto/memzero.c crypto/memzero.h \
utils/read_prop.c utils/read_prop.h regions-adm/region_utils.c regions-adm/rs_shutdown.c regions-adm/region_utils.h regions-adm/rs_processor.c regions-adm/rs_processor.h utils/string_builder.c utils/string_builder.h \
crypto/base64.c crypto/base64.h handle/replica_handler.c handle/replica_handler.h index/index_builder.c index/index_builder.h utils/voids_service.c utils/voids_service.h index/index_resizer.c \
index/index_resizer.h crypto/xxh3.h crypto/xxhash.c crypto/xxhash.h crypto/ripemd160.c crypto/ripemd160.h crypto/lz4.c crypto/lz4.h \
utils/health.c utils/health.h handle/lob.c handle/lob.h \
crypto/csnappy_compat.h crypto/csnappy_compress.c crypto/csnappy_decompress.c crypto/csnappy.h crypto/csnappy_internal.h crypto/csnappy_internal_userspace.h \
-I. -pthread -lm -std=gnu99 -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -fcommon -llzma -rdynamic -no-pie 

mv a.out adb
