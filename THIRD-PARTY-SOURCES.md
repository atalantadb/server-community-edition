# Third party source components

AtalantaDB includes adapted Apache HBase and jmazzitelli Prometheus Scraper
Java sources, as well as the native components listed below. This inventory
covers the 35 handwritten Java files and the listed native files, not Maven dependencies,
generated code or other native core components. Their upstream notices are
preserved in the source headers and in [NOTICE](NOTICE). License texts are in
[licenses/Apache-2.0.txt](licenses/Apache-2.0.txt) for HBase,
[licenses/MIT-prometheus-scraper.txt](licenses/MIT-prometheus-scraper.txt) for
Prometheus Scraper and
[licenses/BSD-2-Clause-sslsplit.txt](licenses/BSD-2-Clause-sslsplit.txt) for
SSLsplit Base64,
[licenses/BSD-3-Clause-csnappy.txt](licenses/BSD-3-Clause-csnappy.txt) for csnappy,
[licenses/BSD-2-Clause-lz4.txt](licenses/BSD-2-Clause-lz4.txt) for LZ4,
[licenses/BSD-2-Clause-xxhash.txt](licenses/BSD-2-Clause-xxhash.txt) for xxHash,
[licenses/MIT-libcrc.txt](licenses/MIT-libcrc.txt) for libcrc and
[licenses/ISC-libsodium.txt](licenses/ISC-libsodium.txt) for the libsodium adaptation.
The mbed TLS RIPEMD-160 adaptation uses the same Apache-2.0 license text as HBase.

The pinned upstream revisions are comparison references for the adapted code,
not a claim that these exact release versions were originally copied or that
the local files are unmodified. The license of AtalantaDB as a whole is not
changed by these third-party notices.

## Apache HBase

License: Apache-2.0. The original ASF header is retained in each adapted file.
Where present, `Copyright (C) 2021–2026 AtalantaDB Contributors` identifies
AtalantaDB modifications only; it does not replace the upstream ASF notice
or change the Apache-2.0 license of these adapted files.

Reference release: `rel/1.2.6`, commit `2f9b9e17d0522e36063bf52ecc58576243d20b3f`.
Upstream: [Apache HBase](https://github.com/apache/hbase/tree/2f9b9e17d0522e36063bf52ecc58576243d20b3f).

Local files are under `atalanta-core/src/main/java/com/light/atalanta/`:

| Local file | Upstream source |
| --- | --- |
| `util/Bytes.java` | Selected methods from `hbase-common/src/main/java/org/apache/hadoop/hbase/util/Bytes.java` |
| `util/BytesComparator.java` | Comparers extracted from the same `Bytes.java` |
| `util/HexStringSplit.java` | `HexStringSplit` in `hbase-server/src/main/java/org/apache/hadoop/hbase/util/RegionSplitter.java` |
| `util/UniformSplit.java` | `UniformSplit` in the same `RegionSplitter.java` |
| `collections/CopyOnWriteArrayMap.java` | `hbase-common/src/main/java/org/apache/hadoop/hbase/types/CopyOnWriteArrayMap.java` |

The following local files were extracted from nested classes in that upstream
`CopyOnWriteArrayMap.java`:

- `collections/ArrayEntryIterator.java`
- `collections/ArrayEntrySet.java`
- `collections/ArrayHolder.java`
- `collections/ArrayKeyIterator.java`
- `collections/ArrayKeySet.java`
- `collections/ArrayValueCollection.java`
- `collections/ArrayValueIterator.java`
- `collections/COWEntry.java`

Adaptations include package relocation, extraction of nested classes, dependency
adjustments and implementation changes. These changes are retained; restoring
attribution does not replace local implementations with upstream versions.

The relevant ASF and Hadoop notices are retained from the upstream
[NOTICE.txt](https://github.com/apache/hbase/blob/2f9b9e17d0522e36063bf52ecc58576243d20b3f/NOTICE.txt).
Unrelated upstream notices for the HBase logo, web assets and
`LimitInputStream` are not included in this source-component notice.

## jmazzitelli Prometheus Scraper

License: MIT. Upstream copyright: Copyright (c) 2022 John Mazzitelli.

Reference commit: `220c3c330d157dd3364bc05eaa8780f1bcd9ba1e`.
Upstream: [jmazzitelli/prometheus-scraper](https://github.com/jmazzitelli/prometheus-scraper/tree/220c3c330d157dd3364bc05eaa8780f1bcd9ba1e).
License: [upstream LICENSE](https://github.com/jmazzitelli/prometheus-scraper/blob/220c3c330d157dd3364bc05eaa8780f1bcd9ba1e/LICENSE).

Local root: `atalanta-web-ui/src/main/java/prometheus/`.
Upstream root: `src/main/java/prometheus/`.

The following paths correspond relative to those roots:

- `PrometheusDataFormat.java`
- `PrometheusMetricDataParser.java`
- `PrometheusMetricsProcessor.java`
- `PrometheusScraper.java`
- `Util.java`
- `binary/BinaryPrometheusMetricDataParser.java`
- `binary/BinaryPrometheusMetricsProcessor.java`
- `text/TextPrometheusMetricDataParser.java`
- `text/TextPrometheusMetricsProcessor.java`
- `text/TextSample.java`
- `types/Counter.java`
- `types/Gauge.java`
- `types/Histogram.java`
- `types/Metric.java`
- `types/MetricFamily.java`
- `types/MetricType.java`
- `types/Summary.java`
- `walkers/CollectorPrometheusMetricsWalker.java`
- `walkers/JSONPrometheusMetricsWalker.java`
- `walkers/PrometheusMetricsWalker.java`
- `walkers/SimplePrometheusMetricsWalker.java`
- `walkers/XMLPrometheusMetricsWalker.java`

Adaptations include use of SLF4J instead of JBoss Logging, metric parsing and
processing changes, and integration with AtalantaDB metrics. The upstream MIT
license text, copyright and local modification notices are included in all
22 files. The upstream package name `prometheus` is retained.

## SSLsplit Base64

License: BSD-2-Clause. Upstream copyright: Copyright (c) 2009-2019,
Daniel Roethlisberger <daniel@roe.ch>. All rights reserved.

Reference commit: `43c3aaf6b381cf9817aa794f8c2861c421690850`.
Upstream: [droe/sslsplit](https://github.com/droe/sslsplit/tree/43c3aaf6b381cf9817aa794f8c2861c421690850).

| Local file | Upstream source |
| --- | --- |
| `adb-core/crypto/base64.c` | [base64.c](https://github.com/droe/sslsplit/blob/43c3aaf6b381cf9817aa794f8c2861c421690850/base64.c) |
| `adb-core/crypto/base64.h` | [base64.h](https://github.com/droe/sslsplit/blob/43c3aaf6b381cf9817aa794f8c2861c421690850/base64.h) |

Both files retain the upstream copyright, license conditions and disclaimer.
Adaptations include AtalantaDB API names, input and size validation, zero output
length on failure, rejection of empty decode input, acceptance of noncanonical
pad bits for compatibility, and no-op initialization/cleanup functions.
The implementation uses standard C facilities without SSLsplit dependencies;
including this component does not enable TLS functionality.

The CE exporter copies these native sources from the sibling `adb-CE` tree
without transforming their license headers. Preserve the headers when
regenerating that tree. The complete upstream notice is also supplied separately
for binary distributions in `licenses/BSD-2-Clause-sslsplit.txt`.

## csnappy

License: BSD-3-Clause. Upstream copyright: Copyright 2011, Google Inc.
All rights reserved.

Reference commit: `6c10c305e8dde193546e6b33cf8a785d5dc123e2`.
Upstream: [zeevt/csnappy](https://github.com/zeevt/csnappy/tree/6c10c305e8dde193546e6b33cf8a785d5dc123e2),
a C port of Google Snappy with Linux Kernel and Sereal changes.

Local root: `adb-core/crypto/`. The upstream filenames are the same:

- `csnappy.h`
- `csnappy_compat.h`
- `csnappy_compress.c`
- `csnappy_decompress.c`
- `csnappy_internal.h`
- `csnappy_internal_userspace.h`

The existing source notices are retained, including credits to Zeev Tarantov,
Steffen Mueller and Yves Orton. The last file also records that code from
`libexfat/byteorder.h` is used under the 3-clause BSD license instead of GPL
with permission from Andrew Nayenko and Albert Lee. Preserve this notice.

The CE exporter copies these native sources from the sibling `adb-CE` tree
without transforming their license headers. The full license and the upstream
notice from `csnappy_compress.c` are supplied separately for binary distributions
in `licenses/BSD-3-Clause-csnappy.txt`.

## LZ4

License: BSD-2-Clause. Copyright (C) 2011-2020, Yann Collet.
Upstream: [lz4/lz4](https://github.com/lz4/lz4).
The local header declares version 1.9.3; this identifies the API version, not
an assertion that the implementation is an unmodified upstream release.

Local files: `adb-core/crypto/lz4.c` and `adb-core/crypto/lz4.h`.
Their original copyright, conditions and disclaimer remain in the source.
The complete notice from the local implementation header is reproduced in
`licenses/BSD-2-Clause-lz4.txt` for binary distributions.

## xxHash

License: BSD-2-Clause. Copyright (C) 2012-2020 Yann Collet;
`xxh3.h` carries Copyright (C) 2019-2020 Yann Collet.
Upstream: [Cyan4973/xxHash](https://github.com/Cyan4973/xxHash).
The local header declares version 0.8.1; this is not a claim of an unmodified release.

Local files: `adb-core/crypto/xxhash.c`, `adb-core/crypto/xxhash.h` and
`adb-core/crypto/xxh3.h`. Local additions in `xxhash.c` integrate hashing with
AtalantaDB. The original BSD notices are preserved. Both copyright variants
and their full conditions and disclaimers are reproduced in
`licenses/BSD-2-Clause-xxhash.txt`. The separate upstream `xxhsum` CLI is not
included; its GPL license is not the license of these library sources.

## libcrc

License: MIT. Copyright (c) 1999-2016 Lammert Bies; the restored `checksum.h`
notice carries Copyright (c) 1999-2018 Lammert Bies.
Upstream: [lammertb/libcrc](https://github.com/lammertb/libcrc).

Local files: `adb-core/crypto/crc32.c`, `adb-core/crypto/crc32.h`,
`adb-core/crypto/checksum.h` and `adb-core/crypto/gentab32.inc`.
`crc32.h` is a local interface and retains its existing libcrc attribution;
`gentab32.inc` is the generated CRC lookup table.
The restored header in `checksum.h` was checked against upstream commit
`7719e2112a9a960b1bba130d02bebdf58e8701f1`; the declarations match apart from
comments and whitespace. That commit is a comparison reference, not a claim
of the historical version originally copied.
Both copyright notices and the complete MIT conditions and disclaimer are
provided in `licenses/MIT-libcrc.txt`.

## libsodium secure memory clearing

License: ISC. Copyright (c) 2013-2019, Frank Denis <j at pureftpd dot org>.
Upstream: [jedisct1/libsodium](https://github.com/jedisct1/libsodium/tree/1647f0d53ae0e370378a9195477e3df0a792408f).
The source's existing provenance comment identifies commit
`1647f0d53ae0e370378a9195477e3df0a792408f` and `src/libsodium/sodium/utils.c`.

Local implementation: `adb-core/crypto/memzero.c`, adapted from `sodium_memzero`.
Local API declaration: `adb-core/crypto/memzero.h`.
Adaptations include the function name and platform-specific clearing branches.
The restored source header and `licenses/ISC-libsodium.txt` preserve the
upstream copyright, permission notice and disclaimer.

## mbed TLS RIPEMD-160

License: Apache-2.0, as stated in the local implementation header.
Copyright (C) 2006-2015, ARM Limited, All Rights Reserved.
Upstream: [Mbed-TLS/mbedtls](https://github.com/Mbed-TLS/mbedtls).

Local files: `adb-core/crypto/ripemd160.c` and `adb-core/crypto/ripemd160.h`.
The implementation is adapted, including API names and use of the local
`memzero` function. The upstream ARM copyright and Apache license notice
remain in the implementation header and are identified in `NOTICE`.
The full license is provided in `licenses/Apache-2.0.txt`.
Only the RIPEMD-160 hash implementation is included here, not the mbed TLS
transport stack; including it does not enable TLS functionality.

## Distribution

EE binary packaging additionally supplies verified FindBugs annotations 3.0.0
upstream sources in `third-party-sources/findbugs-annotations-3.0.0-sources.tar.gz`,
with separate LGPL 2.1, JSR-305 BSD and JCIP CC BY 2.5 license texts and notices.
See `licenses/FindBugs-annotations-NOTICE.txt` in those packages. These are
third-party sources, not EE application sources. Supplying them does not by
itself implement replacement/relinking of shaded copies or certify complete
LGPL compliance. The CE export's source-component inventory below is unchanged.

The CE exporter includes this inventory, `LICENSE`, `NOTICE`,
`licenses/Apache-2.0.txt`, `licenses/MIT-prometheus-scraper.txt`,
`licenses/BSD-2-Clause-sslsplit.txt`, `licenses/BSD-3-Clause-csnappy.txt`,
`licenses/BSD-2-Clause-lz4.txt`, `licenses/BSD-2-Clause-xxhash.txt`,
`licenses/MIT-libcrc.txt` and `licenses/ISC-libsodium.txt`.
Distribution packaging copies them to
`AtalantaDB/` alongside the runtime files.
Each CE application Docker image also includes `LICENSE`, `NOTICE` and this
inventory under `/opt/atalanta/`, with the license texts under
`/opt/atalanta/licenses/`.

The affected modules also carry component-specific notices and a copy of the
license as ordinary resources, with distinct paths to avoid collisions when
dependency JARs are flattened:

- `atalanta-core`: `META-INF/atalanta-third-party/core/` (`NOTICE.txt` and `APACHE-2.0.txt`).
- `atalanta-web-ui`: `META-INF/atalanta-third-party/web-ui/` (`NOTICE.txt` and `MIT.txt`).

Preserve these resources when repackaging the modules. They are separate from
the full dependency license inventory required for a particular binary release.
