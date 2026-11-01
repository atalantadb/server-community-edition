# AtalantaDB Community Edition

Try AtalantaDB locally with prebuilt Docker images: create a table, write a record,
and read it back in a few commands. You can try it on Apple Silicon with Docker
Desktop, too.

## Quick start with prebuilt Docker images

You'll need Docker with Compose V2 2.20.0 or newer and Bash 4 or newer. The images
include the database binaries and runtime dependencies, ready to run. On macOS,
install Bash with `brew install bash`.

```bash
git clone https://github.com/atalantadb/server-community-edition.git
cd server-community-edition/atalanta-docker-compose-ce/atalanta
./1-start.sh
./3-shell.sh
```

In the database shell:

```text
create 'test', 'cf'
put 'test', 'r1', 'cf:q', 'hello'
get 'test', 'r1'
```

Open the Web UI at http://localhost:8433/. The default cluster has one master, one
RegionServer, the Web UI and three ZooKeeper servers. Use `./1-start.sh --full`
for two masters, three RegionServers, Prometheus and Grafana. `./0-stop.sh`
stops either topology without deleting data.

Public images download automatically from GitHub Container Registry. See
[Docker instructions](atalanta-docker-compose-ce/README.md) for requirements,
logs, source builds and publication steps.

## Performance comparison

Three-node cluster load test

Mean throughput · thousands of record operations/s per node · higher is better

### Single-record requests (batch size 1)

| Operation | Aerospike CE | AtalantaDB CE | AtalantaDB EE |
| :--- | ---: | ---: | ---: |
| INSERT | 271.3 | 295.3 | 291.8 |
| UPDATE | 204.7 | 216.2 | 238.9 |
| GET | 528.9 | 530.2 | 502.6 |
| Bytes per key (RF=2) | 128 | ~18 | ~18 |

### Batch requests (batch size 1,000)

| Operation | Aerospike CE | AtalantaDB CE | AtalantaDB EE |
| :--- | ---: | ---: | ---: |
| INSERT | 461.0 | 779.6 | 779.1 |
| UPDATE | 414.4 | 430.5 | 579.1 |
| GET | 681.1 | 973.6 | 1,720.6 |

RF is the replication factor. For benchmark details, see
[Performance benchmarks](https://atalantadb.com/performance/).

## Native/source-build requirements

### Runtime

- Start with at least 4 GB RAM for a small demo deployment, larger workloads
  require more memory.
- Reserve disk space for application files, logs and database data. Required
  capacity depends on the deployment and workload.

Adjust JVM heap limits according to the workload and available RAM, leaving
sufficient memory for the native database core and other processes. Each Java
process has its own heap limit.

### Source builds

Building from source requires additional disk space for Maven dependencies and
generated artifacts. Docker builds also need space for image layers and build
cache. See [Docker disk-space guidance](atalanta-docker-compose-ce/README.md#disk-space)
for the build script's configurable reserves, these are estimates, not measured
minimum requirements.

## How to use

On Linux:

1. Run `./build-adb.sh`.
2. See the usage example in `./AtalantaDB/atalanta-usage-example`.

Before building, the script checks JDK 17+, Maven and native build packages.
If native packages are missing, it offers either their installation using sudo
or a bundled core from `adb-core/bin/` (falling back to `adb-core/`):
`adb_ubuntu`, `adb_rhel8` or `adb_rhel9`. Bundled binaries require Linux x86_64
and compatible runtime libraries. JDK and Maven are still needed with a prebuilt core.
All bundled `adb`/`adb_*` binaries from `adb-core/bin/` are copied without renaming
to `AtalantaDB/bin/core/`, including after a local core build. Setup checks the
active core against the host OS (Ubuntu, RHEL 8 or RHEL 9) and asks before replacing
an incompatible or unidentified binary. Named variants are kept the previous
active binary is backed up as `adb.previous`. Declining the replacement stops setup.

To build Docker images from source (see the platform notes below for macOS):

1. Run `./build-adb-docker-ce.sh` - it builds the images and optionally starts the cluster.
2. Open the Web UI at http://localhost:8433/ and see `atalanta-docker-compose-ce/README.md` for the shell, logs and stop commands.

## Enterprise Edition features

AtalantaDB Enterprise Edition extends Community Edition with:

1. Authentication and authorization (ACL and Kerberos/Ranger).
2. Support for records larger than 60,648 bytes.
3. Append Update mode, which provides faster data updates.
4. Read Cache mode, which caches data in the process memory.

Learn more at [atalantadb.com](https://atalantadb.com).

## Recommendations:

- AtalantaDB currently supports only one column family per table. Use 'cf' in the examples.
- It is strongly recommended always use UniformSplit and the xxHash32 algorithm for key hashing (see XxHash32KeySalter), this will be important in future versions.
- It is recommended to use a sufficiently powerful ZooKeeper instance configured with at least maxClientCnxns=32000 for BigData usage.

## Use the following features with caution:

- Dictionary encoding (DICTIONARY => 'TRUE') should be enabled only when the combined size of all qualifiers is less than approximately 60 KB. 
  Currently, this setting cannot be changed using ALTER TABLE.
- Block compression (COMPRESSION => 'SNAPPY' or COMPRESSION => 'LZ') may significantly reduce performance and has not been thoroughly tested.
  This type of compression is highly effective for certain kinds of data. However intensive use of the associated operations can lead to increased 
  processing times potentially resulting in recurring timeout errors and requires fine-tuning.
- Snapshots are experimental. Data recovery from a snapshot after a failure has not been tested.

## Scan Consistency and Ordering

- When updates occur during a scan, the same key may appear more than once, possibly with different values.
- When a region split occurs during a scan, the same key may appear multiple times.
- Scan results are not guaranteed to be sorted by key.

## Platform notes

The prebuilt images target Linux amd64. On Apple Silicon, Docker Desktop runs
them through amd64 emulation. Table creation and data writes have been verified
in this setup. Failure-recovery and region-move scenarios on Apple Silicon
still need further validation.

Building Docker images from source on macOS remains experimental. The prebuilt
images provide the simplest starting point for trying the database there.

Official website:
https://atalantadb.com/

Performance benchmarks:
https://atalantadb.com/performance/
