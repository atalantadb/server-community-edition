# AtalantaDB Community Edition Docker Compose

This directory runs an AtalantaDB Community Edition (CE) cluster in Docker on
a single host.

## Quick start

From the repository root, build everything and optionally start the cluster:

```bash
./build-adb-docker-ce.sh
```

Then manage the cluster with the helpers in `atalanta-docker-compose-ce/atalanta/`:

```bash
cd atalanta-docker-compose-ce/atalanta
./1-start.sh                           # Start or reuse the containers
./3-shell.sh                           # Open the database shell
./2-logs.sh                            # Last 100 lines from all services
./2-logs.sh -f --tail 100 atalanta-rs  # Follow one RegionServer's logs
./0-stop.sh                            # Stop the cluster, keep the data
```

Open the Web UI at http://localhost:8433/.

## Security

CE has no authentication, authorization, or TLS. Run it only on a trusted
network. Published ports bind to `127.0.0.1` only. Do not expose the cluster
to untrusted clients and do not attach unrelated containers to its network.

## License notices

Each AtalantaDB CE application image includes `LICENSE`, `NOTICE` and
`THIRD-PARTY-SOURCES.md` under `/opt/atalanta/`. The accompanying license
texts are under `/opt/atalanta/licenses/`. These notices cover AtalantaDB
and the reviewed embedded source components; they are not a complete license
inventory of all runtime dependencies or base-image packages.

## Included components

- Two AtalantaDB masters.
- Three RegionServers, each with its own native core in the same container.
- AtalantaDB Web UI and the database shell.
- Three ZooKeeper servers.
- Prometheus and Grafana.

The Compose project is `atalanta-ce`. Images, containers, and named volumes
use the `atalanta-ce-` prefix, and the network is `atalanta-ce-network`.

## Requirements

`build-adb-docker-ce.sh` requires:

- Python 3, Maven, and Java;
- Bash 4 or newer;
- GNU/Linux utilities;
- a local Docker daemon and Docker Compose V2 2.20.0 or newer;
- an interactive terminal.

Building the native core may ask to install missing packages with `sudo`.

macOS with Docker Desktop is an experimental path, not an end-to-end validated
deployment. Install Bash 4 or newer (`brew install bash`); the builder can
re-execute itself with Homebrew Bash. On non-Linux-x86_64 hosts it compiles the
core in an Ubuntu amd64 container, and RegionServers use `linux/amd64`.
Emulation can slow down compilation and execution. Disk-space estimates also
need manual checking against Docker Desktop's Linux VM storage capacity.

## Build

`build-adb-docker-ce.sh` asks for confirmation before each stage:

1. Build the Java modules with Maven `clean install`.
2. Build the native core with `compile-CE.sh` or its dependency-check wrapper.
3. Copy the core to `atalanta-docker-compose-ce/atalanta/core/adb`.
4. Build the master, RegionServer, and Web UI images.
5. Optionally start the cluster with `atalanta/1-start.sh`.

Declining a required stage stops the script. Declining startup keeps the built
images and prints the command to start later. A failed stage stops the
remaining stages. The script never deletes Docker data, images, or volumes.

**Warning:** Maven's `clean` phase deletes the generated `AtalantaDB/`
directory, including any logs or database data stored inside it. If that
directory exists, the script asks for an additional confirmation. Stop any
native deployment and back up its data first. Docker named volumes are not
affected.

The script reuses a native core binary only if it built that binary itself and
the recorded source fingerprint in `atalanta/core/.ce-build-stamp.json` still
matches. Otherwise it rebuilds the core. The project must have an exported
`-ce` version with ordinary CE sources, and the compiled core must be a CE
Linux x86_64 ELF binary. These lightweight guards do not require a private
export report and are not a full publisher provenance audit.

Run `./build-adb-docker-ce.sh --help` for the list of environment variables.

### Disk space

The script checks free disk space before building. The defaults are
conservative estimates, not measured peak requirements:

| Environment variable | Default (GiB) | Location |
| --- | ---: | --- |
| `ADB_CE_DOCKER_MIN_PROJECT_GB` | 5 | Project and build outputs |
| `ADB_CE_DOCKER_MIN_MAVEN_GB` | 5 | Maven repository |
| `ADB_CE_DOCKER_MIN_TMP_GB` | 3 | `${TMPDIR:-/tmp}` |
| `ADB_CE_DOCKER_MIN_DOCKER_GB` | 20 | Docker storage, fresh installation |
| `ADB_CE_DOCKER_MIN_DOCKER_WORK_GB` | 5 | Minimum Docker rebuild reserve |

Locations on the same file system are added together. If space is short, the
script warns and asks whether to continue; continuing does not guarantee a
successful build.

To use a custom local Maven repository, set `ADB_CE_MAVEN_REPOSITORY` to its
absolute path. Remote Docker daemons and contexts are not supported, because
their storage can't be checked.

## Operation

The helpers work from any directory. They always use the same project name,
Compose file, and project directory, and they ignore inherited
`COMPOSE_FILE`, `COMPOSE_PROJECT_NAME`, `COMPOSE_PROFILES`, and
`COMPOSE_ENV_FILES`, as well as `.env` and Compose override files. Keep this in
mind if you run `docker compose` commands directly.

`1-start.sh` waits until every container has stayed running for
`${ATALANTA_DOCKER_STABILITY_PERIOD:-10}` seconds. This checks container state
only, not database readiness. On failure, the containers are left in place for
log inspection.

`0-stop.sh` runs `docker compose stop`, not `down`, so containers, ZooKeeper
state, and database volumes are kept.

`3-shell.sh` runs the shell inside a master container, so no Java client is
needed on the host.

### Local endpoints

| Component | Default local endpoint | Host port variable |
| --- | --- | --- |
| Web UI | http://localhost:8433/ | `ATALANTA_CE_WEB_UI_PORT` |
| Prometheus | http://localhost:9190/ | `ATALANTA_CE_PROMETHEUS_PORT` |
| Grafana | http://localhost:3100/ | `ATALANTA_CE_GRAFANA_PORT` |
| ZooKeeper 1 | `127.0.0.1:2281` | `ATALANTA_CE_ZK1_PORT` |
| ZooKeeper 2 | `127.0.0.1:2282` | `ATALANTA_CE_ZK2_PORT` |
| ZooKeeper 3 | `127.0.0.1:2283` | `ATALANTA_CE_ZK3_PORT` |

To change a host port, set its variable for the build and for every helper
call, for example:

```bash
export ATALANTA_CE_WEB_UI_PORT=18433
./1-start.sh
```

The helpers reject invalid or duplicate ports. Selected ports must be free on
the host.

The published ZooKeeper ports are for local diagnostics only. A database client
on the host can't use them: masters and RegionServers register container host
names and internal ports. Use the shell in the container, or a client attached
to `atalanta-ce-network`.

Only one CE cluster can run per Docker daemon, because the project and resource
names are fixed.

## Runtime configuration

- Java containers use Ubuntu 24.04 with the Java 17 headless runtime.
- RegionServer containers also install `procps`, `iproute2`, and `liblzma5`.
- The replication factor is 2.
- Masters and RegionServers use the `default` Spring profile, so ZooKeeper
  registrations contain container host names. Don't use the legacy `docker`
  profile: it registers `localhost`, which doesn't work across containers.
- Masters, RegionServers, and the Web UI expose HTTP metrics endpoints, which
  Prometheus scrapes.
- JMX is enabled without credentials or encryption, because the master and the
  Web UI read disk and region metrics through it. JMX ports are not published
  on the host.
- The RegionServer launcher runs the native core as a supervised child process.
  If either the core or the Java process exits, the other one is stopped too;
  termination signals are forwarded to both.
- Java services get a 90-second stop grace period. No container is privileged
  and the Docker socket is not mounted.
