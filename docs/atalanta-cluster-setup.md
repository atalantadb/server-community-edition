# AtalantaDB Community Edition: Cluster Setup and Operations

This guide describes how to build and run the native Linux distribution of AtalantaDB
Community Edition (CE) from this repository. Docker deployment is outside the scope of
this guide.

Community Edition does not require a license file. Authentication, authorization,
ACL management, Kerberos integration, and TLS are not available. Security commands
retained in the shell display an Enterprise-only notice rather than applying changes.

The CE core also does not support records larger than 60,648 bytes, Append Update
mode, or Read Cache mode. These features are available in
[AtalantaDB Enterprise Edition](https://atalantadb.com).

## 1. Architecture and terminology

- A namespace groups tables.
- A table is divided into regions.
- Each region has a replication factor (RF): the total number of its replicas.
- Replica `_0` is the leader replica. Replicas `_1` through `_(RF-1)` are reserve replicas.
- Replica metadata includes its region name, replica number, status, and key range.

The cluster consists of the following components:

| Component | Responsibilities |
| --- | --- |
| ZooKeeper | Stores cluster metadata, service registrations, and coordination state. |
| Master | Handles table administration, replica placement, splitting, rebalancing, and recovery. |
| RegionServer (RS) | Manages local replica files and forwards the relevant control commands to its paired core. |
| Core (`adb`) | Handles client data operations and replication between cores. |
| Client / shell | Discovers services through ZooKeeper, sends administrative requests to the active master, and sends data requests directly to cores. |
| Web UI | Displays cluster, table, region, and current service information. |

Each RS is paired with one core on the same host. Multiple pairs may share a host,
provided that their data directories do not overlap and their listening ports do
not conflict.

At least one master and one RS/core pair are required. Additional masters are
standby candidates: ZooKeeper elects one active master. Masters do not form a
separate majority quorum; ZooKeeper availability is required for coordination.

The packaged local example has two masters and four RS/core pairs, with RF = 3.
Running all of them on one host is useful for development but does not protect
against loss of that host.

## 2. Installation and build

### Requirements

- Linux and Bash.
- JDK 17 or later for building and running the Java components.
- Maven.
- GCC and the development libraries required by the supplied CE core build script.
- A reachable ZooKeeper installation.
- Sufficient disk space for Maven downloads, build output, replica data, logs, and
  temporary data created by split/move operations.

The setup helper uses utilities such as `wget`, `tar`, and `pwdx`. Detecting
listeners requires `ss` or `lsof`; detecting their process IDs uses `lsof` or
`fuser`. Package installation by the core dependency checker requires sudo.

### Build a fresh Community source distribution

Run these commands from the Community project root.

**Warning:** Maven's `clean` phase deletes the generated `AtalantaDB/` directory.
If it contains an existing database, preserve its data, configuration, and logs
before rebuilding. Do not run this workflow against a running installation.

~~~bash
mvn -T4 -Dfindbugs.skip -Dcheckstyle.skip -Dpmd.skip=true -Dspotbugs.skip=true -DskipTests=true clean install

(
  cd adb-core
  bash ./check-deps-and-compile-CE.sh
)

install -m 0755 adb-core/adb AtalantaDB/bin/core/adb
~~~

The supplied `check-deps-and-compile-CE.sh` checks native build dependencies,
offers to install missing packages, and then runs `compile-CE.sh`. If dependencies
are already installed and that checker is not supplied, run `compile-CE.sh`
directly from `adb-core/`.

Maven builds and packages the Java components; it does not compile the native core.
Copy the successfully built `adb` binary into the generated distribution as shown
above. Do not copy a license file into a Community installation.

### Distribution layout

The generated distribution uses this layout:

~~~text
AtalantaDB/
├── setup-adb.sh
├── 0-stop.sh
├── 1-start-new.sh
├── 2-start-old.sh
├── 3-shell.sh
├── 4-web-ui.sh
├── 99-env.sh
├── bin/
│   ├── master1/
│   ├── master2/
│   ├── rs1/
│   ├── rs2/
│   ├── rs3/
│   ├── rs4/
│   ├── core/
│   │   ├── adb
│   │   └── atalanta_r1.prop ... atalanta_r4.prop
│   ├── shell/
│   └── web-ui/
├── scripts/
├── data/
└── logs/
~~~

Each master and RS directory contains its JAR, launcher, and configuration.
The `data/` and `logs/` directories are created by the fresh-start workflow.

Use the launchers packaged under `AtalantaDB/bin/`, not the older generic launchers
under individual Java modules' source resources. There is no need to rename
`atalanta-master.jar`, `atalanta-rs.jar`, or the shared `adb` binary.

## 3. Configuration

Configure files inside the generated `AtalantaDB/` directory. Changes to source
templates affect a later build, not an already packaged installation.

### ZooKeeper

All masters, RS instances, the shell, and the usage example must use the same
ZooKeeper ensemble and base path.

In each master/RS YAML configuration:

~~~yaml
zooKeeperConfiguration:
  connectionString: localhost:2181
  connectionBasePath: /atalanta
~~~

The files are:

- `bin/master1/config/master-configuration.yml`
- `bin/master2/config/master-configuration.yml`
- `bin/rs1/config/region-server-configuration.yml` through
  `bin/rs4/config/region-server-configuration.yml`

In `bin/shell/config/atalanta-site.xml`, set:

- `atalanta.zookeeper.connection.string`
- `atalanta.zookeeper.path`

Use the same settings in `atalanta-usage-example/config/atalanta-site.xml`.
A multi-host ensemble connection string lists its hosts separated by commas.

`ZK_HOME` in `99-env.sh` must point to the ZooKeeper `bin/` directory containing
`zkServer.sh` and `zkCli.sh`. This path is used by the local helper scripts.

The native core does not have a `zk_connection` property. Do not add that obsolete
parameter to its configuration.

### Replication factor

Keep these values equal:

| Component | Configuration property |
| --- | --- |
| Master | `dataConfiguration.replicationFactor` |
| RS | `masterConfiguration.replicationFactor` |
| Core | `rf` in its `.prop` file |

The supplied example uses RF = 3. RF = 1 has no reserve copy from which to recover.
For host-loss protection, provide enough independent RS hosts for the desired RF;
several replicas on the same host do not provide independent failure protection.

### RegionServer and core settings

For RS1, edit `bin/rs1/config/region-server-configuration.yml` and
`bin/core/atalanta_r1.prop`. Apply the equivalent changes to the other pairs.

| RS setting | Matching core setting / requirement |
| --- | --- |
| `coreConfiguration.clientPort` | Must equal `client_port`. |
| `coreConfiguration.rsPort` | Must equal `rs_port`. |
| `coreConfiguration.host` | Normally `localhost`; RS and its core run on the same host. |
| `healthCheckerConfiguration.coreName` | Core configuration basename without `.prop`, for example `atalanta_r1`. |
| `rsDirs` | Existing writable data directories used exclusively by this RS. |
| `dataConfiguration.splitSourceBackupDir` | Writable split-backup directory if backups are enabled. |

Do not overlap data directories between RS instances. Resolve relative paths from
the instance directory in which its launcher runs.

A typical core configuration is:

~~~properties
client_port=10011
replication_port=10012
rs_port=20001
rf=3
health_file_name=atalanta1_health.chk
log_level=1
~~~

Use `.prop` files, not `.conf` files. `replication_port` is the core-to-core
replication listener. Give it a non-conflicting port and allow the required traffic.

When cores share a working directory, their `health_file_name` and `exit_file_name`
must be unique. With the normal replicated configuration, omitting
`health_file_name` prevents core startup.

The shipped core ports are:

| Pair | Client port | Replication port | RS control port |
| --- | --- | --- | --- |
| RS1/core1 | 10011 | 10012 | 20001 |
| RS2/core2 | 10022 | 10023 | 20002 |
| RS3/core3 | 10033 | 10034 | 20003 |
| RS4/core4 | 10044 | 10045 | 20004 |

### Java service ports and capacity

The launchers pass distinct HTTP ports with `--server.port`:

| Service | Packaged HTTP port |
| --- | --- |
| master1 | 8301 |
| master2 | 8302 |
| rs1 | 8303 |
| rs2 | 8304 |
| rs3 | 8305 |
| rs4 | 8306 |

Do not confuse these with `grpcConfiguration.masterPort`,
`grpcConfiguration.masterMonitoringPort`, or
`grpcConfiguration.regionServerPort`. The Java modules' common default HTTP port
is 8300; copying generic launchers without overriding it causes conflicts.

`grpcConfiguration.threadPoolSize` controls the gRPC executor's thread count.
It is not a bound on the number of queued requests or their memory consumption.

RS also has `grpcConfiguration.activeProcessingRequests`: the dispatcher uses
half of this value for each of its two task categories. It is not a count of
network ports and is not simply a per-caller quota.

Tune these values together with JVM heap limits, workload concurrency, and native
core capacity. Keep the remaining packaged launcher options intact unless you
understand their role; an unused TCP port is not automatically safe to expose.

## 4. Initial setup, startup, and shutdown

### Configure ZooKeeper

From the distribution directory:

~~~bash
cd AtalantaDB
bash ./setup-adb.sh
~~~

The helper:

1. Makes shell scripts and the native binary executable.
2. Tries to identify an existing ZooKeeper listener on port 2181 and locate its
   working directory using `pwdx`.
3. Asks whether to use that instance.
4. Otherwise offers to download an available ZooKeeper binary release or configure
   an existing installation.
5. Updates the configured ZooKeeper endpoint and asks whether to start AtalantaDB.

Review the fresh-start cleanup helper before accepting startup if ZooKeeper is
remote, uses a non-default client port, or uses a base path other than `/atalanta`.
The cleanup limitations below apply even when setup has updated the Java clients.

For an existing ZooKeeper installation, specify its `bin/` directory (or its
installation root), hostname, and client port. The existing-installation branch
configures AtalantaDB; it does not start that ZooKeeper server for you.

**Warning:** accepting the final startup prompt invokes `1-start-new.sh`, which
clears database data and logs. Decline this prompt when configuring an existing
database, then use the non-clearing restart workflow below.

### Fresh start: destructive

~~~bash
./1-start-new.sh
~~~

This is only for an empty installation or an intentional reset. It stops the
local database processes, deletes the ZooKeeper `/atalanta` subtree, clears
`data/rs1` through `data/rs4` and logs, and starts the packaged services.

It does not restart ZooKeeper or delete all of ZooKeeper's contents. However, the
cleanup helper currently hardcodes `/atalanta`; changing
`connectionBasePath` does not change that cleanup target. It also invokes
`zkCli.sh` without `-server`, so it uses that CLI's default endpoint rather than
the endpoint saved in the Java configurations. For a remote ensemble, a different
client port, or a different base path, adapt `scripts/99-clean-data.sh` before
using fresh start. Its fixed data-directory list must also be reviewed if you
change `rsDirs`. Do not reset an unrelated ZooKeeper instance or cluster path.

The script waits for the packaged service ports before reporting success.
Listening ports are a startup check, not proof that every table and replica is
healthy; inspect logs and perform the smoke test below.
The readiness port list is hardcoded. If you change the packaged HTTP or core
client ports, update that list as well.

### Restart without clearing data

~~~bash
./2-start-old.sh
~~~

This stops and restarts the packaged masters and RS/core pairs without invoking
the data or ZooKeeper cleanup helpers. It does not provide the same final readiness
check as `1-start-new.sh`; verify service logs and a client request afterward.

The top-level stop/restart scripts match process names broadly and may stop
AtalantaDB processes from another installation on the same host. Use them only
where those local processes are intended to be managed together. They are not
per-instance service managers.

### Open the shell

~~~bash
./3-shell.sh
~~~

Alternatively:

~~~bash
cd bin/shell
java -jar atalanta-shell.jar
~~~

Run the shell from its own directory so it can load `config/atalanta-site.xml`.
Press TAB to list commands.

### Start the Web UI

From `AtalantaDB/`:

~~~bash
./4-web-ui.sh
cat bin/web-ui/web-ui-port.txt
~~~

The launcher selects a free port starting at 8333. When 8333 is available, open
`http://localhost:8333/`; otherwise use the port written to the file.

Community Edition has no login or authorization checks. Restrict access at the
network and operating-system level.

### Logs

From `AtalantaDB/`:

~~~bash
tail -n 100 logs/master1.log
tail -n 100 logs/rs1.log
tail -n 100 logs/core1.log
~~~

Other packaged instances use the corresponding numbered log files. Check
`config/log4j.properties` inside an instance directory for its Java log path.

When diagnosing an early startup failure, launch the relevant instance script
from its own directory without the top-level script's output suppression.
The RS launcher also starts its core: do not run it against an instance you intend
to leave running.

### Stop the local installation

~~~bash
./0-stop.sh
~~~

The script sends termination signals and then forcibly kills matching processes
after one second. This is not a guaranteed graceful cluster shutdown.
It does not stop ZooKeeper. Do not delete data, metadata, or configuration to stop
the database.

## 5. Smoke test and basic operations

Use a disposable namespace and table:

~~~text
create_namespace 'setup_test'
create 'setup_test:records', 'cf'
list_regions 'setup_test:records'
put 'setup_test:records', 'key1', 'cf:value', 'hello'
get 'setup_test:records', 'key1'
count 'setup_test:records'
scan 'setup_test:records'
disable 'setup_test:records'
drop 'setup_test:records'
~~~

`create 'namespace:table', 'cf'` creates one region by default. A region-count
argument can be supplied, for example `create 'setup_test:records', 'cf', '100'`.
Only a single column family is currently supported; use `cf`.

Data operations, including `count`, are sent to cores rather than executed by the
master. The current shell `count` implementation counts the whole table; although
it parses optional start/stop arguments, it does not apply them. Do not use it as a
range-count command.

See [Shell commands](shell.md) and [Java API](java-api.md) for further examples.
For salted Java keys, use the recommended four-byte xxHash32 salter
(`XxHash32KeySalter`) consistently when writing and reading.

## 6. Expanding the cluster

### Add a master

1. Copy an existing master directory into a new instance directory. Keep the JAR
   name `atalanta-master.jar` and the launcher name `start-master.sh`.
2. Use the same ZooKeeper ensemble, base path, and replication factor.
3. Review `grpcConfiguration.masterPort`,
   `grpcConfiguration.masterMonitoringPort`, HTTP `--server.port`, and any other
   listening ports in the copied launcher. Avoid conflicts between concurrently
   listening services on the same host.
4. Give the instance its own log path.
5. Start it from its instance directory and verify its role in the logs.

An additional master is a standby candidate, not an additional active coordinator.
On one host, the supplied masters can share the administrative `masterPort`
because that listener belongs to the active leader; their monitoring and HTTP
listeners must not conflict.

The top-level scripts have a fixed instance list. Adding a directory alone does
not make those scripts start or stop the new instance safely; review their commands
and readiness checks.

### Add an RS/core pair

1. Copy an existing RS directory, keeping the JAR name `atalanta-rs.jar`.
2. Create a separate core `.prop` configuration. The binary may remain named
   `adb`; multiple processes can use the same binary with different configurations.
3. Assign exclusive `rsDirs`, matching RS/core control and client ports, and a
   non-conflicting core replication port.
4. Assign unique core health/exit filenames when sharing a working directory.
5. Set `healthCheckerConfiguration.coreName` to the new core configuration basename.
6. Update the copied launcher to select the new `.prop` file, return to the correct
   RS directory, and write to a distinct core log.
7. Review all Java listening ports and update its Java log path.
8. Start the pair from its RS directory and verify its registration and health.

The packaged RS launcher starts the corresponding core before the Java RS.
Do not start a second core with the same configuration.

### RS identity and existing data

An RS reuses its ZooKeeper ID by matching its published host/port identity.
Changing that identity may register it as a new RS rather than the previous one.

Existing replicas no longer assigned to the resulting RS are treated as orphans:
the startup logic renames their directories, and the orphan cleaner eventually
deletes them according to `dataConfiguration.keepOrphanHours`. This is not a
promise that every file is deleted immediately.

Treat endpoint changes, data-directory moves, and ZooKeeper metadata changes as
data migrations. Preserve metadata and data together, and review replica placement
before restarting with changed settings.

### Multi-host deployment

- Place RS/core pairs on independent hosts for host-loss protection.
- Keep each RS and its paired core on the same host.
- Use one shared ZooKeeper ensemble and base path.
- Ensure advertised hostnames resolve to reachable addresses from all participants.
  Do not use the `docker` profile for this native multi-host deployment: it
  advertises localhost.
- Use persistent writable storage and retain instance configuration across restarts.
- Copy the relevant instance directories and core configurations; preserve the
  relative layout expected by their launchers.
- Use host-specific service management. Do not run the packaged fresh-start/reset
  script on a host joining an existing cluster.
- Never point independent clusters at the same ZooKeeper base path.

The bundled setup/reset scripts are a single-host example, not a distributed
cluster installer. Copying them unchanged to several hosts does not coordinate a
cluster-wide restart.

The supplied metric-collection settings target the local example. Cross-host
metric collection must be verified separately: do not assume that opening the
administrative gRPC ports alone makes automatic splitting work. Until the required
replica and disk metrics are available, set `dataConfiguration.regionsAutoSplitEnabled`
to `false`. Manual splitting also needs those metrics and can fail if they are
unavailable.

## 7. Network communication

Restrict access to trusted hosts. CE has no built-in authentication, authorization,
or transport encryption.

| Communication | Listener / configuration |
| --- | --- |
| Clients and RS to active master | `grpcConfiguration.masterPort` in master YAML. |
| Service monitoring consumers to masters | `grpcConfiguration.masterMonitoringPort`, when configured. |
| Master and other RS to an RS | `grpcConfiguration.regionServerPort` in RS YAML. |
| Clients to cores | `client_port` in the core `.prop` file. |
| Core-to-core replication | `replication_port` in the core `.prop` file. |
| Local RS to its paired core | `rs_port`, matching `coreConfiguration.rsPort`. |
| RS-to-RS file transfers | OS-assigned dynamic TCP ports on the receiving RS. |
| HTTP endpoints and Web UI | Each launcher's `--server.port`. |
| Java services and clients to ZooKeeper | ZooKeeper ensemble client ports. |

For file transfers, the receiving RS binds a TCP socket to port 0. The operating
system chooses the port; the address and port are sent to the sending RS over gRPC.
The sender then streams the file using the zero-copy transfer path.

Allow the appropriate dynamic listener range between trusted RS hosts. Opening
an arbitrary number of ports is insufficient; `activeProcessingRequests / 2`
does not specify the allowed port numbers. On Linux, inspect the configured
ephemeral range with:

~~~bash
cat /proc/sys/net/ipv4/ip_local_port_range
~~~

Ensure network policy covers every required service and operational endpoint,
not only the ports listed for data access. Keep local-only endpoints private and
do not expose debugging or management listeners to untrusted networks.

## 8. Administrative operations

State-changing table operations use table locks and recorded operation state.
Operations on the same table may wait for one another. Cluster-wide operations
may also acquire a global lock.

Client deadlines limit how long the client waits. A timeout does not by itself
cancel the master's operation. Before retrying or restarting services, check the
table's current status and the master/RS logs.

| Operation | Shell command / behavior |
| --- | --- |
| Create namespace | `create_namespace 'ns'` |
| Create table | `create 'ns:table', 'cf'`; master plans replica placement and RS forwards creation to cores. |
| Disable table | `disable 'ns:table'`; blocks data operations and makes the table eligible for deletion. |
| Enable table | `enable 'ns:table'`; re-enables a disabled table. |
| Drop table | `drop 'ns:table'`; normally disable the table first. Replica files and table metadata are removed. |
| List replicas | `list_regions 'ns:table'`; client reads placement metadata from ZooKeeper. |
| Move replica | `move 'regionId_replicaNumber', 'destinationRsId'` |
| Split region | `split 'regionId'`; omit the replica number. |
| Split table | `split 'ns:table'` |
| Rebalance regions | `rebalance_regions 'criteria'` |
| Rebalance replicas | `rebalance_replicas 'criteria'` |
| Stop rebalancing | `stop_rebalance_regions 'criteria'` or `stop_rebalance_replicas 'criteria'` |
| Synchronize reserve replicas | `sync_replicas 'criteria'`; experimental, not a routine recovery command. |

### Table operation retries

Create, disable, enable, and drop operations record their progress and retry
through the master's existing operation runners. Relevant delays include
`createTableRetryDelayMs`, `disableTableRetryDelayMs`, `enableTableRetryDelayMs`,
and `dropTableRetryDelayMs`, under `dataConfiguration`.

If an RS is lost, recovery can intervene after the configured recovery timeout.
After recovery, the intended operation is completed: enabling finishes the enable
operation, and dropping finishes the drop operation. Neither is a disable request.

Corresponding client properties in `atalanta-site.xml` include
`atalanta.create.table.timeout.ms`, `atalanta.disable.table.timeout.ms`,
`atalanta.enable.table.timeout.ms`, and `atalanta.drop.table.timeout.ms`.

### Splitting a table

A whole-table split includes each current region once across its replicas,
regardless of its size or automatic-split thresholds. All region names are sent
in one request and processed through the existing parallel split operation.

A qualified name such as `ns:table` always identifies a table. For an unqualified
argument such as `test`, the shell first looks for a table in `default`; if it
does not exist, the argument is treated as a region ID.

Before splitting starts, the master checks the combined extra disk space needed
by the replicas. If configured, RS keeps the pre-split source data in
`dataConfiguration.splitSourceBackupDir`. Account for backup retention when
estimating disk usage.

### Rebalance criteria

Supported selection forms include:

- `!`: all namespaces and tables.
- `@namespace`: all tables in that namespace.
- `namespace:table`: one table.
- `namespace:test*`: matching tables for region rebalancing.

Use the same criteria when requesting a stop. Start/stop rebalance commands
initiate asynchronous work; an accepted request is not proof that it has finished.

`dataConfiguration.regionsRebalanceAnalyticsOnly=true` logs the rebalancing plan
without applying the moves. `rebalanceTableThreadsCount` controls table-level
rebalance concurrency, not the gRPC executor size.

### Disable an RS for placement

`stop_region_server 'regionServerId'` moves replicas off the RS and marks it
`DISABLED` after successful moves. It does not mean killing its Java/core processes.
`start_region_server 'regionServerId'` allows the registered RS to participate
again; it does not launch an operating-system process.

The stop command:

- Acquires the master's global operation lock and waits for active table operations.
- Marks the RS `DISABLING` and schedules tables in descending order of the number
  of replicas to move off that RS, with table-name ordering for ties.
- Uses the existing `master-operation-pool`. Its admission estimate is one slot
  per replica plus one for the table coordinator.
- Waits for running work when the next table cannot fit; an oversized table runs
  alone. This is an estimate, not a strict transfer limit.
- Waits for all submitted table moves, including after failures. A failed move
  leaves the RS `DISABLING` rather than declaring it successfully disabled.
- On interruption, stops submitting further tables but still waits for work
  already submitted before releasing the global lock.

These settings and estimates are separate from `grpcConfiguration.threadPoolSize`.

## 9. Recovery and automatic maintenance

### Failover and recovery

ZooKeeper elects a replacement when the active master is lost and another master
is available. The replacement resumes recorded administrative work.

If an RS managing replicas becomes unavailable, the master waits
`dataConfiguration.regionsRecoveryTimeoutMs` and initiates recovery for affected
tables. Recovery depends on healthy surviving replicas, reachable services, and
sufficient resources; replication is not a substitute for backups.

Relevant master settings include:

- `regionsRecoveryInitialCheckDelayMs`
- `regionsRecoveryCheckDelayMs`
- `regionsRecoveryTimeoutMs`
- `regionsRecoveryTableThreadsCount`
- `regionsRecoveryOperationsStoppageTableThreadsCount`
- `regionsRecoveryOperationsStoppageRSThreadsCount`
- `recoverTableRetryDelayMs`

They are under `dataConfiguration`. The master's precommit runner completes or
recovers interrupted recorded operations rather than assuming that a client
timeout undid them.

### Automatic splitting

When `dataConfiguration.regionsAutoSplitEnabled=true`:

1. The master waits `regionsAutoSplitInitialCheckDelayMs` after startup.
2. It checks region sizes between iterations using `regionsAutoSplitCheckDelayMs`.
3. A leader replica reaching `regionsAutoSplitTriggerMb` triggers selection of
   eligible regions in the same table.
4. Regions above the near-limit threshold defined by
   `regionsAutoSplitBatchMarginPercent` may join that operation. The default margin
   is 5%; 0 disables adding regions below the main threshold.
5. All selected replicas must be running and have the required metrics. The master
   accounts for combined extra disk space, including other tables planned in the
   same iteration, and preserves `regionsAutoSplitReservedMb` of free space.
6. Regions at or above the main threshold take priority. Regions without enough
   space are skipped.
7. Selected tables run through the existing split operation. The master waits for
   that iteration's work before the next check.

For a 100 MB threshold and a 5% margin, a region at 100 MB can trigger splitting
together with regions larger than 95 MB. Exactly 95 MB does not qualify. One MB
here means 1,048,576 bytes. Near-limit regions do not trigger a split on their own.

Batching reduces repeated operations but can increase disk pressure and operation
duration. It does not remove table locks or client waits for a stable region map.
Regions are not added to a split operation that is already running.

### Automatic rebalancing

When `dataConfiguration.regionsAutoRebalanceEnabled=true`, the master waits
`regionsAutoRebalanceInitialCheckDelayMs`, then evaluates placement between
iterations using `regionsAutoRebalanceCheckDelayMs`. It applies the existing
region/replica rebalancing logic, unless analytics-only mode is enabled.

Review `rebalanceTableThreadsCount`, disk capacity, and transfer traffic before
enabling it on a busy cluster.

### Operation progress

RS reports progress to the master using
`masterConfiguration.progressCalculatorIntervalMs`. Align this with the master's
`grpcConfiguration.rsProgressCalculatorIntervalMs`.

`grpcConfiguration.rsResponseTimeoutMs` is the master's operation wait budget.
Timely progress can extend the wait; prolonged missing progress causes the
operation to be treated as failed. Logs and table state distinguish a slow,
queued operation from an unavailable RS.

## 10. Production precautions and troubleshooting

- Run a resilient ZooKeeper ensemble and monitor its storage, sessions, and
  connection limits.
- Isolate the cluster on a trusted network. Do not expose CE client, administrative,
  monitoring, or HTTP ports to untrusted users.
- Apply restrictive filesystem permissions to data, configuration, and ZooKeeper
  storage. Use external storage encryption if needed.
- Keep persistent data and ZooKeeper metadata together when planning backups or
  migrations. Test restoration independently.
- Configure log rotation and retention: the example file appenders can otherwise
  grow without bounds.
- Tune JVM heap limits and native memory use according to measured workloads.
- Keep adequate free space for replicas, temporary copies, split backups, and logs.
- Use dedicated service management for production; the bundled scripts are a
  development example, including broad process matching and forced termination.
- Do not treat listening ports alone as proof of healthy replicas or completed
  recovery.

Use the following features with caution:

- Dictionary encoding is suitable only when the combined qualifier size remains
  below approximately 60 KB. The current dictionary setting cannot be changed
  using ALTER TABLE.
- Block compression can reduce performance and has not been thoroughly tested.
- Snapshots are experimental; restoration after a failure has not been validated.
- `sync_replicas` is for controlled recovery scenarios, not normal operation.

For startup failures, check ZooKeeper connectivity, writable data paths, port
conflicts, the native binary, and the matching RS/core configuration. For stalled
administrative work, collect master, RS, and core logs before deleting metadata or
restarting services.

Additional references:

- [Client configuration](atalanta-site.md)
- [Shell commands](shell.md)
- [Java API](java-api.md)
