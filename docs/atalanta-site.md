# `atalanta-site.xml` client configuration

This document describes the client properties supported in `atalanta-site.xml`.
The file is used by AtalantaDB Shell and can also be loaded through the Java API
when creating a database connection.

Unless stated otherwise, all timeout and delay values are specified in milliseconds.
Authentication, authorization, and TLS properties are intentionally not included.

## ZooKeeper

- **`atalanta.zookeeper.connection.string`**  
  Comma-separated ZooKeeper host and port pairs. This property is required.

- **`atalanta.zookeeper.path`**  
  Base znode containing AtalantaDB metadata. This property is required.

- **`atalanta.zookeeper.retry.count`**  
  Maximum number of ZooKeeper connection attempts.

- **`atalanta.zookeeper.retry.delay.ms`**  
  Delay between ZooKeeper connection attempts.

- **`atalanta.zookeeper.session.timeout.ms`**  
  ZooKeeper session timeout.

## Master connection and administrative operations

- **`atalanta.master.connection.timeout.ms`**  
  Timeout for establishing a connection to the master and for master requests.

- **`atalanta.master.disconnection.timeout.ms`**  
  Maximum time to wait for the master channel to terminate during disconnect.

- **`atalanta.create.namespace.timeout.ms`**  
  Timeout for creating a namespace.

- **`atalanta.create.table.timeout.ms`**  
  Timeout for table and snapshot creation-related requests.

- **`atalanta.enable.table.timeout.ms`**  
  Timeout for enabling a table.

- **`atalanta.disable.table.timeout.ms`**  
  Timeout for disabling a table.

- **`atalanta.drop.table.timeout.ms`**  
  Timeout for dropping a table.

- **`atalanta.move.region.timeout.ms`**  
  Timeout for moving a region to another region server.

- **`atalanta.split.region.timeout.ms`**  
  Timeout for splitting or merging regions.

- **`atalanta.rebalance.region.start.timeout.ms`**  
  Timeout for starting a region rebalance operation.

- **`atalanta.stop.rebalance.tasks.timeout.ms`**  
  Timeout for stopping region rebalance tasks.

- **`atalanta.stop.region.timeout.ms`**  
  Timeout for stopping a region.

- **`atalanta.start.region.timeout.ms`**  
  Timeout for starting a region.

- **`atalanta.shutdown.rs.timeout.ms`**  
  Timeout for shutting down a region server and waiting for the operation to
  complete.

## Connection pools and NIO

- **`atalanta.connection.pool.size`**  
  Number of write workers in the connection pool. A value of `0` selects a size
  based on the number of available processors.

- **`atalanta.connection.pool.queue.size`**  
  Maximum number of queued write operations per asynchronous mutation worker.

- **`atalanta.connection.read.pool.size`**  
  Number of read workers in the connection pool.

- **`atalanta.client.nio.selector.loop.size`**  
  Number of NIO selector loops. A value of `0` derives the number from the
  connection pool size.

- **`atalanta.connection.pool.reusable`**  
  Deprecated legacy switch for reusable connection pooling.

- **`atalanta.connection.pool.name`**  
  Connection pool name.

- **`atalanta.connection.pool.disconnect.timeout.ms`**  
  Timeout for disconnecting a connection pool.

- **`atalanta.connection.pool.hash.by.thread.id`**  
  Controls how NIO operations are distributed across pooled connections.
  The default value is `true`: each application thread is assigned to one
  connection. When set to `false`, a MurmurHash of the row key selects the
  connection. Key-based distribution can improve throughput when a small
  number of application threads must utilize several connections, including
  workloads with many small tables. Benchmark both modes for the target
  workload.

- **`atalanta.client.nio.direct.dispatch`**  
  Controls how NIO operations are submitted. The default value is `true`,
  which dispatches an operation from the calling thread. When set to `false`,
  operations are queued and processed by a dedicated worker thread. Queued
  dispatch can help under high producer contention, but adds queueing and
  context-switch overhead. Keep enabled unless benchmarks show an advantage
  for queued dispatch.

## Client retries, scans, and batching

- **`atalanta.client.scan.iteration.timeout`**  
  Maximum time, in milliseconds, to wait for one scan iteration response from
  the core.

- **`atalanta.connection.retry.count`**  
  Maximum number of attempts to reconnect to a core.

- **`atalanta.connection.retry.delay.ms`**  
  Delay between core reconnection attempts.

- **`atalanta.client.get.table.retry`**  
  Number of attempts to obtain current table metadata.

- **`atalanta.client.get.table.timeout.ms`**  
  Delay between attempts to obtain current table metadata.

- **`atalanta.client.table.retries.number`**  
  Maximum number of retries for retryable table operations.

- **`atalanta.client.table.retries.timeout.ms`**  
  Delay between retries for retryable table operations.

- **`atalanta.client.enable.bulk.api`**  
  Enables bulk requests for supported table operations.

- **`atalanta.client.min.batch.size`**  
  Minimum batch size used by asynchronous NIO mutations.

- **`atalanta.client.auto.flush.size`**  
  Number of accumulated mutations that triggers an automatic flush.
  Can significantly boost performance, but it should be used with caution.
  Increase when using small batches or high-latency connections, 
  but only if benchmarks show higher throughput.
  Increasing it usually provides little benefit when large batches 
  or many concurrent threads already saturate the database.
  Important: it counts pending queue elements, not the number of 
  records or bytes. With one queue element per call, the approximate 
  calculation is: autoFlushSize × batch size × data volume per record
  For example, a threshold of 1,000 and a batch size of 1,000 records 
  could hold around a million mutations. In a multithreaded environment, 
  this is not a strict upper limit.
  Warning: excessively high values can significantly increase memory 
  usage and GC overhead, especially with concurrent buffered writes. 
  Use the smallest value that achieves the desired throughput.

- **`atalanta.client.bulk.header.v2`**  
  Deprecated compatibility property. The bulk header is now selected
  automatically from the connection type: blocking connections use the
  legacy header, while NIO connections include the table position. Setting
  this property has no effect.

- **`atalanta.hbase.scan.filter`**  
  Deprecated compatibility switch for the scan wire format. The default
  value is `true`, which selects the current V2 scan protocol. Do not set it
  to `false`; that value selects the obsolete V1 format.

## Transport

- **`atalanta.transport.socket.timeout`**  
  Socket and request timeout in milliseconds.

- **`atalanta.transport.socket.receive.buffer.size`**  
  Requested socket receive-buffer size. A value of `0` leaves the operating
  system default unchanged.

- **`atalanta.transport.socket.send.buffer.size`**  
  Requested socket send-buffer size. A value of `0` leaves the operating system
  default unchanged.

- **`atalanta.transport.socket.tcp.no.delay`**  
  Enables or disables `TCP_NODELAY`.

- **`atalanta.transport.socket.retries.number`**  
  Maximum number of retries for repeatable transport operations.

- **`atalanta.transport.socket.retries.timeout`**  
  Incremental delay, in milliseconds, used by repeatable transport operations.

- **`atalanta.transport.core.unavailable.timeout`**  
  Timeout, in milliseconds, reported when a core remains unavailable.

- **`atalanta.transport.bulk.chunk.size`**  
  Maximum chunk size used to split bulk transport requests.

- **`atalanta.transport.enable.table.map.timeout`**  
  Enables propagation of `atalanta.transport.socket.timeout` to the core
  during the table-map handshake. The core uses the supplied value to detect
  and close stalled client connections. The default value is `true`. When
  disabled, the core falls back to its legacy fixed timeout.

## XML example

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>atalanta.zookeeper.connection.string</name>
        <value>localhost:2181</value>
    </property>
    <property>
        <name>atalanta.zookeeper.path</name>
        <value>/atalanta</value>
    </property>
    <property>
        <name>atalanta.connection.pool.size</name>
        <value>8</value>
    </property>
    <property>
        <name>atalanta.transport.socket.timeout</name>
        <value>60000</value>
    </property>
</configuration>
```
