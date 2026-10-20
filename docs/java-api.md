# Java API

This guide shows how to connect to AtalantaDB, create a namespace and table,
and write, read, and delete rows using the Java client. The examples use the
current `com.light.atalanta` API.

The examples use these imports:

```java
import com.light.atalanta.*;
import com.light.atalanta.client.factories.AtalantaDbNioClientObjectFactory;
import com.light.atalanta.client.factories.AtalantaDbObjectFactory;
import com.light.atalanta.factory.ObjectFactory;
import com.light.atalanta.util.Bytes;
import com.light.atalanta.util.UniformSplit;
import com.light.atalanta.util.XxHash32KeySalter;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
```

## Choose a client

`ObjectFactory` creates configurations, connections, table names, and row
operations. Import it from `com.light.atalanta.factory.ObjectFactory`.

- `AtalantaDbObjectFactory` provides the LocalThread connection. Its table
  objects are not designed to be shared between application threads; give
  each thread its own table or synchronize access.
- `AtalantaDbNioClientObjectFactory` provides the NIO connection pool for
  concurrent workloads. Share one connection for the application lifetime.
- `AtalantaDbConnectionPoolObjectFactory` provides the older connection pool.
  It is deprecated; use the NIO factory for new applications.

The factory implementations are in
`com.light.atalanta.client.factories`. For example:

```java
ObjectFactory factory = new AtalantaDbNioClientObjectFactory();
```

For NIO, `atalanta.connection.pool.size` controls the number of pooled
connections (`0` resolves to two times the number of available processors).
`atalanta.connection.pool.queue.size` limits each worker's operation queue.
See [client configuration](atalanta-site.md) for other settings.

## Connect

Load the client configuration from `atalanta-site.xml` and create a
connection through the factory:

```java
ObjectFactory factory = new AtalantaDbNioClientObjectFactory();
Configuration configuration = factory.configuration();
configuration.addResource(new File("./atalanta-site.xml"));

try (Connection connection = factory.connection(configuration)) {
    // Perform database operations here.
}
```

The configuration must provide the ZooKeeper connection string and base
path. Close the connection when the application is finished with it. For
LocalThread mode, replace `AtalantaDbNioClientObjectFactory` with
`AtalantaDbObjectFactory`.

## Create a namespace and table

Obtain `Admin` from the connection. Create a namespace, then create a table
descriptor with at least one column family:

```java
String namespace = "example";
TableName tableName = factory.tableName(namespace, "records");

try (Admin admin = connection.getAdmin()) {
    if (!admin.isNamespaceExists(namespace)) {
        admin.createNamespace(factory.namespaceDescriptor(namespace));
    }

    if (!admin.tableExists(tableName)) {
        TableDescriptor descriptor = factory.tableDescriptor(tableName);
        descriptor.addFamily(factory.columnDescriptor("cf"));
        admin.createTable(descriptor);
    }
}
```

To create multiple regions, pass split keys to `createTable`:

```java
int regionCount = 100;
byte[][] splitKeys = new UniformSplit().split(regionCount);
admin.createTable(descriptor, splitKeys);
```

`UniformSplit` is in `com.light.atalanta.util`. It produces
`regionCount - 1` split keys. Use the no-split `createTable(descriptor)`
overload for one region.

To remove a table later, call `admin.disableTable(tableName)` followed by
`admin.deleteTable(tableName)`. These operations delete database data, so
do not include them in initialization code.

## Row keys and columns

Run the following operation examples inside the connection's
try-with-resources block, after creating the table. These variables are
shared by the examples:

```java
TableName tableName = factory.tableName("example", "records");
byte[] family = Bytes.toBytes("cf");
byte[] qualifier = Bytes.toBytes("name");
byte[] rowKey = XxHash32KeySalter.saltKey(Bytes.toBytes("row-1"));
```

`Bytes` is in `com.light.atalanta.util`. Salting prepends a four-byte
xxHash32 value to distribute keys across regions. It is optional, but if
you salt keys when writing, use exactly the same key transformation when
reading or deleting. Salted keys are binary: do not convert them to strings
and back.

## Why use mutations?

A mutation is an operation that changes data. `Put` and `Delete` implement
`DbMutation`; `Get` is a read operation and is not a mutation. Calling
`table.put(put)` already performs a mutation: you do not need an additional
`mutate` call to make the write take effect.

There are two useful ways to submit changes:

- Use `Table.put`, `Table.get`, and `Table.delete` for straightforward
  request/response code. In the NIO pool, `put` and `delete` wait for their
  requests to complete before returning; `get` returns the data. Batch
  overloads accept lists of the corresponding operation type.
- Use `BufferedMutator.mutate` when a processing pipeline produces
  `DbMutation` objects, when you want to submit puts and deletes through one
  interface, or when you want to submit several writes before waiting for
  completion. In the NIO pool, mutations can remain in flight after
  `mutate` returns; call `flush()` to wait for the submitted changes and
  surface failures. Queues and automatic flush thresholds can still cause
  `mutate` itself to wait. In LocalThread mode, `mutate` executes changes
  synchronously and `flush()` is a no-op.

A mutation list is not a transaction: it does not provide all-or-nothing
updates across rows. Do not use a mixed list to express dependent changes
such as deleting and then recreating the same row; separate those steps
with completion boundaries. If a batch fails, some changes may already
have been applied.

The following examples do not use the experimental `putAndGet` or
`mutateAndGet` methods, which are currently disabled. A separate `get`
followed by `put` is not an atomic read-modify-write operation.

## Ordinary put/get/delete operations

The Java method corresponding to the shell's `del` command is `delete`,
not `del`.

### Write or update one row

Create a `Put`, add the column value, and pass it directly to `Table.put`:

```java
try (Table table = connection.getTable(tableName)) {
    Put put = factory.put(rowKey);
    put.addColumn(family, qualifier, Bytes.toBytes("Alice"));
    table.put(put);
}
```

To update that column, create another `Put` with the same row key, family,
and qualifier and the new value. `put` does not return the previous value.

### Read one row

Use the same key to retrieve the row, check that it exists, and extract a
column value:

```java
try (Table table = connection.getTable(tableName)) {
    Result result = table.get(factory.get(rowKey));
    if (result.getExists()) {
        byte[] value = result.getValue(family, qualifier);
        if (value != null) {
            System.out.println(Bytes.toString(value));
        }
    }
}
```

### Delete one row

Constructing a `Delete` with only the row key deletes the entire row:

```java
try (Table table = connection.getTable(tableName)) {
    table.delete(factory.delete(rowKey));

    Result result = table.get(factory.get(rowKey));
    System.out.println("Row exists after delete: " + result.getExists());
}
```

### Write, read, and delete a batch

`put(List<Put>)` and `delete(List<Delete>)` return no data.
`get(List<Get>)` returns `Result[]`. This example uses the NIO factory
selected above and distinct row keys:

```java
int batchSize = 1_000;
List<Put> puts = new ArrayList<>(batchSize);
List<Get> gets = new ArrayList<>(batchSize);
List<Delete> deletes = new ArrayList<>(batchSize);

for (int i = 0; i < batchSize; i++) {
    byte[] key = XxHash32KeySalter.saltKey(Bytes.toBytes("batch-row-" + i));

    Put put = factory.put(key);
    put.addColumn(family, qualifier, Bytes.toBytes("User " + i));
    puts.add(put);
    gets.add(factory.get(key));
    deletes.add(factory.delete(key));
}

try (Table table = connection.getTable(tableName)) {
    table.put(puts);

    Result[] results = table.get(gets);
    for (int i = 0; i < results.length; i++) {
        Result result = results[i];
        if (result != null && result.getExists()) {
            byte[] value = result.getValue(family, qualifier);
            if (value != null) {
                System.out.println("batch-row-" + i + ": " + Bytes.toString(value));
            }
        }
    }

    // Destructive: remove the rows written by this example.
    table.delete(deletes);
}
```

For the NIO pool, results correspond to the input `Get` order. The client
routes operations to workers and regions, so one list does not necessarily
become one network request. Depending on the connection implementation
and configuration, list overloads can also execute individual operations;
for example, LocalThread `delete(List<Delete>)` loops over single deletes.

Keep batches bounded rather than creating a list containing the entire
dataset. The batch size above is an example, not a required value or a
recommendation for every workload. Account for record size, region count,
and available memory when choosing it. Regular `put/get/delete` calls in
the NIO pool do not need an additional `flush()`; flush any previously
submitted buffered mutations before reading their results.

## Buffered mutations

### Write a row and wait for completion

Use a `BufferedMutator` when you want to submit changes through the generic
mutation interface:

```java
try (BufferedMutator mutator = connection.getBufferedMutator(tableName)) {
    Put put = factory.put(rowKey);
    put.addColumn(family, qualifier, Bytes.toBytes("Alice"));
    mutator.mutate(put);
    mutator.flush();
}
```

Call `flush()` after the last mutation and before reading the changed rows.
Use an explicit flush even though closing the mutator also flushes in the
current implementations: it makes the completion boundary and the point
at which errors are reported visible in your code.

### Submit a mixed batch of puts and deletes

Use `List<DbMutation>` to submit different change types through one call.
The keys below are different, so the operations do not depend on each
other's execution order:

```java
byte[] firstKey = XxHash32KeySalter.saltKey(Bytes.toBytes("mutation-row-1"));
byte[] secondKey = XxHash32KeySalter.saltKey(Bytes.toBytes("mutation-row-2"));

Put firstPut = factory.put(firstKey);
firstPut.addColumn(family, qualifier, Bytes.toBytes("Alice"));
Put secondPut = factory.put(secondKey);
secondPut.addColumn(family, qualifier, Bytes.toBytes("Bob"));

List<DbMutation> mutations = new ArrayList<>();
mutations.add(firstPut);
mutations.add(secondPut);
mutations.add(factory.delete(rowKey)); // Destructive: delete the earlier row.

try (BufferedMutator mutator = connection.getBufferedMutator(tableName)) {
    mutator.mutate(mutations);
    mutator.flush();
}

// Read only after the buffered changes have completed.
try (Table table = connection.getTable(tableName)) {
    Result result = table.get(factory.get(firstKey));
    System.out.println("First row exists: " + result.getExists());
}
```

A `List<Put>` is not a `List<DbMutation>` in Java. For a homogeneous write
batch, either use `table.put(puts)` as shown above or create a
`List<DbMutation>` containing those puts before calling `mutate`.

### Delete a row through the mutation interface

Create a `Delete` for the row key and send it through a mutator:

```java
try (BufferedMutator mutator = connection.getBufferedMutator(tableName)) {
    Delete delete = factory.delete(rowKey);
    mutator.mutate(delete);
    mutator.flush();
}
```
