# AtalantaDB Community Edition

System Requirements (Minimum):
- Disk Space: 2 GB free
- RAM: 4 GB (for BigData, replace in start-*.sh "java -Xmx250m" with "java -Xmx16G" or higher)

## How to use
On Linux:
1. Run `./build-adb.sh`.
2. See the usage example in `./AtalantaDB/atalanta-usage-example`.

In Docker (Linux; macOS with Docker Desktop is experimental):
1. Run `./build-adb-docker-ce.sh`; it builds the images and optionally starts the cluster.
2. Open the Web UI at http://localhost:8433/ and see `atalanta-docker-compose-ce/README.md` for the shell, logs and stop commands.

## The Community Edition does not support:
1. Authentication and authorization (ACL and Kerberos/Ranger) features.
2. Writing records larger than 60,648 bytes.
3. Append Update mode, which provides faster data updates and NVME care.
4. Read Cache mode, which caches data in the process memory.

These features are available in AtalantaDB Enterprise Edition. For more information, visit [atalantadb.com](https://atalantadb.com).

## Recommendations:
- At the moment AtalantaDB does not support multiple ColumnFamily, better always specify 'cf'.
- It is strongly recommended always use UniformSplit and the xxHash32 algorithm for key hashing (see XxHash32KeySalter), this will be important in future versions.
- It is recommended to use a sufficiently powerful ZooKeeper instance configured with at least maxClientCnxns=32000 for BigData usage.

## Use the following features with caution:
- Dictionary encoding (DICTIONARY => 'TRUE') should be enabled only when the combined size of all qualifiers is less than approximately 60 KB. 
  Currently, this setting cannot be changed using ALTER TABLE.
- Block compression (COMPRESSION => 'SNAPPY' or COMPRESSION => 'LZ') significantly reduce performance and has not been thoroughly tested.
  This type of compression is highly effective for certain kinds of data. However intensive use of the associated operations can lead to increased 
  processing times potentially resulting in recurring timeout errors and requires fine-tuning.
- Snapshots are experimental. Data recovery from a snapshot after a failure has not been tested.

## Scan Consistency and Ordering
- When updates occur during a scan, the scan may return the same key-value more than once.
- When a region split occurs during a scan, the scan may return the same key-value multiple times.
- Scan results are not guaranteed to be sorted by key.

Official website:
https://atalantadb.com/

Performance benchmarks:
https://atalantadb.com/performance/