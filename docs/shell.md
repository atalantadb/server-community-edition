# AtalantaDB shell

This brief guide explains how to use atalanta-shell to read and write data in AtalantaDB,
and how to perform administrative operations such as creating and deleting tables and namespaces.

## Installation and startup

1. Build the AtalantaDB distribution or download it from the Nexus repository.
2. Copy atalanta-shell from the distribution into a separate directory:
   ```
     $ mkdir atalanta-shell
     $ cp -r ./build/app/atalanta-mpack/custom-services/ATALANTA/1.22.2.13-SNAPSHOT/package/files/shell/ atalanta_shell
   ```
3. Edit the client configuration file, atalanta-shell/config/atalanta-site.xml. See the appendix for a complete description of the parameters.
4. Start atalanta-shell:
   ```
   $ cd atalanta-shell
   $ ./atalanta-shell.sh
   atalanta-client#atalanta-shell>
   ```
5.  Press TAB to list the available commands:
    ```
    atalanta-client#atalanta-shell> 
    clone_snapshot            disable                   get_salt                  is_enabled                list_region_servers       rebalance_replicas        sync_replicas
    count                     drop                      grant                     is_exists                 list_regions              revoke                    user_permission
    create                    enable                    is_assigned               is_namespace_exists       list_snapshots            salt
    create_namespace          exit                      is_created                is_recovering             move                      scan
    delete                    get                       is_creating               is_splitting              put                       snapshot
    delete_bytes              get_binary                is_disabled               list                      put_bytes                 split
    delete_salt               get_binary_salt           is_disabling              list_namespace            put_salt                  stop_rebalance_regions
    delete_snapshot           get_bytes                 is_dropping               list_namespace_tables     rebalance_regions         stop_rebalance_replicas
    ```
    
## Administrative operations

#### 1. Creating a namespace

   To create a namespace, use the create_namespace command with the name of the new namespace:
   ``` 
   atalanta-client#atalanta-shell> create_namespace 'ns'
   Namespace 'ns' created successfully. Time: 378 ms.
   ```
#### 2. Listing namespaces

   To list namespaces, use the list_namespace command:
```
    atalanta-client#atalanta-shell> list_namespace 
    LT
    ns
    Found 2 namespace(s). Time: 7 ms.
```

#### 3. Creating tables

  To create a table, use the create command with the table name and column family name:
  ```  
    atalanta-client#atalanta-shell> create 'ns:test', 'cf'
    Table 'ns:test', 'cf' created successfully. Time: 877 ms.
  ```
#### 4. Listing tables

  To list tables, use the list command:
  ```
    atalanta-client#atalanta-shell> list
    ns:test
    Found 1 table(s). Time: 8 ms.
  ```

#### 5. Listing tables in a namespace

  To list the tables in a namespace, use the list_namespace_tables command with the namespace name:
  ```
    atalanta-client#atalanta-shell> list_namespace_tables 'ns'
    ns:test
    Found 1 table(s). Time: 13 ms.
  ```
#### 6. Disabling and deleting tables

Before deleting a table, disable it using the disable command with the table name:
```
atalanta-client#atalanta-shell> disable 'ns:test'
Table 'ns:test' disabled successfully. Time: 623 ms.
```
To delete a disabled table, use the drop command with the table name:
```
atalanta-client#atalanta-shell> drop 'ns:test'
Table 'ns:test' dropped successfully. Time: 148 ms.
```

## Working with data

#### 1. Writing data

To add a row to a table, use the put command. Its parameters are the table name, key, column name, and value.
To write keys and values in the [HBase binary string format](https://hbase.apache.org/1.2/apidocs/org/apache/hadoop/hbase/util/Bytes.html#toStringBinary-byte:A-)
or hexadecimal format (with the 0x prefix), enclose them in double quotes:
```
atalanta-client#atalanta-shell> put_salt 'ns:test', 'row1', 'cf:a', 'value1'
Table 'ns:test' put data complete. Time: 3 ms.

atalanta-client#atalanta-shell> put 'ns:test', "0x01", 'cf:a', 'value2'
Table 'ns:test' put data complete. Time: 3 ms.

```

#### 2. Reading by key

To read a row by its key, use the get command. Its parameters are the table name and key.
As with put, keys can be specified in the HBase binary string format or hexadecimal format by enclosing them in double quotes:
```
atalanta-client#atalanta-shell> get_salt 'ns:test', 'row1'
COLUMN                                             CELL
  cf:a                                              timestamp=2024-05-19 T07:17:27.074, value=value1
 1 row(s)
 Table 'ns:test' get data successfully. Time: 1716105113581 ms.

atalanta-client#atalanta-shell> get 'ns:test', "0x01"
COLUMN                                             CELL
  cf:a                                              timestamp=2024-05-19 T07:22:42.966, value=value2
 1 row(s)
 Table 'ns:test' get data successfully. Time: 1716105154611 ms.
```

#### 3. Scanning tables

To retrieve all rows in a table, use the scan command with the table name:

```
atalanta-client#atalanta-shell> scan 'ns:test'
key=\xC5\x1F@\xDErow1	cf:qualifier=cf:a, timestamp=1790866273868, value=value1
key=\x01		cf:qualifier=cf:a, timestamp=1790866280613, value=value2
Table 'ns:test' scan successfully. Found 2 record(s). Time: 724 ms.

```

#### 4. Deleting data

To delete a row by its key, use the delete command. Its parameters are the table name and key.
As with put, keys can be specified in the HBase binary string format or hexadecimal format by enclosing them in double quotes:
```
atalanta-client#atalanta-shell> delete 'ns:test', 'row1'
Table 'ns:test' delete data successfully. Time: 0 ms.
```

#### 5. Counting rows

To count rows in a table, use the count command with the table name.
```
atalanta-client#atalanta-shell> count 'ns:test'
Total count: 4 record(s). Time: 1 ms.
Table 'ns:test' counting successfully. Found 4 record(s). Time: 1 ms.
```
### Working with table regions

#### 1. Listing region servers for a table

To list the region servers for a table, use the list_region_servers command with the table name.
The response lists the table's region servers in this format: Server ID:host:port:core port:JMX port.
```
atalanta-client#atalanta-shell> list_region_servers 'ns:test'
0bc03a39-844e-4598-b35b-7d2182914604:rs1:8585:14444:8340
0c681de1-a0f4-422a-91ab-4e46b877db5c:rs2:8585:14444:8340
Found 2 region server(s). Time: 11 ms.
```
#### 2. Listing regions for a table

To list the regions of a table, use the list_regions command with the table name.
The response lists the table's regions in this format: region: region ID:replication factor:start key:stop key, server: Server ID:host:port:core port:JMX port.
```
atalanta-client#atalanta-shell> list_regions 'ns:test'
region: 9ae444e8c2fffdea_0:2:0000000000000000:ffffffffffffffff, server: 0c681de1-a0f4-422a-91ab-4e46b877db5c:rs2:8585:14444:8340
region: 9ae444e8c2fffdea_1:2:0000000000000000:ffffffffffffffff, server: 0bc03a39-844e-4598-b35b-7d2182914604:rs1:8585:14444:8340
Found 2 region replica(s). Time: 17 ms.
```

#### 3. Moving a region

To move a region to another region server, use the move command with the region ID and destination region server ID:
```
atalanta-client#atalanta-shell> move '9ae444e8c2fffdea_0', '0bc03a39-844e-4598-b35b-7d2182914604'
Region '9ae444e8c2fffdea_0' moved to '0bc03a39-844e-4598-b35b-7d2182914604' successfully. Time: 418 ms.
```
#### 4. Splitting a region or table

To split a region in two, use the split command with the region ID, without the replica number:
```
atalanta-client#atalanta-shell> split '9ae444e8c2fffdea'
Region '9ae444e8c2fffdea' split was successful. Time: 295 ms.
```

To split all regions of a table in one operation, specify the table name:
```
atalanta-client#atalanta-shell> split 'ns:test'
```
For a table in the `default` namespace, you can use `split 'test'`.
Each existing region of the table is split in two regardless of its size; automatic split thresholds do not apply.
All regions are sent to the master in a single request and processed in parallel. Before splitting starts, the total free disk space for all replicas is checked.
A name containing `:` always identifies a table. Without `:`, the command first looks for a table in `default`; if no such table exists, the argument is treated as a region ID.
