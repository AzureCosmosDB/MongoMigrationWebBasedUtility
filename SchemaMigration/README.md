# Schema Transformer: Migrating MongoDB to Azure DocumentDB

Schema Transformer is a Python script designed to analyze MongoDB collection schemas and efficiently transform them into an Azure DocumentDB optimized structure. This ensures seamless compatibility and enhances query performance.

With this tool, you can generate index and sharding recommendations tailored specifically to your workload, making your migration smoother and more efficient.

## Supported Versions

The tool supports the following versions:

- **Source:** MongoDB (version 4.0 and above)
- **Target:** Azure DocumentDB (all versions)

## How to Run the Script

### Prerequisites

Before running the assessment, ensure that the client machine meets the following requirements:

- Access to both source MongoDB endpoint and target Azure DocumentDB endpoint, either over a private or public network via the specified IP or hostname.
- Python (version 3.10 or above) must be installed.
- PyMongo library must be installed (`pip install pymongo`).

### Steps to Run the Assessment

1. Navigate to the SchemaMigration directory.
2. Open the command prompt and navigate to this directory.
3. Create a JSON file to define the collections to be migrated. Each section in the configuration will define the schema migration options for a set of collections (you can specify `*` to refer to all collections in the cluster and `db.*` to refer all collections within a database). Refer the next section for more details on configuration options. Here are some examples -

    1. To specify all collections present in the cluster
    
        ```json
        {
            "sections": [
                {
                    "include": [
                        "*"
                    ],
                    "exclude": [],
                    "migrate_shard_key": "false",
                    "drop_if_exists": "true",
                    "optimize_compound_indexes": "true"
                }
            ]
        }
        ```
    2. To specify all collections except a particular database
    
        ```json
        {
            "sections": [
                {
                    "include": [
                        "*"
                    ],
                    "exclude": [
                        "db1.*"
                    ],
                    "migrate_shard_key": "false",
                    "drop_if_exists": "true",
                    "optimize_compound_indexes": "true"
                }
            ]
        }
        ```

    3. To specify all collections except few
    
        ```json
        {
            "sections": [
                {
                    "include": [
                        "*"
                    ],
                    "exclude": [
                        "db1.coll1",
                        "db2.coll2"
                    ],
                    "migrate_shard_key": "false",
                    "drop_if_exists": "true",
                    "optimize_compound_indexes": "true"
                }
            ]
        }
        ```

    4. To migrate specific collections
    
        ```json
        {
            "sections": [
                {
                    "include": [
                        "db1.coll1",
                        "db2.coll2"
                    ],
                    "migrate_shard_key": "false",
                    "drop_if_exists": "true",
                    "optimize_compound_indexes": "true"
                }
            ]
        }
        ```

    5. To migrate different set of collections with different configuration options
    
        ```json
        {
            "sections": [
                {
                    "include": [
                        "*"
                    ],
                    "exclude": [
                        "db1.coll1",
                        "db2.coll2"
                    ],
                    "migrate_shard_key": "false",
                    "drop_if_exists": "true",
                    "optimize_compound_indexes": "true"
                },
                {
                    "include": [
                        "db1.coll1",
                        "db2.coll2"
                    ],
                    "migrate_shard_key": "true",
                    "drop_if_exists": "true",
                    "optimize_compound_indexes": "true"
                }
            ]
        }
        ```

    6. To colocate collections with a reference collection
    
        ```json
        {
            "sections": [
                {
                    "include": [
                        "db1.coll2",
                        "db1.coll3"
                    ],
                    "migrate_shard_key": "false",
                    "drop_if_exists": "true",
                    "optimize_compound_indexes": "true",
                    "co_locate_with": "coll1"
                }
            ]
        }
        ```
        
        **Note:** The collection specified in `co_locate_with` must already exist in the same database as the collection being processed. If the reference collection is not found, the script will fail with an error.

    7. To pin unsharded collections to a specific destination shard

        ```json
        {
            "sections": [
                {
                    "include": [
                        "db1.coll1",
                        "db1.coll2"
                    ],
                    "migrate_shard_key": "false",
                    "drop_if_exists": "true",
                    "optimize_compound_indexes": "true",
                    "move_to": "shard_0"
                }
            ]
        }
        ```

        **Note:** `move_to` issues `db.adminCommand({ moveCollection: "<db>.<coll>", toShard: "<value>" })` on the destination after the collection is created. It cannot be combined with `migrate_shard_key: "true"` — a sharded collection cannot be pinned to a single shard, and the configuration will be rejected at parse time. If the collection already resides on the requested shard, the move is treated as a no-op.

        **Tip:** To discover the valid shard names to use as the `move_to` value, run the following against the destination cluster:

        ```javascript
        db.adminCommand({ listShards: 1 })
        ```

        Use the `_id` field of each returned shard document as the `move_to` value.

4. Run the following command, providing the full path of the JSON file created in the previous step:

    ```cmd
    python main.py --config-file <path_to_your_json_file> --source-uri <source_mongo_connection_string> --dest-uri <destination_documentdb_connection_string>
    ```

    **Optional: Enable verbose mode** for detailed logging of the migration process:

    ```cmd
    python main.py --config-file <path_to_your_json_file> --source-uri <source_mongo_connection_string> --dest-uri <destination_documentdb_connection_string> --verbose
    ```

    The `--verbose` flag provides detailed output showing:
    - Connection status to source and destination databases
    - Configuration parsing details (include/exclude patterns, collections found)
    - Step-by-step migration progress (drop, create, colocation, shard keys, indexes)
    - Detailed decision-making logic (e.g., why certain indexes are optimized or skipped)
    - Success/failure status for each operation

    **Optional: Run collection migrations in parallel** (recommended for large collection counts):

    ```cmd
    python main.py --config-file <path_to_your_json_file> --source-uri <source_mongo_connection_string> --dest-uri <destination_documentdb_connection_string> --workers 16
    ```

    The `--workers` flag controls how many collections are processed concurrently. Use `1` for sequential behavior. Default is `5`.

    **Optional: Control index migration strategy** using the `--mode` parameter:

    ```cmd
    # Pre-ingestion phase (create unique indexes only, before data migration)
    python main.py --config-file <path_to_your_json_file> --source-uri <source_mongo_connection_string> --dest-uri <destination_documentdb_connection_string> --mode preIngestion

    # Post-ingestion phase (create non-unique indexes only, after data migration)
    python main.py --config-file <path_to_your_json_file> --source-uri <source_mongo_connection_string> --dest-uri <destination_documentdb_connection_string> --mode postIngestion

    # Post-ingestion with blocking (prioritize index builds over new write operations)
    python main.py --config-file <path_to_your_json_file> --source-uri <source_mongo_connection_string> --dest-uri <destination_documentdb_connection_string> --mode postIngestion --blocking
    ```

    The `--mode` flag controls which indexes are created:
    - `complete` (default): Creates all indexes (both unique and non-unique)
    - `preIngestion`: Creates only unique indexes before data ingestion
    - `postIngestion`: Creates only non-unique indexes after data ingestion and skips collection drop/create and shard-key migration

    When using `postIngestion` mode, the optional `--blocking` flag enables blocking index builds, which prioritizes index creation over new write operations by using the `createIndexes` command with `blocking: true`. This is useful if you want to ensure indexes are built quickly without allowing concurrent writes during the indexing process.

This process will generate an Azure DocumentDB-optimized schema with index and sharding recommendations based on your workload.


### Configuration Options

| **Option** | **Description** |
|-----------|---------------|
| **migrate_shard_key** | Determines whether the existing shard key definition should be migrated. If set to `True`, the shard key is retained; if `False`, the target collection remains unsharded. Collections that are originally unsharded in the source will remain unsharded in the target, regardless of this setting. **Default:** `False`. |
| **drop_if_exists** | Specifies whether collections with the same name in the target should be dropped and recreated. If `True`, existing collections are removed before migration; if `False`, they remain unchanged. **Default:** `False`. |
| **optimize_compound_indexes** | Controls whether compound indexes should be optimized. If `True`, the script identifies redundant indexes and excludes them from migration; if `False`, all indexes are migrated as-is. **Default:** `False`. |
| **co_locate_with** | Specifies the name of a reference collection from the same database to colocate with. When specified, the target collection will be colocated with the reference collection for improved query performance. The reference collection must exist in the same database before colocation is applied, or an error will be thrown. This option is useful for optimizing queries that join or access related collections together. **Default:** `None`. |
| **move_to** | Pins an unsharded collection to a specific destination shard by issuing `db.adminCommand({ moveCollection: "<db>.<coll>", toShard: "<value>" })` after the collection is created. Cannot be combined with `migrate_shard_key: "true"` (configuration will be rejected at parse time). If the collection is already on the requested shard, the move is silently skipped. Ignored in `postIngestion` mode since collection drop/create is skipped. **Default:** `None`. |

### Command Line Options

| **Option** | **Required** | **Description** |
|-----------|-------------|---------------|
| **--config-file** | Yes | Path to the JSON configuration file that defines collections to migrate and their migration settings. |
| **--source-uri** | Yes | MongoDB connection string for the source database (e.g., `mongodb://localhost:27017`). |
| **--dest-uri** | Yes | MongoDB/DocumentDB connection string for the destination database. |
| **--workers** | No | Number of worker threads used to process collections in parallel. Default is `5`. Use `1` for sequential behavior. Increase this value for faster migrations with many collections. |
| **--verbose** | No | Enable verbose output mode. When set, displays detailed logging of all operations including connection status, configuration parsing, collection enumeration, and step-by-step migration progress. Useful for debugging and monitoring long-running migrations. |
| **--mode** | No | Index migration mode: `complete`, `preIngestion`, or `postIngestion`. Default is `complete`. See the "Index Migration Modes" section below for details. |
| **--blocking** | No | (postIngestion mode only) Enable blocking index builds to prioritize index creation over new write operations. When set, indexes are created using the `createIndexes` command with `blocking: true`. Only applicable when `--mode postIngestion` is used. |

### Index Migration Modes

The schema migration tool supports three index migration modes to optimize your migration workflow:

#### 1. **Complete Mode (default)**
Creates all indexes (both unique and non-unique) in a single pass.

**Use case:** When you want to migrate all indexes at once. Suitable for smaller datasets or when downtime is not a concern.

```cmd
python main.py --config-file config.json --source-uri <source> --dest-uri <dest>
# or explicitly
python main.py --config-file config.json --source-uri <source> --dest-uri <dest> --mode complete
```

#### 2. **Pre-Ingestion Mode**
Creates only unique indexes before data migration begins.

**Use case:** Run this phase before ingesting data into the destination database. Unique indexes help ensure data integrity during the migration process.

```cmd
python main.py --config-file config.json --source-uri <source> --dest-uri <dest> --mode preIngestion
```

**Workflow:**
1. Create destination collections with unique indexes
2. Verify unique constraints are in place
3. Begin data migration (from source to destination)
4. Once data migration completes, proceed to post-ingestion phase

#### 3. **Post-Ingestion Mode**
Creates only non-unique indexes after data has been migrated.

**Use case:** Run this phase after all data has been ingested into the destination database. Non-unique indexes are created after the data is in place for optimal performance.

In this mode, schema-level operations are skipped:
- No collection drop/recreate
- No collection creation
- No colocation changes
- No shard-key migration
- Existing destination indexes are detected and skipped

```cmd
python main.py --config-file config.json --source-uri <source> --dest-uri <dest> --mode postIngestion
```

**With blocking option (prioritize index builds):**
```cmd
python main.py --config-file config.json --source-uri <source> --dest-uri <dest> --mode postIngestion --blocking
```

When `--blocking` is enabled, indexes are built using the `createIndexes` command with `blocking: true`, which:
- Prioritizes index creation over new write operations
- Holds exclusive locks during index build
- Faster index creation for non-unique indexes
- Better for scenarios where index completion time is critical

Refer to the [Azure Cosmos DB index build documentation](https://learn.microsoft.com/en-us/azure/documentdb/how-to-create-indexes#prioritizing-index-builds-over-new-write-operations-using-the-blocking-option) for more information on the blocking option.

**Example Migration Workflow:**

```bash
# Step 1: Pre-ingestion - Create unique indexes only
python main.py --config-file config.json --source-uri mongodb://source --dest-uri mongodb://dest --mode preIngestion

# Step 2: Data Migration (using separate tools or scripts)
# Migrate data from source to destination...

# Step 3: Post-ingestion - Create non-unique indexes with blocking
python main.py --config-file config.json --source-uri mongodb://source --dest-uri mongodb://dest --mode postIngestion --blocking
```
