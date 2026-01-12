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

4. Run the following command, providing the full path of the JSON file created in the previous step:

    ```cmd
    python main.py --config-file <path_to_your_json_file> --source-uri <source_mongo_connection_string> --dest-uri <destination_documentdb_connection_string>
    ```

This process will generate an Azure DocumentDB-optimized schema with index and sharding recommendations based on your workload.


### Configuration Options

| **Option** | **Description** |
|-----------|---------------|
| **migrate_shard_key** | Determines whether the existing shard key definition should be migrated. If set to `True`, the shard key is retained; if `False`, the target collection remains unsharded. Collections that are originally unsharded in the source will remain unsharded in the target, regardless of this setting. **Default:** `False`. |
| **drop_if_exists** | Specifies whether collections with the same name in the target should be dropped and recreated. If `True`, existing collections are removed before migration; if `False`, they remain unchanged. **Default:** `False`. |
| **optimize_compound_indexes** | Controls whether compound indexes should be optimized. If `True`, the script identifies redundant indexes and excludes them from migration; if `False`, all indexes are migrated as-is. **Default:** `False`. |
| **co_locate_with** | Specifies the name of a reference collection from the same database to colocate with. When specified, the target collection will be colocated with the reference collection for improved query performance. The reference collection must exist in the same database before colocation is applied, or an error will be thrown. This option is useful for optimizing queries that join or access related collections together. **Default:** `None`. |
