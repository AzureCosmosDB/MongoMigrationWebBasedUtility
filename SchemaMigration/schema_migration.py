from typing import List, Tuple, Dict, Any
from pymongo import MongoClient
from pymongo.database import Database
from collection_config import CollectionConfig
from console_utils import Colors, print_warning, print_error, print_success

class SchemaMigration:
    """
    A class to handle schema migration tasks between MongoDB databases.

    This class provides functionality to migrate indexes and shard keys
    from source collections to destination collections. It ensures that
    the destination collections are properly created, and shard keys and
    indexes are replicated as needed.

    Methods:
        migrate_schema(source_client, dest_client, collection_configs):
            Migrates indexes and shard keys from source to destination collections.
    """

    # Supported operators in partialFilterExpression for DocumentDB
    SUPPORTED_PARTIAL_FILTER_OPERATORS = {'$eq', '$gt', '$gte', '$lt', '$lte', '$type', '$exists'}

    def __init__(self, verbose: bool = False):
        """
        Initialize the SchemaMigration class.

        :param verbose: Enable verbose output for detailed flow logging.
        """
        self.verbose = verbose
        self.incompatible_indexes = []  # Track indexes with unsupported partialFilterExpression

    def _print_verbose(self, message: str) -> None:
        """Print a message if verbose mode is enabled."""
        if self.verbose:
            print(message)

    def _print_warning(self, message: str) -> None:
        """Print a warning message in yellow."""
        print_warning(message)

    def _print_error(self, message: str) -> None:
        """Print an error message in red."""
        print_error(message)

    def _print_success(self, message: str) -> None:
        """Print a success/milestone message in green."""
        print_success(message)

    def _validate_connections(
            self,
            source_client: MongoClient,
            dest_client: MongoClient) -> bool:
        """
        Validate that both source and destination MongoDB connections are working.

        :param source_client: MongoDB client connected to the source database.
        :param dest_client: MongoDB client connected to the destination database.
        :return: True if both connections are valid, False otherwise.
        :raises ConnectionError: If either connection fails.
        """
        print("Validating database connections...")
        self._print_verbose(f"Validating source and destination connections")
        
        # Validate source connection
        try:
            self._print_verbose(f"  Testing source connection...")
            source_info = source_client.server_info()
            source_version = source_info.get('version', 'unknown')
            self._print_success(f"-- Source connection OK (MongoDB version: {source_version})")
            self._print_verbose(f"  Source server info: {source_info}")
        except Exception as e:
            self._print_error(f"-- Source connection FAILED: {str(e)}")
            self._print_error(f"   Please check your source connection string and ensure the server is accessible.")
            raise ConnectionError(f"Failed to connect to source database: {str(e)}")
        
        # Validate destination connection
        try:
            self._print_verbose(f"  Testing destination connection...")
            dest_info = dest_client.server_info()
            dest_version = dest_info.get('version', 'unknown')
            self._print_success(f"-- Destination connection OK (MongoDB version: {dest_version})")
            self._print_verbose(f"  Destination server info: {dest_info}")
        except Exception as e:
            self._print_error(f"-- Destination connection FAILED: {str(e)}")
            self._print_error(f"   Please check your destination connection string and ensure the server is accessible.")
            raise ConnectionError(f"Failed to connect to destination database: {str(e)}")
        
        self._print_success("-- Both connections validated successfully.\n")
        self._print_verbose(f"Connection validation complete")
        return True

    def migrate_schema(
            self,
            source_client: MongoClient,
            dest_client: MongoClient,
            collection_configs: List[CollectionConfig]) -> None:
        """
        Migrate indexes and shard keys from source collections to destination collections.

        :param source_client: MongoDB client connected to the source database.
        :param dest_client: MongoDB client connected to the destination database.
        :param collection_configs: A list of CollectionConfig objects containing
                                   configuration details for each collection to migrate.
        :raises ConnectionError: If source or destination connection fails.
        """
        # Validate connections before starting migration
        self._validate_connections(source_client, dest_client)
        
        self._print_verbose(f"Starting migration for {len(collection_configs)} collection(s)")
        self.incompatible_indexes = []  # Reset incompatible indexes list for each migration run
        
        for collection_index, collection_config in enumerate(collection_configs):
            db_name = collection_config.db_name
            collection_name = collection_config.collection_name

            print(f"\nMigrating schema for collection: {db_name}.{collection_name}")
            
            self._print_verbose(f"Processing collection {collection_index + 1}/{len(collection_configs)}")
            self._print_verbose(f"  Database: {db_name}")
            self._print_verbose(f"  Collection: {collection_name}")

            source_db = source_client[db_name]
            source_collection = source_db[collection_name]

            dest_db = dest_client[db_name]
            dest_collection = dest_db[collection_name]

            # Check if the destination collection should be dropped
            if collection_config.drop_if_exists:
                print("-- Running drop command on target collection")
                self._print_verbose(f"Dropping existing collection {db_name}.{collection_name} on destination")
                dest_collection.drop()
                self._print_verbose(f"Collection dropped successfully")
            else:
                self._print_verbose(f"drop_if_exists=False, keeping existing collection if present")

            # Create the destination collection if it doesn't exist
            if not collection_name in dest_db.list_collection_names():
                print("-- Creating target collection")
                self._print_verbose(f"Collection does not exist on destination, creating new collection")
                dest_db.create_collection(collection_name)
                self._print_verbose(f"Collection created successfully")
            else:
                print("-- Target collection already exists. Skipping creation.")
                self._print_verbose(f"Collection already exists, skipping creation step")

            # Handle colocation if specified
            if collection_config.co_locate_with:
                print(f"-- Setting up colocation with collection: {collection_config.co_locate_with}")
                self._print_verbose(f"Colocation requested with reference collection: {collection_config.co_locate_with}")
                self._setup_colocation(dest_db, collection_name, collection_config.co_locate_with)
                self._verify_colocation(dest_client, db_name, collection_name, collection_config.co_locate_with)
            else:
                self._print_verbose(f"No colocation configured for this collection")

            # Check if shard key should be created
            if collection_config.migrate_shard_key:
                self._print_verbose(f"migrate_shard_key=True, checking for shard key on source")
                try:
                    source_shard_key = self._get_shard_key(source_db, collection_config)
                    if (source_shard_key is not None):
                        print(f"-- Migrating shard key - {source_shard_key}.")
                        self._print_verbose(f"Found shard key on source: {source_shard_key}")
                        self._print_verbose(f"Running shardCollection command on destination")
                        dest_client.admin.command(
                            "shardCollection",
                            f"{db_name}.{collection_name}",
                            key=source_shard_key)
                        self._print_verbose(f"Shard key applied successfully")
                    else:
                        self._print_warning(f"-- No shard key found for collection {collection_name}. Skipping shard key setup.")
                        self._print_verbose(f"Source collection is not sharded, skipping shard key migration")
                except PermissionError as e:
                    # Permission error already reported in _get_shard_key, continue with migration
                    self._print_verbose(f"Skipping shard key migration due to permission error")
            else:
                print("-- Skipping shard key migration for collection")
                self._print_verbose(f"migrate_shard_key=False, skipping shard key migration")

            # Migrate indexes
            self._print_verbose(f"Reading indexes from source collection")
            index_list = []
            source_indexes = source_collection.index_information()
            self._print_verbose(f"Found {len(source_indexes)} index(es) on source collection")
            
            for source_index_name, source_index_info in source_indexes.items():
                self._print_verbose(f"  Processing index: {source_index_name}")
                index_keys = source_index_info['key']
                index_options = {k: v for k, v in source_index_info.items() if k not in ['key', 'v']}
                index_options['name'] = source_index_name
                index_list.append((index_keys, index_options))
                self._print_verbose(f"    Keys: {index_keys}")
                self._print_verbose(f"    Options: {index_options}")

            if collection_config.optimize_compound_indexes:
                print("-- Optimizing compound indexes if available")
                self._print_verbose(f"optimize_compound_indexes=True, analyzing compound indexes")
                self._print_verbose(f"Index count before optimization: {len(index_list)}")
                index_list = self._optimize_compound_indexes(index_list)
                self._print_verbose(f"Index count after optimization: {len(index_list)}")
            else:
                self._print_verbose(f"optimize_compound_indexes=False, using all indexes as-is")

            print("-- Migrating indexes for collection")
            self._print_verbose(f"Creating {len(index_list)} index(es) on destination")
            for index_keys, index_options in index_list:
                index_name = index_options.get('name', 'unnamed')
                
                # Transform hashed indexes to regular composite indexes
                index_keys, was_hashed = self._transform_hashed_index(index_keys, index_name)
                
                # Transform partialFilterExpression if present
                if 'partialFilterExpression' in index_options:
                    self._print_verbose(f"  Processing partialFilterExpression for index: {index_name}")
                    transformed, is_compatible, issues = self._transform_partial_filter_expression(
                        index_options['partialFilterExpression'],
                        f"{db_name}.{collection_name}",
                        index_name
                    )
                    if not is_compatible:
                        self._print_verbose(f"  Index has incompatible partialFilterExpression, skipping")
                        self._print_warning(f"---- Skipping index '{index_name}' due to unsupported partialFilterExpression")
                        continue
                    index_options['partialFilterExpression'] = transformed
                
                self._print_success(f"---- Created index: {index_keys} with options: {index_options}")
                self._print_verbose(f"  Creating index on destination: {index_keys}")
                dest_collection.create_index(index_keys, **index_options)
                self._print_verbose(f"  Index created successfully")
        
        # Report all incompatible indexes at the end
        self._report_incompatible_indexes()
        
        self._print_verbose(f"Migration completed for all {len(collection_configs)} collection(s)")

    def _transform_hashed_index(
            self,
            index_keys: List[Tuple[str, Any]],
            index_name: str) -> Tuple[List[Tuple[str, Any]], bool]:
        """
        Transform hashed indexes to regular composite indexes.
        
        DocumentDB does not support hashed indexes, so indexes like 
        { "partition": "hashed", "_id": "hashed" } are converted to regular 
        ascending indexes (e.g., { "partition": 1, "_id": 1 }).
        
        :param index_keys: The original index keys as a list of tuples
        :param index_name: The index name for reporting
        :return: Tuple of (transformed_keys, was_transformed)
        """
        transformed_keys = []
        was_hashed = False
        hashed_fields = []
        
        for field, direction in index_keys:
            if direction == 'hashed':
                # Convert hashed to ascending (1)
                transformed_keys.append((field, 1))
                was_hashed = True
                hashed_fields.append(field)
            else:
                transformed_keys.append((field, direction))
        
        if was_hashed:
            original_keys_str = ', '.join([f"'{f}': 'hashed'" for f, d in index_keys if d == 'hashed'])
            new_keys_str = ', '.join([f"'{f}': 1" for f in hashed_fields])
            self._print_warning(f"---- [MODIFIED] Index '{index_name}': Converted hashed index to regular composite index")
            self._print_warning(f"         Hashed fields converted: {{{original_keys_str}}} -> {{{new_keys_str}}}")
            self._print_verbose(f"  Transformed hashed index '{index_name}' to regular composite index")
            self._print_verbose(f"    Original keys: {index_keys}")
            self._print_verbose(f"    Transformed keys: {transformed_keys}")
        
        return transformed_keys, was_hashed

    def _transform_partial_filter_expression(
            self,
            partial_filter: Dict[str, Any],
            collection_namespace: str,
            index_name: str) -> Tuple[Dict[str, Any], bool, List[str]]:
        """
        Transform partialFilterExpression to be compatible with supported operators.
        
        Supported operators: $eq, $gt, $gte, $lt, $lte, $type, $exists
        
        Transformations:
        - $in with single value -> $eq (or direct equality)
        - $in with multiple values -> incompatible
        
        :param partial_filter: The original partialFilterExpression
        :param collection_namespace: The namespace (db.collection) for reporting
        :param index_name: The index name for reporting
        :return: Tuple of (transformed_filter, is_compatible, list_of_issues)
        """
        transformed = {}
        issues = []
        is_compatible = True
        
        self._print_verbose(f"    Original partialFilterExpression: {partial_filter}")
        
        for field, condition in partial_filter.items():
            if isinstance(condition, dict):
                # Check for operators in the condition
                new_condition = {}
                field_compatible = True
                
                for op, value in condition.items():
                    if op == '$in':
                        # $in operator found - check if single value
                        if isinstance(value, list) and len(value) == 1:
                            # Single value - convert to $eq
                            single_value = value[0]
                            self._print_verbose(f"    Converting $in with single value to $eq for field '{field}'")
                            new_condition['$eq'] = single_value
                        elif isinstance(value, list) and len(value) > 1:
                            # Multiple values - not compatible
                            issue = f"Field '{field}' uses $in with multiple values {value} (not supported)"
                            issues.append(issue)
                            field_compatible = False
                            is_compatible = False
                            self._print_verbose(f"    INCOMPATIBLE: {issue}")
                        else:
                            # Empty or non-list - treat as incompatible
                            issue = f"Field '{field}' uses $in with invalid value {value}"
                            issues.append(issue)
                            field_compatible = False
                            is_compatible = False
                            self._print_verbose(f"    INCOMPATIBLE: {issue}")
                    elif op.startswith('$') and op not in self.SUPPORTED_PARTIAL_FILTER_OPERATORS:
                        # Unsupported operator
                        issue = f"Field '{field}' uses unsupported operator '{op}'"
                        issues.append(issue)
                        field_compatible = False
                        is_compatible = False
                        self._print_verbose(f"    INCOMPATIBLE: {issue}")
                    else:
                        # Supported operator - keep as is
                        new_condition[op] = value
                
                if field_compatible:
                    if new_condition:
                        transformed[field] = new_condition
                    else:
                        # If condition became empty after transformation, skip the field
                        pass
            elif isinstance(condition, list):
                # Array equality (e.g., keywords: []) - check if empty array
                if len(condition) == 0:
                    # Empty array equality - this is direct equality, which is supported
                    transformed[field] = condition
                    self._print_verbose(f"    Field '{field}' with empty array equality - keeping as is")
                else:
                    # Non-empty array equality
                    issue = f"Field '{field}' uses array equality with non-empty array {condition} (may not be supported)"
                    issues.append(issue)
                    is_compatible = False
                    self._print_verbose(f"    INCOMPATIBLE: {issue}")
            else:
                # Direct value comparison (implicit $eq) - this is supported
                transformed[field] = condition
                self._print_verbose(f"    Field '{field}' with direct value - keeping as is")
        
        self._print_verbose(f"    Transformed partialFilterExpression: {transformed}")
        self._print_verbose(f"    Is compatible: {is_compatible}")
        
        # Record incompatibility if found
        if not is_compatible:
            self.incompatible_indexes.append({
                'collection': collection_namespace,
                'index_name': index_name,
                'issues': issues,
                'original_filter': partial_filter
            })
        
        return transformed, is_compatible, issues

    def _report_incompatible_indexes(self) -> None:
        """
        Report all incompatible indexes found during migration.
        """
        if not self.incompatible_indexes:
            self._print_success("\n✓ All indexes are compatible with partialFilterExpression requirements.")
            return
        
        self._print_warning("\n" + "="*80)
        self._print_warning("INCOMPATIBLE INDEXES REPORT")
        self._print_warning("="*80)
        self._print_warning(f"\nFound {len(self.incompatible_indexes)} index(es) with unsupported partialFilterExpression:")
        self._print_warning("\nSupported operators: $eq, $gt, $gte, $lt, $lte, $type, $exists")
        self._print_warning("Note: $in is only supported when checking for a single value (converted to $eq)\n")
        
        for idx, incompatible in enumerate(self.incompatible_indexes, 1):
            self._print_warning(f"{idx}. Collection: {incompatible['collection']}")
            self._print_warning(f"   Index Name: {incompatible['index_name']}")
            self._print_warning(f"   Original partialFilterExpression: {incompatible['original_filter']}")
            self._print_warning(f"   Issues:")
            for issue in incompatible['issues']:
                self._print_warning(f"     - {issue}")
            print()
        
        self._print_warning("="*80)
        self._print_warning("Please review these indexes and manually adjust the partialFilterExpression")
        self._print_warning("to use only supported operators before re-running the migration.")
        self._print_warning("="*80 + "\n")

    def _get_shard_key(self, source_db: Database, collection_config: CollectionConfig):
        """
        Retrieve the shard key definition for a given collection.

        :param source_db: The source database object.
        :param collection_config: The configuration object for the collection.
        :return: The shard key, or None if not sharded.
        :raises PermissionError: If user lacks permissions to read shard info.
        """
        from pymongo.errors import OperationFailure
        
        try:
            self._print_verbose(f"  Querying config.collections for shard key")
            # Query config.collections to get shard key information
            config_db = source_db.client['config']
            collection_info = config_db.collections.find_one(
                {"_id": f"{source_db.name}.{collection_config.collection_name}"}
            )
            
            if collection_info and 'key' in collection_info:
                self._print_verbose(f"  Shard key found: {collection_info['key']}")
                return collection_info['key']
            self._print_verbose(f"  No shard key found in config.collections")
            return None
        except OperationFailure as e:
            # Check for authorization/permission errors
            error_code = getattr(e, 'code', None)
            error_msg = str(e).lower()
            
            # Common permission-related error codes: 13 (Unauthorized), 18 (AuthenticationFailed)
            # Also check for common permission-related keywords in error message
            permission_indicators = ['unauthorized', 'not authorized', 'permission', 'auth', 'access denied']
            is_permission_error = (
                error_code in [13, 18] or 
                any(indicator in error_msg for indicator in permission_indicators)
            )
            
            if is_permission_error:
                self._print_verbose(f"  Permission error querying for shard key: {str(e)}")
                self._print_warning(f"---- WARNING: Insufficient permissions to read shard key information from config database.")
                self._print_warning(f"----          Error: {str(e)}")
                self._print_warning(f"----          Please ensure the user has 'read' access to the 'config' database.")
                self._print_warning(f"----          Skipping shard key migration for this collection.")
                raise PermissionError(
                    f"Insufficient permissions to read shard key info for "
                    f"{source_db.name}.{collection_config.collection_name}. "
                    f"Grant 'read' access to the 'config' database."
                )
            else:
                # Other operation failure - collection may not be sharded
                self._print_verbose(f"  Exception querying for shard key: {str(e)}")
                return None
        except Exception as e:
            # Unexpected error
            self._print_verbose(f"  Unexpected exception querying for shard key: {str(e)}")
            self._print_warning(f"---- WARNING: Unexpected error reading shard key: {str(e)}")
            return None

    def _optimize_compound_indexes(self, index_list: List[Tuple]) -> List[Tuple]:
        """
        Optimize compound indexes for the given collection configuration.
        """
        self._print_verbose(f"    Separating compound and non-compound indexes")
        
        compound_indexes = []
        not_compound_indexes = []
        for index in index_list:
            keys, options = index
            if self._is_compound_index(index):
                compound_indexes.append(index)
            else:
                not_compound_indexes.append(index)

        self._print_verbose(f"    Found {len(compound_indexes)} compound index(es)")
        self._print_verbose(f"    Found {len(not_compound_indexes)} non-compound index(es)")

        # Sort compound indexes by the number of keys in descending order
        compound_indexes.sort(key=lambda x: len(x[0]), reverse=True)
        self._print_verbose(f"    Sorted compound indexes by key count (descending)")

        optimized_compound_indexes = []
        for compound_index in compound_indexes:
            keys, options = compound_index
            is_redundant = False
            self._print_verbose(f"    Checking index {options.get('name', 'unnamed')} for redundancy")
            for optimized_index in optimized_compound_indexes:
                optimized_keys, optimized_options = optimized_index
                if self._is_subarray(keys, optimized_keys):
                    is_redundant = True
                    self._print_verbose(f"      Index is redundant (covered by {optimized_options.get('name', 'unnamed')})")
                    break
            if not is_redundant:
                optimized_compound_indexes.append(compound_index)
                self._print_verbose(f"      Index is not redundant, keeping it")
            else:
                self._print_verbose(f"      Removing redundant index")
        
        self._print_verbose(f"    Optimization result: {len(optimized_compound_indexes)} compound index(es) retained")
        
        return optimized_compound_indexes + not_compound_indexes

    def _is_compound_index(self, index: Tuple) -> bool:
        """
        Check if the given index is a compound index.

        :param index: The index to check.
        :return: True if the index is compound, False otherwise.
        """
        not_compound_options = ['unique', 'sparse', 'expireAfterSeconds']
        keys, options = index
        if len(keys) > 1 and not any(opt in options for opt in not_compound_options):
            return True
        return False

    def _is_subarray(self, sub: List, main: List) -> bool:
        """
        Check if the list `sub` is an subarray of the list `main`.

        :param sub: The list to check as a subset.
        :param main: The list to check against.
        :return: True if `sub` is an subarray of `main`, False otherwise.
        """
        sub_len = len(sub)
        main_len = len(main)

        if sub_len > main_len:
            return False

        for i in range(main_len - sub_len + 1):
            if main[i:i + sub_len] == sub:
                return True
        return False

    def _setup_colocation(self, dest_db: Database, collection_name: str, reference_collection: str) -> None:
        """
        Set up colocation for a collection with a reference collection.

        :param dest_db: The destination database object.
        :param collection_name: The name of the collection to colocate.
        :param reference_collection: The name of the reference collection to colocate with.
        :raises ValueError: If the reference collection does not exist.
        """
        self._print_verbose(f"  Checking if reference collection '{reference_collection}' exists")
        
        # Check if reference collection exists
        if reference_collection not in dest_db.list_collection_names():
            self._print_verbose(f"  ERROR: Reference collection '{reference_collection}' not found")
            raise ValueError(
                f"Reference collection '{reference_collection}' not found in database '{dest_db.name}'. "
                f"Cannot colocate collection '{collection_name}'."
            )

        self._print_verbose(f"  Reference collection exists, running collMod command")

        # Run collMod command to set up colocation
        try:
            dest_db.command({
                "collMod": collection_name,
                "colocation": {
                    "collection": reference_collection
                }
            })
            self._print_success(f"---- Successfully colocated '{collection_name}' with '{reference_collection}'")
            self._print_verbose(f"  collMod command successful")
        except Exception as e:
            self._print_verbose(f"  ERROR: collMod command failed: {str(e)}")
            self._print_error(f"---- Failed to colocate '{collection_name}' with '{reference_collection}': {str(e)}")
            raise ValueError(
                f"Failed to colocate collection '{collection_name}' with '{reference_collection}': {str(e)}"
            )

    def _verify_colocation(self, dest_client: MongoClient, db_name: str, collection_name: str, reference_collection: str) -> None:
        """
        Verify that a collection has been successfully colocated with a reference collection.
        This method queries the config database to check if both collections are on the same shard.

        :param dest_client: MongoDB client connected to the destination database.
        :param db_name: The name of the database.
        :param collection_name: The name of the collection to verify.
        :param reference_collection: The name of the reference collection.
        :raises ValueError: If the collections are not colocated as expected.
        """
        try:
            self._print_verbose(f"  Verifying colocation by querying config.chunks")
            
            # Query the config database to get shard information
            config_db = dest_client['config']
            
            # Aggregate chunks to get collections grouped by shard
            pipeline = [
                {
                    "$group": {
                        "_id": "$shard",
                        "shards": {"$addToSet": "$ns"}
                    }
                },
                {
                    "$sort": {"_id": 1}
                }
            ]
            
            results = list(config_db.chunks.aggregate(pipeline))
            
            self._print_verbose(f"  Found {len(results)} shard(s) with chunk information")
            
            # Format collection names
            target_ns = f"{db_name}.{collection_name}"
            reference_ns = f"{db_name}.{reference_collection}"
            
            self._print_verbose(f"  Looking for target namespace: {target_ns}")
            self._print_verbose(f"  Looking for reference namespace: {reference_ns}")
            
            # Find which shard each collection is on
            target_shard = None
            reference_shard = None
            
            for shard_info in results:
                shards_list = shard_info.get('shards', [])
                if target_ns in shards_list:
                    target_shard = shard_info['_id']
                    self._print_verbose(f"  Target collection found on shard: {target_shard}")
                if reference_ns in shards_list:
                    reference_shard = shard_info['_id']
                    self._print_verbose(f"  Reference collection found on shard: {reference_shard}")
            
            # Verify colocation
            if target_shard is None:
                self._print_warning(f"Collection '{target_ns}' not found in any shard.")
                self._print_verbose(f"  WARNING: Target collection not found in chunk information")
            if reference_shard is None:
                self._print_warning(f"Reference collection '{reference_ns}' not found in any shard.")
                self._print_verbose(f"  WARNING: Reference collection not found in chunk information")
            
            if target_shard == reference_shard:
                self._print_success(f"---- ✓ Colocation verified: '{collection_name}' and '{reference_collection}' are on shard '{target_shard}'")
                self._print_verbose(f"  Colocation verification successful")
            else:
                self._print_error(
                    f"Colocation verification failed: '{collection_name}' is on shard '{target_shard}' "
                    f"but '{reference_collection}' is on shard '{reference_shard}'. They should be on the same shard."
                )
                self._print_verbose(f"  ERROR: Collections are on different shards")
        except Exception as e:
            self._print_verbose(f"  ERROR: Exception during verification: {str(e)}")
            self._print_error(f"Error verifying colocation: {str(e)}")
            raise ValueError(f"Error verifying colocation: {str(e)}")
