from typing import List, Tuple
from pymongo import MongoClient
from pymongo.database import Database
from collection_config import CollectionConfig

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
        """
        for collection_config in collection_configs:
            db_name = collection_config.db_name
            collection_name = collection_config.collection_name

            print(f"\nMigrating schema for collection: {db_name}.{collection_name}")

            source_db = source_client[db_name]
            source_collection = source_db[collection_name]

            dest_db = dest_client[db_name]
            dest_collection = dest_db[collection_name]

            # Check if the destination collection should be dropped
            if collection_config.drop_if_exists:
                print("-- Running drop command on target collection")
                dest_collection.drop()

            # Create the destination collection if it doesn't exist
            if not collection_name in dest_db.list_collection_names():
                print("-- Creating target collection")
                dest_db.create_collection(collection_name)
            else:
                print("-- Target collection already exists. Skipping creation.")

            # Handle colocation if specified
            if collection_config.co_locate_with:
                print(f"-- Setting up colocation with collection: {collection_config.co_locate_with}")
                self._setup_colocation(dest_db, collection_name, collection_config.co_locate_with)
                self._verify_colocation(dest_client, db_name, collection_name, collection_config.co_locate_with)

            # Check if shard key should be created
            if collection_config.migrate_shard_key:
                source_shard_key = self._get_shard_key(source_db, collection_config)
                if (source_shard_key is not None):
                    print(f"-- Migrating shard key - {source_shard_key}.")
                    dest_client.admin.command(
                        "shardCollection",
                        f"{db_name}.{collection_name}",
                        key=source_shard_key)
                else:
                    print(f"-- No shard key found for collection {collection_name}. Skipping shard key setup.")
            else:
                print("-- Skipping shard key migration for collection")

            # Migrate indexes
            index_list = []
            source_indexes = source_collection.index_information()
            for source_index_name, source_index_info in source_indexes.items():
                index_keys = source_index_info['key']
                index_options = {k: v for k, v in source_index_info.items() if k not in ['key', 'v']}
                index_options['name'] = source_index_name
                index_list.append((index_keys, index_options))

            if collection_config.optimize_compound_indexes:
                print("-- Optimizing compound indexes if available")
                index_list = self._optimize_compound_indexes(index_list)

            print("-- Migrating indexes for collection")
            for index_keys, index_options in index_list:
                if self._is_ts_ttl_index(index_keys, index_options):
                    raise ValueError(f"Cannot migrate TTL index on _ts field for collection {collection_name}.")
                print(f"---- Creating index: {index_keys} with options: {index_options}")
                dest_collection.create_index(index_keys, **index_options)

    def _get_shard_key(self, source_db: Database, collection_config: CollectionConfig):
        """
        Retrieve the shard key definition for a given collection.

        :param source_db: The source database object.
        :param collection_config: The configuration object for the collection.
        :return: The shard key.
        """
        try:
            # Query config.collections to get shard key information
            config_db = source_db.client['config']
            collection_info = config_db.collections.find_one(
                {"_id": f"{source_db.name}.{collection_config.collection_name}"}
            )
            
            if collection_info and 'key' in collection_info:
                return collection_info['key']
            return None
        except Exception:
            # Collection is not sharded
            return None

    def _is_ts_ttl_index(self, index_keys: List[Tuple], index_options: dict) -> bool:
        """
        Check if the given index is a TTL (Time-To-Live) index on _ts field.

        :param index: The index to check.
        :return: True if the index is a TTL index, False otherwise.
        """
        if 'expireAfterSeconds' in index_options and any('_ts' ==  index_key[0] for index_key in index_keys):
            return True
        return False

    def _optimize_compound_indexes(self, index_list: List[Tuple]) -> List[Tuple]:
        """
        Optimize compound indexes for the given collection configuration.
        """
        compound_indexes = []
        not_compound_indexes = []
        for index in index_list:
            keys, options = index
            if self._is_compound_index(index):
                compound_indexes.append(index)
            else:
                not_compound_indexes.append(index)

        # Sort compound indexes by the number of keys in descending order
        compound_indexes.sort(key=lambda x: len(x[0]), reverse=True)

        optimized_compound_indexes = []
        for compound_index in compound_indexes:
            keys, options = compound_index
            is_redundant = False
            for optimized_index in optimized_compound_indexes:
                optimized_keys, optimized_options = optimized_index
                if self._is_subarray(keys, optimized_keys):
                    is_redundant = True
                    break
            if not is_redundant:
                optimized_compound_indexes.append(compound_index)
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
        # Check if reference collection exists
        if reference_collection not in dest_db.list_collection_names():
            raise ValueError(
                f"Reference collection '{reference_collection}' not found in database '{dest_db.name}'. "
                f"Cannot colocate collection '{collection_name}'."
            )

        # Run collMod command to set up colocation
        try:
            dest_db.command({
                "collMod": collection_name,
                "colocation": {
                    "collection": reference_collection
                }
            })
            print(f"---- Successfully colocated '{collection_name}' with '{reference_collection}'")
        except Exception as e:
            print(f"---- Failed to colocate '{collection_name}' with '{reference_collection}': {str(e)}")
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
            
            # Format collection names
            target_ns = f"{db_name}.{collection_name}"
            reference_ns = f"{db_name}.{reference_collection}"
            
            # Find which shard each collection is on
            target_shard = None
            reference_shard = None
            
            for shard_info in results:
                shards_list = shard_info.get('shards', [])
                if target_ns in shards_list:
                    target_shard = shard_info['_id']
                if reference_ns in shards_list:
                    reference_shard = shard_info['_id']
            
            # Verify colocation
            if target_shard is None:
                print(f"Collection '{target_ns}' not found in any shard.")
            if reference_shard is None:
                print(f"Reference collection '{reference_ns}' not found in any shard.")
            
            if target_shard == reference_shard:
                print(f"---- ✓ Colocation verified: '{collection_name}' and '{reference_collection}' are on shard '{target_shard}'")
            else:
                print(
                    f"Colocation verification failed: '{collection_name}' is on shard '{target_shard}' "
                    f"but '{reference_collection}' is on shard '{reference_shard}'. They should be on the same shard."
                )
        except Exception as e:
            print(f"Error verifying colocation: {str(e)}")
            raise ValueError(f"Error verifying colocation: {str(e)}")
