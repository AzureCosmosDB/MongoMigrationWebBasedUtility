from typing import List
from pymongo import MongoClient
from collection_config import CollectionConfig

class JsonParser:
    """
    A parser for JSON configuration files that generates a list of CollectionConfig objects.

    This class provides methods to parse JSON configurations and retrieve collections
    based on include and exclude patterns.
    """
    def __init__(self, config: dict, mongo_client: MongoClient, verbose: bool = False):
        self.config = config
        self.mongo_client = mongo_client
        self.verbose = verbose

    def _print_verbose(self, message: str) -> None:
        """Print a message if verbose mode is enabled."""
        if self.verbose:
            print(message)

    def parse_json(self) -> List[CollectionConfig]:
        """
        Parse the JSON configuration file and return a list of CollectionConfig objects.

        :return: A list of CollectionConfig objects.
        """
        collection_configs = {}

        self._print_verbose(f"[VERBOSE] Parsing {len(self.config.get('sections', []))} section(s) from configuration")

        for section_index, section in enumerate(self.config.get("sections", [])):
            self._print_verbose(f"[VERBOSE] Processing section {section_index + 1}")
            
            include = section.get("include", [])
            exclude = section.get("exclude", [])
            
            self._print_verbose(f"[VERBOSE]   Include patterns: {include}")
            self._print_verbose(f"[VERBOSE]   Exclude patterns: {exclude}")
            
            include_collection_set = self._get_collections(include)
            exclude_collection_set = self._get_collections(exclude)

            self._print_verbose(f"[VERBOSE]   Found {len(include_collection_set)} collection(s) from include patterns")
            self._print_verbose(f"[VERBOSE]   Found {len(exclude_collection_set)} collection(s) from exclude patterns")

            collections_to_migrate = include_collection_set.difference(exclude_collection_set)

            self._print_verbose(f"[VERBOSE]   Collections to migrate after exclusion: {len(collections_to_migrate)}")
            for coll in collections_to_migrate:
                self._print_verbose(f"[VERBOSE]     - {coll}")

            migrate_shard_key = section.get("migrate_shard_key", "false").lower() == "true"
            drop_if_exists = section.get("drop_if_exists", "false").lower() == "true"
            optimize_compound_indexes = section.get("optimize_compound_indexes", "false").lower() == "true"
            co_locate_with = section.get("co_locate_with")

            self._print_verbose(f"[VERBOSE]   Configuration:")
            self._print_verbose(f"[VERBOSE]     - migrate_shard_key: {migrate_shard_key}")
            self._print_verbose(f"[VERBOSE]     - drop_if_exists: {drop_if_exists}")
            self._print_verbose(f"[VERBOSE]     - optimize_compound_indexes: {optimize_compound_indexes}")
            self._print_verbose(f"[VERBOSE]     - co_locate_with: {co_locate_with}")

            for collection in collections_to_migrate:
                if collection in collection_configs:
                    raise ValueError(f"Duplicate collection entry found: {collection}")

                db_name, collection_name = collection.split(".", 1)
                collection_config = CollectionConfig(
                    db_name=db_name,
                    collection_name=collection_name,
                    migrate_shard_key=migrate_shard_key,
                    drop_if_exists=drop_if_exists,
                    optimize_compound_indexes=optimize_compound_indexes,
                    co_locate_with=co_locate_with
                )
                collection_configs[collection] = collection_config
        
        self._print_verbose(f"[VERBOSE] Total unique collections parsed: {len(collection_configs)}")
        
        return collection_configs.values()

    def _get_collections(self, collection_list: List[str]) -> set:
        """
        Retrieve a set of fully qualified collection names based on the input list.

        :param collection_list: A list of collection patterns (e.g., "*", "db.*", "db.collection").
        :return: A set of fully qualified collection names (e.g., "db.collection").
        """
        collection_set = set()
        for collection in collection_list:
            if collection == "*":
                self._print_verbose(f"[VERBOSE]     Pattern '*' detected - enumerating all databases and collections")
                # Include all collections in all databases
                for db_name in self.mongo_client.list_database_names():
                    source_db = self.mongo_client[db_name]
                    for collection_name in source_db.list_collection_names():
                        collection_set.add(f"{db_name}.{collection_name}")
                self._print_verbose(f"[VERBOSE]     Found {len(collection_set)} total collection(s) across all databases")
            elif ".*" in collection:
                # Include all collections in a specific database
                db_name = collection.split(".*")[0]
                self._print_verbose(f"[VERBOSE]     Pattern '{collection}' detected - enumerating collections in database '{db_name}'")
                source_db = self.mongo_client[db_name]
                db_collections = []
                for collection_name in source_db.list_collection_names():
                    full_name = f"{db_name}.{collection_name}"
                    collection_set.add(full_name)
                    db_collections.append(full_name)
                self._print_verbose(f"[VERBOSE]     Found {len(db_collections)} collection(s) in database '{db_name}'")
                for coll in db_collections:
                    self._print_verbose(f"[VERBOSE]       - {coll}")
            else:
                # Include specific collections
                db_name, collection_name = collection.split(".", 1)
                self._print_verbose(f"[VERBOSE]     Specific collection pattern: {collection}")
                source_db = self.mongo_client[db_name]
                if collection_name in source_db.list_collection_names():
                    collection_set.add(f"{db_name}.{collection_name}")
                    self._print_verbose(f"[VERBOSE]     Collection '{collection}' found and added")
                else:
                    self._print_verbose(f"[VERBOSE]     WARNING: Collection '{collection}' not found in source database")
        return collection_set
