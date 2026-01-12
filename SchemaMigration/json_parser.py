from typing import List
from pymongo import MongoClient
from collection_config import CollectionConfig

class JsonParser:
    """
    A parser for JSON configuration files that generates a list of CollectionConfig objects.

    This class provides methods to parse JSON configurations and retrieve collections
    based on include and exclude patterns.
    """
    def __init__(self, config: dict, mongo_client: MongoClient):
        self.config = config
        self.mongo_client = mongo_client

    def parse_json(self) -> List[CollectionConfig]:
        """
        Parse the JSON configuration file and return a list of CollectionConfig objects.

        :return: A list of CollectionConfig objects.
        """
        collection_configs = {}

        for section in self.config.get("sections", []):
            include = section.get("include", [])
            exclude = section.get("exclude", [])
            include_collection_set = self._get_collections(include)
            exclude_collection_set = self._get_collections(exclude)

            collections_to_migrate = include_collection_set.difference(exclude_collection_set)

            migrate_shard_key = section.get("migrate_shard_key", "false").lower() == "true"
            drop_if_exists = section.get("drop_if_exists", "false").lower() == "true"
            optimize_compound_indexes = section.get("optimize_compound_indexes", "false").lower() == "true"
            co_locate_with = section.get("co_locate_with")

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
                # Include all collections in all databases
                for db_name in self.mongo_client.list_database_names():
                    source_db = self.mongo_client[db_name]
                    for collection_name in source_db.list_collection_names():
                        collection_set.add(f"{db_name}.{collection_name}")
            elif ".*" in collection:
                # Include all collections in a specific database
                db_name = collection.split(".*")[0]
                source_db = self.mongo_client[db_name]
                for collection_name in source_db.list_collection_names():
                    collection_set.add(f"{db_name}.{collection_name}")
            else:
                # Include specific collections
                db_name, collection_name = collection.split(".", 1)
                source_db = self.mongo_client[db_name]
                if collection_name in source_db.list_collection_names():
                    collection_set.add(f"{db_name}.{collection_name}")
        return collection_set
