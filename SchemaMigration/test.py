from dataclasses import dataclass
from typing import List
import json
import unittest
import random
import string
from pymongo import MongoClient
from json_parser import JsonParser
from schema_migration import SchemaMigration

SOURCE_MONGODB_URI: str = ""
DEST_DOCUMENTDB_URI: str = ""

@dataclass
class CollectionConfigSection:
    include: List[str]
    exclude: List[str]
    migrate_shard_key: bool
    drop_if_exists: bool
    optimize_compound_indexes: bool

class TestSchemaMigration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Set up test fixtures for the TestSchemaMigration class.
        """
        cls.source_client = MongoClient(SOURCE_MONGODB_URI)
        cls.dest_client = MongoClient(DEST_DOCUMENTDB_URI)
        cls.db_name = cls._generate_random_string(5)

    @classmethod
    def tearDownClass(cls):
        """
        Clean up test fixtures for the TestSchemaMigration class.
        """
        cls.source_client.close()
        cls.dest_client.close()

    def tearDown(self):
        """
        Clean up resources after each test.
        """
        # Drop the test database
        self.source_client.drop_database(self.db_name)
        self.dest_client.drop_database(self.db_name)

    def test_optimize_compound_indexes_true(self):
        """
        Test that the migrate_schema method optimizes compound indexes.
        """
        # Create the source collection and index information
        source_collection = self.source_client[self.db_name]["test_optimize"]
        source_collection.create_index([("a", 1), ("b", 1), ("c", 1), ("d", 1)])
        source_collection.create_index([("a", 1), ("b", 1), ("c", 1)])
        source_collection.create_index([("b", 1), ("c", 1)])
        source_collection.create_index([("c", 1), ("d", 1)])
        source_collection.create_index([("b", 1), ("d", 1)])
        source_collection.create_index([("d", 1)])

        collection_config_sections = []
        collection_config_sections.append(CollectionConfigSection([f'{self.db_name}.*'], [], False, False, True))
        migrate_all_config = json.loads(self._generate_config(collection_config_sections))
        collection_configs = JsonParser(migrate_all_config, self.source_client).parse_json()

        # Create a SchemaMigration instance and call migrate_schema
        schema_migration = SchemaMigration()
        schema_migration.migrate_schema(self.source_client, self.dest_client, collection_configs)

        # Verify that the indexes were optimized in the destination collection
        dest_collection = self.dest_client[self.db_name]["test_optimize"]
        dest_indexes = dest_collection.index_information()
        self.assertIn("a_1_b_1_c_1_d_1", dest_indexes, "Compound index on 'a', 'b', 'c', 'd' was not migrated successfully.")
        self.assertIn("b_1_d_1", dest_indexes, "Compound index on 'b' and 'd' was not migrated successfully.")
        self.assertIn("d_1", dest_indexes, "Compound index on 'd' was not migrated successfully.")
        self.assertNotIn("a_1_b_1_c_1", dest_indexes, "Compound index on 'a', 'b', 'c' was not optimized successfully.")
        self.assertNotIn("b_1_c_1", dest_indexes, "Compound index on 'b' and 'c' was not optimized successfully.")
        self.assertNotIn("c_1_d_1", dest_indexes, "Compound index on 'c' and 'd' was not optimized successfully.")

    def test_optimize_compound_indexes_false(self):
        """
        Test that the migrate_schema method doesn't optimizes compound indexes.
        """
        # Create the source collection and index information
        source_collection = self.source_client[self.db_name]["test_optimize"]
        source_collection.create_index([("a", 1), ("b", 1), ("c", 1), ("d", 1)])
        source_collection.create_index([("a", 1), ("b", 1), ("c", 1)])
        source_collection.create_index([("b", 1), ("c", 1)])
        source_collection.create_index([("c", 1), ("d", 1)])
        source_collection.create_index([("b", 1), ("d", 1)])
        source_collection.create_index([("d", 1)])

        collection_config_sections = []
        collection_config_sections.append(CollectionConfigSection([f'{self.db_name}.*'], [], False, False, False))
        migrate_all_config = json.loads(self._generate_config(collection_config_sections))
        collection_configs = JsonParser(migrate_all_config, self.source_client).parse_json()

        # Create a SchemaMigration instance and call migrate_schema
        schema_migration = SchemaMigration()
        schema_migration.migrate_schema(self.source_client, self.dest_client, collection_configs)

        # Verify that the indexes were not optimized in the destination collection
        dest_collection = self.dest_client[self.db_name]["test_optimize"]
        dest_indexes = dest_collection.index_information()
        self.assertIn("a_1_b_1_c_1_d_1", dest_indexes, "Compound index on 'a', 'b', 'c', 'd' was not migrated successfully.")
        self.assertIn("a_1_b_1_c_1", dest_indexes, "Compound index on 'a', 'b', 'c' was not migrated successfully.")
        self.assertIn("b_1_c_1", dest_indexes, "Compound index on 'b' and 'c' was not migrated successfully.")
        self.assertIn("c_1_d_1", dest_indexes, "Compound index on 'c' and 'd' was not migrated successfully.")
        self.assertIn("b_1_d_1", dest_indexes, "Compound index on 'b' and 'd' was not migrated successfully.")
        self.assertIn("d_1", dest_indexes, "Compound index on 'd' was not migrated successfully.")

    def test_optimize_compound_indexes_filters_index_with_options(self):
        """
        Test that the migrate_schema method doesn't optimizes compound indexes when it has options.
        """
        # Create the source collection and index information
        source_collection = self.source_client[self.db_name]['test_optimize']
        source_collection.create_index([('a', 1), ('b', 1), ('c', 1)], name='a_1_b_1_c_1', unique=True)
        source_collection.create_index([('a', 1), ('b', 1), ('c', 1), ('d', 1)])

        collection_config_sections = []
        collection_config_sections.append(CollectionConfigSection([f'{self.db_name}.*'], [], False, False, True))
        migrate_all_config = json.loads(self._generate_config(collection_config_sections))
        collection_configs = JsonParser(migrate_all_config, self.source_client).parse_json()

        # Create a SchemaMigration instance and call migrate_schema
        schema_migration = SchemaMigration()
        schema_migration.migrate_schema(self.source_client, self.dest_client, collection_configs)

        # Verify that the indexes were created in the destination collection
        dest_collection = self.dest_client[self.db_name]["test_optimize"]
        dest_indexes = dest_collection.index_information()
        self.assertIn("a_1_b_1_c_1_d_1", dest_indexes, "Compound index on 'a', 'b', 'c', 'd' was not migrated successfully.")
        self.assertIn("a_1_b_1_c_1", dest_indexes, "Compound index on 'a', 'b', 'c' was not migrated successfully.")
        self.assertEqual(dest_indexes["a_1_b_1_c_1"]["unique"], True, "Unique option is not set.")

    def test_optimize_compound_indexes_filters_index_with_other_indexes(self):
        """
        Test that the migrate_schema method doesn't optimizes compound indexes when it has options.
        """
        # Create the source collection and index information
        source_collection = self.source_client[self.db_name]['test_optimize']
        source_collection.create_index([('a', 1), ('b', 1), ('c', 1)], name='a_1_b_1_c_1', unique=True)
        source_collection.create_index(
            [('b', 1), ('c', 1)], 
            name='b_1_c_1', 
            unique=True, 
            partialFilterExpression={"a": {"$gt": 0}}
        )
        source_collection.create_index([("a", 1), ("b", 1), ("c", 1), ("d", 1)])
        source_collection.create_index([("a", 1), ("b", 1)])
        source_collection.create_index([("d", 1)], expireAfterSeconds=10)

        collection_config_sections = []
        collection_config_sections.append(CollectionConfigSection([f'{self.db_name}.*'], [], False, False, True))
        migrate_all_config = json.loads(self._generate_config(collection_config_sections))
        collection_configs = JsonParser(migrate_all_config, self.source_client).parse_json()

        # Create a SchemaMigration instance and call migrate_schema
        schema_migration = SchemaMigration()
        schema_migration.migrate_schema(self.source_client, self.dest_client, collection_configs)

        # Verify that the indexes in destination
        dest_collection = self.dest_client[self.db_name]["test_optimize"]
        dest_indexes = dest_collection.index_information()
        self.assertIn("a_1_b_1_c_1_d_1", dest_indexes, "Compound index on 'a', 'b', 'c', 'd' was not migrated successfully.")
        self.assertIn("a_1_b_1_c_1", dest_indexes, "Compound index on 'a', 'b', 'c' was not migrated successfully.")
        self.assertEqual(dest_indexes["a_1_b_1_c_1"]["unique"], True, "Unique option is not set.")
        self.assertIn("b_1_c_1", dest_indexes, "Compound index on 'b' and 'c' was not migrated successfully.")
        self.assertTrue("partialFilterExpression" in dest_indexes["b_1_c_1"], "Partial filter expression is not set.")
        self.assertIn("d_1", dest_indexes, "Compound index on 'd' was not migrated successfully.")
        self.assertEqual(dest_indexes["d_1"]["expireAfterSeconds"], 10, "TTL index on 'd' field has incorrect expireAfterSeconds.")

    def test_ts_ttl_throws_error(self):
        """
        Test that the migrate_schema method throws an error when a TTL index is created on a _ts field.
        """
        # Create the source collection and _ts ttl index information
        source_collection = self.source_client[self.db_name]["test_ttl"]
        source_collection.create_index([("_ts", 1)], expireAfterSeconds=10)

        collection_config_sections = []
        collection_config_sections.append(CollectionConfigSection([f'{self.db_name}.*'], [], False, False, False))
        migrate_all_config = json.loads(self._generate_config(collection_config_sections))
        collection_configs = JsonParser(migrate_all_config, self.source_client).parse_json()

        # Create a SchemaMigration instance and call migrate_schema
        schema_migration = SchemaMigration()
        with self.assertRaises(ValueError):
            schema_migration.migrate_schema(self.source_client, self.dest_client, collection_configs)

    def test_ttl_index_migration(self):
        """
        Test that the migrate_schema method is successfulwhen a TTL index is not on a _ts field.
        """
        # Create the source collection and ttl index information
        source_collection = self.source_client[self.db_name]["test_ttl"]
        source_collection.create_index([("abc", 1)], expireAfterSeconds=10)

        collection_config_sections = []
        collection_config_sections.append(CollectionConfigSection([f'{self.db_name}.*'], [], False, False, False))
        migrate_all_config = json.loads(self._generate_config(collection_config_sections))
        collection_configs = JsonParser(migrate_all_config, self.source_client).parse_json()

        # Create a SchemaMigration instance and call migrate_schema
        schema_migration = SchemaMigration()
        schema_migration.migrate_schema(self.source_client, self.dest_client, collection_configs)

        # Verify that the index was created in the destination collection
        dest_collection = self.dest_client[self.db_name]["test_ttl"]
        dest_indexes = dest_collection.index_information()
        self.assertIn("abc_1", dest_indexes, "TTL index on 'abc' field was not migrated successfully.")
        dest_index_info = dest_indexes["abc_1"]
        self.assertEqual(dest_index_info["expireAfterSeconds"], 10, "TTL index on 'abc' field has incorrect expireAfterSeconds.")

    def test_migrate_shard_key_set_true(self):
        """
        Test that the migrate_schema method correctly migrates the shard key.
        """
        # Create sharded source collection
        self.source_client.admin.command('enableSharding', self.db_name)
        self.source_client.admin.command(
            'shardCollection',
            f'{self.db_name}.test_shard_key',
            key={'_id': 'hashed'}
        )

        collection_config_sections = []
        collection_config_sections.append(CollectionConfigSection([f'{self.db_name}.*'], [], True, False, False))
        migrate_all_config = json.loads(self._generate_config(collection_config_sections))
        collection_configs = JsonParser(migrate_all_config, self.source_client).parse_json()

        # Create a SchemaMigration instance and call migrate_schema
        schema_migration = SchemaMigration()
        schema_migration.migrate_schema(self.source_client, self.dest_client, collection_configs)

        # Verify that the shard key was migrated correctly
        shard_key_info = self.dest_client[self.db_name].command('listCollections')['cursor']['firstBatch'][0]['info'].get('shardKey')
        self.assertIsNotNone(shard_key_info, 'Shard key was not migrated to the destination collection.')
        self.assertEqual(shard_key_info, {'_id': 'hashed'}, 'Shard key in the destination collection is incorrect.')

    def test_migrate_shard_key_set_false(self):
        """
        Test that the migrate_schema method doesn't migrate the shard key.
        """
        # Create sharded source collection
        self.source_client.admin.command('enableSharding', self.db_name)
        self.source_client.admin.command(
            'shardCollection',
            f'{self.db_name}.test_shard_key',
            key={'_id': 'hashed'}
        )

        collection_config_sections = []
        collection_config_sections.append(CollectionConfigSection([f'{self.db_name}.*'], [], False, False, False))
        migrate_all_config = json.loads(self._generate_config(collection_config_sections))
        collection_configs = JsonParser(migrate_all_config, self.source_client).parse_json()

        # Create a SchemaMigration instance and call migrate_schema
        schema_migration = SchemaMigration()
        schema_migration.migrate_schema(self.source_client, self.dest_client, collection_configs)

        # Verify that the shard key was not migrated
        shard_key_info = self.dest_client[self.db_name].command('listCollections')['cursor']['firstBatch'][0]['info'].get('shardKey')
        self.assertIsNone(shard_key_info, 'Shard key was migrated to the destination collection.')

    def test_drop_if_exists_set_true(self):
        """
        Test that the migrate_schema method drops the collection in target.
        """
        # Create the dest collection with an index
        dest_collection = self.dest_client[self.db_name]["test_drop"]
        dest_collection.create_index([("foo", 1)])

        # Create the source collection with an index
        source_collection = self.source_client[self.db_name]["test_drop"]
        source_collection.create_index([("bar", 1)])

        collection_config_sections = []
        collection_config_sections.append(CollectionConfigSection([f'{self.db_name}.*'], [], False, True, False))
        migrate_all_config = json.loads(self._generate_config(collection_config_sections))
        collection_configs = JsonParser(migrate_all_config, self.source_client).parse_json()

        # Create a SchemaMigration instance and call migrate_schema
        schema_migration = SchemaMigration()
        schema_migration.migrate_schema(self.source_client, self.dest_client, collection_configs)

        # Verify that the target has been dropped and recreated
        dest_collection = self.dest_client[self.db_name]["test_drop"]
        dest_indexes = dest_collection.index_information()
        self.assertIn("bar_1", dest_indexes, "bar_1 was not migrated successfully.")
        self.assertNotIn("foo_1", dest_indexes, "foo_1 was not dropped.")

    def test_drop_if_exists_set_false(self):
        """
        Test that the migrate_schema method drops the collection in target.
        """
        # Create the dest collection with an index
        dest_collection = self.dest_client[self.db_name]["test_drop"]
        dest_collection.create_index([("foo", 1)])

        # Create the source collection with an index
        source_collection = self.source_client[self.db_name]["test_drop"]
        source_collection.create_index([("bar", 1)])

        collection_config_sections = []
        collection_config_sections.append(CollectionConfigSection([f'{self.db_name}.*'], [], False, False, False))
        migrate_all_config = json.loads(self._generate_config(collection_config_sections))
        collection_configs = JsonParser(migrate_all_config, self.source_client).parse_json()

        # Create a SchemaMigration instance and call migrate_schema
        schema_migration = SchemaMigration()
        schema_migration.migrate_schema(self.source_client, self.dest_client, collection_configs)

        # Verify that the target has not been dropped
        dest_collection = self.dest_client[self.db_name]["test_drop"]
        dest_indexes = dest_collection.index_information()
        self.assertIn("bar_1", dest_indexes, "bar_1 was not migrated successfully.")
        self.assertIn("foo_1", dest_indexes, "foo_1 was dropped.")

    def test_verify_configurations_apply_within_sections(self):
        """
        Test that the migrate_schema method applies configurations within sections.
        """
        # Create the source collections and index information
        source_collection_1 = self.source_client[self.db_name]["test_config_1"]
        source_collection_1.create_index([("foo", 1)])
        source_collection_2 = self.source_client[self.db_name]["test_config_2"]
        source_collection_2.create_index([("foo", 1)])

        # Create the destination collection
        dest_collection_1 = self.dest_client[self.db_name]["test_config_1"]
        dest_collection_1.create_index([("bar", 1)])
        dest_collection_2 = self.dest_client[self.db_name]["test_config_2"]
        dest_collection_2.create_index([("bar", 1)])

        collection_config_sections = []
        collection_config_sections.append(CollectionConfigSection([f'{self.db_name}.test_config_1'], [], False, True, False))
        collection_config_sections.append(CollectionConfigSection([f'{self.db_name}.test_config_2'], [], False, False, False))
        migrate_all_config = json.loads(self._generate_config(collection_config_sections))
        collection_configs = JsonParser(migrate_all_config, self.source_client).parse_json()

        # Create a SchemaMigration instance and call migrate_schema
        schema_migration = SchemaMigration()
        schema_migration.migrate_schema(self.source_client, self.dest_client, collection_configs)

        # Verify that test_config_1 was dropped and test_config_2 was not
        dest_collection_1 = self.dest_client[self.db_name]["test_config_1"]
        dest_indexes_1 = dest_collection_1.index_information()
        self.assertIn("foo_1", dest_indexes_1, "foo_1 was not migrated successfully.")
        self.assertNotIn("bar_1", dest_indexes_1, "bar_1 was not dropped.")

        dest_collection_2 = self.dest_client[self.db_name]["test_config_2"]
        dest_indexes_2 = dest_collection_2.index_information()
        self.assertIn("foo_1", dest_indexes_2, "foo_1 was not migrated successfully.")
        self.assertIn("bar_1", dest_indexes_2, "bar_1 was dropped.")

    @staticmethod
    def _generate_random_string(length=10):
        """
        Generate a random string of the specified length.
        
        :param length: Length of the random string (default is 10)
        :return: Randomly generated string
        """
        characters = string.ascii_letters
        return ''.join(random.choice(characters) for _ in range(length))

    def _generate_config(
        self,
        collection_config_sections: List[CollectionConfigSection]) -> str:
            """
            Generate a configuration dictionary for testing.
            
            :param include: List of collections to include
            :param exclude: List of collections to exclude
            :param migrate_shard_key: Boolean for migrating shard key
            :param drop_if_exists: Boolean for dropping collection if it exists
            :param optimize_compound_indexes: Boolean for optimizing compound indexes
            :return: Configuration dictionary
            """
            collection_configs = {
                "sections": []
            }

            for collection_config_section in collection_config_sections:
                collection_configs["sections"].append({
                    "include": collection_config_section.include,
                    "exclude": collection_config_section.exclude,
                    "migrate_shard_key": str(collection_config_section.migrate_shard_key).lower(),
                    "drop_if_exists": str(collection_config_section.drop_if_exists).lower(),
                    "optimize_compound_indexes": str(collection_config_section.optimize_compound_indexes).lower()
                })
            return json.dumps(collection_configs)

unittest.main()
