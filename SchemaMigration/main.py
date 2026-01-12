import argparse
import json
from pymongo import MongoClient
from json_parser import JsonParser
from schema_migration import SchemaMigration

if __name__ == "__main__":
    # Take user input for URIs and configuration JSON file
    parser = argparse.ArgumentParser(description="MongoDB Schema Transformer")
    parser.add_argument("--source-uri", required=True, help="Source MongoDB URI")
    parser.add_argument("--dest-uri", required=True, help="Destination MongoDB URI")
    parser.add_argument("--config-file", required=True, help="Path to the configuration JSON file")
    args = parser.parse_args()

    source_uri = args.source_uri
    dest_uri = args.dest_uri
    config_file_path = args.config_file

    # Connect to the source and destination MongoDB instances
    source_client = MongoClient(source_uri)
    dest_client = MongoClient(dest_uri)

    # Load the configuration from the JSON file
    with open(config_file_path, 'r', encoding='utf-8') as config_file:
        json_config = json.load(config_file)

    # Parse the configuration into CollectionConfig objects
    parsed_collection_configs = JsonParser(json_config, source_client).parse_json()

    # Perform schema migration
    schema_migration = SchemaMigration()
    schema_migration.migrate_schema(source_client, dest_client, parsed_collection_configs)
