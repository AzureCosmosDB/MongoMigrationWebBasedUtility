import argparse
import json
from pymongo import MongoClient
from json_parser import JsonParser
from schema_migration import SchemaMigration
from console_utils import print_verbose, print_success, print_error

if __name__ == "__main__":
    # Take user input for URIs and configuration JSON file
    parser = argparse.ArgumentParser(description="MongoDB Schema Transformer")
    parser.add_argument("--source-uri", required=True, help="Source MongoDB URI")
    parser.add_argument("--dest-uri", required=True, help="Destination MongoDB URI")
    parser.add_argument("--config-file", required=True, help="Path to the configuration JSON file")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output for detailed flow")
    args = parser.parse_args()

    source_uri = args.source_uri
    dest_uri = args.dest_uri
    config_file_path = args.config_file
    verbose = args.verbose

    print_verbose(verbose, "Starting MongoDB Schema Migration Tool")
    print_verbose(verbose, f"Source URI: {source_uri}")
    print_verbose(verbose, f"Destination URI: {dest_uri}")
    print_verbose(verbose, f"Configuration file: {config_file_path}")

    # Connect to the source and destination MongoDB instances
    print_verbose(verbose, "Connecting to source MongoDB instance...")
    source_client = MongoClient(source_uri)
    print_verbose(verbose, "Successfully connected to source")
    print_verbose(verbose, "Connecting to destination MongoDB instance...")
    dest_client = MongoClient(dest_uri)
    print_verbose(verbose, "Successfully connected to destination")

    # Load the configuration from the JSON file
    print_verbose(verbose, f"Loading configuration from {config_file_path}...")
    with open(config_file_path, 'r', encoding='utf-8') as config_file:
        json_config = json.load(config_file)
    print_verbose(verbose, f"Configuration loaded successfully")
    print_verbose(verbose, f"Number of sections in config: {len(json_config.get('sections', []))}")

    # Parse the configuration into CollectionConfig objects
    print_verbose(verbose, "Parsing configuration into CollectionConfig objects...")
    parsed_collection_configs = JsonParser(json_config, source_client, verbose).parse_json()
    print_verbose(verbose, f"Parsed {len(parsed_collection_configs)} collection(s) to migrate")

    # Perform schema migration
    print_verbose(verbose, "Starting schema migration process...")
    schema_migration = SchemaMigration(verbose)
    schema_migration.migrate_schema(source_client, dest_client, parsed_collection_configs)
    
    print_verbose(verbose, "Schema migration completed successfully")
