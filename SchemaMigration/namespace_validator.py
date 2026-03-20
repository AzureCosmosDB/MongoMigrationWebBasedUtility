"""
Namespace validation module for MongoDB schema migration.

This module provides validation utilities for database and collection names,
ensuring compatibility across MongoDB and Azure CosmosDB migration tools.
Mirrors the validation logic from C# Helper.cs for consistency.
"""

from typing import Tuple, Optional


def is_valid_database_pattern(database_name: str) -> Tuple[bool, str]:
    """
    Validate a database name pattern.
    
    Allows:
    - The wildcard '*'
    - Database names without MongoDB-illegal characters
    
    Blocks:
    - Empty/null strings
    - Whitespace
    - Characters: / \ . " $ * < > : | ? , null
    
    :param database_name: The database name to validate.
    :return: (is_valid, error_message) tuple.
    """
    error_message = ""
    
    if database_name == "*":
        return True, ""
    
    if not database_name or not database_name.strip():
        return False, "Database name cannot be empty."
    
    # Check for null characters
    if '\0' in database_name:
        return False, "Database name cannot contain null characters."
    
    # Check for whitespace
    if any(c.isspace() for c in database_name):
        return False, "Database name cannot contain whitespace."
    
    # Check for MongoDB-illegal characters
    illegal_chars = ['/', '\\', '.', '"', '$', '*', '<', '>', ':', '|', '?', ',']
    for char in illegal_chars:
        if char in database_name:
            return False, "Database name contains unsupported characters."
    
    return True, ""


def is_valid_collection_pattern(collection_name: str) -> Tuple[bool, str]:
    """
    Validate a collection name pattern.
    
    Allows:
    - The wildcard '*'
    - Collection names with special characters (quotes, angle brackets, etc.)
    
    Blocks:
    - Empty/null strings
    - Null character (system reserved)
    - Comma (CSV separator in config)
    
    :param collection_name: The collection name to validate.
    :return: (is_valid, error_message) tuple.
    """
    error_message = ""
    
    if collection_name == "*":
        return True, ""
    
    if not collection_name:
        return False, "Collection name cannot be empty."
    
    # Check for null characters (reserved by MongoDB)
    if '\0' in collection_name:
        return False, "Collection name cannot contain null characters."
    
    # Check for comma (CSV separator in configuration)
    if ',' in collection_name:
        return False, "Collection name cannot contain ','."
    
    return True, ""


def try_parse_namespace_pattern(value: str) -> Tuple[bool, Optional[str], str]:
    """
    Parse and validate a namespace pattern (database.collection).
    
    Splits on the first dot and validates both parts independently.
    
    :param value: The namespace string to parse and validate.
    :return: (is_valid, normalized_namespace, error_message) tuple.
    """
    normalized_namespace = None
    error_message = ""
    
    if not value or not value.strip():
        return False, None, "Namespace cannot be empty."
    
    # Find the first dot
    first_dot_index = value.find('.')
    if first_dot_index <= 0 or first_dot_index >= len(value) - 1:
        return False, None, "Expected exactly one database prefix before the first '.'."
    
    database_name = value[:first_dot_index]
    collection_name = value[first_dot_index + 1:]
    
    # Validate database pattern
    db_valid, db_error = is_valid_database_pattern(database_name)
    if not db_valid:
        return False, None, db_error
    
    # Validate collection pattern
    col_valid, col_error = is_valid_collection_pattern(collection_name)
    if not col_valid:
        return False, None, col_error
    
    normalized_namespace = f"{database_name}.{collection_name}"
    return True, normalized_namespace, ""


def validate_namespace_pattern_list(input_str: str, split: bool = True) -> Tuple[bool, str, str]:
    """
    Validate a comma-separated list of namespace patterns.
    
    :param input_str: Comma-separated namespace patterns or a single pattern.
    :param split: If True, split on commas; if False, treat as single pattern.
    :return: (is_valid, cleaned_namespaces, error_message) tuple.
    """
    error_message = ""
    
    if not input_str or not input_str.strip():
        return False, "", "Namespaces cannot be null or empty."
    
    # Parse items
    if not split:
        items = [input_str.strip()]
    else:
        items = input_str.split(',')
    
    # Track unique valid items (avoid duplicates)
    valid_items = []
    seen = set()
    
    for item in items:
        trimmed_item = item.strip()
        
        if not trimmed_item:
            continue
            
        is_valid, normalized, parse_error = try_parse_namespace_pattern(trimmed_item)
        
        if is_valid and normalized:
            if normalized not in seen:
                valid_items.append(normalized)
                seen.add(normalized)
        else:
            suffix = f" {parse_error}" if parse_error else ""
            error_message = (
                f"Invalid namespace format: '{trimmed_item}'. "
                f"Use 'database.collection', '*.collection', 'database.*', "
                f"or '*.*' for wildcards.{suffix}"
            )
            return False, "", error_message
    
    if not valid_items:
        return False, "", "No valid namespaces found."
    
    cleaned_namespaces = ",".join(valid_items)
    return True, cleaned_namespaces, ""
