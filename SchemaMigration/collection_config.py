from dataclasses import dataclass
from typing import Optional

@dataclass
class CollectionConfig:
    """
    Configuration for a MongoDB collection, including database name, 
    collection name, and various options for sharding and optimization.
    """
    db_name: str
    collection_name: str
    migrate_shard_key: bool
    drop_if_exists: bool
    optimize_compound_indexes: bool = False
    co_locate_with: Optional[str] = None
