from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass
class FieldMapping:
    category_name: str                 # e.g. "Industry"
    source_collection: str             # e.g. "sitemaps"
    field_path: str                    # e.g. "categoryAttribute"
    requires_join: bool                # whether we need to resolve ObjectId → name
    reference_collection: Optional[str]  # which collection to join to
    join_config: Optional[Dict[str, Any]]  # join details if needed
    is_array: bool = False             # ✅ NEW: true if field is a list of references


@dataclass
class CollectionInfo:
    name: str
    id_field: str
    name_field: str
    tenant_field: str


@dataclass
class TenantSchema:
    tenant_id: str
    categories: Dict[str, list]                   # category_name → list of values
    field_mappings: Dict[str, FieldMapping]       # field_name → FieldMapping
    collections_info: Dict[str, CollectionInfo]   # collection_name → CollectionInfo
