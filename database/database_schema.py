from pymongo import MongoClient
from bson import ObjectId
from typing import Dict, List, Optional, Any
import logging
from dataclasses import dataclass
from database.category_extracter import extract_categorical_fields

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "my_database"
TENANT_ID = ObjectId("6875f3afc8337606d54a7f37")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class FieldMapping:
    """Database field mapping information"""
    category_name: str
    source_collection: str
    field_path: str
    requires_join: bool
    reference_collection: Optional[str] = None
    join_config: Optional[Dict[str, Any]] = None

@dataclass
class TenantSchema:
    """Complete tenant schema information"""
    tenant_id: str
    categories: Dict[str, List[str]]  # from category_extractor
    field_mappings: Dict[str, FieldMapping]  # discovered mappings

class SimplifiedSchemaExtractor:
    """
    Simplified schema extractor that uses existing category_extractor
    and only focuses on discovering field mappings
    """
    
    def __init__(self, mongo_uri: str = MONGO_URI, db_name: str = DB_NAME):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
    
    def extract_tenant_schema(self, tenant_id: str = str(TENANT_ID)) -> TenantSchema:
        """
        Extract schema using existing category_extractor + field mapping discovery
        """
        try:
            tenant_object_id = ObjectId(tenant_id)
            logger.info(f"Extracting schema for tenant: {tenant_id}")
            
            # Step 1: Use existing category extractor
            categories = extract_categorical_fields()
            
            # Step 2: Discover field mappings for these categories
            field_mappings = self._discover_field_mappings(tenant_object_id, categories)
            
            schema = TenantSchema(
                tenant_id=tenant_id,
                categories=categories,
                field_mappings=field_mappings
            )
            
            logger.info(f"Successfully extracted schema for tenant {tenant_id}")
            return schema
            
        except Exception as e:
            logger.error(f"Error extracting schema for tenant {tenant_id}: {str(e)}")
            raise
    
    def _discover_field_mappings(self, tenant_id: ObjectId, categories: Dict[str, List[str]]) -> Dict[str, FieldMapping]:
        """
        Discover field mappings based on known patterns and category types
        """
        field_mappings = {}
        
        # Get category ID mappings for join configs
        category_name_to_id = self._get_category_name_to_id_mapping(tenant_id)
        
        for category_name in categories.keys():
            mapping = self._create_field_mapping(category_name, category_name_to_id.get(category_name))
            if mapping:
                field_mappings[category_name] = mapping
        
        return field_mappings
    
    def _get_category_name_to_id_mapping(self, tenant_id: ObjectId) -> Dict[str, ObjectId]:
        """Get mapping from category names to their ObjectIds"""
        try:
            categories = self.db.categories.find({"tenant": tenant_id})
            return {cat["name"]: cat["_id"] for cat in categories}
        except Exception as e:
            logger.error(f"Error getting category mappings: {str(e)}")
            return {}
    
    def _create_field_mapping(self, category_name: str, category_id: Optional[ObjectId]) -> Optional[FieldMapping]:
        """
        Create field mapping based on category name and known patterns
        """
        # Direct field mappings (no joins required)
        direct_mappings = {
            "Language": FieldMapping(
                category_name="Language",
                source_collection="sitemaps",
                field_path="geoFocus",
                requires_join=False
            )
        }
        
        if category_name in direct_mappings:
            return direct_mappings[category_name]
        
        # Reference collection mappings (require joins)
        reference_mappings = {
            "Content Type": ("content_types", "contentType"),
            "Custom Tags": ("custom_tags", "tag"),
            "Topics": ("topics", "topic")
        }
        
        if category_name in reference_mappings:
            ref_collection, field_path = reference_mappings[category_name]
            return FieldMapping(
                category_name=category_name,
                source_collection="sitemaps",
                field_path=field_path,
                requires_join=True,
                reference_collection=ref_collection,
                join_config={
                    "from": ref_collection,
                    "local_field": field_path,
                    "foreign_field": "_id"
                }
            )
        
        # Category attribute mappings (most complex joins)
        if category_id:
            return FieldMapping(
                category_name=category_name,
                source_collection="sitemaps",
                field_path="categoryAttribute",
                requires_join=True,
                reference_collection="category_attributes",
                join_config={
                    "from": "category_attributes",
                    "local_field": "categoryAttribute",
                    "foreign_field": "_id",
                    "filter_field": "category",
                    "filter_value": category_id
                }
            )
        
        logger.warning(f"Could not create mapping for category: {category_name}")
        return None
    
    def get_database_field_mapping(self, tenant_id: str = str(TENANT_ID)) -> Dict[str, Dict[str, Any]]:
        """
        Get field mapping in the format expected by your query parser
        """
        try:
            schema = self.extract_tenant_schema(tenant_id)
            
            # Convert to original format for backwards compatibility
            field_mapping = {}
            for category_name, mapping in schema.field_mappings.items():
                field_mapping[category_name] = {
                    "collection": mapping.reference_collection if mapping.requires_join else mapping.source_collection,
                    "field_path": mapping.field_path,
                    "lookup_field": "name" if mapping.requires_join else None,
                    "requires_join": mapping.requires_join
                }
            
            return field_mapping
            
        except Exception as e:
            logger.error(f"Error getting field mapping: {str(e)}")
            return {}
    
    def close(self):
        """Close database connection"""
        self.client.close()


# Example usage and integration functions
def get_tenant_categories_and_mappings(tenant_id: str = str(TENANT_ID)) -> tuple[Dict[str, List[str]], Dict[str, Dict[str, Any]]]:
    """
    Convenience function to get both categories and field mappings
    Returns: (categories_dict, field_mappings_dict)
    """
    extractor = SimplifiedSchemaExtractor()
    try:
        schema = extractor.extract_tenant_schema(tenant_id)
        field_mappings = extractor.get_database_field_mapping(tenant_id)
        return schema.categories, field_mappings
    finally:
        extractor.close()


def get_dynamic_tenant_categories(tenant_id: str = str(TENANT_ID)) -> Dict[str, List[str]]:
    """
    Drop-in replacement for extract_categorical_fields() with tenant support
    """
    # For now, just use the existing function, but this can be extended
    return extract_categorical_fields()


def get_dynamic_field_mappings(tenant_id: str = str(TENANT_ID)) -> Dict[str, Dict[str, Any]]:
    """
    Dynamic field mappings to replace hardcoded get_database_field_mapping()
    """
    extractor = SimplifiedSchemaExtractor()
    try:
        return extractor.get_database_field_mapping(tenant_id)
    finally:
        extractor.close()


# Example usage
if __name__ == "__main__":
    extractor = SimplifiedSchemaExtractor()
    
    try:
        # Extract complete schema
        schema = extractor.extract_tenant_schema()
        
        print(f"Categories discovered: {len(schema.categories)}")
        for cat_name, values in schema.categories.items():
            print(f"  {cat_name}: {values} values")
        
        print(f"\nField mappings discovered: {len(schema.field_mappings)}")
        for cat_name, mapping in schema.field_mappings.items():
            join_info = "with join" if mapping.requires_join else "direct"
            print(f"  {cat_name}: {mapping.source_collection}.{mapping.field_path} ({join_info})")
        
        # Test convenience functions
        print("\n" + "="*50)
        print("Testing convenience functions:")
        
        categories, mappings = get_tenant_categories_and_mappings()
        print(f"Categories via convenience function: {len(categories)}")
        print(f"Mappings via convenience function: {len(mappings)}")
        
    finally:
        extractor.close()