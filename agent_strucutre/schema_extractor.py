from pymongo import MongoClient
from bson import ObjectId
from typing import Dict, List, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_tenant_schema(mongo_uri: str, db_name: str, tenant_id: str) -> Dict[str, Any]:
    """
    Extract complete schema information for a tenant including:
    - Categories with their values
    - Field mappings for filtering
    - Collection schemas
    
    Args:
        mongo_uri: MongoDB connection string
        db_name: Database name
        tenant_id: Tenant ID to extract schema for
    
    Returns:
        Dictionary containing categories, field_mappings, and collection_schemas
    """
    
    client = MongoClient(mongo_uri)
    db = client[db_name]
    tenant_obj_id = ObjectId(tenant_id) if isinstance(tenant_id, str) else tenant_id
    
    # ===========================
    # 1. Extract Categories and Values
    # ===========================
    categories_data = {}
    
    try:
        # Get all categories for this tenant
        categories = {str(cat["_id"]): cat["name"] 
                     for cat in db.categories.find({"tenant": tenant_obj_id})}
        
        # Get category attributes mapping
        category_attrs = {}
        for attr in db.category_attributes.find({"tenant": tenant_obj_id}):
            category_id = str(attr["category"])
            category_name = categories.get(category_id)
            if category_name:
                category_attrs[str(attr["_id"])] = {
                    "category_name": category_name,
                    "attribute_name": attr["name"]
                }
        
        # Extract values from sitemaps
        for doc in db.sitemaps.find({"tenant": tenant_obj_id}):
            # Category attributes
            attr_ids = doc.get("categoryAttribute", [])
            for attr_id in attr_ids:
                attr_info = category_attrs.get(str(attr_id))
                if attr_info:
                    cat_name = attr_info["category_name"]
                    if cat_name not in categories_data:
                        categories_data[cat_name] = set()
                    categories_data[cat_name].add(attr_info["attribute_name"])
            
            # Language (geoFocus)
            geo_focus = doc.get("geoFocus")
            if geo_focus:
                if "Language" not in categories_data:
                    categories_data["Language"] = set()
                categories_data["Language"].add(geo_focus)
        
        # Content Types
        content_types = db.content_types.find({"tenant": tenant_obj_id})
        categories_data["Content Type"] = {ct["name"] for ct in content_types if ct.get("name")}
        
        # Topics
        topics = db.topics.find({"tenant": tenant_obj_id})
        categories_data["Topics"] = {topic["name"] for topic in topics if topic.get("name")}
        
        # Custom Tags
        custom_tags = db.custom_tags.find({"tenant": tenant_obj_id})
        categories_data["Custom Tags"] = {tag["name"] for tag in custom_tags if tag.get("name")}
        
        # Convert sets to sorted lists
        categories_final = {k: sorted(list(v)) for k, v in categories_data.items() if v}
        
    except Exception as e:
        logger.error(f"Error extracting categories: {e}")
        categories_final = {}
    
    # ===========================
    # 2. Field Mappings (Static)
    # ===========================
    field_mappings = {
        "Language": {
            "collection": "sitemaps",
            "field": "geoFocus",
            "requires_join": False,
            "field_type": "string"
        },
        "Content Type": {
            "collection": "sitemaps",
            "field": "contentType",
            "reference_collection": "content_types",
            "requires_join": True,
            "join_on": "_id",
            "display_field": "name"
        },
        "Topics": {
            "collection": "sitemaps",
            "field": "topic",
            "reference_collection": "topics",
            "requires_join": True,
            "join_on": "_id",
            "display_field": "name"
        },
        "Custom Tags": {
            "collection": "sitemaps",
            "field": "tag",
            "reference_collection": "custom_tags",
            "requires_join": True,
            "join_on": "_id",
            "display_field": "name",
            "is_array": True
        }
    }
    
    # Add dynamic category mappings (from categories collection)
    for category_name in categories.values():
        if category_name not in field_mappings:
            field_mappings[category_name] = {
                "collection": "sitemaps",
                "field": "categoryAttribute",
                "reference_collection": "category_attributes",
                "requires_join": True,
                "join_on": "_id",
                "display_field": "name",
                "is_array": True,
                "filter_by_category": True  # Special flag for category filtering
            }
    
    # ===========================
    # 3. Collection Schemas (Static based on your examples)
    # ===========================
    collection_schemas = {
        "sitemaps": [
            "_id", "name", "fullUrl", "path", "domain", "hideForm",
            "contentType", "topic", "tag", "categoryAttribute", "tenant",
            "isMarketingContent", "wordCount", "geoFocus", "description",
            "summary", "readerBenefit", "confidence", "explanation",
            "datePublished", "dateModified", "createdAt", "updatedAt", "__v"
        ],
        "categories": [
            "_id", "name", "tenant", "providerId", "createdAt", 
            "updatedAt", "__v", "slug", "description"
        ],
        "category_attributes": [
            "_id", "category", "tenant", "__v", "createdAt", 
            "name", "updatedAt"
        ],
        "content_types": [
            "_id", "tenant", "__v", "createdAt", "name", "updatedAt"
        ],
        "topics": [
            "_id", "tenant", "__v", "createdAt", "name", "updatedAt"
        ],
        "custom_tags": [
            "_id", "name", "tenant", "providerId", "createdAt", 
            "updatedAt", "__v"
        ]
    }
    
    # ===========================
    # 4. Get Document Counts
    # ===========================
    doc_counts = {}
    try:
        for collection_name in collection_schemas.keys():
            count = db[collection_name].count_documents({"tenant": tenant_obj_id})
            doc_counts[collection_name] = count
    except Exception as e:
        logger.error(f"Error getting document counts: {e}")
    
    client.close()
    
    # ===========================
    # Return Complete Schema
    # ===========================
    return {
        "tenant_id": str(tenant_obj_id),
        "categories": categories_final,
        "field_mappings": field_mappings,
        "collection_schemas": collection_schemas,
        "document_counts": doc_counts,
        "summary": {
            "total_categories": len(categories_final),
            "total_values": sum(len(v) for v in categories_final.values()),
            "collections": len(collection_schemas)
        }
    }


def print_schema_summary(schema: Dict[str, Any]) -> None:
    """Pretty print the schema information"""
    
    print(f"\n{'='*60}")
    print(f"📊 TENANT SCHEMA SUMMARY")
    print(f"{'='*60}")
    print(f"Tenant ID: {schema['tenant_id']}")
    print(f"Total Categories: {schema['summary']['total_categories']}")
    print(f"Total Values: {schema['summary']['total_values']}")
    
    print(f"\n📂 CATEGORIES AND VALUES:")
    print(f"{'-'*40}")
    for category, values in schema['categories'].items():
        print(f"\n{category} ({len(values)} values):")
        # Show first 5 values as sample
        sample = values[:5]
        if len(values) > 5:
            print(f"  {sample} ... and {len(values)-5} more")
        else:
            print(f"  {values}")
    
    print(f"\n🔗 FIELD MAPPINGS:")
    print(f"{'-'*40}")
    for category, mapping in schema['field_mappings'].items():
        join_info = "with join" if mapping.get('requires_join') else "direct"
        print(f"{category}: {mapping['collection']}.{mapping['field']} ({join_info})")
    
    print(f"\n📋 COLLECTION DOCUMENT COUNTS:")
    print(f"{'-'*40}")
    for collection, count in schema.get('document_counts', {}).items():
        print(f"{collection}: {count} documents")
    
    print(f"\n{'='*60}\n")


# ===========================
# Example Usage
# ===========================
if __name__ == "__main__":
    # Configuration
    MONGO_URI = "mongodb://localhost:27017"
    DB_NAME = "my_database"
    TENANT_ID = "6875f3afc8337606d54a7f37"
    
    # Get schema
    schema = get_tenant_schema(MONGO_URI, DB_NAME, TENANT_ID)
    
    # Print summary
    print_schema_summary(schema)
    
    # # Access specific information
    # print("\n💡 EXAMPLE USAGE:")
    # print(f"Getting 'Buyer Journey' values: {schema['categories'].get('Buyer Journey', [])}")
    # print(f"Getting field mapping for 'Language': {schema['field_mappings'].get('Language')}")
    # print(f"Getting sitemaps fields: {schema['collection_schemas']['sitemaps']}")