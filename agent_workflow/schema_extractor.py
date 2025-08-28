from pymongo import MongoClient
from bson import ObjectId
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import logging
"""
extract_categorical_fields & _get_collection_names --> to get categories 
get_sample_documents --> to get samples for llm context(does have problems only gets categories not the sample we want)
get_collection_counts --> get the counts for each collection(tenant wise)

to do : 
- samples related issue (we don't need samples)
- provide field mapping as well (right now only collection info)
"""

@dataclass
class CollectionInfo:
    name: str
    id_field: str
    name_field: str
    tenant_field: str

@dataclass
class TenantSchema:
    tenant_id: str
    collections: Dict[str, CollectionInfo]
    categories: Dict[str, List[str]]
    sample_documents: Dict[str, List[Dict]]
    total_documents: Dict[str, int]

class TenantSchemaUtil:
    def __init__(self, mongo_client: MongoClient, database_name: str):
        self.client = mongo_client
        self.db = self.client[database_name]
        
        # Fixed collection structure
        self.collections_info = {
            "categories": CollectionInfo(name="categories", id_field="_id", name_field="name", tenant_field="tenant"),
            "category_attributes": CollectionInfo(name="category_attributes", id_field="_id", name_field="name", tenant_field="tenant"),
            "content_types": CollectionInfo(name="content_types", id_field="_id", name_field="name", tenant_field="tenant"),
            "topics": CollectionInfo(name="topics", id_field="_id", name_field="name", tenant_field="tenant"),
            "custom_tags": CollectionInfo(name="custom_tags", id_field="_id", name_field="name", tenant_field="tenant"),
            "sitemaps": CollectionInfo(name="sitemaps", id_field="_id", name_field="name", tenant_field="tenant"),
        }
    
    def validate_tenant(self, tenant_id: str) -> bool:
        """Check if tenant exists in any collection"""
        try:
            tenant_obj_id = ObjectId(tenant_id)
            # Check in categories collection (most likely to have data)
            exists = self.db.sitemaps.find_one({"tenant": tenant_obj_id}) is not None
            return exists
        except Exception as e:
            logging.error(f"Error validating tenant {tenant_id}: {e}")
            return False
    
    def extract_categorical_fields(self, tenant_id: str) -> Dict[str, List[str]]:
        """Extract all categorical values for the tenant"""
        try:
            tenant_obj_id = ObjectId(tenant_id)
            categories = {}
            
            # Get all category names first
            category_docs = list(self.db.categories.find({"tenant": tenant_obj_id}))
            
            for category_doc in category_docs:
                category_name = category_doc.get("name", "Unknown") #if name exists then get name otherwise unknown instead of throwing error 
                category_id = category_doc["_id"]
                
                # Get attributes for this category
                attributes = list(self.db.category_attributes.find({
                    "tenant": tenant_obj_id,
                    "category": category_id
                }))
                
                # Extract unique attribute names
                attribute_names = [attr.get("name", "") for attr in attributes if attr.get("name")]
                unique_attributes = list(set(attribute_names))
                
                if unique_attributes:
                    categories[category_name] = unique_attributes
            
            # Also extract other categorical data
            categories["Topics"] = self._get_collection_names(tenant_obj_id, "topics")
            categories["Content Types"] = self._get_collection_names(tenant_obj_id, "content_types")
            categories["Custom Tags"] = self._get_collection_names(tenant_obj_id, "custom_tags")
            
            return categories # categories would be in list format 
            
        except Exception as e:
            logging.error(f"Error extracting categories for tenant {tenant_id}: {e}")
            return {}
    
    def _get_collection_names(self, tenant_obj_id: ObjectId, collection_name: str) -> List[str]:
        """Helper to get all names from a collection for a tenant"""
        try:
            docs = list(self.db[collection_name].find({"tenant": tenant_obj_id}))
            names = [doc.get("name", "") for doc in docs if doc.get("name")]
            return list(set(names))  # Remove duplicates
        except Exception as e:
            logging.error(f"Error getting names from {collection_name}: {e}")
            return []
    
    def get_sample_documents(self, tenant_id: str, sample_size: int = 3) -> Dict[str, List[Dict]]:
        """Get sample documents from each collection for the tenant
        Things to do : doesn't work properly have to think of logic that fetches data from all collections  """
        try:
            tenant_obj_id = ObjectId(tenant_id)
            samples = {}
            
            for collection_name in self.collections_info.items():
                # Get sample documents
                docs = list(self.db[collection_name].find(
                    {"tenant": tenant_obj_id}
                ).limit(sample_size))
                
                # Clean documents for readability
                cleaned_docs = []
                for doc in docs:
                    # Convert ObjectIds to strings for JSON serialization
                    cleaned_doc = self._clean_document(doc)
                    cleaned_docs.append(cleaned_doc)
                
                samples[collection_name] = cleaned_docs
            
            return samples
            
        except Exception as e:
            logging.error(f"Error getting sample documents for tenant {tenant_id}: {e}")
            return {}
    
    def get_collection_counts(self, tenant_id: str) -> Dict[str, int]:
        """Get document counts for each collection for the tenant"""
        try:
            tenant_obj_id = ObjectId(tenant_id)
            counts = {}
            
            for collection_name in self.collections_info.keys():
                count = self.db[collection_name].count_documents({"tenant": tenant_obj_id})
                counts[collection_name] = count
            
            return counts
            
        except Exception as e:
            logging.error(f"Error getting collection counts for tenant {tenant_id}: {e}")
            return {}
    
    def _clean_document(self, doc: Dict) -> Dict:
        """Clean document for better readability, convert ObjectIds to strings"""
        cleaned = {}
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                cleaned[key] = str(value)
            elif isinstance(value, list):
                cleaned[key] = [str(item) if isinstance(item, ObjectId) else item for item in value]
            else:
                cleaned[key] = value
        return cleaned
    
    def get_tenant_schema(self, tenant_id: str, include_samples: bool = True) -> Optional[TenantSchema]:
        """Main function to get complete tenant schema information"""
        try:
            # Validate tenant exists
            if not self.validate_tenant(tenant_id):
                logging.warning(f"Tenant {tenant_id} not found")
                return None
            
            # Extract categorical data
            categories = self.extract_categorical_fields(tenant_id)
            
            # Get sample documents if requested
            samples = self.get_sample_documents(tenant_id) if include_samples else {}

            
            # Get document counts
            counts = self.get_collection_counts(tenant_id)
            
            # Create schema object
            schema = TenantSchema(
                tenant_id=tenant_id,
                collections=self.collections_info,
                categories=categories,
                sample_documents=samples,
                total_documents=counts
            )
            
            return schema
            
        except Exception as e:
            logging.error(f"Error getting tenant schema for {tenant_id}: {e}")
            return None
    
    def format_schema_for_llm(self, schema: TenantSchema) -> str:
        """Format schema information for LLM consumption"""
        if not schema:
            return "No schema data available"
        
        output = f"📊 **Tenant Schema Summary** (ID: {schema.tenant_id})\n\n"
        
        # Collections overview
        output += "📂 **Collections Structure:**\n"
        for name, info in schema.collections.items():
            count = schema.total_documents.get(name, 0)
            output += f"- {name}: {count} documents (ID: {info.id_field}, Name: {info.name_field})\n"
        
        # Categories breakdown
        output += "\n📋 **Available Categories & Values:**\n"
        for category, values in schema.categories.items():
            if values:
                output += f"- {category} ({len(values)} unique): {values}\n"
        
        # Sample data structure (just schema, not full documents)
        if schema.sample_documents:
            output += "\n🔍 **Sample Data Fields:**\n"
            for collection_name, docs in schema.sample_documents.items():
                if docs:
                    fields = list(docs[0].keys()) if docs else []
                    output += f"- {collection_name}: {fields}\n"
        
        return output

# Usage example and helper functions
def create_schema_util(connection_string: str, database_name: str) -> TenantSchemaUtil:
    """Factory function to create schema utility"""
    client = MongoClient(connection_string)
    return TenantSchemaUtil(client, database_name)


if __name__ == "__main__":
    schema_util = create_schema_util("mongodb://localhost:27017", "my_database")
    categories = schema_util.extract_categorical_fields(tenant_id="6875f3afc8337606d54a7f37")
    for key, value in categories.items():
        print(f"{key}: {value}")
