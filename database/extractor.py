import logging
from typing import Dict
from bson import ObjectId

from connection import get_database
from models import TenantSchema, FieldMapping, CollectionInfo
from category_extracter import extract_categorical_fields

logger = logging.getLogger(__name__)


class DynamicTenantSchemaExtractor:
    """Extracts and builds tenant schema (demo: static schema)."""

    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.db = get_database()

    def extract_schema(self) -> TenantSchema:
        """
        For demo purposes, return a static schema.
        Later: query categories + category_attributes dynamically.
        """
        logger.info("Extracting schema for tenant_id=%s", self.tenant_id)

        categories = {
            "Page Type": ["Product Page", "Legal Page", "Promotional Page", "Resource Hub", "Careers Page", "Podcast Page", "Webinar"],
            "Funnel Stage": ["MOFU", "TOFU", "BOFU"],
            "Primary Audience": ["Individual Investors", "Live Nation Employees", "Job Seekers", "Collectors", "General Audience", "Women of Color", "Businesses"],
            "Industry": ["Financial Services", "Fashion", "General", "Alternative Investments", "Semiconductors", "Digital Infrastructure", "Biotech", "Telecommunications"],
            "Secondary Audience": ["Financial Advisors", "Businesses", "Collectors", "Individual Investors", "General Audience", "Freelancers", "Job Seekers", "College Students", "Women of Color"],
        }
        field_mappings: Dict[str, FieldMapping] = {
            "Page Type": FieldMapping(
                category_name="Page Type",
                source_collection="sitemaps",
                field_path="categoryAttribute",
                requires_join=True,
                reference_collection="category_attributes",
                join_config={
                    "from": "category_attributes",
                    "local_field": "categoryAttribute",
                    "foreign_field": "_id",
                    "filter_field": "category",
                    "filter_value": ObjectId("6875f3afa677f67a172c63a6"),
                },
                is_array=True,   # ✅ categoryAttribute is an array
            ),
            "Funnel Stage": FieldMapping(
                category_name="Funnel Stage",
                source_collection="sitemaps",
                field_path="categoryAttribute",
                requires_join=True,
                reference_collection="category_attributes",
                join_config={
                    "from": "category_attributes",
                    "local_field": "categoryAttribute",
                    "foreign_field": "_id",
                    "filter_field": "category",
                    "filter_value": ObjectId("6875f3afa677f67a172c63a7"),
                },
                is_array=True,   # ✅ also from categoryAttribute
            ),
            "Primary Audience": FieldMapping(
                category_name="Primary Audience",
                source_collection="sitemaps",
                field_path="categoryAttribute",
                requires_join=True,
                reference_collection="category_attributes",
                join_config={
                    "from": "category_attributes",
                    "local_field": "categoryAttribute",
                    "foreign_field": "_id",
                    "filter_field": "category",
                    "filter_value": ObjectId("6875f3afa677f67a172c63a8"),
                },
                is_array=True,
            ),
            "Industry": FieldMapping(
                category_name="Industry",
                source_collection="sitemaps",
                field_path="categoryAttribute",
                requires_join=True,
                reference_collection="category_attributes",
                join_config={
                    "from": "category_attributes",
                    "local_field": "categoryAttribute",
                    "foreign_field": "_id",
                    "filter_field": "category",
                    "filter_value": ObjectId("6875f3afa677f67a172c63aa"),
                },
                is_array=True,
            ),
            "Secondary Audience": FieldMapping(
                category_name="Secondary Audience",
                source_collection="sitemaps",
                field_path="categoryAttribute",
                requires_join=True,
                reference_collection="category_attributes",
                join_config={
                    "from": "category_attributes",
                    "local_field": "categoryAttribute",
                    "foreign_field": "_id",
                    "filter_field": "category",
                    "filter_value": ObjectId("6875f3afa677f67a172c63a9"),
                },
                is_array=True,
            ),
            "Topic": FieldMapping(
                category_name="Topic",
                source_collection="sitemaps",
                field_path="topic",
                requires_join=True,
                reference_collection="topics",
                join_config={
                    "from": "topics",
                    "local_field": "topic",
                    "foreign_field": "_id",
                },
                is_array=False,  # 👈 assuming topic is single ref (change to True if array)
            ),
            "Content Type": FieldMapping(
                category_name="Content Type",
                source_collection="sitemaps",
                field_path="contentType",
                requires_join=True,
                reference_collection="content_types",
                join_config={
                    "from": "content_types",
                    "local_field": "contentType",
                    "foreign_field": "_id",
                },
                is_array=False,
            ),
            "Language": FieldMapping(
                category_name="Language",
                source_collection="sitemaps",
                field_path="geoFocus",
                requires_join=False,
                reference_collection=None,
                join_config=None,
                is_array=False,  # 👈 scalar field
            ),
        }

        collections_info = {
            "categories": CollectionInfo(name="categories", id_field="_id", name_field="name", tenant_field="tenant"),
            "category_attributes": CollectionInfo(name="category_attributes", id_field="_id", name_field="name", tenant_field="tenant"),
            "content_types": CollectionInfo(name="content_types", id_field="_id", name_field="name", tenant_field="tenant"),
            "topics": CollectionInfo(name="topics", id_field="_id", name_field="name", tenant_field="tenant"),
            "custom_tags": CollectionInfo(name="custom_tags", id_field="_id", name_field="name", tenant_field="tenant"),
            "sitemaps": CollectionInfo(name="sitemaps", id_field="_id", name_field="name", tenant_field="tenant"),
        }

        return TenantSchema(
            tenant_id=self.tenant_id,
            categories=categories,
            field_mappings=field_mappings,
            collections_info=collections_info,
        )

if __name__ == "__main__":
    extractor = DynamicTenantSchemaExtractor
    extracted_categories = extract_categorical_fields("6875f3afc8337606d54a7f37")
    categories = extractor.extract_schema("6875f3afc8337606d54a7f37")


    print(extracted_categories)
    print("=----------------=")
    print(categories.categories)
