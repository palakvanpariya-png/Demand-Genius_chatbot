# database/queries.py - Enhanced functions for ControlFlow integration

from typing import List, Dict, Optional, Any, Union
from bson import ObjectId
from database.connection import get_database
from utils.logger import get_logger, log_error
from database.extractor import DynamicTenantSchemaExtractor
import re
# from sentence_transformers import SentenceTransformer

logger = get_logger("database_queries")

# Removed semantic model dependencies for MVP - using regex search instead

# ================================
# EXISTING FUNCTIONS (ENHANCED)
# ================================


def fetch_content(tenant_id: str, limit: int = 30, skip: int = 0) -> List[Dict]:
    """Fetch basic content with all relationships resolved"""
    db = get_database()
    try:
        extractor = DynamicTenantSchemaExtractor(tenant_id)
        tenant_schema = extractor.extract_schema()

        cursor = db.sitemaps.find({"tenant": ObjectId(tenant_id)}).skip(skip).limit(limit)
        return [_clean_content_doc_enhanced(db, doc, tenant_schema) for doc in cursor]
    except Exception as e:
        log_error(e, {"operation": "fetch_content", "tenant_id": tenant_id})
        return []


def fetch_content_by_filters(tenant_id: str, filters: Dict[str, Union[str, List[str]]], 
                           limit: int = 30, skip: int = 0) -> List[Dict]:
    """Enhanced filtering with multi-value and cross-collection support"""
    db = get_database()
    try:
        extractor = DynamicTenantSchemaExtractor(tenant_id)
        tenant_schema = extractor.extract_schema()

        mongo_filters = {"tenant": ObjectId(tenant_id)}
        resolved_filters = _resolve_complex_filters_to_query(db, tenant_schema, filters)
        mongo_filters.update(resolved_filters)

        cursor = db.sitemaps.find(mongo_filters).skip(skip).limit(limit)
        return [_clean_content_doc_enhanced(db, doc, tenant_schema) for doc in cursor]
    except Exception as e:
        log_error(e, {"operation": "fetch_content_by_filters", "tenant_id": tenant_id, "filters": filters})
        return []

def fetch_content_count(tenant_id: str, filters: Optional[Dict[str, Union[str, List[str]]]] = None) -> int:
    """
    Count the number of sitemap documents for a tenant, with optional category/field filters.
    """
    db = get_database()
    try:
        extractor = DynamicTenantSchemaExtractor(tenant_id)
        tenant_schema = extractor.extract_schema()

        mongo_filters = {"tenant": ObjectId(tenant_id)}

        if filters:
            resolved_filters = _resolve_complex_filters_to_query(db, tenant_schema, filters)
            mongo_filters.update(resolved_filters)

        return db.sitemaps.count_documents(mongo_filters)
    except Exception as e:
        log_error(e, {"operation": "fetch_content_count", "tenant_id": tenant_id, "filters": filters})
        return 0

def fetch_content_with_complex_filters(tenant_id: str, category_filters: Dict[str, List[str]], 
                                     additional_filters: Dict[str, Any] = None,
                                     limit: int = 30, skip: int = 0) -> Dict[str, Any]:
    """
    Handle complex multi-category filtering like 'TOFU Product Pages in Financial Services'
    Returns both data and metadata for ControlFlow agent
    """
    db = get_database()
    try:
        extractor = DynamicTenantSchemaExtractor(tenant_id)
        tenant_schema = extractor.extract_schema()

        # Build complex MongoDB query
        mongo_filters = {"tenant": ObjectId(tenant_id)}
        
        # Handle category attribute filters (multiple categories)
        category_conditions = []
        for category_name, values in category_filters.items():
            if isinstance(values, str):
                values = [values]
            
            object_ids = _resolve_category_values_to_ids(db, tenant_schema, category_name, values)
            if object_ids:
                category_conditions.extend(object_ids)
        
        if category_conditions:
            mongo_filters["categoryAttribute"] = {"$all": category_conditions}

        # Handle additional filters (topic, content_type, etc.)
        if additional_filters:
            for field, value in additional_filters.items():
                mapping = tenant_schema.field_mappings.get(field)
                if mapping and mapping.requires_join:
                    resolved_id = _resolve_friendly_name_to_id(db, mapping.reference_collection, value, tenant_id)
                    if resolved_id:
                        mongo_filters[mapping.field_path] = resolved_id
                else:
                    mongo_filters[field] = value

        # Execute query
        cursor = db.sitemaps.find(mongo_filters).skip(skip).limit(limit)
        results = [_clean_content_doc_enhanced(db, doc, tenant_schema) for doc in cursor]
        
        # Get total count
        total_count = db.sitemaps.count_documents(mongo_filters)

        return {
            "data": results,
            "total_count": total_count,
            "query_info": {
                "filters_applied": category_filters,
                "additional_filters": additional_filters,
                "results_returned": len(results)
            }
        }
    except Exception as e:
        log_error(e, {"operation": "fetch_content_with_complex_filters", "tenant_id": tenant_id})
        return {"data": [], "total_count": 0, "query_info": {}}


def fetch_distribution_analysis(tenant_id: str, primary_field: str, secondary_field: str = None,
                               include_examples: bool = True) -> Dict[str, Any]:
    """
    Get distribution analysis for any category field(s) with examples
    Used for analytics queries like 'Show funnel distribution' or 'Industry breakdown by audience'
    """
    db = get_database()
    try:
        extractor = DynamicTenantSchemaExtractor(tenant_id)
        tenant_schema = extractor.extract_schema()

        # Build aggregation pipeline based on field type
        pipeline = [{"$match": {"tenant": ObjectId(tenant_id)}}]
        
        primary_mapping = tenant_schema.field_mappings.get(primary_field)
        if not primary_mapping:
            return {"error": f"Field {primary_field} not found in schema"}

        # Handle categoryAttribute fields with aggregation
        if primary_mapping.field_path == "categoryAttribute":
            pipeline.extend([
                {"$unwind": "$categoryAttribute"},
                {"$lookup": {
                    "from": "category_attributes",
                    "localField": "categoryAttribute",
                    "foreignField": "_id",
                    "as": "categoryData"
                }},
                {"$unwind": "$categoryData"},
                {"$match": {"categoryData.category": primary_mapping.join_config["filter_value"]}}
            ])
            
            if secondary_field:
                secondary_mapping = tenant_schema.field_mappings.get(secondary_field)
                if secondary_mapping and secondary_mapping.field_path == "categoryAttribute":
                    # Handle two-dimensional analysis
                    pipeline.extend([
                        {"$lookup": {
                            "from": "category_attributes", 
                            "localField": "categoryAttribute",
                            "foreignField": "_id",
                            "as": "secondaryData"
                        }},
                        {"$unwind": "$secondaryData"},
                        {"$match": {"secondaryData.category": secondary_mapping.join_config["filter_value"]}}
                    ])
                    
                    group_stage = {
                        "_id": {
                            "primary": "$categoryData.name",
                            "secondary": "$secondaryData.name"
                        },
                        "count": {"$sum": 1}
                    }
                else:
                    # Mixed field types
                    group_stage = {
                        "_id": {
                            "primary": "$categoryData.name",
                            "secondary": f"${secondary_mapping.field_path}"
                        },
                        "count": {"$sum": 1}
                    }
            else:
                group_stage = {
                    "_id": "$categoryData.name",
                    "count": {"$sum": 1}
                }
                
            if include_examples:
                group_stage["examples"] = {"$push": {"title": "$name", "id": "$_id"}}
        else:
            # Handle simple fields
            group_stage = {
                "_id": f"${primary_mapping.field_path}",
                "count": {"$sum": 1}
            }
            if include_examples:
                group_stage["examples"] = {"$push": {"title": "$name", "id": "$_id"}}

        pipeline.append({"$group": group_stage})
        pipeline.append({"$sort": {"count": -1}})

        results = list(db.sitemaps.aggregate(pipeline))
        
        return {
            "distribution": results,
            "total_analyzed": sum(r["count"] for r in results),
            "field_analyzed": primary_field,
            "secondary_field": secondary_field,
            "includes_examples": include_examples
        }
        
    except Exception as e:
        log_error(e, {"operation": "fetch_distribution_analysis", "tenant_id": tenant_id})
        return {"error": str(e)}


def create_search_indexes(db):
    """
    Create MongoDB text indexes for better search performance
    Call this during application initialization
    """
    try:
        # Create compound text index on searchable fields
        db.sitemaps.create_index([
            ("description", "text"),
            ("summary", "text"), 
            ("readerBenefit", "text"),
            ("name", "text")
        ], name="content_text_search")
        
        logger.info("Created text search indexes on sitemaps collection")
        
        # Create index on tags for tag search performance
        db.custom_tags.create_index([("name", "text")], name="tags_text_search")
        logger.info("Created text search index on custom_tags collection")
        
    except Exception as e:
        logger.warning(f"Failed to create search indexes: {e}")


def search_content_by_text(tenant_id: str, search_query: str, 
                          search_fields: List[str] = ["description", "summary", "readerBenefit", "name"],
                          include_category_search: bool = True,
                          include_tag_search: bool = True,
                          limit: int = 20) -> Dict[str, Any]:
    """
    Text-based search across content fields, category names, and tags using MongoDB text search + regex fallback
    """
    db = get_database()
    try:
        extractor = DynamicTenantSchemaExtractor(tenant_id)
        tenant_schema = extractor.extract_schema()
        
        results = []
        
        # 1. Primary text search using MongoDB text index (faster)
        try:
            text_search_pipeline = [
                {"$match": {"tenant": ObjectId(tenant_id)}},
                {"$match": {"$text": {"$search": search_query}}},
                {"$addFields": {"textScore": {"$meta": "textScore"}}}
            ]
            
            text_results = list(db.sitemaps.aggregate(text_search_pipeline))
            
            for doc in text_results:
                cleaned_doc = _clean_content_doc_enhanced(db, doc, tenant_schema)
                cleaned_doc["relevance_score"] = doc.get("textScore", 0) * 10  # Scale text score
                cleaned_doc["match_reason"] = "content_text_indexed"
                results.append(cleaned_doc)
                
        except Exception as text_search_error:
            logger.warning(f"Text index search failed, falling back to regex: {text_search_error}")
            
            # Fallback to regex search if text index not available
            text_search_conditions = []
            for field in search_fields:
                text_search_conditions.append({field: {"$regex": search_query, "$options": "i"}})
            
            if text_search_conditions:
                text_search_pipeline = [
                    {"$match": {"tenant": ObjectId(tenant_id)}},
                    {"$match": {"$or": text_search_conditions}}
                ]
                
                text_results = list(db.sitemaps.aggregate(text_search_pipeline))
                
                for doc in text_results:
                    cleaned_doc = _clean_content_doc_enhanced(db, doc, tenant_schema)
                    
                    # Calculate relevance score based on term frequency
                    relevance_score = 0
                    search_terms = search_query.lower().split()
                    
                    for field in search_fields:
                        if field in doc and doc[field]:
                            field_text = doc[field].lower()
                            for term in search_terms:
                                if term in field_text:
                                    relevance_score += field_text.count(term)
                    
                    cleaned_doc["relevance_score"] = relevance_score
                    cleaned_doc["match_reason"] = "content_text_regex"
                    results.append(cleaned_doc)

        # 2. Tag-based search
        if include_tag_search:
            try:
                # First find matching tags using text search
                matching_tags = list(db.custom_tags.find({
                    "tenant": ObjectId(tenant_id),
                    "$text": {"$search": search_query}
                }, {"_id": 1, "name": 1}))
                
            except Exception:
                # Fallback to regex search for tags
                matching_tags = list(db.custom_tags.find({
                    "tenant": ObjectId(tenant_id),
                    "name": {"$regex": search_query, "$options": "i"}
                }, {"_id": 1, "name": 1}))
            
            if matching_tags:
                tag_ids = [tag["_id"] for tag in matching_tags]
                tag_content = list(db.sitemaps.find({
                    "tenant": ObjectId(tenant_id),
                    "tag": {"$in": tag_ids}
                }))
                
                for doc in tag_content:
                    cleaned_doc = _clean_content_doc_enhanced(db, doc, tenant_schema)
                    cleaned_doc["relevance_score"] = len(search_query.split()) * 3  # Higher score for tag matches
                    cleaned_doc["match_reason"] = "tag_match"
                    
                    # Add matched tag names
                    matched_tag_names = [tag["name"] for tag in matching_tags if tag["_id"] in doc.get("tag", [])]
                    cleaned_doc["matched_tags"] = matched_tag_names
                    results.append(cleaned_doc)

        # 3. Category-based search
        if include_category_search:
            category_matches = []
            for category_name, values in tenant_schema.categories.items():
                matching_values = [v for v in values if search_query.lower() in v.lower()]
                if matching_values:
                    category_matches.extend([(category_name, v) for v in matching_values])
            
            # Fetch content matching these categories
            for category_name, value in category_matches:
                category_results = fetch_content_by_filters(
                    tenant_id, {category_name: value}, limit=10
                )
                for doc in category_results:
                    doc["relevance_score"] = len(search_query.split()) * 2  # Higher base score for category matches
                    doc["match_reason"] = f"category_{category_name}"
                    doc["matched_category_value"] = value
                    results.append(doc)

        # Remove duplicates and sort by relevance
        seen_ids = set()
        unique_results = []
        for doc in sorted(results, key=lambda x: x.get("relevance_score", 0), reverse=True):
            if doc.get("_id") not in seen_ids:
                seen_ids.add(doc.get("_id"))
                unique_results.append(doc)
        
        return {
            "results": unique_results[:limit],
            "total_found": len(unique_results),
            "search_query": search_query,
            "search_fields": search_fields,
            "category_search_enabled": include_category_search,
            "tag_search_enabled": include_tag_search
        }
        
    except Exception as e:
        log_error(e, {"operation": "search_content_by_text", "tenant_id": tenant_id})
        return {"results": [], "total_found": 0, "error": str(e)}


def fetch_content_gap_analysis(tenant_id: str, primary_dimension: str, secondary_dimension: str = None) -> Dict[str, Any]:
    """
    Identify content gaps and recommendations for strategic analysis
    """
    db = get_database()
    try:
        extractor = DynamicTenantSchemaExtractor(tenant_id)
        tenant_schema = extractor.extract_schema()
        
        # Get current distribution
        current_distribution = fetch_distribution_analysis(
            tenant_id, primary_dimension, secondary_dimension, include_examples=False
        )
        
        if "error" in current_distribution:
            return current_distribution
        
        # Calculate gaps and recommendations
        all_possible_values = tenant_schema.categories.get(primary_dimension, [])
        current_values = [item["_id"] for item in current_distribution["distribution"]]
        
        missing_values = [v for v in all_possible_values if v not in current_values]
        underrepresented = [
            item for item in current_distribution["distribution"] 
            if item["count"] < (current_distribution["total_analyzed"] * 0.1)  # Less than 10% of total
        ]
        
        return {
            "current_distribution": current_distribution,
            "gaps_identified": {
                "missing_completely": missing_values,
                "underrepresented": underrepresented,
                "total_content_analyzed": current_distribution["total_analyzed"]
            },
            "recommendations": _generate_content_recommendations(
                missing_values, underrepresented, primary_dimension
            )
        }
        
    except Exception as e:
        log_error(e, {"operation": "fetch_content_gap_analysis", "tenant_id": tenant_id})
        return {"error": str(e)}


# ================================
# HELPER FUNCTIONS
# ================================

def _clean_content_doc_enhanced(db, doc: Dict, tenant_schema) -> Dict:
    """Enhanced version with all relationships resolved"""
    if not doc:
        return {}

    cleaned = {
        "_id": str(doc.get("_id")),
        "title": doc.get("name", ""),
        "fullUrl": doc.get("fullUrl", ""),
        "domain": doc.get("domain", ""),
        "description": doc.get("description", ""),
        "summary": doc.get("summary", ""),
        "readerBenefit": doc.get("readerBenefit", ""),
        "wordCount": doc.get("wordCount", 0),
        "confidence": doc.get("confidence", ""),
        "isMarketingContent": doc.get("isMarketingContent", False),
        "geoFocus": doc.get("geoFocus", ""),
        "dateModified": doc.get("dateModified", "")
    }

    # Resolve all relationship fields
    if tenant_schema:
        for field_name, mapping in tenant_schema.field_mappings.items():
            raw_value = doc.get(mapping.field_path)

            if mapping.requires_join and mapping.reference_collection:
                if mapping.field_path == "categoryAttribute" and isinstance(raw_value, list):
                    # Handle category attribute arrays with category filtering
                    if mapping.join_config:
                        resolved_values = _resolve_category_attributes_for_sitemap(
                            db, raw_value, mapping.join_config["filter_value"]
                        )
                        cleaned[field_name] = resolved_values
                elif isinstance(raw_value, list):
                    # Handle other array references
                    cleaned[field_name] = [
                        _resolve_reference(db, mapping.reference_collection, v) 
                        for v in raw_value if v
                    ]
                else:
                    # Handle single references
                    cleaned[field_name] = _resolve_reference(db, mapping.reference_collection, raw_value)
            else:
                cleaned[field_name] = raw_value

    return cleaned


def _resolve_complex_filters_to_query(db, tenant_schema, filters: Dict[str, Union[str, List[str]]]) -> Dict:
    """Convert complex filters to MongoDB query"""
    query_filters = {}
    category_conditions = []
    
    for field_name, values in filters.items():
        if isinstance(values, str):
            values = [values]
            
        mapping = tenant_schema.field_mappings.get(field_name)
        if not mapping:
            continue

        if mapping.field_path == "categoryAttribute" and mapping.join_config:
            # Handle category attribute filters
            object_ids = _resolve_category_values_to_ids(db, tenant_schema, field_name, values)
            category_conditions.extend(object_ids)
        elif mapping.requires_join:
            # Handle other reference fields
            resolved_ids = []
            for value in values:
                resolved_id = _resolve_friendly_name_to_id(db, mapping.reference_collection, value, tenant_schema.tenant_id)
                if resolved_id:
                    resolved_ids.append(resolved_id)
            
            if len(resolved_ids) == 1:
                query_filters[mapping.field_path] = resolved_ids[0]
            elif len(resolved_ids) > 1:
                query_filters[mapping.field_path] = {"$in": resolved_ids}
        else:
            # Handle scalar fields
            if len(values) == 1:
                query_filters[mapping.field_path] = values[0]
            else:
                query_filters[mapping.field_path] = {"$in": values}
    
    if category_conditions:
        query_filters["categoryAttribute"] = {"$all": category_conditions}
    
    return query_filters


def _resolve_category_values_to_ids(db, tenant_schema, category_name: str, values: List[str]) -> List[ObjectId]:
    """Resolve category values to ObjectIds"""
    mapping = tenant_schema.field_mappings.get(category_name)
    if not mapping or not mapping.join_config:
        return []
    
    try:
        cursor = db.category_attributes.find({
            "name": {"$in": values},
            "category": mapping.join_config["filter_value"],
            "tenant": ObjectId(tenant_schema.tenant_id)
        }, {"_id": 1})
        
        return [doc["_id"] for doc in cursor]
    except Exception as e:
        log_error(e, {"operation": "resolve_category_values_to_ids"})
        return []


def _resolve_friendly_name_to_id(db, collection_name: str, friendly_name: str, tenant_id: str) -> Optional[ObjectId]:
    """Resolve friendly name to ObjectId in any collection"""
    try:
        doc = db[collection_name].find_one({
            "name": friendly_name,
            "tenant": ObjectId(tenant_id)
        }, {"_id": 1})
        return doc["_id"] if doc else None
    except Exception as e:
        log_error(e, {"operation": "resolve_friendly_name_to_id"})
        return None


def _resolve_reference(db, collection_name: str, object_id: Optional[ObjectId]) -> Optional[str]:
    """Resolve ObjectId to the 'name' field in the reference collection"""
    if not object_id:
        return None
    try:
        doc = db[collection_name].find_one({"_id": object_id}, {"name": 1})
        return doc.get("name") if doc else None
    except Exception as e:
        log_error(e, {"operation": "resolve_reference", "collection": collection_name})
        return None


def _resolve_category_attributes_for_sitemap(db, object_ids: List[ObjectId], category_filter_id: ObjectId) -> List[str]:
    """Given categoryAttribute IDs, return names filtered by category"""
    if not object_ids:
        return []
    try:
        cursor = db.category_attributes.find({
            "_id": {"$in": object_ids},
            "category": category_filter_id
        }, {"name": 1})
        return [doc["name"] for doc in cursor]
    except Exception as e:
        log_error(e, {"operation": "resolve_category_attributes_for_sitemap"})
        return []


def _generate_content_recommendations(missing_values: List[str], underrepresented: List[Dict], dimension: str) -> List[str]:
    """Generate strategic recommendations based on gap analysis"""
    recommendations = []
    
    if missing_values:
        recommendations.append(
            f"Consider creating content for {dimension} categories: {', '.join(missing_values[:3])}"
        )
    
    if underrepresented:
        underrep_names = [item["_id"] for item in underrepresented[:3]]
        recommendations.append(
            f"Increase content volume for underrepresented {dimension}: {', '.join(underrep_names)}"
        )
    
    return recommendations