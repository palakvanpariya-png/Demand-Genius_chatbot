from pymongo import MongoClient
from bson import ObjectId
from typing import Dict, List, Any, Optional
import re
from datetime import datetime

class MongoQueryExecutor:
    def __init__(self, mongo_uri: str, db_name: str):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
    
    def _get_db(self):
        """Get database connection"""
        client = MongoClient(self.mongo_uri)
        return client[self.db_name]
    
    def _build_category_lookup(self, category_names: List[str]) -> List[ObjectId]:
        """Convert category attribute names to ObjectIds"""
        if not category_names:
            return []
        
        db = self._get_db()
        
        # Get category attribute ObjectIds by name
        category_attrs = list(db.category_attributes.find({
            "name": {"$in": category_names}
        }, {"_id": 1}))
        
        return [attr["_id"] for attr in category_attrs]
    
    def _build_reference_lookup(self, collection: str, names: List[str]) -> List[ObjectId]:
        """Convert names to ObjectIds for reference collections"""
        if not names:
            return []
        
        db = self._get_db()
        docs = list(db[collection].find({
            "name": {"$in": names}
        }, {"_id": 1}))
        
        return [doc["_id"] for doc in docs]
    
    def _build_lookup_pipeline(self, match_query: Dict[str, Any], skip: int = 0, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Helper function to build consistent lookup pipeline for sitemap data
        """
        pipeline = [
            {"$match": match_query},
            {"$lookup": {
                "from": "topics",
                "localField": "topic", 
                "foreignField": "_id",
                "as": "topic_info"
            }},
            {"$lookup": {
                "from": "content_types",
                "localField": "contentType",
                "foreignField": "_id", 
                "as": "content_type_info"
            }},
            {"$lookup": {
                "from": "category_attributes",
                "localField": "categoryAttribute",
                "foreignField": "_id",
                "as": "category_info"
            }},
            {"$lookup": {
                "from": "custom_tags",
                "localField": "tag",
                "foreignField": "_id",
                "as": "tag_info"
            }}
        ]
        
        if skip > 0:
            pipeline.append({"$skip": skip})
        if limit > 0:
            pipeline.append({"$limit": limit})
            
        return pipeline

    def fetch_content_by_filters(self, tenant_id: str, filters: Dict[str, List[str]], 
                            date_filter: Optional[Dict[str, str]] = None,
                            marketing_filter: Optional[bool] = None,
                            is_negation: bool = False,
                            page: int = 1,
                                page_size: int = 50) -> Dict[str, Any]:
        """
        Fetch content with complex filters including dates and marketing content
        Returns both data and total count for pagination and advisory insights
        """
        db = self._get_db()
        tenant_obj_id = ObjectId(tenant_id)
        
        # Base query
        match_query = {"tenant": tenant_obj_id}
        
        # Date filters
        if date_filter:
            date_conditions = {}
            start_date = date_filter.get("start_date")
            end_date = date_filter.get("end_date")

            if start_date:
                date_conditions["$gte"] = datetime.fromisoformat(start_date)
            if end_date:
                date_conditions["$lte"] = datetime.fromisoformat(end_date)

            if date_conditions:
                match_query["createdAt"] = date_conditions

        # Marketing filter
        if marketing_filter is not None:
            match_query["isMarketingContent"] = marketing_filter
        
        # Category filters
        category_conditions = []
        reference_conditions = []
        
        for category, values in filters.items():
            if not values:
                continue
                
            if category == "Language":
                condition = {"geoFocus": {"$nin" if is_negation else "$in": values}}
            elif category == "Topics":
                topic_ids = self._build_reference_lookup("topics", values)
                condition = {"topic": {"$nin" if is_negation else "$in": topic_ids}}
            elif category == "Content Type":
                content_type_ids = self._build_reference_lookup("content_types", values)
                condition = {"contentType": {"$nin" if is_negation else "$in": content_type_ids}}
            elif category == "Custom Tags":
                tag_ids = self._build_reference_lookup("custom_tags", values)
                condition = {"tag": {"$nin" if is_negation else "$in": tag_ids}}
            else:
                attr_ids = self._build_category_lookup(values)
                condition = {"categoryAttribute": {"$nin" if is_negation else "$in": attr_ids}}
            
            if category in ["Language", "Topics", "Content Type", "Custom Tags"]:
                reference_conditions.append(condition)
            else:
                category_conditions.append(condition)
        
        # Combine conditions
        if reference_conditions:
            match_query.update({k: v for cond in reference_conditions for k, v in cond.items()})
        if category_conditions:
            match_query.update({k: v for cond in category_conditions for k, v in cond.items()})
        
        # Calculate pagination
        skip = (page - 1) * page_size
        
        # Get total count
        count_pipeline = [{"$match": match_query}, {"$count": "total"}]
        count_result = list(db.sitemaps.aggregate(count_pipeline))
        total_count = count_result[0]["total"] if count_result else 0
        
        # Execute query with lookups
        pipeline = self._build_lookup_pipeline(match_query, skip, page_size)
        data = list(db.sitemaps.aggregate(pipeline))
        total_pages = (total_count + page_size - 1) // page_size
        
        return {
            "data": data,
            "total_count": total_count,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_prev": page > 1
        }

    def fetch_content_by_semantic_search(self, tenant_id: str, search_terms: List[str],
                                        additional_filters: Dict[str, List[str]] = None) -> List[Dict[str, Any]]:
        """
        Search content using regex on text fields
        """
        db = self._get_db()
        tenant_obj_id = ObjectId(tenant_id)
        
        # Base query
        match_query = {"tenant": tenant_obj_id}
        
        # Apply additional filters
        if additional_filters:
            for category, values in additional_filters.items():
                if category == "Language":
                    match_query["geoFocus"] = {"$in": values}
                elif category == "Topics":
                    topic_ids = self._build_reference_lookup("topics", values)
                    match_query["topic"] = {"$in": topic_ids}
                elif category == "Content Type":
                    content_type_ids = self._build_reference_lookup("content_types", values)
                    match_query["contentType"] = {"$in": content_type_ids}
                elif category == "Custom Tags":
                    tag_ids = self._build_reference_lookup("custom_tags", values)
                    match_query["tag"] = {"$in": tag_ids}
                else:
                    # Category attributes (Funnel Stage, Primary Audience, etc.)
                    attr_ids = self._build_category_lookup(values)
                    match_query["categoryAttribute"] = {"$in": attr_ids}
        
        # Build regex search for semantic terms
        if search_terms:
            regex_patterns = [re.compile(term, re.IGNORECASE) for term in search_terms]
            
            text_conditions = []
            for pattern in regex_patterns:
                text_conditions.extend([
                    {"name": {"$regex": pattern}},
                    {"description": {"$regex": pattern}}, 
                    {"summary": {"$regex": pattern}}
                ])
            
            match_query["$or"] = text_conditions
        
        # Execute query with lookups
        pipeline = self._build_lookup_pipeline(match_query, limit=50)
        return list(db.sitemaps.aggregate(pipeline))
        
    def fetch_content_by_distribution(self, tenant_id: str, category: str, 
                                  values: List[str] = None,
                                  additional_filters: Dict[str, List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get distribution/count of content by category.
        If values provided, filter by those values first then count.
        If no values, count all values in that category.
        """
        db = self._get_db()
        tenant_obj_id = ObjectId(tenant_id)

        # Base match
        match_stage = {"tenant": tenant_obj_id}

        # Apply additional filters first if provided
        if additional_filters:
            for filter_cat, filter_vals in additional_filters.items():
                if filter_cat == "Language":
                    match_stage["geoFocus"] = {"$in": filter_vals}
                elif filter_cat == "Topics":
                    topic_ids = self._build_reference_lookup("topics", filter_vals)
                    match_stage["topic"] = {"$in": topic_ids}
                elif filter_cat == "Content Type":
                    content_type_ids = self._build_reference_lookup("content_types", filter_vals)
                    match_stage["contentType"] = {"$in": content_type_ids}
                elif filter_cat == "Custom Tags":
                    tag_ids = self._build_reference_lookup("custom_tags", filter_vals)
                    match_stage["tag"] = {"$in": tag_ids}
                else:
                    # Generic category attributes (Funnel Stage, Primary Audience, etc.)
                    attr_ids = self._build_category_lookup(filter_vals)
                    match_stage["categoryAttribute"] = {"$in": attr_ids}

        pipeline = [{"$match": match_stage}]

        def simple_group(field: str):
            return [
                {"$group": {"_id": f"${field}", "count": {"$sum": 1}}},
                {"$project": {"value": "$_id", "count": "$count", "_id": 0}}
            ]

        def lookup_group(local_field: str, from_collection: str, alias_field: str):
            return [
                {"$lookup": {
                    "from": from_collection,
                    "localField": local_field,
                    "foreignField": "_id",
                    "as": "info"
                }},
                {"$unwind": "$info"},
                {"$group": {"_id": f"$info.{alias_field}", "count": {"$sum": 1}}},
                {"$project": {"value": "$_id", "count": "$count", "_id": 0}}
            ]

        if category == "Language":
            pipeline.extend(simple_group("geoFocus"))

        elif category == "Topics":
            pipeline.extend(lookup_group("topic", "topics", "name"))

        elif category == "Content Type":
            pipeline.extend(lookup_group("contentType", "content_types", "name"))

        elif category == "Custom Tags":
            pipeline.extend([
                {"$unwind": "$tag"},
                *lookup_group("tag", "custom_tags", "name")
            ])

        else:
            # Generic category attributes
            pipeline.extend([
                {"$unwind": "$categoryAttribute"},
                {"$lookup": {
                    "from": "category_attributes",
                    "localField": "categoryAttribute",
                    "foreignField": "_id",
                    "as": "attr_info"
                }},
                {"$unwind": "$attr_info"},
                {"$lookup": {
                    "from": "categories",
                    "localField": "attr_info.category",
                    "foreignField": "_id",
                    "as": "cat_info"
                }},
                {"$unwind": "$cat_info"},
                {"$match": {"cat_info.name": category}},
                {"$group": {"_id": "$attr_info.name", "count": {"$sum": 1}}},
                {"$project": {"value": "$_id", "count": "$count", "_id": 0}}
            ])

        # Case-insensitive filtering by values
        if values:
            normalized_values = [v.lower() for v in values]
            pipeline.append({
                "$match": {"$expr": {"$in": [{"$toLower": "$value"}, normalized_values]}}
            })

        pipeline.append({"$sort": {"count": -1}})

        return list(db.sitemaps.aggregate(pipeline))


# Factory function
def create_mongo_executor(mongo_uri: str, db_name: str) -> MongoQueryExecutor:
    return MongoQueryExecutor(mongo_uri, db_name)


# # Example usage
# if __name__ == "__main__":
#     executor = create_mongo_executor("mongodb://localhost:27017", "my_database")
#     tenant_id = "6875f3afc8337606d54a7f37"
    
#     # Test filter query with pagination
#     print("=== FILTER QUERY WITH PAGINATION ===")
#     results = executor.fetch_content_by_filters(
#         tenant_id=tenant_id,
#         filters={"Funnel Stage": ["TOFU"], "Language": ["English"]},
#         marketing_filter=False,
#         page=1,
#         page_size=25
#     )
#     print(f"Page {results['page']}/{results['total_pages']}")
#     print(f"Total matching documents: {results['total_count']}")
#     print(f"Showing {len(results['data'])} documents")
#     print(f"Has next page: {results['has_next']}")
    
#     # Get next page
#     if results['has_next']:
#         page2_results = executor.fetch_content_by_filters(
#             tenant_id=tenant_id,
#             filters={"Funnel Stage": ["TOFU"], "Language": ["English"]},
#             marketing_filter=False,
#             page=2,
#             page_size=25
#         )
#         print(f"\nNext page has {len(page2_results['data'])} documents")
    
#     # Test distribution query  
#     print("\n=== DISTRIBUTION QUERY ===")
#     distribution = executor.fetch_content_by_distribution(
#         tenant_id=tenant_id,
#         values=["Tofu"],
#         category="Funnel Stage"
#     )
#     print(distribution)
#     print(f"Funnel Stage distribution: {distribution}")
    
#     # Test semantic search
#     print("\n=== SEMANTIC SEARCH ===") 
#     semantic_results = executor.fetch_content_by_semantic_search(
#         tenant_id=tenant_id,
#         search_terms=["investment", "crypto"]
#     )
#     print(f"Found {len(semantic_results)} semantic results")