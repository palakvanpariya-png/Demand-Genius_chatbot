import os
import json
from openai import OpenAI
from bson import ObjectId
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

"""
now it should have only 3 functions 
        * for filters (use this from the queries.py)
        * for distribution (use this from the queries.py)
        * for semantic search (keep this based on the text search for now )"""

load_dotenv()

class MongoQueryBuilder:
    def __init__(self, tenant_schema_util, openai_api_key: str = None):
        self.schema_util = tenant_schema_util
        self.client = OpenAI(api_key=openai_api_key or os.getenv("OPENAI_API_KEY"))
    
    def build_query(self, query_params: Dict) -> Dict:
        """Build MongoDB query from parser results"""
        
        tenant_id = query_params["tenant_id"]
        operation = query_params["operation"]
        filters = query_params.get("filters", {})
        semantic_terms = query_params.get("semantic_terms", [])
        
        # Get tenant schema for reference
        schema = self.schema_util.get_tenant_schema(tenant_id)
        if not schema:
            raise ValueError(f"Tenant {tenant_id} not found")
        
        # Route to appropriate query builder
        if operation == "list":
            return self._build_list_query(tenant_id, filters, semantic_terms)
        elif operation == "count":
            return self._build_count_query(tenant_id, filters)
        elif operation == "aggregate":
            return self._build_aggregate_query(tenant_id, filters, schema.categories)
        elif operation == "insight":
            return self._build_insight_query(tenant_id, filters, schema.categories)
        else:
            raise ValueError(f"Unknown operation: {operation}")
    
    def _build_list_query(self, tenant_id: str, filters: Dict, semantic_terms: List[str]) -> Dict:
        """Build query to list/retrieve content"""
        
        pipeline = [
            {"$match": {"tenant": ObjectId(tenant_id)}}
        ]
        
        # Add category filtering if needed
        if filters:
            category_stages = self._build_category_filters(tenant_id, filters)
            pipeline.extend(category_stages)
        
        # Add semantic search if needed
        if semantic_terms:
            semantic_condition = {
                "$or": [
                    {"name": {"$regex": "|".join(semantic_terms), "$options": "i"}},
                    {"description": {"$regex": "|".join(semantic_terms), "$options": "i"}},
                    {"summary": {"$regex": "|".join(semantic_terms), "$options": "i"}}
                ]
            }
            pipeline.append({"$match": semantic_condition})
        
        # Add final stages
        pipeline.extend([
            {"$sort": {"createdAt": -1}},
            {"$limit": 300},
            {"$project": {
                "_id": 1,
                "name": 1,
                "fullUrl": 1,
                "description": 1,
                "createdAt": 1,
                "wordCount": 1
            }}
        ])
        
        return {
            "collection": "sitemaps",
            "operation": "aggregate",
            "pipeline": pipeline
        }
    
    def _build_count_query(self, tenant_id: str, filters: Dict) -> Dict:
        """Build query to count documents"""
        
        if not filters:
            # Simple count with just tenant filter
            return {
                "collection": "sitemaps",
                "operation": "count_documents",
                "filter": {"tenant": ObjectId(tenant_id)}
            }
        
        # For filtered counts, we need aggregation pipeline
        pipeline = [
            {"$match": {"tenant": ObjectId(tenant_id)}}
        ]
        
        # Add category filtering stages
        category_stages = self._build_category_filters(tenant_id, filters)
        pipeline.extend(category_stages)
        
        # Add count stage
        pipeline.append({"$count": "total"})
        
        return {
            "collection": "sitemaps",
            "operation": "aggregate",
            "pipeline": pipeline
        }
    
    def _build_aggregate_query(self, tenant_id: str, filters: Dict, categories: Dict) -> Dict:
        """Build aggregation query for grouping/analysis"""
        
        base_match = {"tenant": ObjectId(tenant_id)}
        
        # Add category filters
        if filters:
            category_conditions = self._build_category_filters(tenant_id, filters)
            base_match.update(category_conditions)
        
        pipeline = [
            {"$match": base_match}
        ]
        
        # Group by main categories (this could be enhanced with LLM)
        group_stage = {
            "$group": {
                "_id": "$categoryAttribute",
                "count": {"$sum": 1},
                "avg_word_count": {"$avg": "$wordCount"}
            }
        }
        
        pipeline.extend([
            group_stage,
            {"$sort": {"count": -1}},
            {"$limit": 300}
        ])
        
        return {
            "collection": "sitemaps", 
            "operation": "aggregate",
            "pipeline": pipeline
        }
    
    def _build_insight_query(self, tenant_id: str, filters: Dict, categories: Dict) -> Dict:
        """Build query for advisory insights - get broad data for analysis"""
        
        base_match = {"tenant": ObjectId(tenant_id)}
        
        # For insights, we want broader data unless specific filters provided
        if filters:
            category_conditions = self._build_category_filters(tenant_id, filters)
            base_match.update(category_conditions)
        
        # Get aggregated data for insight generation
        pipeline = [
            {"$match": base_match},
            {"$group": {
                "_id": "$categoryAttribute",
                "count": {"$sum": 1},
                "total_words": {"$sum": "$wordCount"},
                "avg_words": {"$avg": "$wordCount"}
            }},
            {"$sort": {"count": -1}},
            {"$limit": 300}
        ]
        
        return {
            "collection": "sitemaps",
            "operation": "aggregate", 
            "pipeline": pipeline,
            "insight_context": {
                "categories": categories,
                "filters_applied": filters
            }
        }
    
    def _build_category_filters(self, tenant_id: str, filters: Dict) -> List[Dict]:
        
        if not filters:
            return []
        
        # Build pipeline stages for category filtering
        pipeline_stages = []
        
        # Add lookup to get category details
        pipeline_stages.append({
            "$lookup": {
                "from": "category_attributes",
                "localField": "categoryAttribute", 
                "foreignField": "_id",
                "as": "categoryDetails"
            }
        })
        
        # Build match conditions for each filter
        match_conditions = []
        for category_name, values in filters.items():
            if values:
                # First get the category ObjectId for this category name
                category_id = self._get_category_id(tenant_id, category_name)
                if category_id:
                    match_conditions.append({
                        "$and": [
                            {"categoryDetails.category": category_id},
                            {"categoryDetails.name": {"$in": values}}
                        ]
                    })
        
        if match_conditions:
            pipeline_stages.append({
                "$match": {
                    "$or": match_conditions
                }
            })
        
        return pipeline_stages
    
    def _get_category_id(self, tenant_id: str, category_name: str) -> Optional[ObjectId]:
        """Get ObjectId for category name"""
        try:
            db = self.schema_util.db
            category_doc = db.categories.find_one({
                "tenant": ObjectId(tenant_id),
                "name": category_name
            })
            return category_doc["_id"] if category_doc else None
        except Exception as e:
            print(f"Error getting category ID: {e}")
            return None
    
    def execute_query(self, mongo_db, query_spec: Dict) -> Any:
        """Execute the built query against MongoDB"""
        
        collection = mongo_db[query_spec["collection"]]
        operation = query_spec["operation"]
        
        try:
            if operation == "aggregate":
                return list(collection.aggregate(query_spec["pipeline"]))
            elif operation == "count_documents":
                return collection.count_documents(query_spec["filter"])
            elif operation == "find":
                return list(collection.find(query_spec["filter"]).limit(query_spec.get("limit", 300)))
            else:
                raise ValueError(f"Unknown operation: {operation}")
                
        except Exception as e:
            print(f"Query execution error: {e}")
            return None

# Factory function
def create_query_builder(tenant_schema_util) -> MongoQueryBuilder:
    return MongoQueryBuilder(tenant_schema_util)

# Example usage
if __name__ == "__main__":
    from schema_extractor import create_schema_util
    from pymongo import MongoClient
    from query_parser import create_parser
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017")
    mongo_db = client["my_database"] 
        # Initialize
    schema_util = create_schema_util("mongodb://localhost:27017", "my_database")
    query_builder = create_query_builder(schema_util)
    parser = create_parser(schema_util)

    query = "are we too focused on tofu content"
    results = parser.parse(query, tenant_id="6875f3afc8337606d54a7f37")
    print(results)
    print()
    test_params = parser.get_database_query_params(results)
    query_mongo = query_builder.build_query(test_params)
    
    # Test with parser results
    # test_params = [
    #     {
    #         "operation": "list",
    #         "filters": {"Funnel Stage": ["TOFU"]},
    #         "semantic_terms": [], 
    #         "tenant_id": "6875f3afc8337606d54a7f37"
    #     },
    #     {
    #         "operation": "count", 
    #         "filters": {"Funnel Stage": ["BOFU"]},
    #         "semantic_terms": [],
    #         "tenant_id": "6875f3afc8337606d54a7f37"
    #     }
    # ]
    
    fetcher = MongoQueryBuilder(schema_util)
    database_results = fetcher.execute_query(mongo_db, query_mongo)
    if database_results:
        print(len(database_results))
    # for params in test_params:
    #     print(f"\nBuilding query for: {params}")
    #     try:
    #         query = query_builder.build_query(params)
    #         print(f"Generated query: {json.dumps(query, indent=2, default=str)}")
    #         # for stage in query["pipeline"]:
    #         #     print(stage)
    #         #     if "$match" in stage:
    #         #         print(f"tenant type: {type(stage['$match']['tenant'])}")
    #     except Exception as e:
    #         print(f"Error: {e}")