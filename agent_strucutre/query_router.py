from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from query_parser import SmartQueryParser, QueryResult
from query_builder import MongoQueryExecutor

@dataclass 
class QueryResponse:
    success: bool
    data: Any
    operation_type: str
    total_count: Optional[int] = None
    pagination: Optional[Dict] = None
    error_message: Optional[str] = None

class QueryRouter:
    """
    Bridge between SmartQueryParser and MongoQueryExecutor
    Handles the translation of parsed queries into database operations
    """
    
    def __init__(self, mongo_uri: str, db_name: str, openai_api_key: str = None):
        self.parser = SmartQueryParser(mongo_uri, db_name, openai_api_key)
        self.executor = MongoQueryExecutor(mongo_uri, db_name)
    
    def execute_query(self, query_text: str, tenant_id: str, 
                     page: int = 1, page_size: int = 50) -> QueryResponse:
        """
        Main entry point: Parse natural language query and execute against database
        """
        try:
            # Step 1: Parse the natural language query
            parsed_query = self.parser.parse(query_text, tenant_id)
            print(parsed_query)
            
            # Step 2: Route to appropriate executor method based on operation
            if parsed_query.operation == "pure_advisory":
                return QueryResponse(
                    success=True,
                    data={"message": "This query requires strategic advice without database access"},
                    operation_type="advisory"
                )
            
            elif parsed_query.operation == "list":
                return self._execute_list_query(parsed_query, page, page_size)
            
            elif parsed_query.operation == "distribution": 
                return self._execute_distribution_query(parsed_query)
            
            elif parsed_query.operation == "semantic":
                return self._execute_semantic_query(parsed_query)
            
            else:
                return QueryResponse(
                    success=False,
                    data=None,
                    operation_type="unknown",
                    error_message=f"Unknown operation: {parsed_query.operation}"
                )
                
        except Exception as e:
            return QueryResponse(
                success=False,
                data=None, 
                operation_type="error",
                error_message=str(e)
            )
    
    def _execute_list_query(self, parsed_query: QueryResult, page: int, page_size: int) -> QueryResponse:
        """Execute content listing queries with filters"""
        try:
            results = self.executor.fetch_content_by_filters(
                tenant_id=parsed_query.tenant_id,
                filters=parsed_query.filters,
                date_filter=parsed_query.date_filter,
                marketing_filter=parsed_query.marketing_filter,
                is_negation=parsed_query.is_negation,
                page=page,
                page_size=page_size
            )
            
            return QueryResponse(
                success=True,
                data=results["data"],
                operation_type="list",
                total_count=results["total_count"],
                pagination={
                    "page": results["page"],
                    "page_size": results["page_size"], 
                    "total_pages": results["total_pages"],
                    "has_next": results["has_next"],
                    "has_prev": results["has_prev"]
                }
            )
            
        except Exception as e:
            return QueryResponse(
                success=False,
                data=None,
                operation_type="list",
                error_message=f"List query failed: {str(e)}"
            )
    
    def _execute_distribution_query(self, parsed_query: QueryResult) -> QueryResponse:
        """
        Execute distribution queries - trust the parser output
        """
        try:
            # Since parser gives good results, just extract the first category as the main one
            if not parsed_query.filters:
                return QueryResponse(
                    success=False,
                    data=None,
                    operation_type="distribution",
                    error_message="No category specified for distribution query"
                )
            
            # Take the first category as the distribution target
            main_category = list(parsed_query.filters.keys())[0]
            main_category_values = parsed_query.filters[main_category]
            
            # Additional filters are the remaining categories (if any)
            additional_filters = {k: v for k, v in parsed_query.filters.items() if k != main_category}
            
            results = self.executor.fetch_content_by_distribution(
                tenant_id=parsed_query.tenant_id,
                category=main_category,
                values=main_category_values,
                additional_filters=additional_filters if additional_filters else None
            )
            
            return QueryResponse(
                success=True,
                data=results,
                operation_type="distribution",
                total_count=sum(item.get("count", 0) for item in results)
            )
            
        except Exception as e:
            return QueryResponse(
                success=False,
                data=None,
                operation_type="distribution", 
                error_message=f"Distribution query failed: {str(e)}"
            )
    
    def _execute_semantic_query(self, parsed_query: QueryResult) -> QueryResponse:
        """Execute semantic/text search queries"""
        try:
            results = self.executor.fetch_content_by_semantic_search(
                tenant_id=parsed_query.tenant_id,
                search_terms=parsed_query.semantic_terms,
                additional_filters=parsed_query.filters if parsed_query.filters else None
            )
            
            return QueryResponse(
                success=True,
                data=results,
                operation_type="semantic",
                total_count=len(results)
            )
            
        except Exception as e:
            return QueryResponse(
                success=False,
                data=None,
                operation_type="semantic",
                error_message=f"Semantic query failed: {str(e)}"
            )

# Factory function
def create_query_router(mongo_uri: str, db_name: str, openai_api_key: str = None) -> QueryRouter:
    return QueryRouter(mongo_uri, db_name, openai_api_key)

# Example usage
if __name__ == "__main__":
    router = create_query_router("mongodb://localhost:27017", "my_database")
    tenant_id = "6875f3afc8337606d54a7f37"
    
    test_queries = [
        "what is the distribution of funnel stages",
        "show all content for investors", 
        # "List all pages with no assigned funnel stage",
        # "What are the newest assets added in the last 30 days?",
        # "search for content about cryptocurrency",
        # "show me bofu content distribution over content pages"
    ]
    
    for query in test_queries:
        print(f"\n{'='*50}")
        print(f"Query: '{query}'")
        print('='*50)
        
        result = router.execute_query(query, tenant_id)
        
        if result.success:
            print(f"✅ Operation: {result.operation_type}")
            if result.total_count is not None:
                print(f"📊 Total Count: {result.total_count}")
            if result.pagination:
                print(f"📄 Pagination: Page {result.pagination['page']}/{result.pagination['total_pages']}")
            print(f"📝 Data Sample: {len(result.data) if isinstance(result.data, list) else 'N/A'} items")
        else:
            print(f"❌ Error: {result.error_message}")