# Add these imports at the top of your existing query_router.py
import json
import time
from functools import wraps
from dataclasses import dataclass
from typing import Dict, List, Any, Optional
from query_parser import SmartQueryParser
from query_builder import MongoQueryExecutor
from dotenv import load_dotenv
import os

load_dotenv()

@dataclass 
class QueryResponse:
    success: bool
    data: Any
    operation_type: str
    total_count: Optional[int] = None
    pagination: Optional[Dict] = None
    error_message: Optional[str] = None


@dataclass
class QueryResult:
    route: str
    operation: str
    filters: Dict[str, List[str]]
    date_filter: Optional[Dict[str, str]]
    marketing_filter: Optional[bool]
    is_negation: bool
    semantic_terms: List[str]
    tenant_id: str
    needs_data: bool

# Add this caching decorator before your QueryRouter class
def cached_with_ttl(seconds: int = 300):
    """Simple cache decorator with time-to-live"""
    def decorator(func):
        func.cache_time = {}
        func.cache_data = {}
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = str(args[1:]) + str(sorted(kwargs.items()))  # Skip 'self' in args
            now = time.time()
            
            if key in func.cache_data and (now - func.cache_time[key]) < seconds:
                return func.cache_data[key]
            
            result = func(*args, **kwargs)
            func.cache_data[key] = result
            func.cache_time[key] = now
            return result
        return wrapper
    return decorator

class QueryRouter:
    """
    Bridge between SmartQueryParser and MongoQueryExecutor
    Handles the translation of parsed queries into database operations
    """
    
    def __init__(self, mongo_uri: str, db_name: str, openai_api_key: str = None):
        self.parser = SmartQueryParser(mongo_uri, db_name, openai_api_key)
        self.executor = MongoQueryExecutor(mongo_uri, db_name)
        
        # ADD THESE NEW LINES for caching and insights
        self.insights_cache = {}  # Simple insights cache
        self.insights_cache_ttl = 1800  # 30 minutes
    
    # ADD THIS NEW METHOD for cached context
    @cached_with_ttl(300)  # 5 minute cache
    def get_tenant_context(self, tenant_id: str) -> Dict[str, Any]:
        """Get and cache basic tenant context data"""
        try:
            context = {
                "funnel_distribution": self.executor.fetch_content_by_distribution(
                    tenant_id, "Funnel Stage"
                ),
                "content_types": self.executor.fetch_content_by_distribution(
                    tenant_id, "Content Type"  
                ),
                "primary_audience": self.executor.fetch_content_by_distribution(
                    tenant_id, "Primary Audience"
                ),
                "total_content": self.executor.fetch_content_by_filters(
                    tenant_id, {}, page_size=1
                )["total_count"]
            }
            
            # Add simple stats
            context["stats"] = {
                "total_pieces": context["total_content"],
                "funnel_stages": len(context["funnel_distribution"]),
                "content_types": len(context["content_types"]),
                "audiences": len(context["primary_audience"])
            }
            
            return context
            
        except Exception as e:
            return {"error": f"Could not fetch context: {str(e)}"}
    
    # ADD THIS NEW METHOD for proactive insights
    def generate_proactive_insights(self, tenant_id: str, context_data: Dict) -> List[str]:
        """Use OpenAI to generate actionable insights"""
        try:
            # Use your existing OpenAI client from parser
            insight_prompt = f"""
Based on this content data, provide exactly 3 actionable insights:

Data: {json.dumps(context_data.get('stats', {}), indent=2)}
Funnel Distribution: {context_data.get('funnel_distribution', [])}
Content Types: {context_data.get('content_types', [])}

Focus on content gaps, optimization opportunities, and strategic recommendations.
Format as 3 numbered points, each under 50 words.
"""
            
            response = self.parser.client.chat.completions.create(
                model="gpt-4.1",
                messages=[{"role": "user", "content": insight_prompt}],
                max_tokens=300,
                temperature=0.7
            )
            
            insights_text = response.choices[0].message.content
            insights = [line.strip() for line in insights_text.split('\n') 
                       if line.strip() and any(char.isdigit() for char in line[:3])]
            
            return insights[:3]
            
        except Exception as e:
            return [f"Could not generate insights: {str(e)}"]
    
    # ADD THIS NEW METHOD for insight caching  
    def get_or_generate_insights(self, tenant_id: str) -> List[str]:
        """Get cached insights or generate new ones"""
        now = time.time()
        
        # Check cache
        if tenant_id in self.insights_cache:
            cached = self.insights_cache[tenant_id]
            if (now - cached["timestamp"]) < self.insights_cache_ttl:
                return cached["insights"]
        
        # Generate fresh insights
        context = self.get_tenant_context(tenant_id)
        if "error" in context:
            return ["Context unavailable - cannot generate insights"]
            
        insights = self.generate_proactive_insights(tenant_id, context)
        
        # Cache new insights
        self.insights_cache[tenant_id] = {
            "insights": insights,
            "timestamp": now
        }
        
        return insights
    
    # MODIFY YOUR EXISTING execute_query METHOD - add this condition
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
                # MODIFY THIS LINE - use the new advisory method
                return self._execute_advisory_query(parsed_query, query_text)
            
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
    
    # REPLACE YOUR EXISTING _execute_advisory_query or ADD if missing
    def _execute_advisory_query(self, parsed_query: QueryResult, original_query: str) -> QueryResponse:
        """Execute advisory queries with cached context and insights"""
        try:
            # Get cached context
            context = self.get_tenant_context(parsed_query.tenant_id)
            if "error" in context:
                return QueryResponse(
                    success=False,
                    data=None,
                    operation_type="advisory", 
                    error_message=context["error"]
                )
            
            # Get proactive insights
            insights = self.get_or_generate_insights(parsed_query.tenant_id)
            
            # Generate advisory response
            advisory_prompt = f"""
You are a content strategist. Based on this data, answer the question:

Context: {json.dumps(context.get('stats', {}), indent=2)}
Current Insights: {chr(10).join(f'• {insight}' for insight in insights)}

Question: {original_query}

Provide specific, actionable advice based on the context.
"""
            
            response = self.parser.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert content marketing strategist."},
                    {"role": "user", "content": advisory_prompt}
                ],
                max_tokens=500,
                temperature=0.7
            )
            
            return QueryResponse(
                success=True,
                data={
                    "advice": response.choices[0].message.content,
                    "proactive_insights": insights,
                    "context_stats": context.get("stats", {}),
                    "cache_info": "Context cached 5min, Insights cached 30min"
                },
                operation_type="advisory"
            )
            
        except Exception as e:
            return QueryResponse(
                success=False,
                data=None,
                operation_type="advisory",
                error_message=f"Advisory failed: {str(e)}"
            )
    
    # ADD THIS METHOD for cache management
    def clear_cache(self, tenant_id: str = None):
        """Clear cache for tenant or all"""
        if tenant_id and tenant_id in self.insights_cache:
            del self.insights_cache[tenant_id]
            print(f"Cache cleared for tenant {tenant_id}")
        elif not tenant_id:
            self.insights_cache.clear()
            # Clear context cache
            if hasattr(self.get_tenant_context, 'cache_data'):
                self.get_tenant_context.cache_data.clear()
                self.get_tenant_context.cache_time.clear()
            print("All caches cleared")

    # Keep all your existing methods unchanged:
    # _execute_list_query, _execute_distribution_query, _execute_semantic_query


if __name__ == "__main__":
    router = QueryRouter("mongodb://localhost:27017", "my_database", os.getenv("OPENAI_API_KEY"))

    # This will now use caching + proactive insights
    result = router.execute_query("What should our content strategy focus be?", "6875f3afc8337606d54a7f37")
    print(result)  # AI insights