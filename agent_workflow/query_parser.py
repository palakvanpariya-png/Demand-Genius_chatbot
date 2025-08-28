import os
import json
from openai import OpenAI
from dotenv import load_dotenv
from typing import Dict, List, Optional
from dataclasses import dataclass

load_dotenv()

""" look into the operations 
to do : 
* add date ranges in the output schema and is marketing column info and language info 
* in operations we only need two things 
    (have or not have -> would show results 
     distribution on particular filter -> would not show results (but needs filter trigger(from the parser itself) and the results directly to advisory) 
     pure advisory -> always would have overall data analysis(basic))
* change prompt as well [issue with primary and secondary audience] (decide for each operation what it will do and what it needs)
* will use gpt-4o cause it allows the temprature attribute
"""
@dataclass
class QueryResult:
    route: str  # 'database' or 'advisory'
    operation: str  # 'list' 'distribution' 'semantic' 'pure advisory'
    filters: Dict[str, List[str]]
    semantic_terms: List[str]
    tenant_id: str
    needs_data: bool  # Whether advisory needs supporting data

class QueryParser:
    def __init__(self, tenant_schema_util, openai_api_key: str = None):
        self.schema_util = tenant_schema_util
        self.client = OpenAI(api_key=openai_api_key or os.getenv("OPENAI_API_KEY"))
    
    def parse(self, query_text: str, tenant_id: str) -> QueryResult:
        
        # Get tenant categories
        schema = self.schema_util.get_tenant_schema(tenant_id)
        if not schema:
            raise ValueError(f"Tenant {tenant_id} not found")
        
        # Parse with single LLM call
        parsed = self._parse_query(query_text, schema.categories)
        
        return QueryResult(
            route=parsed["route"],
            operation=parsed["operation"],
            filters=parsed.get("filters", {}),
            semantic_terms=parsed.get("semantic_terms", []),
            tenant_id=tenant_id,
            needs_data=parsed.get("needs_supporting_data", False)
        )
    
    def _parse_query(self, query_text: str, categories: Dict[str, List[str]]) -> Dict:
        """Single LLM call to parse everything"""
        
        # Build filter properties dynamically
        filter_props = {}
        for cat, values in categories.items():
            if values:
                filter_props[cat] = {
                    "type": "array",
                    "items": {"type": "string", "enum": values}
                }
        
        schema = [{
            "type": "function",
            "function": {
                "name": "parse_query",
                "description": "Parse query and determine routing",
                "strict": True,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "route": {
                            "type": "string",
                            "enum": ["database", "advisory"],
                            "description": "database=get data, advisory=business insights"
                        },
                        "operation": {
                            "type": "string",
                            "enum": ["list", "distribution", "semantic", "pure advisory"],
                            "description": "list=show items, count=total, aggregate=group by, insight=advisory"
                        },
                        "filters": {
                            "type": "object",
                            "properties": filter_props,
                            "description": "Category filters found in query"
                        },
                        "semantic_terms": {
                            "type": "array", 
                            "items": {"type": "string"},
                            "description": "Important terms not in categories"
                        },
                        "needs_supporting_data": {
                            "type": "boolean",
                            "description": "For advisory: does it need data analysis?"
                        }
                    },
                    "required": ["route", "operation"]
                }
            }
        }]
        
        categories_str = "\n".join([f"- {cat}: {vals}" for cat, vals in categories.items() if vals])
        
        system_message = f"""Parse user queries for a content management system.

Available categories:
{categories_str}

ROUTING:
- database: "Show me X", "How many X", "List X", "Count X" 
- advisory: "Are we X?", "Should we X?", "Why X?", "What's our strategy?"

OPERATIONS:
- list: Get specific content
- count: Simple totals  
- aggregate: Group by categories
- insight: Business advisory

Extract exact category values that match the query. For advisory questions, determine if supporting data analysis is needed."""

        completion = self.client.chat.completions.create(
            model="gpt-5",
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": query_text}
            ],
            tools=schema,
            tool_choice={"type": "function", "function": {"name": "parse_query"}},
            # temperature=0,
            # top_p=1
        )
        
        return json.loads(completion.choices[0].message.tool_calls[0].function.arguments)
    
    def get_database_query_params(self, result: QueryResult) -> Optional[Dict]:
        """Get parameters for MongoDB query builder"""
        if result.route != "database" and not result.needs_data:
            return None
        
        return {
            "operation": result.operation,
            "filters": result.filters,
            "semantic_terms": result.semantic_terms,
            "tenant_id": result.tenant_id
        }
    
    def should_use_database(self, result: QueryResult) -> bool:
        """Simple decision: use database or not"""
        return result.route == "database" or result.needs_data

# Factory function
def create_parser(tenant_schema_util) -> QueryParser:
    return QueryParser(tenant_schema_util)

# Example usage
if __name__ == "__main__":
    from schema_extractor import create_schema_util
    
    schema_util = create_schema_util("mongodb://localhost:27017", "my_database")
    parser = create_parser(schema_util)
    
    queries = [
        "Show me TOFU content which is relevant to investors",           # → database/list
        "How many BOFU pages?",           # → database/count  
        "Are we overly focused on TOFU?", # → advisory/insight + needs_data
        "What should our content strategy be?", # → advisory/insight
        "Hey what can you help me with?" 
    ]
    
    tenant_id = "6875f3afc8337606d54a7f37"
    
    for query in queries:
        result = parser.parse(query, tenant_id)
        # print(result)
        print(f"\nQuery: {query}")
        print(f"Route: {result.route} | Operation: {result.operation}")
        print(f"Use DB: {parser.should_use_database(result)}")
        
        if parser.should_use_database(result):
            db_params = parser.get_database_query_params(result)
            print(f"DB Params: {db_params}")