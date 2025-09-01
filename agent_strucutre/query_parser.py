import os
import json
from datetime import datetime
from openai import OpenAI
from dotenv import load_dotenv
from typing import Dict, List, Optional
from dataclasses import dataclass

load_dotenv()

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


class SmartQueryParser:
    def __init__(self, mongo_uri: str, db_name: str, openai_api_key: str = None):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.client = OpenAI(api_key=openai_api_key or os.getenv("OPENAI_API_KEY"))
        self._schema_cache = {}  # Cache schemas to avoid redundant calls
    
    def parse(self, query_text: str, tenant_id: str) -> QueryResult:
        """Parse query using OpenAI with full schema context"""
        schema_data = self._get_cached_schema(tenant_id)
        parsed = self._ai_parse(query_text, schema_data)

        return QueryResult(
            route=parsed["route"],
            operation=parsed["operation"],
            filters=parsed.get("filters", {}),
            date_filter=parsed.get("date_filter"),
            marketing_filter=parsed.get("marketing_filter"),
            is_negation=parsed.get("is_negation", False),
            semantic_terms=parsed.get("semantic_terms", []),
            tenant_id=tenant_id,
            needs_data=parsed.get("needs_data", False),
        )
    
    def _get_cached_schema(self, tenant_id: str) -> Dict:
        """Get schema with caching to avoid redundant database calls"""
        if tenant_id not in self._schema_cache:
            from schema_extractor import get_tenant_schema
            schema_data = get_tenant_schema(self.mongo_uri, self.db_name, tenant_id)
            if not schema_data:
                raise ValueError(f"Tenant {tenant_id} not found")
            self._schema_cache[tenant_id] = schema_data
        return self._schema_cache[tenant_id]
    
    def _ai_parse(self, query_text: str, schema_data: Dict) -> Dict:
        """Use OpenAI to parse natural language query into structured fields"""
        
        categories = schema_data.get("categories", {}) 
        field_mappings = schema_data.get("field_mappings", {})

        # Build dynamic schema from tenant metadata
        filter_props = {} 
        for cat, values in categories.items(): 
            if values:
                filter_props[cat] = { 
                    "type": "array", 
                    "items": {"type": "string", "enum": values}, 
                    "description": f"Values for {cat} category" 
                }

        # Define tool schema dynamically
        tool_schema = [
            {
                "type": "function",
                "function": {
                    "name": "parse_query",
                    "description": "Parse user queries into structured JSON for database routing.",
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
                                "description": "list=show items, distribution=group by, semantic=text search, pure advisory=insights only"
                            },
                            "filters": {
                                "type": "object",
                                "properties": filter_props,
                                "additionalProperties": False
                            },
                            "date_filter": {
                                "anyOf": [
                                    {
                                        "type": "object",
                                        "properties": {
                                            "start_date": {"anyOf": [{"type": "string", "format": "date"}, {"type": "null"}]},
                                            "end_date":   {"anyOf": [{"type": "string", "format": "date"}, {"type": "null"}]}
                                        },
                                        "required": ["start_date", "end_date"],
                                        "additionalProperties": False
                                    },
                                    {"type": "null"}
                                ]
                            },
                            "marketing_filter": {"type": ["boolean", "null"]},
                            "is_negation": {"type": "boolean"},
                            "semantic_terms": {
                                "type": "array",
                                "items": {"type": "string"}
                            },
                            "needs_data": {"type": "boolean"},
                            "pagination": {
                                "type": ["object", "null"],
                                "properties": {
                                    "skip":  {"type": "integer", "minimum": 0,  "default": 0},
                                    "limit": {"type": "integer", "minimum": 1,  "maximum": 200, "default": 50}
                                },
                                "required": [],
                                "additionalProperties": False
                            }
                        },
                        "required": ["route", "operation", "filters", "is_negation", "needs_data"]
                    }
                }
            }
        ]

        
        categories_context = json.dumps(categories, indent=2) 
        field_mappings_context = json.dumps(field_mappings, indent=2)
        today = datetime.today().strftime("%Y-%m-%d")


        # Refined system message
        system_message = f"""
You are a query parser for a content management system. 
Your job is to classify user queries into structured operations and extract parameters. 
Always return valid JSON that matches the provided schema, the categories you exrtact should match with the category values we have in our schema and always give 
priority to exact match to fuzzy match that is scrict rule.

RULES:
- If query explicitly mentions a **category name or field** (e.g., "funnel stage", "persona"), 
  set the operation to `distribution` and include **all values** of that category in `filters`.
- If the query uses negation (e.g., "not TOFU", "without investors"), 
  still include the positive category value in `filters` (e.g., ["TOFU"]) 
  and set `is_negation` = true. 
  ❌ Never use operators like $ne, exclude, not_in inside filters. 

- If query explicitly mentions a **category value** (e.g., "TOFU", "Investors"), 
  set the operation appropriately (`distribution` or `list`) and include that value under the correct category in `filters`.
- Parse natural language date ranges into start_date and end_date (YYYY-MM-DD).
- Detect negations (e.g., "missing", "without", "don’t have").
- Detect if query refers to marketing vs non-marketing content.
- Always map mentioned fields/terms to closest category or value from schema.
- Do not make categories on your own always try to match it with categories taken from schems.
- Reference date for all relative time expressions (like "last 6 months" or "more than 3 weeks ago") is {today}.
- "last N days/weeks/months" → start_date = today - N, end_date = today
- "more than N days/weeks/months ago" → start_date = None (or very old), end_date = today - N
- "between DATE1 and DATE2" → start_date = DATE1, end_date = DATE2
- Always return dates in YYYY-MM-DD format
- If only one date boundary is specified in the query, set the other to null
PAGINATION RULES:
- Output pagination as: { "pagination": { "skip": <int>, "limit": <int> } }.
- Default: skip=0, limit=50. Cap limit at 200.
- "top N", "first N", "show N", "limit N" → set limit=N (skip stays 0 unless page is specified).
- "page P of size S" or "page P, S per page" → limit=S, skip=(P-1)*S. (P is 1-based.)
- "page P" with no size → use default S=50 → limit=50, skip=(P-1)*50.
- "offset K" → skip=K, keep existing or default limit.
- If user asks for "all", set limit=200 (cap) unless business rules specify otherwise.
- Never encode pagination into filters; keep it only under "pagination".



CONTEXT:
- Categories: {categories_context}
- Field mappings: {field_mappings_context}

OPERATIONS:
- list → request to fetch content/items
- distribution → request to analyze proportions, balance, or breakdown of categories also trigger the filter field for example
  " show me X content distribution over Y content then trigger both those filters   "
- semantic → free-text or topical search when not directly tied to categories
- pure_advisory → high-level strategic advice with no database need
"""

        completion = self.client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": query_text}
            ],
            tools=tool_schema,
            tool_choice={"type": "function", "function": {"name": "parse_query"}},
            temperature=0
        )

        return json.loads(completion.choices[0].message.tool_calls[0].function.arguments)

    def clear_cache(self):
        """Clear schema cache if needed"""
        self._schema_cache.clear()


# Factory function
def create_smart_parser(mongo_uri: str, db_name: str) -> SmartQueryParser:
    return SmartQueryParser(mongo_uri, db_name)


# # Example usage
# if __name__ == "__main__":
#     parser = create_smart_parser("mongodb://localhost:27017", "my_database")
    
#     test_queries = [
#         # "what is the distribution of funnel stages",
#         # "what is the distribution of TOFU",
#         # "show all content for investors",
#         # "List all pages targeting Revenue Teams", 
#         # "List all pages with no assigned funnel stage",
#         # "What are the newest assets added in the last 30 days?",
#         # " What funnel stages do we have the least content for?"
#         # "show me bofu content distribution over videos",
#         # "list all the content that is not tofu",
#         # "List all MOFU pages created after January 1st, 2025",
#         # "Give me TOFU content published more than 6 months ago"
#         "show me all content in tofu funnel stage"
#     ]
    
#     tenant_id = "6875f3afc8337606d54a7f37"

#     for query in test_queries:
#         try:
#             result = parser.parse(query, tenant_id)
#             print(f"\n📝 Query: '{query}'")
#             print(f"   Route: {result.route}")
#             print(f"   Operation: {result.operation}")
#             print(f"   Filters: {result.filters}")
#             print(f"   Date Filter: {result.date_filter}")
#             print(f"   Marketing Filter: {result.marketing_filter}")
#             print(f"   Negation: {result.is_negation}")
#             print(f"   Semantic Terms: {result.semantic_terms}")
#             print(f"   Needs Data: {result.needs_data}")
#         except Exception as e:
#             print(f"Error parsing '{query}': {e}")
