import os
import json
import time
import logging
from datetime import datetime
from openai import OpenAI
from dotenv import load_dotenv
from typing import Dict, List, Optional
from dataclasses import dataclass, field

load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

@dataclass
class QueryResult:
    route: str
    operation: str
    filters: Dict[str, Dict[str, List[str]]] 
    date_filter: Optional[Dict[str, str]]
    marketing_filter: Optional[bool]
    is_negation: bool 
    semantic_terms: List[str]
    tenant_id: str
    needs_data: bool
    distribution_fields: List[str] = field(default_factory=list)
    pagination: Dict[str, int] = field(default_factory=lambda: {"skip": 0, "limit": 50})  # ADD THIS LINE




class SmartQueryParser:
    def __init__(self, mongo_uri: str, db_name: str, openai_api_key: str = None):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.client = OpenAI(api_key=openai_api_key or os.getenv("OPENAI_API_KEY"))
        self._schema_cache = {}  # Cache schemas to avoid redundant calls
    
    def parse(self, query_text: str, tenant_id: str) -> QueryResult:
        """Parse query using OpenAI with full schema context"""
        # Input validation
        if not query_text or not query_text.strip():
            raise ValueError("query_text cannot be empty")
        if not tenant_id or not tenant_id.strip():
            raise ValueError("tenant_id cannot be empty")
            
        schema_data = self._get_cached_schema(tenant_id)
        parsed = self._ai_parse(query_text, schema_data)

        filters = parsed.get("filters", {})

        # Backward-compatible: if filters are list-style, wrap into {"include": ...}
        for cat, val in list(filters.items()):
            if isinstance(val, list):
                filters[cat] = {"include": val, "exclude": []}
            elif isinstance(val, dict):
                filters[cat].setdefault("include", [])
                filters[cat].setdefault("exclude", [])
            else:
                filters[cat] = {"include": [str(val)], "exclude": []}

        pagination_data = parsed.get("pagination", {"skip": 0, "limit": 50})
        if pagination_data is None:
            pagination_data = {"skip": 0, "limit": 50}

        return QueryResult(
            route=parsed["route"],
            operation=parsed["operation"],
            filters=filters,
            date_filter=parsed.get("date_filter"),
            marketing_filter=parsed.get("marketing_filter"),
            is_negation=parsed.get("is_negation", False),
            semantic_terms=parsed.get("semantic_terms", []),
            tenant_id=tenant_id,
            needs_data=parsed.get("needs_data", False),
            distribution_fields=parsed.get("distribution_fields", []),
            pagination=pagination_data  # ADD THIS LINE
        )
    
    def _get_cached_schema(self, tenant_id: str) -> Dict:
        """Get schema with caching to avoid redundant database calls"""
        if tenant_id not in self._schema_cache:
            from schema_extractor import get_tenant_schema
            schema_data = get_tenant_schema(self.mongo_uri, self.db_name, tenant_id)
            if not schema_data:
                raise ValueError(f"Tenant {tenant_id} not found")
            
            # Validate schema structure
            if not isinstance(schema_data.get("categories", {}), dict):
                raise ValueError(f"Invalid schema structure for tenant {tenant_id}: categories must be dict")
                
            self._schema_cache[tenant_id] = schema_data
        return self._schema_cache[tenant_id]
    
    def _get_fallback_response(self, query_text: str) -> Dict:
        return {
            "route": "semantic",
            "operation": "semantic",
            "filters": {},
            "date_filter": None,
            "marketing_filter": None,
            "is_negation": False,
            "semantic_terms": [query_text],
            "needs_data": False,
            "distribution_fields": [],
            "pagination": {"skip": 0, "limit": 50}  # ADD THIS LINE
        }

    
    def _handle_large_schema(self, query_text: str, schema_data: Dict) -> Dict:
        """Handle queries for tenants with large schemas (>5000 values)"""
        logger.info(f"Using fallback for large schema with {schema_data.get('summary', {}).get('total_values', 0)} values")
        return self._get_fallback_response(query_text)
    
    def _ai_parse(self, query_text: str, schema_data: Dict) -> Dict:
        """Use OpenAI to parse natural language query into structured fields with retry logic"""
        
        # Check for large schema safeguard
        summary = schema_data.get("summary", {})
        total_values = summary.get("total_values", 0)
        
        if total_values > 5000:
            logger.info(f"Schema too large ({total_values} values), using dynamic handler")
            return self._handle_large_schema(query_text, schema_data)
        
        # Proceed with normal schema building for smaller schemas
        categories = schema_data.get("categories", {}) 
        field_mappings = schema_data.get("field_mappings", {})

        # Build dynamic schema from tenant metadata - include all categories for negation support
        filter_props = {} 
        for cat, values in categories.items():
            # Include categories even if they have empty values (for negation queries)
            filter_props[cat] = { 
                "type": "array", 
                "items": {"type": "string", "enum": values if values else []}, 
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
                                "enum": ["list", "distribution", "semantic", "pure_advisory"],
                                "description": "list=show items, distribution=group by, semantic=text search, pure advisory=insights only"
                            },
                            "filters": {
                                "type": "object",
                                "properties": {
                                    cat: {
                                        "type": "object",
                                        "properties": {
                                            "include": {
                                                "type": "array",
                                                "items": {"type": "string", "enum": values if values else []}
                                            },
                                            "exclude": {
                                                "type": "array",
                                                "items": {"type": "string", "enum": values if values else []}
                                            }
                                        },
                                        "additionalProperties": False
                                    }
                                    for cat, values in categories.items()
                                },
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
                                    "skip":  {"type": "integer", "minimum": -2, "default": 0},  # Allow -1, -2
                                    "limit": {"type": "integer", "minimum": 0,  "maximum": 200, "default": 50}
                                },
                                "required": [],
                                "additionalProperties": False
                            },
                            "distribution_fields": {
                                "type": "array",
                                "items": {
                                    "type": "string",
                                    "enum": list(categories.keys())  # Only allow valid category names
                                },
                                "description": "List of categories to group by for multi-dim distributions. Must be from available categories."
                            },
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
You are a query parser for a content management system. Parse user queries into structured JSON operations matching the provided schema.

**CORE RULES:**
- Always match to existing categories/values - never create new ones
- Prioritize exact matches, but attempt semantic matching for unmatched terms
- ALWAYS return exact schema values, regardless of input format variations
- Return valid JSON matching the schema


**OPERATIONS:**
- `list` → fetch content/items
- `distribution` → analyze proportions/breakdowns of categories  
- `semantic` → free-text search not tied to categories
- `pure_advisory` → strategic advice requiring no database

**KEY LOGIC:**
- Category name mentioned → `distribution` with all category values in filters
- Category value mentioned → appropriate operation with that value filtered  
- Advisory questions needing data → use `distribution` operation
- Marketing detection → set `marketing_filter: true/false` based on if the query is related to marketing related context or explicitly says marketing content
- Negation ("not X", "without Y") → `"is_negation": true` + exclude arrays
- "Distribution of x"-> "distribution_fields": ["X"] or "Distribution of X across Y" → `"distribution_fields": ["X", "Y"]`

**DATES:** Reference: {today}
- "last N days/weeks/months" → start_date = today - N, end_date = today
- "more than N ago" → end_date = today - N, start_date = null
- Format: YYYY-MM-DD

**PAGINATION:** Default: skip=0, limit=50
- "top N" → limit = N
- "page P" → skip = (P-1)*50, limit = 50  
- "last N" → skip = -1, limit = N
- "count only" → skip = -2, limit = 0
- "all" → limit = 200

**CONTEXT:**
- Categories: {categories_context}
- Field mappings: {field_mappings_context}
"""



        # Retry logic with exponential backoff
        max_retries = 3
        base_delay = 1  # Start with 1 second delay
        
        for attempt in range(max_retries):
            try:
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
                
                # Parse and return the result
                result = json.loads(completion.choices[0].message.tool_calls[0].function.arguments)
                return result
                
            except json.JSONDecodeError as e:
                logger.error(f"JSON parse error on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    logger.warning("AI fallback triggered due to JSON parse errors")
                    return self._get_fallback_response(query_text)
            except Exception as e:
                # Log the specific error type for better debugging
                error_type = type(e).__name__
                logger.error(f"API error ({error_type}) on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    logger.warning("AI fallback triggered due to API errors")
                    return self._get_fallback_response(query_text)
            
            # Exponential backoff: wait 1s, 2s, 4s between retries
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                time.sleep(delay)
        
        # This should never be reached, but included for safety
        logger.warning("AI fallback triggered - unexpected code path")
        return self._get_fallback_response(query_text)

    def clear_cache(self):
        """Clear schema cache if needed"""
        self._schema_cache.clear()


# Factory function
def create_smart_parser(mongo_uri: str, db_name: str) -> SmartQueryParser:
    return SmartQueryParser(mongo_uri, db_name)


# Example usage
# if __name__ == "__main__":
#     parser = create_smart_parser("mongodb://localhost:27017", "my_database")
    
#     test_queries = [
#         # "what is the distribution of funnel stages",
#         # "what is the distribution of TOFU",
#         # "show all content for investors",
#         # "List all pages targeting Revenue Teams", 
#         # "List all pages with no assigned funnel stage",
#         # "What are the newest assets added in the last 30 days?",
#         # " What funnel stages do we have the least content for?",
#         # "show me bofu content distribution over videos",
#         # "list all the content that is not tofu",
#         # "List all MOFU pages created after January 1st, 2025",
#         # "show me all content in tofu content which is not blogs",
#         # "Give me TOFU content published more than 6 months ago",
#         # "Hello what can you help me with"
#         # "Give me blog posts tagged Marketing and Demand Generation that are in German",
#         # "List all pages with no assigned funnel stage",
#         # "What German language MOFU content … focuses on customer acquisition",
#         # "List pages tagged as Thought Leadership but not gated",
#         # "show me last 100 tofu content"
       
#     ]
    
#     tenant_id = "6875f3afc8337606d54a7f37"

#     for query in test_queries:
#         try:
#             result = parser.parse(query, tenant_id)
#             # print(result)
#             print(f"\n📝 Query: '{query}'")
#             print(f"   Route: {result.route}")
#             print(f"   Operation: {result.operation}")
#             print(f"   Filters: {result.filters}")
#             print(f"   Date Filter: {result.date_filter}")
#             print(f"   Marketing Filter: {result.marketing_filter}")
#             print(f"   Negation: {result.is_negation}")
#             print(f"   Semantic Terms: {result.semantic_terms}")
#             print(f"   Needs Data: {result.needs_data}")
#             print(f"   Pagination: {result.pagination}")
#             print(f"   Distribution fields: {result.distribution_fields}")
#         except Exception as e:
#             print(f"Error parsing '{query}': {e}")
