import os
import json
from openai import OpenAI
from rapidfuzz import fuzz
from dotenv import load_dotenv
from database.database_schema import get_tenant_categories_and_mappings

load_dotenv()

# Load API Key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)

# ----------------------------
# Dynamic Schema Integration
# ----------------------------
def get_tenant_data(tenant_id: str = "6875f3afc8337606d54a7f37"):
    """Get dynamic tenant categories and field mappings"""
    try:
        categories, field_mappings = get_tenant_categories_and_mappings(tenant_id)
        return categories, field_mappings
    except Exception as e:
        print(f"Error loading tenant data: {e}")
        return {}, {}

# ----------------------------
# Enhanced Tools Schema for Structured Query Routing
# ----------------------------
def build_schema(categories):
    """Build schema for query parsing with routing information"""
    filters_properties = {
        cat: {
            "type": "array",
            "items": {
                "type": "string",
                "enum": categories[cat]
            }
        }
        for cat in categories.keys()
    }

    schema = [
        {
            "type": "function",
            "function": {
                "name": "parse_query",
                "description": "Parse query and determine execution strategy",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "classification": {"type": "string", "enum": ["structured", "non_structured"]},
                        "is_executable": {"type": "boolean"},
                        "execution_strategy": {"type": "string", "enum": ["database_query", "llm_advisory", "hybrid"]},
                        "category_filters": {"type": "object", "properties": filters_properties, "additionalProperties": False},
                        "additional_filters": {"type": "object", "properties": {"geoFocus": {"type": "string"}, "isMarketingContent": {"type": "boolean"}}, "additionalProperties": True},
                        "search_query": {"type": "string"},
                        "pagination": {"type": "object", "properties": {"limit": {"type": "integer", "default": 30}, "skip": {"type": "integer", "default": 0}}},
                        "operation_type": {"type": "string", "enum": ["fetch_content", "fetch_content_by_filters", "fetch_with_complex_filters", "distribution_analysis", "gap_analysis", "text_search", "fetch_content_count"]},
                        "user_intent": {"type": "string", "enum": ["list_content", "analyze_distribution", "find_gaps", "search_text", "get_recommendations", "strategic_advice", "performance_review"]},
                        "response_expectation": {"type": "string", "enum": ["data_list", "statistics", "insights", "recommendations", "advisory"]},
                        "quoted_entities": {"type": "array", "items": {"type": "string"}},
                        "advisory_context": {"type": "object", "properties": {"business_question": {"type": "string"}, "requires_domain_expertise": {"type": "boolean"}, "related_data_points": {"type": "array", "items": {"type": "string"}}}}
                    },
                    "required": ["classification", "is_executable", "execution_strategy", "category_filters", "additional_filters", "operation_type", "user_intent"]
                }
            }
        }
    ]
    return schema

# ----------------------------
# Fuzzy Matching
# ----------------------------
def intelligent_fuzzy_matching(query_text, categories, threshold=80):
    """Enhanced fuzzy matching for category values"""
    matches = {}
    query_lower = query_text.lower()

    for category, values in categories.items():
        category_matches = []
        for value in values:
            if value.lower() in query_lower:
                category_matches.append(value)
                continue

            score = fuzz.partial_ratio(value.lower(), query_lower)
            if score >= threshold:
                category_matches.append(value)
        if category_matches:
            matches[category] = list(set(category_matches))

    return matches

# ----------------------------
# Post-Processing
# ----------------------------
def normalize_filters(filters: dict) -> dict:
    """Ensure every filter value is always a list of strings"""
    if not filters:
        return {}
    normalized = {}
    for key, vals in filters.items():
        if not vals:
            continue
        normalized[key] = vals if isinstance(vals, list) else [vals]
    return normalized

def enhanced_post_processing(parsed_data, categories, field_mappings):
    query_text = parsed_data.get("query_text", "")
    quoted_entities = parsed_data.get("quoted_entities", [])

    category_filters = normalize_filters(parsed_data.get("category_filters", {}))
    for entity in quoted_entities:
        for cat, values in categories.items():
            if entity in values and entity not in category_filters.get(cat, []):
                category_filters.setdefault(cat, []).append(entity)

    fuzzy_matches = intelligent_fuzzy_matching(query_text, categories)
    for category, matched_values in fuzzy_matches.items():
        for value in matched_values:
            if value not in category_filters.get(category, []):
                category_filters.setdefault(category, []).append(value)

    additional_filters = parsed_data.get("additional_filters", {})
    search_query = parsed_data.get("search_query", "")

    database_params = {
        "tenant_id": parsed_data.get("tenant_id"),
        "category_filters": normalize_filters(category_filters),
        "additional_filters": additional_filters,
        "search_query": search_query,
        "pagination": parsed_data.get("pagination", {"limit": 30, "skip": 0}),
        "operation_type": parsed_data.get("operation_type"),
        "aggregation_config": parsed_data.get("aggregation_config", {})
    }

    parsed_data.update({
        "category_filters": normalize_filters(category_filters),
        "additional_filters": additional_filters,
        "search_query": search_query,
        "database_params": database_params,
        "field_mappings": field_mappings
    })

    return parsed_data

# ----------------------------
# Main Query Parser
# ----------------------------
def parse_query_with_enhanced_tools(query_text, tenant_id: str = "6875f3afc8337606d54a7f37"):
    tenant_categories, field_mappings = get_tenant_data(tenant_id)
    if not tenant_categories:
        raise ValueError(f"No categories found for tenant {tenant_id}")

    tools_schema = build_schema(tenant_categories)
    system_message = f"""You are an advanced query parser for a content analytics system.  
Available categories: {list(tenant_categories.keys())}.  

Your job is to:  
1. Classify the query (structured or advisory).  
2. Extract **all possible filters** from the query by matching against available categories.  
   - If a query term matches a category (e.g., "gaming" → Topics, "white paper" → Content Type), always map it there. even if the exact match is not there if it matches the context you should match it   
   - If no category matches, put the term in `search_query` instead.  
3. Decide the correct `operation_type` using the rules below:  

- Use `fetch_content` → if no filters are provided.  
- Use `fetch_content_by_filters` → if there is only one filter category (e.g. just Funnel Stage, just Topics, etc.).  
- Use `fetch_content_with_complex_filters` → if there are multiple different filter categories (e.g. Funnel Stage + Topics).  
- Use `fetch_distribution_analysis` → if the query is about "distribution" or "breakdown". and it should trigger on which filter we have to do analysis.  
- Use `fetch_content_gap_analysis` → if the query is about "missing", "gaps", or "coverage".  
- Use `search_content_by_text` → if the query contains free-text terms that do not map to known categories.  
- Use `fetch_content_count` → if the query asks for counts, **but still include all extracted filters**.  

STRICT RULES:  
- Always extract filters first, then determine `operation_type`.  
- If unsure, return empty dicts instead of inventing values.  
"""

    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_message},
            {"role": "user", "content": query_text}
        ],
        tools=tools_schema,
        tool_choice={"type": "function", "function": {"name": "parse_query"}}
    )

    tool_output = completion.choices[0].message.tool_calls[0].function.arguments
    parsed_data = json.loads(tool_output)
    parsed_data["query_text"] = query_text
    parsed_data["tenant_id"] = tenant_id

    return enhanced_post_processing(parsed_data, tenant_categories, field_mappings)

# ----------------------------
# Example Usage
# ----------------------------
if __name__ == "__main__":
    tenant_id = "6875f3afc8337606d54a7f37"
    test_queries = ["Show me all MOFU content"]

    print(f"Testing Enhanced Query Parser with tenant: {tenant_id}")
    print("="*80)

    for query in test_queries:
        print(f"\nQuery: {query}")
        print("-"*50)
        try:
            result = parse_query_with_enhanced_tools(query, tenant_id)
            print(json.dumps(result, indent=2))
        except Exception as e:
            print(f"Error: {e}")
