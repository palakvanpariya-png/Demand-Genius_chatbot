# controlflow_core/tools.py - ControlFlow Tools that wrap database functions

import controlflow as cf
from typing import Dict, List, Any, Optional, Union
from database.queries import (
    fetch_content,
    fetch_content_by_filters, 
    fetch_content_with_complex_filters,
    fetch_distribution_analysis,
    fetch_content_gap_analysis,
    search_content_by_text,
    fetch_content_count
)
from database.extractor import DynamicTenantSchemaExtractor
from utils.logger import get_logger

logger = get_logger("controlflow_tools")


@cf.tool
def get_tenant_schema_info(tenant_id: str) -> Dict[str, Any]:
    """
    Get available categories, fields, and values for the tenant.
    Use this to understand what data is available for filtering and analysis.
    """
    try:
        extractor = DynamicTenantSchemaExtractor(tenant_id)
        schema = extractor.extract_schema()
        
        return {
            "tenant_id": tenant_id,
            "available_categories": list(schema.categories.keys()),
            "category_values": schema.categories,
            "total_categories": len(schema.categories),
            "field_mappings": {k: v.category_name for k, v in schema.field_mappings.items()},
            "collections_available": list(schema.collections_info.keys())
        }
    except Exception as e:
        logger.error(f"Failed to get schema info: {e}")
        return {"error": str(e), "tenant_id": tenant_id}


@cf.tool
def fetch_basic_content(tenant_id: str, limit: int = 20, skip: int = 0) -> Dict[str, Any]:
    """
    Fetch basic content for the tenant with all relationships resolved.
    Use this for queries like 'show me some content' or 'list recent pages'.
    """
    try:
        results = fetch_content(tenant_id, limit=limit, skip=skip)
        
        return {
            "data": results,
            "count": len(results),
            "limit": limit,
            "skip": skip,
            "operation": "basic_content_fetch"
        }
    except Exception as e:
        logger.error(f"Failed to fetch basic content: {e}")
        return {"error": str(e), "operation": "basic_content_fetch"}


@cf.tool
def filter_content_by_categories(
    tenant_id: str, 
    category_filters: Dict[str, Union[str, List[str]]], 
    limit: int = 20,
    skip: int = 0
) -> Dict[str, Any]:
    """
    Filter content by one or more category values.
    
    Examples:
    - category_filters = {"Funnel Stage": "TOFU"}
    - category_filters = {"Funnel Stage": "TOFU", "Primary Audience": "Individual Investors"}
    - category_filters = {"Page Type": ["Product Page", "Legal Page"]}
    """
    try:
        results = fetch_content_by_filters(
            tenant_id=tenant_id,
            filters=category_filters,
            limit=limit,
            skip=skip
        )
        
        return {
            "data": results,
            "count": len(results),
            "filters_applied": category_filters,
            "limit": limit,
            "operation": "filtered_content_fetch"
        }
    except Exception as e:
        logger.error(f"Failed to filter content: {e}")
        return {"error": str(e), "filters_applied": category_filters}


@cf.tool
def filter_content_with_complex_criteria(
    tenant_id: str,
    category_filters: Dict[str, List[str]],
    additional_filters: Dict[str, Any] = None,
    limit: int = 20
) -> Dict[str, Any]:
    """
    Handle complex multi-category filtering with additional criteria.
    Use this for queries like 'TOFU Product Pages in Financial Services for Individual Investors'.
    
    Args:
        category_filters: {"Funnel Stage": ["TOFU"], "Page Type": ["Product Page"]}
        additional_filters: {"Language": "English", "isMarketingContent": True}
    """
    try:
        if additional_filters is None:
            additional_filters = {}
            
        results = fetch_content_with_complex_filters(
            tenant_id=tenant_id,
            category_filters=category_filters,
            additional_filters=additional_filters,
            limit=limit
        )
        
        return results
    except Exception as e:
        logger.error(f"Failed complex filtering: {e}")
        return {"error": str(e), "category_filters": category_filters}


@cf.tool
def count_content_by_criteria(
    tenant_id: str, 
    filters: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Count content matching specific criteria.
    Use this for queries like 'How many TOFU articles?' or 'Count of Product Pages'.
    """
    try:
        count = fetch_content_count(tenant_id, filters)
        
        return {
            "count": count,
            "filters": filters or "no filters",
            "operation": "content_count"
        }
    except Exception as e:
        logger.error(f"Failed to count content: {e}")
        return {"error": str(e), "filters": filters}


@cf.tool
def analyze_content_distribution(
    tenant_id: str,
    primary_field: str,
    secondary_field: Optional[str] = None,
    include_examples: bool = True
) -> Dict[str, Any]:
    """
    Analyze distribution of content across categories.
    
    Use this for queries like:
    - 'Funnel stage distribution' (primary_field = "Funnel Stage")
    - 'Industry breakdown by audience' (primary_field = "Industry", secondary_field = "Primary Audience")
    
    Available fields: Funnel Stage, Page Type, Primary Audience, Secondary Audience, Industry, Topic, Language
    """
    try:
        results = fetch_distribution_analysis(
            tenant_id=tenant_id,
            primary_field=primary_field,
            secondary_field=secondary_field,
            include_examples=include_examples
        )
        
        return results
    except Exception as e:
        logger.error(f"Failed distribution analysis: {e}")
        return {"error": str(e), "primary_field": primary_field}


@cf.tool
def analyze_content_gaps(
    tenant_id: str,
    primary_dimension: str,
    secondary_dimension: Optional[str] = None
) -> Dict[str, Any]:
    """
    Identify content gaps and provide strategic recommendations.
    
    Use this for queries like:
    - 'Content gap analysis'
    - 'What are we missing in our funnel?'
    - 'Are we focused too much on TOFU?'
    
    Available dimensions: Funnel Stage, Page Type, Primary Audience, Secondary Audience, Industry
    """
    try:
        results = fetch_content_gap_analysis(
            tenant_id=tenant_id,
            primary_dimension=primary_dimension,
            secondary_dimension=secondary_dimension
        )
        
        return results
    except Exception as e:
        logger.error(f"Failed gap analysis: {e}")
        return {"error": str(e), "primary_dimension": primary_dimension}


@cf.tool  
def search_content_by_text(
    tenant_id: str,
    search_query: str,
    search_fields: List[str] = ["description", "summary", "readerBenefit", "name"],
    include_category_search: bool = True,
    include_tag_search: bool = True,
    limit: int = 15
) -> Dict[str, Any]:
    """
    Search content using text matching across content fields and categories.
    
    Use this for queries like:
    - 'Content about investment tools'
    - 'Articles mentioning crypto'
    - 'Pages related to financial planning'
    
    The function searches across:
    - Content descriptions, summaries, and titles
    - Category names (Funnel Stage, Industry, etc.)
    - Tag names
    """
    try:
        results = search_content_by_text(
            tenant_id=tenant_id,
            search_query=search_query,
            search_fields=search_fields,
            include_category_search=include_category_search,
            include_tag_search=include_tag_search,
            limit=limit
        )
        
        return results
    except Exception as e:
        logger.error(f"Failed text search: {e}")
        return {"error": str(e), "search_query": search_query}


@cf.tool
def validate_category_values(tenant_id: str, category_name: str, values: List[str]) -> Dict[str, Any]:
    """
    Validate if category values exist in the tenant's schema.
    Use this when you're unsure if user-mentioned categories or values are valid.
    """
    try:
        extractor = DynamicTenantSchemaExtractor(tenant_id)
        schema = extractor.extract_schema()
        
        if category_name not in schema.categories:
            return {
                "valid": False,
                "error": f"Category '{category_name}' not found",
                "available_categories": list(schema.categories.keys())
            }
        
        valid_values = []
        invalid_values = []
        available_values = schema.categories[category_name]
        
        for value in values:
            if value in available_values:
                valid_values.append(value)
            else:
                invalid_values.append(value)
        
        # Suggest similar values for invalid ones
        suggestions = {}
        for invalid_value in invalid_values:
            similar = [v for v in available_values if invalid_value.lower() in v.lower() or v.lower() in invalid_value.lower()]
            if similar:
                suggestions[invalid_value] = similar[:3]
        
        return {
            "valid": len(invalid_values) == 0,
            "category_name": category_name,
            "valid_values": valid_values,
            "invalid_values": invalid_values,
            "suggestions": suggestions,
            "available_values": available_values
        }
    except Exception as e:
        logger.error(f"Failed to validate category values: {e}")
        return {"error": str(e), "category_name": category_name}


@cf.tool
def get_content_summary_stats(tenant_id: str) -> Dict[str, Any]:
    """
    Get high-level summary statistics about the tenant's content.
    Use this for general overview queries or when user asks 'what data do you have?'
    """
    try:
        # Get basic counts
        total_count = fetch_content_count(tenant_id)
        
        # Get funnel distribution
        funnel_dist = analyze_content_distribution(tenant_id, "Funnel Stage", include_examples=False)
        
        # Get schema info
        schema_info = get_tenant_schema_info(tenant_id)
        
        return {
            "total_content": total_count,
            "funnel_distribution": funnel_dist.get("distribution", []) if "error" not in funnel_dist else [],
            "available_categories": schema_info.get("available_categories", []),
            "summary_generated": True,
            "operation": "content_summary"
        }
    except Exception as e:
        logger.error(f"Failed to generate summary stats: {e}")
        return {"error": str(e), "operation": "content_summary"}


# Tool registry for easy access
AVAILABLE_TOOLS = [
    get_tenant_schema_info,
    fetch_basic_content,
    filter_content_by_categories,
    filter_content_with_complex_criteria,
    count_content_by_criteria,
    analyze_content_distribution,
    analyze_content_gaps,
    search_content_by_text,
    validate_category_values,
    get_content_summary_stats
]


def get_tools_for_query_type(query_type: str) -> List:
    """Return relevant tools based on query type for optimized agent performance"""
    
    tool_mapping = {
        "SIMPLE_FILTER": [
            fetch_basic_content,
            filter_content_by_categories,
            validate_category_values,
            get_tenant_schema_info
        ],
        "COMPLEX_FILTER": [
            filter_content_with_complex_criteria,
            filter_content_by_categories,
            validate_category_values,
            get_tenant_schema_info
        ],
        "COUNT_ANALYTICS": [
            count_content_by_criteria,
            analyze_content_distribution,
            get_content_summary_stats,
            validate_category_values
        ],
        "DISTRIBUTION_ANALYTICS": [
            analyze_content_distribution,
            get_content_summary_stats,
            filter_content_by_categories,
            validate_category_values
        ],
        "STRATEGIC_ANALYSIS": [
            analyze_content_gaps,
            analyze_content_distribution,
            get_content_summary_stats,
            filter_content_by_categories
        ],
        "SEARCH": [
            search_content_by_text,
            filter_content_by_categories,
            validate_category_values,
            get_tenant_schema_info
        ],
        "GENERAL_CHAT": [
            get_tenant_schema_info,
            get_content_summary_stats,
            fetch_basic_content,
            validate_category_values
        ]
    }
    
    return tool_mapping.get(query_type, AVAILABLE_TOOLS)