# query_executor.py

from typing import Dict, Any
import json
from database.try_query_parser import parse_query_with_enhanced_tools

# Import database query functions
from database.queries import (
    fetch_content,
    fetch_content_by_filters,
    fetch_content_with_complex_filters,
    fetch_distribution_analysis,
    fetch_content_gap_analysis,
    search_content_by_text,
    fetch_content_count
)

def execute_query(parsed_result: Dict[str, Any]):
    """
    Execute database queries based on parsed_result from query_parser.
    """

    if not parsed_result.get("is_executable", False):
        return {"status": "not_executable", "reason": "Query is advisory or incomplete"}

    operation_type = parsed_result.get("operation_type")
    print(operation_type)
    params = parsed_result.get("database_params", {})

    # Pagination defaults
    limit = params.get("pagination", {}).get("limit", 30)
    skip = params.get("pagination", {}).get("skip", 0)

    if operation_type == "fetch_content":
        return fetch_content(params["tenant_id"], limit, skip)

    elif operation_type == "fetch_content_by_filters":
        return fetch_content_by_filters(
            params["tenant_id"],
            params["category_filters"],
            limit,
            skip
        )

    elif operation_type == "fetch_content_with_complex_filters":
        return fetch_content_with_complex_filters(
            params["tenant_id"],
            params["category_filters"],
            params["additional_filters"],
            limit,
            skip
        )

    elif operation_type == "fetch_distribution_analysis":
        return fetch_distribution_analysis(
            params["tenant_id"],
            params["aggregation_config"],
            params["category_filters"]
        )

    elif operation_type == "fetch_content_gap_analysis":
        return fetch_content_gap_analysis(params["tenant_id"], params["category_filters"])

    elif operation_type == "search_content_by_text":
        return search_content_by_text(
            params["tenant_id"],
            params["search_query"],
            limit,
            skip
        )

    elif operation_type == "fetch_content_count":
        return fetch_content_count(params["tenant_id"], params["category_filters"])

    else:
        return {"status": "error", "reason": f"Unknown operation_type: {operation_type}"}


# Example runner
if __name__ == "__main__":

    query = "what is distribution of funnel stage"
    # Fake parsed result (normally comes from query_parser)
    parsed_result = parse_query_with_enhanced_tools(query)
    print(json.dumps(parsed_result, indent=2))

    result = execute_query(parsed_result)
    print(result)
    # print("Execution Result:")
    # print(json.dumps(result, indent=2))