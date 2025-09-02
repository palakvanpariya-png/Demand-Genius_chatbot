from dataclasses import dataclass
from typing import Dict, List, Optional

# --- Mock QueryResult to simulate parser output ---
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
    distribution_fields: Optional[List[str]] = None
    pagination: Optional[Dict[str, int]] = None


# --- Import your executor ---
from query_builder import create_mongo_executor   # adjust if file name differs

executor = create_mongo_executor("mongodb://localhost:27017", "test_db")

# Instead of mocking return values, let’s print the queries
def debug_fetch_content_by_filters(tenant_id, filters, date_filter=None,
                                   marketing_filter=None, is_negation=False,
                                   skip=0, limit=50):
    print("\n--- fetch_content_by_filters called ---")
    print("Tenant:", tenant_id)
    print("Filters:", filters)
    print("Date filter:", date_filter)
    print("Marketing filter:", marketing_filter)
    print("Negation:", is_negation)
    print("Skip:", skip, "Limit:", limit)
    return {"query": "built filter query (mocked)"}

def debug_fetch_content_by_semantic_search(tenant_id, search_terms, additional_filters=None):
    print("\n--- fetch_content_by_semantic_search called ---")
    print("Tenant:", tenant_id)
    print("Search terms:", search_terms)
    print("Additional filters:", additional_filters)
    return {"query": "built semantic query (mocked)"}

def debug_fetch_content_by_distribution(tenant_id, category, values=None, additional_filters=None):
    print("\n--- fetch_content_by_distribution called ---")
    print("Tenant:", tenant_id)
    print("Category:", category)
    print("Values:", values)
    print("Additional filters:", additional_filters)
    return {"query": f"built distribution query on {category} (mocked)"}

# Patch executor methods with debug versions
executor.fetch_content_by_filters = debug_fetch_content_by_filters
executor.fetch_content_by_semantic_search = debug_fetch_content_by_semantic_search
executor.fetch_content_by_distribution = debug_fetch_content_by_distribution
executor.fetch_count_only = lambda *a, **kw: print("\n--- fetch_count_only called ---") or 123


# --- Example Queries ---
qr1 = QueryResult(
    route="database",
    operation="list",
    filters={"Funnel Stage": {"include": ["TOFU"], "exclude": []}},
    date_filter={"start_date": "2024-01-01", "end_date": "2024-12-31"},
    marketing_filter=True,
    is_negation=False,
    semantic_terms=[],
    tenant_id="dummy_id",
    needs_data=True,
    pagination={"skip": 0, "limit": 20}
)

qr2 = QueryResult(
    route="database",
    operation="semantic",
    filters={},
    date_filter=None,
    marketing_filter=None,
    is_negation=False,
    semantic_terms=["AI tools"],
    tenant_id="dummy_id",
    needs_data=True,
)

qr3 = QueryResult(
    route="database",
    operation="distribution",
    filters={"Content Type": {"include": ["Blog"], "exclude": []}},
    date_filter=None,
    marketing_filter=None,
    is_negation=False,
    semantic_terms=[],
    tenant_id="dummy_id",
    needs_data=True,
    distribution_fields=["Funnel Stage", "Content Type"]
)

qr4 = QueryResult(
    route="database",
    operation="list",
    filters={},
    date_filter=None,
    marketing_filter=None,
    is_negation=False,
    semantic_terms=[],
    tenant_id="dummy_id",
    needs_data=True,
    pagination={"skip": -2, "limit": 0}
)


# --- Run and see queries ---
print("\n>>> LIST query")
executor.execute_parsed_query(qr1)

print("\n>>> SEMANTIC query")
executor.execute_parsed_query(qr2)

print("\n>>> DISTRIBUTION query")
executor.execute_parsed_query(qr3)

print("\n>>> COUNT-only query")
executor.execute_parsed_query(qr4)
