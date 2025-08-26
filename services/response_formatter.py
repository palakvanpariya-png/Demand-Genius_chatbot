# Response formatting service

class ResponseFormatter:
    """Format ControlFlow responses for Streamlit display"""

def format_filtered_data_response(data: list, message: str):
    """Format response for filtered data queries"""

def format_analytics_response(insights: list, data: dict):
    """Format response for analytical queries"""

def format_advisory_response(recommendations: list):
    """Format response for advisory queries"""

def format_semantic_search_response(results: list, query: str):
    """Format response for semantic search queries"""

def format_chat_response(message: str):
    """Format response for general chat"""

def sanitize_response_for_tenant(response, tenant_id: str):
    """Ensure response doesn't contain other tenant data"""