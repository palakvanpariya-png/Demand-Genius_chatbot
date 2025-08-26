# Security and validation utilities

# def validate_tenant_access(tenant_id: str):
#     """Validate tenant access permissions"""

# def sanitize_input(query: str):
#     """Sanitize user input for security"""

# def check_prompt_injection(query: str):
#     """Check for potential prompt injection attempts"""

# def validate_database_query(query_params: dict):
#     """Validate database query parameters"""

# def ensure_tenant_isolation(tenant_id: str, data: dict):
#     """Ensure data belongs to correct tenant"""

from bson import ObjectId
from database.connection import get_database
import re


def sanitize_input(query: str) -> str:
    """
    Sanitize user input for security.
    - Strip leading/trailing spaces
    - Remove dangerous characters (basic sanitization)
    - Prevent MongoDB/SQL injections by blocking $ operators and semicolons
    """
    if not isinstance(query, str):
        return ""

    # Basic cleanup
    cleaned = query.strip()

    # Remove Mongo/Mongo operators like $ne, $or, etc.
    cleaned = re.sub(r"\$[a-zA-Z0-9_]+", "", cleaned)

    # Remove semicolons or other potential injection symbols
    cleaned = cleaned.replace(";", "")

    return cleaned


def validate_tenant_access(tenant_id) -> bool:
    """
    Validate tenant access.
    Checks if tenant exists in the database.
    Handles both string and ObjectId input.
    """
    db = get_database()

    try:
        # Normalize tenant_id
        if isinstance(tenant_id, str):
            tenant_id = ObjectId(tenant_id)

        tenant = db.sitemaps.find_one({"tenant": tenant_id})
        return tenant is not None
    except Exception:
        return False
