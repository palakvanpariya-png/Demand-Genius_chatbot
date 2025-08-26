# controlflow_core/agent.py - Main ControlFlow Agent Implementation

import controlflow as cf
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from database.extractor import DynamicTenantSchemaExtractor
from utils.logger import get_logger


logger = get_logger("controlflow_agent")


class QueryContext(BaseModel):
    """Context information for query processing"""
    tenant_id: str 
    user_query: str
    query_type: Optional[str] = None
    extracted_entities: Dict[str, Any] = Field(default_factory=dict)
    confidence_score: float = 0.0


class QueryResponse(BaseModel):
    """Structured response from the agent"""
    response_type: str  # "filtered_data", "analytics", "advisory", "semantic", "chat"
    message: str        # Human-readable response
    data: Optional[List[Dict]] = None      # Structured data if applicable
    insights: Optional[List[str]] = None   # Key insights for analytics
    recommendations: Optional[List[str]] = None  # Advisory suggestions
    metadata: Optional[Dict] = None        # Additional context
    query_info: Optional[Dict] = None      # Query processing info


def create_tenant_agent(tenant_id: str) -> cf.Agent:
    """
    Create a specialized ControlFlow agent for a specific tenant
    """
    # Get tenant schema for dynamic instructions
    try:
        extractor = DynamicTenantSchemaExtractor(tenant_id)
        tenant_schema = extractor.extract_schema()
        available_categories = list(tenant_schema.categories.keys())
        category_values = {k: v[:5] for k, v in tenant_schema.categories.items()}  # Limit for prompt
    except Exception as e:
        logger.warning(f"Failed to load schema for tenant {tenant_id}: {e}")
        available_categories = ["Page Type", "Funnel Stage", "Primary Audience"]
        category_values = {}

    agent_instructions = f"""
You are a specialized Content Intelligence Assistant for tenant {tenant_id}.

CAPABILITIES:
- Content filtering and retrieval with complex multi-field queries
- Statistical analysis and distribution reporting
- Strategic content analysis and recommendations  
- Text-based search across content descriptions and categories
- Schema-aware query interpretation with friendly names

AVAILABLE DATA CATEGORIES:
{', '.join(available_categories)}

SAMPLE CATEGORY VALUES:
{category_values}

SECURITY & TENANT ISOLATION:
- CRITICAL: Only access data for tenant {tenant_id}
- All database operations must include proper tenant scoping
- Validate all user inputs for security issues
- Never access or reference data from other tenants

QUERY PROCESSING APPROACH:
1. Classify the user's query type (filter/analytics/advisory/search/chat)
2. Extract relevant entities and map them to available categories
3. Select appropriate tools and execute with proper tenant scoping
4. Format response based on query type and user expectations
5. Always provide context about data scope and any limitations

RESPONSE FORMATS BY TYPE:
- FILTERED_DATA: Return structured content with friendly field names and full relationship resolution
- ANALYTICS: Provide counts, distributions, and key insights with supporting examples
- ADVISORY: Give strategic recommendations with supporting data and clear reasoning
- SEARCH: Return relevant content ranked by relevance with match explanations
- CHAT: Provide helpful information about capabilities, data, or general assistance

ENTITY EXTRACTION RULES:
- Map user terms to schema categories (e.g., "TOFU content" → Funnel Stage: TOFU)
- Handle multiple filters (e.g., "TOFU Product Pages" → Funnel Stage: TOFU, Page Type: Product Page)
- Recognize search intent vs. filtering intent
- Identify analytical questions vs. data retrieval questions

QUALITY GUIDELINES:
- Always explain what data you're analyzing and any limitations
- Provide specific numbers and examples when possible
- If results are empty or limited, suggest alternative queries
- Use friendly, professional tone while being technically accurate
- When making recommendations, explain the reasoning behind them
"""

    return cf.Agent(
        name=f"ContentIntelligence_{tenant_id}",
        instructions=agent_instructions,
        model='openai/gpt-4o-mini',  # Use GPT-4 for better reasoning
    )


class QueryClassifier:
    """Classify user queries into different types for proper tool selection"""
    
    QUERY_PATTERNS = {
        "SIMPLE_FILTER": {
            "keywords": ["show", "get", "find", "display", "list"],
            "patterns": [r"show me \w+", r"get \w+ content", r"find all \w+"],
            "examples": ["Show me TOFU content", "Get Fashion industry articles"]
        },
        "COMPLEX_FILTER": {
            "keywords": ["and", "with", "that are", "in", "for"],
            "patterns": [r"\w+ and \w+", r"\w+ in \w+", r"\w+ for \w+"],
            "examples": ["Show me TOFU content that are Product Pages", "Financial Services content for Individual Investors"]
        },
        "COUNT_ANALYTICS": {
            "keywords": ["how many", "count", "total", "number of"],
            "patterns": [r"how many \w+", r"count of \w+", r"total \w+"],
            "examples": ["How many TOFU articles?", "Count of Product Pages"]
        },
        "DISTRIBUTION_ANALYTICS": {
            "keywords": ["distribution", "breakdown", "analysis", "split", "overview"],
            "patterns": [r"\w+ distribution", r"breakdown of \w+", r"\w+ analysis"],
            "examples": ["Funnel stage distribution", "Industry breakdown", "Content analysis"]
        },
        "STRATEGIC_ANALYSIS": {
            "keywords": ["gap", "missing", "should", "recommend", "strategy", "focus", "too much", "enough"],
            "patterns": [r"content gap", r"are we \w+", r"should we \w+", r"recommend \w+"],
            "examples": ["Content gap analysis", "Are we focused too much on TOFU?", "What should we create more of?"]
        },
        "SEARCH": {
            "keywords": ["about", "related to", "containing", "mentioning", "like"],
            "patterns": [r"about \w+", r"related to \w+", r"content like \w+"],
            "examples": ["Content about investment tools", "Articles mentioning crypto", "Pages like our homepage"]
        },
        "GENERAL_CHAT": {
            "keywords": ["hello", "hi", "help", "what can", "explain", "how does"],
            "patterns": [r"what (can|do)", r"how (does|do)", r"explain \w+"],
            "examples": ["Hello", "What can you help with?", "Explain TOFU", "How does this work?"]
        }
    }
    
    @classmethod
    def classify_query(cls, query: str) -> str:
        """Classify a user query into one of the defined types"""
        query_lower = query.lower()
        
        # Score each query type
        scores = {}
        for query_type, config in cls.QUERY_PATTERNS.items():
            score = 0
            
            # Keyword matching
            for keyword in config["keywords"]:
                if keyword in query_lower:
                    score += 1
            
            # Pattern matching (simplified - would use regex in production)
            for pattern in config.get("patterns", []):
                # Simple contains check for MVP (could use regex)
                if any(word in query_lower for word in pattern.replace(r"\w+", "").split()):
                    score += 2
            
            scores[query_type] = score
        
        # Return the highest scoring type, default to SEARCH if tied/none
        if max(scores.values()) == 0:
            return "SEARCH" if any(word in query_lower for word in ["about", "find", "search"]) else "GENERAL_CHAT"
        
        return max(scores.items(), key=lambda x: x[1])[0]


class EntityExtractor:
    """Extract relevant entities from user queries based on tenant schema"""
    
    def __init__(self, tenant_schema):
        self.tenant_schema = tenant_schema
    
    def extract_entities(self, query: str, query_type: str) -> Dict[str, Any]:
        """Extract entities from query based on available schema"""
        entities = {
            "categories": {},
            "search_terms": [],
            "modifiers": [],
            "confidence": 0.0
        }
        
        query_lower = query.lower()
        
        # Extract category values
        for category_name, values in self.tenant_schema.categories.items():
            matched_values = []
            for value in values:
                if value.lower() in query_lower:
                    matched_values.append(value)
            
            if matched_values:
                entities["categories"][category_name] = matched_values
                entities["confidence"] += 0.2
        
        # Extract search terms (for SEARCH queries)
        if query_type == "SEARCH":
            # Remove common words and extract meaningful terms
            stop_words = {"about", "related", "to", "content", "articles", "pages", "show", "me", "find"}
            words = [word.strip(".,!?") for word in query_lower.split() if word not in stop_words and len(word) > 2]
            entities["search_terms"] = words[:5]  # Limit to 5 terms
        
        # Extract modifiers
        modifiers = []
        if "top" in query_lower:
            modifiers.append("limit_results")
        if "recent" in query_lower or "latest" in query_lower:
            modifiers.append("sort_by_date")
        if "best" in query_lower or "high" in query_lower:
            modifiers.append("high_quality")
        
        entities["modifiers"] = modifiers
        
        # Calculate confidence based on entities found
        if entities["categories"]:
            entities["confidence"] += 0.3
        if entities["search_terms"]:
            entities["confidence"] += 0.2
        
        entities["confidence"] = min(entities["confidence"], 1.0)
        
        return entities


def create_query_context(tenant_id: str, user_query: str) -> QueryContext:
    """Create query context with classification and entity extraction"""
    try:
        # Classify query
        query_type = QueryClassifier.classify_query(user_query)
        
        # Extract entities
        extractor = DynamicTenantSchemaExtractor(tenant_id)
        tenant_schema = extractor.extract_schema()
        entity_extractor = EntityExtractor(tenant_schema)
        extracted_entities = entity_extractor.extract_entities(user_query, query_type)
        
        return QueryContext(
            tenant_id=tenant_id,
            user_query=user_query,
            query_type=query_type,
            extracted_entities=extracted_entities,
            confidence_score=extracted_entities.get("confidence", 0.0)
        )
        
    except Exception as e:
        logger.error(f"Failed to create query context: {e}")
        return QueryContext(
            tenant_id=tenant_id,
            user_query=user_query,
            query_type="GENERAL_CHAT",
            confidence_score=0.0
        )