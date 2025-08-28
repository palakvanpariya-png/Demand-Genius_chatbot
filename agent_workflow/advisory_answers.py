import os
import json
from openai import OpenAI
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional

load_dotenv()

class LLMAdvisor:
    """
    Simple LLM advisory layer that interprets analytics results 
    and provides business intelligence based on user queries.
    """
    
    def __init__(self, openai_api_key: str = None):
        self.client = OpenAI(api_key=openai_api_key or os.getenv("OPENAI_API_KEY"))
    
    def generate_advisory_response(self, 
                                 original_query: str, 
                                 analytics_results: Dict[str, Any], 
                                 query_context: Dict[str, Any]) -> str:
        """
        Main function: Generate advisory response based on query and analytics.
        
        Args:
            original_query: User's original question
            analytics_results: Output from analytics_engine
            query_context: Context from query_parser (operation, filters, etc.)
        
        Returns:
            Formatted advisory response with insights and recommendations
        """
        try:
            # Build context-aware prompt
            prompt = self._build_advisory_prompt(original_query, analytics_results, query_context)
            
            # Get LLM response
            completion = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": self._get_system_prompt()},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=1000
            )
            
            response = completion.choices[0].message.content
            return response
            
        except Exception as e:
            return f"Error generating advisory response: {str(e)}"
    
    def _build_advisory_prompt(self, 
                             original_query: str, 
                             analytics_results: Dict[str, Any], 
                             query_context: Dict[str, Any]) -> str:
        """Build context-aware prompt combining query, data, and context"""
        
        # Format analytics results for LLM
        analytics_summary = self._format_analytics_for_llm(analytics_results)
        
        # Build prompt based on query type
        query_type = self._detect_query_type(original_query)
        
        prompt_template = self._get_prompt_template(query_type)
        
        prompt = prompt_template.format(
            original_query=original_query,
            analytics_summary=analytics_summary,
            operation=query_context.get("operation", "unknown"),
            filters_applied=self._format_filters(query_context.get("filters", {})),
            total_documents=analytics_results.get("total_documents", 0)
        )
        
        return prompt
    
    def _get_system_prompt(self) -> str:
        """System prompt defining the advisor's role and expertise"""
        return """You are an intelligent business advisor specializing in content strategy and data analysis. 

Your role:
- Interpret data analytics results in business context
- Provide actionable insights and recommendations
- Answer user questions directly and specifically
- Focus on practical, implementable advice

Guidelines:
- Always address the user's specific question first
- Support insights with specific data points from the analytics
- Provide clear, actionable recommendations
- Keep responses focused and practical
- Use business language, not technical jargon
- If data is limited, acknowledge it but still provide value

Response format:
1. Direct answer to the user's question
2. Key insights from the data
3. Actionable recommendations
4. Additional considerations (if relevant)"""
    
    def _detect_query_type(self, query: str) -> str:
        """Simple query type detection for prompt customization"""
        query_lower = query.lower()
        
        if any(word in query_lower for word in ['are we', 'is our', 'do we', 'should we']):
            return 'diagnostic'
        elif any(word in query_lower for word in ['what should', 'how can', 'strategy', 'recommend']):
            return 'strategic'
        elif any(word in query_lower for word in ['compare', 'versus', 'vs', 'difference']):
            return 'comparative'
        elif any(word in query_lower for word in ['show', 'list', 'find', 'get']):
            return 'informational'
        else:
            return 'general'
    
    def _get_prompt_template(self, query_type: str) -> str:
        """Get appropriate prompt template based on query type"""
        
        templates = {
            'diagnostic': """
User asked: "{original_query}"

Analytics Results:
{analytics_summary}

Query Details:
- Operation: {operation}
- Filters Applied: {filters_applied}
- Total Documents Analyzed: {total_documents}

Please analyze this data to directly answer the user's question. Provide a diagnostic assessment with specific evidence from the data, and recommend concrete actions if improvements are needed.
            """,
            
            'strategic': """
User asked: "{original_query}"

Analytics Results:
{analytics_summary}

Query Details:
- Operation: {operation}  
- Filters Applied: {filters_applied}
- Total Documents Analyzed: {total_documents}

Based on this data, provide strategic recommendations that directly address the user's question. Focus on actionable strategies and prioritized next steps.
            """,
            
            'comparative': """
User asked: "{original_query}"

Analytics Results:
{analytics_summary}

Query Details:
- Operation: {operation}
- Filters Applied: {filters_applied} 
- Total Documents Analyzed: {total_documents}

Provide a comparative analysis based on the data. Highlight key differences, performance gaps, and implications for strategy.
            """,
            
            'informational': """
User asked: "{original_query}"

Analytics Results:
{analytics_summary}

Query Details:
- Operation: {operation}
- Filters Applied: {filters_applied}
- Total Documents Analyzed: {total_documents}

Provide a clear summary of the data that answers the user's information request. Include key metrics and any notable patterns.
            """,
            
            'general': """
User asked: "{original_query}"

Analytics Results:
{analytics_summary}

Query Details:
- Operation: {operation}
- Filters Applied: {filters_applied}
- Total Documents Analyzed: {total_documents}

Please analyze this data to provide insights that address the user's question. Include specific findings and practical recommendations.
            """
        }
        
        return templates.get(query_type, templates['general'])
    
    def _format_analytics_for_llm(self, analytics_results: Dict[str, Any]) -> str:
        """Format analytics results in a clear way for LLM consumption"""
        
        if analytics_results.get("error"):
            return f"Error in analytics: {analytics_results['error']}"
        
        formatted = []
        
        # Summary statistics
        if "summary" in analytics_results:
            formatted.append("Summary Statistics:")
            for field, stats in analytics_results["summary"].items():
                if isinstance(stats, dict) and "count" in stats:
                    formatted.append(f"  - {field}: {stats['count']} items, avg: {stats.get('average', 'N/A')}, total: {stats.get('sum', 'N/A')}")
        
        # Distribution analysis
        if "distribution" in analytics_results:
            formatted.append(f"\nDistribution by {analytics_results.get('group_by_field', 'category')}:")
            for item in analytics_results["distribution"][:10]:  # Top 10 items
                formatted.append(f"  - {item['category']}: {item['value']} ({item['percentage']}%)")
        
        # Meta information
        if analytics_results.get("total_documents"):
            formatted.append(f"\nTotal documents analyzed: {analytics_results['total_documents']}")
        
        if analytics_results.get("unique_categories"):
            formatted.append(f"Unique categories found: {analytics_results['unique_categories']}")
        
        return "\n".join(formatted) if formatted else "No specific analytics data available."
    
    def _format_filters(self, filters: Dict[str, List[str]]) -> str:
        """Format filters for display in prompt"""
        if not filters:
            return "None"
        
        filter_strs = []
        for category, values in filters.items():
            if values:
                filter_strs.append(f"{category}: {', '.join(values)}")
        
        return "; ".join(filter_strs) if filter_strs else "None"


# Factory function
def create_llm_advisor(openai_api_key: str = None) -> LLMAdvisor:
    """Factory function to create LLM advisor instance"""
    return LLMAdvisor(openai_api_key)


# Integration function to tie everything together
def get_complete_advisory_response(query_text: str, 
                                 tenant_id: str,
                                 query_parser,
                                 query_builder, 
                                 analytics_engine,
                                 mongo_db,
                                 llm_advisor: LLMAdvisor) -> Dict[str, Any]:
    """
    Complete pipeline: Parse → Build Query → Execute → Analyze → Advise
    
    Args:
        query_text: User's question
        tenant_id: Tenant identifier
        query_parser: QueryParser instance
        query_builder: MongoQueryBuilder instance  
        analytics_engine: AnalyticsEngine instance
        mongo_db: MongoDB database connection
        llm_advisor: LLMAdvisor instance
    
    Returns:
        Complete response with data and advisory
    """
    try:
        # Step 1: Parse query
        parsed_result = query_parser.parse(query_text, tenant_id)
        
        # Step 2: Build and execute database query if needed
        database_results = []
        analytics_results = {}
        
        if query_parser.should_use_database(parsed_result):
            query_params = query_parser.get_database_query_params(parsed_result)
            mongo_query = query_builder.build_query(query_params)
            database_results = query_builder.execute_query(mongo_db, mongo_query)
            
            # Step 3: Analyze the data
            if database_results:
                # Auto-analyze based on operation type
                if parsed_result.operation in ['aggregate', 'insight']:
                    # For aggregated data, analyze the distribution
                    analytics_results = analytics_engine.analyze_distribution(database_results, "_id", "count")
                else:
                    # For raw documents, get summary stats
                    analytics_results = analytics_engine.calculate_summary_stats(database_results)
        
        # Step 4: Generate advisory response
        query_context = {
            "operation": parsed_result.operation,
            "filters": parsed_result.filters,
            "route": parsed_result.route,
            "tenant_id": tenant_id
        }
        
        advisory_response = llm_advisor.generate_advisory_response(
            original_query=query_text,
            analytics_results=analytics_results,
            query_context=query_context
        )
        
        return {
            "query": query_text,
            "parsed_result": parsed_result,
            "data_found": len(database_results) if database_results else 0,
            "analytics": analytics_results,
            "advisory_response": advisory_response,
            "success": True
        }
        
    except Exception as e:
        return {
            "query": query_text,
            "error": str(e),
            "success": False
        }


# Example usage
if __name__ == "__main__":
    from query_parser import create_parser
    from query_builder import create_query_builder  
    from schema_extractor import create_schema_util
    from analytics_engine import create_analytics_engine
    from pymongo import MongoClient
    
    # Initialize all components
    client = MongoClient("mongodb://localhost:27017")
    mongo_db = client["my_database"]
    
    schema_util = create_schema_util("mongodb://localhost:27017", "my_database")
    query_parser = create_parser(schema_util)
    query_builder = create_query_builder(schema_util)
    analytics_engine = create_analytics_engine()
    llm_advisor = create_llm_advisor()
    
    # Test queries
    test_queries = [
        "Are we too focused on TOFU content?",
        "How many BOFU pages do we have?", 
        "What should our content strategy be?",
        "Show me all content about investors"
    ]
    
    tenant_id = "6875f3afc8337606d54a7f37"
    
    for query in test_queries:
        print(f"\n{'='*50}")
        print(f"Query: {query}")
        print('='*50)
        
        response = get_complete_advisory_response(
            query_text=query,
            tenant_id=tenant_id,
            query_parser=query_parser,
            query_builder=query_builder,
            analytics_engine=analytics_engine,
            mongo_db=mongo_db,
            llm_advisor=llm_advisor
        )
        
        if response["success"]:
            print(f"Data Found: {response['data_found']} documents")
            print(f"Advisory Response:\n{response['advisory_response']}")
        else:
            print(f"Error: {response['error']}")