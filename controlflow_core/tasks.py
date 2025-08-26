# controlflow_core/tasks.py - ControlFlow Task Definitions

import controlflow as cf
from typing import Dict, Any, List
from pydantic import BaseModel, Field
from controlflow_core.agent import QueryContext, QueryResponse, create_tenant_agent
from controlflow_core.tools import get_tools_for_query_type
from utils.logger import get_logger

logger = get_logger("controlflow_tasks")


class TaskResult(BaseModel):
    """Result from a ControlFlow task execution"""
    success: bool
    result: Any = None
    error: str = None
    task_type: str = ""
    execution_time: float = 0.0


def create_query_analysis_task(query_context: QueryContext) -> cf.Task:
    """
    Task to analyze and validate the user query, extracting entities and determining approach
    """
    return cf.Task(
        objective=f"""
        Analyze the user query: "{query_context.user_query}"
        
        ANALYSIS REQUIREMENTS:
        1. Confirm the query classification: {query_context.query_type}
        2. Validate extracted entities: {query_context.extracted_entities}
        3. Determine if additional entity extraction is needed
        4. Plan the execution approach (which tools to use)
        5. Identify any potential issues or missing information
        
        RETURN a detailed analysis plan with:
        - Confirmed query type
        - Validated entities and categories
        - Recommended tool sequence
        - Any clarifications needed from user
        """,
        result_type=Dict[str, Any],
        agents=[create_tenant_agent(query_context.tenant_id)],
        tools=get_tools_for_query_type(query_context.query_type),
    )


def create_data_retrieval_task(query_context: QueryContext, analysis_plan: Dict[str, Any]) -> cf.Task:
    """
    Task to execute the actual data retrieval based on the analysis plan
    """
    return cf.Task(
        objective=f"""
        Execute data retrieval for the user query: "{query_context.user_query}"
        
        EXECUTION PLAN: {analysis_plan}
        QUERY TYPE: {query_context.query_type}
        ENTITIES: {query_context.extracted_entities}
        
        EXECUTION REQUIREMENTS:
        1. Use the appropriate tools based on the analysis plan
        2. Ensure proper tenant isolation (tenant_id: {query_context.tenant_id})
        3. Handle any errors gracefully with fallback approaches
        4. Collect all relevant data needed for the response
        5. Validate data quality and completeness
        
        RETURN the raw data results ready for response formatting
        """,
        result_type=Dict[str, Any],
        agents=[create_tenant_agent(query_context.tenant_id)],
        tools=get_tools_for_query_type(query_context.query_type),
    )


def create_response_synthesis_task(
    query_context: QueryContext, 
    raw_data: Dict[str, Any]
) -> cf.Task:
    """
    Task to synthesize the raw data into a user-friendly response
    """
    return cf.Task(
        objective=f"""
        Synthesize a comprehensive response for the user query: "{query_context.user_query}"
        
        RAW DATA: {raw_data}
        QUERY TYPE: {query_context.query_type}
        
        SYNTHESIS REQUIREMENTS:
        1. Format the response according to query type:
           - FILTERED_DATA: Present content with clear categorization
           - ANALYTICS: Provide insights, trends, and key findings
           - STRATEGIC_ANALYSIS: Include recommendations and reasoning
           - SEARCH: Rank results by relevance and explain matches
           - GENERAL_CHAT: Provide helpful, conversational response
        
        2. Generate appropriate insights and recommendations if applicable
        3. Include relevant metadata and context
        4. Ensure the response is actionable and valuable to the user
        5. Handle any data limitations or issues transparently
        
        RETURN a properly formatted QueryResponse object with:
        - Clear, user-friendly message
        - Structured data if applicable  
        - Insights for analytics queries
        - Recommendations for strategic queries
        - Proper metadata and query info
        """,
        result_type=QueryResponse,
        agents=[create_tenant_agent(query_context.tenant_id)],
        tools=[],  # No tools needed for synthesis, just reasoning
    )


def create_error_handling_task(
    query_context: QueryContext, 
    error: Exception, 
    attempted_approach: str
) -> cf.Task:
    """
    Task to handle errors gracefully and provide helpful fallback responses
    """
    return cf.Task(
        objective=f"""
        Handle the error that occurred while processing: "{query_context.user_query}"
        
        ERROR: {str(error)}
        ATTEMPTED APPROACH: {attempted_approach}
        
        ERROR HANDLING REQUIREMENTS:
        1. Analyze what went wrong and why
        2. Determine if a fallback approach is possible
        3. Provide a helpful error message to the user
        4. Suggest alternative queries if appropriate
        5. Ensure the user understands what data is available
        
        RETURN a user-friendly error response with suggestions for next steps
        """,
        result_type=QueryResponse,
        agents=[create_tenant_agent(query_context.tenant_id)],
        tools=[
            # Import these at module level to avoid circular imports
            # get_tenant_schema_info, get_content_summary_stats
        ],
    )


class ControlFlowPipeline:
    """Main pipeline orchestrator for query processing"""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.agent = create_tenant_agent(tenant_id)
    
    async def process_query(self, query_context: QueryContext) -> QueryResponse:
        """
        Process a query through the complete ControlFlow pipeline
        """
        try:
            logger.info(f"Processing query: {query_context.user_query}", extra={"tenant_id": self.tenant_id})
            
            # Phase 1: Query Analysis and Planning
            analysis_task = create_query_analysis_task(query_context)
            analysis_result = await analysis_task.run_async()
            
            if not analysis_result or "error" in str(analysis_result):
                return await self._handle_analysis_error(query_context, analysis_result)
            
            # Phase 2: Data Retrieval
            retrieval_task = create_data_retrieval_task(query_context, analysis_result)
            raw_data = await retrieval_task.run_async()
            
            if not raw_data or "error" in str(raw_data):
                return await self._handle_retrieval_error(query_context, raw_data)
            
            # Phase 3: Response Synthesis
            synthesis_task = create_response_synthesis_task(query_context, raw_data)
            final_response = await synthesis_task.run_async()
            
            if not isinstance(final_response, QueryResponse):
                # Convert to proper format if needed
                final_response = QueryResponse(
                    response_type=query_context.query_type.lower().replace("_", "_"),
                    message=str(final_response),
                    metadata={"processing_phases": ["analysis", "retrieval", "synthesis"]}
                )
            
            logger.info(f"Query processed successfully", extra={
                "tenant_id": self.tenant_id,
                "query_type": query_context.query_type,
                "confidence": query_context.confidence_score
            })
            
            return final_response
            
        except Exception as e:
            logger.error(f"Pipeline error: {e}", extra={"tenant_id": self.tenant_id})
            return await self._handle_pipeline_error(query_context, e)
    
    def process_query_sync(self, query_context: QueryContext) -> QueryResponse:
        """
        Synchronous version of query processing for easier integration
        """
        try:
            logger.info(f"Processing query (sync): {query_context.user_query}", extra={"tenant_id": self.tenant_id})
            
            # Phase 1: Query Analysis and Planning
            analysis_task = create_query_analysis_task(query_context)
            analysis_result = analysis_task.run()
            
            if not analysis_result or "error" in str(analysis_result):
                return self._handle_analysis_error_sync(query_context, analysis_result)
            
            # Phase 2: Data Retrieval
            retrieval_task = create_data_retrieval_task(query_context, analysis_result)
            raw_data = retrieval_task.run()
            
            if not raw_data or "error" in str(raw_data):
                return self._handle_retrieval_error_sync(query_context, raw_data)
            
            # Phase 3: Response Synthesis
            synthesis_task = create_response_synthesis_task(query_context, raw_data)
            final_response = synthesis_task.run()
            
            if not isinstance(final_response, QueryResponse):
                # Convert to proper format if needed
                final_response = QueryResponse(
                    response_type=query_context.query_type.lower().replace("_", "_"),
                    message=str(final_response),
                    metadata={"processing_phases": ["analysis", "retrieval", "synthesis"]}
                )
            
            logger.info(f"Query processed successfully (sync)", extra={
                "tenant_id": self.tenant_id,
                "query_type": query_context.query_type,
                "confidence": query_context.confidence_score
            })
            
            return final_response
            
        except Exception as e:
            logger.error(f"Pipeline error (sync): {e}", extra={"tenant_id": self.tenant_id})
            return self._handle_pipeline_error_sync(query_context, e)
    
    async def _handle_analysis_error(self, query_context: QueryContext, error_result: Any) -> QueryResponse:
        """Handle errors during query analysis phase"""
        error_task = create_error_handling_task(
            query_context, 
            Exception(str(error_result)), 
            "query_analysis"
        )
        return await error_task.run_async()
    
    def _handle_analysis_error_sync(self, query_context: QueryContext, error_result: Any) -> QueryResponse:
        """Handle errors during query analysis phase (sync)"""
        return QueryResponse(
            response_type="error",
            message=f"I had trouble understanding your query. Could you try rephrasing it? For example: 'Show me TOFU content' or 'How many Product Pages do we have?'",
            metadata={"error_phase": "analysis", "original_query": query_context.user_query}
        )
    
    async def _handle_retrieval_error(self, query_context: QueryContext, error_result: Any) -> QueryResponse:
        """Handle errors during data retrieval phase"""
        error_task = create_error_handling_task(
            query_context,
            Exception(str(error_result)),
            "data_retrieval" 
        )
        return await error_task.run_async()
    
    def _handle_retrieval_error_sync(self, query_context: QueryContext, error_result: Any) -> QueryResponse:
        """Handle errors during data retrieval phase (sync)"""
        return QueryResponse(
            response_type="error",
            message=f"I encountered an issue retrieving the data. This might be due to invalid filters or a temporary system issue. Please try a simpler query.",
            metadata={"error_phase": "retrieval", "original_query": query_context.user_query}
        )
    
    async def _handle_pipeline_error(self, query_context: QueryContext, error: Exception) -> QueryResponse:
        """Handle general pipeline errors"""
        error_task = create_error_handling_task(query_context, error, "pipeline")
        return await error_task.run_async()
    
    def _handle_pipeline_error_sync(self, query_context: QueryContext, error: Exception) -> QueryResponse:
        """Handle general pipeline errors (sync)"""
        return QueryResponse(
            response_type="error",
            message="I'm having trouble processing your request right now. Please try again or contact support if the issue persists.",
            metadata={"error_phase": "pipeline", "error": str(error)}
        )


# Convenience functions for easy integration
def process_user_query(tenant_id: str, user_query: str) -> QueryResponse:
    """
    Simple function to process a user query - main entry point for the chatbot
    """
    from controlflow_core.agent import create_query_context
    
    # Create query context
    query_context = create_query_context(tenant_id=str(tenant_id), user_query=user_query)
    
    # Process through pipeline
    pipeline = ControlFlowPipeline(tenant_id)
    return pipeline.process_query_sync(query_context)


async def process_user_query_async(tenant_id: str, user_query: str) -> QueryResponse:
    """
    Async version of query processing
    """
    from controlflow_core.agent import create_query_context
    
    # Create query context
    query_context = create_query_context(tenant_id=str(tenant_id), user_query=user_query)
    
    # Process through pipeline
    pipeline = ControlFlowPipeline(tenant_id)
    return await pipeline.process_query(query_context)


# Task factory functions for specific query types
def create_simple_filter_task(tenant_id: str, user_query: str, entities: Dict) -> cf.Task:
    """Create optimized task for simple filtering queries"""
    return cf.Task(
        objective=f"""
        Handle simple content filtering query: "{user_query}"
        
        Extracted entities: {entities}
        
        TASK: Use filter_content_by_categories tool to find matching content.
        If entities are unclear, validate them first using validate_category_values.
        Return results in a user-friendly format with clear categorization.
        """,
        result_type=Dict[str, Any],
        agents=[create_tenant_agent(tenant_id)],
        tools=get_tools_for_query_type("SIMPLE_FILTER")
    )


def create_analytics_task(tenant_id: str, user_query: str, entities: Dict) -> cf.Task:
    """Create optimized task for analytics queries"""
    return cf.Task(
        objective=f"""
        Handle analytics query: "{user_query}"
        
        Extracted entities: {entities}
        
        TASK: Use analyze_content_distribution or count_content_by_criteria to provide insights.
        Include specific numbers, trends, and examples in your response.
        Make the analysis actionable and valuable.
        """,
        result_type=Dict[str, Any],
        agents=[create_tenant_agent(tenant_id)],
        tools=get_tools_for_query_type("DISTRIBUTION_ANALYTICS")
    )


def create_search_task(tenant_id: str, user_query: str, entities: Dict) -> cf.Task:
    """Create optimized task for search queries"""
    return cf.Task(
        objective=f"""
        Handle search query: "{user_query}"
        
        Search terms: {entities.get('search_terms', [])}
        
        TASK: Use search_content_by_text to find relevant content.
        Rank results by relevance and explain why each result matches.
        Include both content text matches and category matches.
        """,
        result_type=Dict[str, Any],
        agents=[create_tenant_agent(tenant_id)],
        tools=get_tools_for_query_type("SEARCH")
    )