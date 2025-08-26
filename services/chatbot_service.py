# services/chatbot_service.py - Main ControlFlow Chatbot Service

import time
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from controlflow_core.tasks import process_user_query, process_user_query_async, ControlFlowPipeline
from controlflow_core.agent import create_query_context, QueryResponse
from database.queries import create_search_indexes
from database.connection import get_database
from utils.logger import get_logger
from utils.security import sanitize_input, validate_tenant_access

logger = get_logger("chatbot_service")


@dataclass
class ChatbotConfig:
    """Configuration for the chatbot service"""
    tenant_id: str
    max_tokens_per_query: int = 4000
    response_timeout: int = 30  # seconds
    enable_async: bool = False
    enable_logging: bool = True
    enable_caching: bool = False  # For future implementation


@dataclass
class ChatSession:
    """Chat session context for maintaining conversation state"""
    session_id: str
    tenant_id: str
    created_at: float
    last_activity: float
    query_count: int = 0
    total_tokens_used: int = 0
    conversation_history: list = None
    
    def __post_init__(self):
        if self.conversation_history is None:
            self.conversation_history = []


class ControlFlowChatbotService:
    """Main service class for the ControlFlow-powered chatbot"""
    
    def __init__(self, config: ChatbotConfig):
        self.config = config
        self.pipeline = ControlFlowPipeline(config.tenant_id)
        self.sessions: Dict[str, ChatSession] = {}
        self._initialize_service()
    
    def _initialize_service(self):
        """Initialize the service with required setup"""
        try:
            # Create search indexes
            db = get_database()
            create_search_indexes(db)
            logger.info(f"Chatbot service initialized for tenant {self.config.tenant_id}")
        except Exception as e:
            logger.warning(f"Failed to initialize search indexes: {e}")
    
    def create_session(self, session_id: str) -> ChatSession:
        """Create a new chat session"""
        session = ChatSession(
            session_id=session_id,
            tenant_id=self.config.tenant_id,
            created_at=time.time(),
            last_activity=time.time()
        )
        self.sessions[session_id] = session
        logger.info(f"Created chat session {session_id}")
        return session
    
    def get_session(self, session_id: str) -> Optional[ChatSession]:
        """Get existing chat session"""
        return self.sessions.get(session_id)
    
    def process_message(
        self, 
        user_message: str, 
        session_id: str,
        include_metadata: bool = True
    ) -> Dict[str, Any]:
        """
        Process a user message and return structured response
        
        Args:
            user_message: The user's query/message
            session_id: Unique session identifier
            include_metadata: Whether to include processing metadata in response
            
        Returns:
            Dictionary containing response data, metadata, and session info
        """
        start_time = time.time()
        session = None
        
        try:
            # Security validation
            sanitized_message = sanitize_input(user_message)
            if not validate_tenant_access(self.config.tenant_id):
                raise ValueError("Invalid tenant access")
            
            # Session management
            session = self.get_session(session_id)
            if not session:
                session = self.create_session(session_id)
            
            session.last_activity = time.time()
            session.query_count += 1
            
            # Rate limiting check
            if session.query_count > 100:  # Basic rate limiting
                raise ValueError("Query limit exceeded for this session")
            
            # Process query through ControlFlow
            logger.info(f"Processing message: {sanitized_message[:100]}...", extra={
                "session_id": session_id,
                "tenant_id": self.config.tenant_id
            })
            
            response = process_user_query(self.config.tenant_id, sanitized_message)
            
            # Update session
            session.conversation_history.append({
                "timestamp": time.time(),
                "user_message": sanitized_message,
                "response_type": response.response_type,
                "success": True
            })
            
            # Prepare response
            processing_time = time.time() - start_time
            result = {
                "success": True,
                "response": asdict(response),
                "session_info": {
                    "session_id": session_id,
                    "query_count": session.query_count,
                    "processing_time": round(processing_time, 2)
                }
            }
            
            if include_metadata:
                result["metadata"] = {
                    "tenant_id": self.config.tenant_id,
                    "query_classification": getattr(response, "query_info", {}).get("query_type"),
                    "data_count": len(response.data) if response.data else 0,
                    "has_insights": bool(response.insights),
                    "has_recommendations": bool(response.recommendations)
                }
            
            logger.info(f"Message processed successfully", extra={
                "processing_time": processing_time,
                "session_id": session_id,
                "response_type": response.response_type
            })
            
            return result
            
        except Exception as e:
            processing_time = time.time() - start_time
            
            logger.error(f"Message processing failed: {e}", extra={
                "session_id": session_id,
                "processing_time": processing_time,
                "user_message": user_message[:100]
            })
            
            # Update session with error
            if session:
                session.conversation_history.append({
                    "timestamp": time.time(),
                    "user_message": sanitized_message if 'sanitized_message' in locals() else user_message,
                    "error": str(e),
                    "success": False
                })
            
            return {
                "success": False,
                "error": str(e),
                "response": {
                    "response_type": "error",
                    "message": "I encountered an error processing your request. Please try again or rephrase your question.",
                    "metadata": {"error_type": type(e).__name__}
                },
                "session_info": {
                    "session_id": session_id,
                    "processing_time": round(processing_time, 2)
                }
            }
    
    async def process_message_async(
        self,
        user_message: str,
        session_id: str,
        include_metadata: bool = True
    ) -> Dict[str, Any]:
        """Async version of message processing"""
        start_time = time.time()
        session = None
        
        try:
            # Security validation
            sanitized_message = sanitize_input(user_message)
            if not validate_tenant_access(self.config.tenant_id):
                raise ValueError("Invalid tenant access")
            
            # Session management
            session = self.get_session(session_id)
            if not session:
                session = self.create_session(session_id)
            
            session.last_activity = time.time()
            session.query_count += 1
            
            # Process query through ControlFlow async
            response = await process_user_query_async(self.config.tenant_id, sanitized_message)
            
            # Update session and prepare response (same as sync version)
            session.conversation_history.append({
                "timestamp": time.time(),
                "user_message": sanitized_message,
                "response_type": response.response_type,
                "success": True
            })
            
            processing_time = time.time() - start_time
            result = {
                "success": True,
                "response": asdict(response),
                "session_info": {
                    "session_id": session_id,
                    "query_count": session.query_count,
                    "processing_time": round(processing_time, 2)
                }
            }
            
            if include_metadata:
                result["metadata"] = {
                    "tenant_id": self.config.tenant_id,
                    "query_classification": getattr(response, "query_info", {}).get("query_type"),
                    "data_count": len(response.data) if response.data else 0,
                    "processing_mode": "async"
                }
            
            return result
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Async message processing failed: {e}")
            
            return {
                "success": False,
                "error": str(e),
                "response": {
                    "response_type": "error", 
                    "message": "I encountered an error processing your request.",
                    "metadata": {"error_type": type(e).__name__, "processing_mode": "async"}
                },
                "session_info": {
                    "session_id": session_id,
                    "processing_time": round(processing_time, 2)
                }
            }
    
    def get_session_history(self, session_id: str, limit: int = 10) -> Dict[str, Any]:
        """Get conversation history for a session"""
        session = self.get_session(session_id)
        if not session:
            return {"error": "Session not found"}
        
        return {
            "session_id": session_id,
            "total_queries": session.query_count,
            "session_duration": time.time() - session.created_at,
            "history": session.conversation_history[-limit:] if session.conversation_history else []
        }
    
    def get_service_stats(self) -> Dict[str, Any]:
        """Get service-level statistics"""
        active_sessions = len(self.sessions)
        total_queries = sum(session.query_count for session in self.sessions.values())
        
        return {
            "tenant_id": self.config.tenant_id,
            "active_sessions": active_sessions,
            "total_queries_processed": total_queries,
            "service_uptime": time.time() - getattr(self, '_start_time', time.time()),
            "configuration": {
                "max_tokens_per_query": self.config.max_tokens_per_query,
                "response_timeout": self.config.response_timeout,
                "async_enabled": self.config.enable_async
            }
        }
    
    def cleanup_old_sessions(self, max_age_hours: int = 24):
        """Clean up old inactive sessions"""
        current_time = time.time()
        cutoff_time = current_time - (max_age_hours * 3600)
        
        old_sessions = [
            session_id for session_id, session in self.sessions.items()
            if session.last_activity < cutoff_time
        ]
        
        for session_id in old_sessions:
            del self.sessions[session_id]
        
        if old_sessions:
            logger.info(f"Cleaned up {len(old_sessions)} old sessions")
        
        return len(old_sessions)


# Factory function for easy service creation
def create_chatbot_service(tenant_id: str, **config_overrides) -> ControlFlowChatbotService:
    """
    Factory function to create a chatbot service with default configuration
    """
    config = ChatbotConfig(
        tenant_id=tenant_id,
        **config_overrides
    )
    return ControlFlowChatbotService(config)


# Convenience function for quick testing
def quick_query(tenant_id: str, user_query: str) -> Dict[str, Any]:
    """
    Quick query function for testing without session management
    """
    service = create_chatbot_service(tenant_id)
    return service.process_message(user_query, f"test_session_{int(time.time())}")


# # Example usage
# if __name__ == "__main__":
#     # Example of how to use the service
#     DEMO_TENANT_ID = "6875f3afc8337606d54a7f37"
    
#     # Create service
#     service = create_chatbot_service(DEMO_TENANT_ID)
    
#     # Test queries
#     test_queries = [
#         "Hello, what can you help me with?",
#         "Show me TOFU content",
#         "How many Product Pages do we have?", 
#         "Content gap analysis for funnel stages",
#         "Search for investment platform content"
#     ]
    
#     session_id = "demo_session"
    
#     for query in test_queries:
#         print(f"\n🔍 Query: {query}")
#         result = service.process_message(query, session_id)
        
#         if result["success"]:
#             response = result["response"]
#             print(f"✅ Response: {response['message'][:200]}...")
#             if response.get("data"):
#                 print(f"📊 Data: {len(response['data'])} items")
#         else:
#             print(f"❌ Error: {result['error']}")
    
#     # Print session history
#     print(f"\n📈 Session Stats: {service.get_session_history(session_id)}")
#     print(f"🔧 Service Stats: {service.get_service_stats()}")