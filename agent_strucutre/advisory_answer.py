import os
import json
from openai import OpenAI
from dotenv import load_dotenv
from typing import List, Dict, Any
from datetime import datetime

# Import your existing components
from query_router import create_query_router
from schema_extractor import get_tenant_schema

load_dotenv()

class SimpleAdvisoryAgent:
    """
    Super simple advisory agent - LLM does all the work
    """
    
    def __init__(self, mongo_uri: str, db_name: str, openai_api_key: str = None):
        self.client = OpenAI(api_key=openai_api_key or os.getenv("OPENAI_API_KEY"))
        self.query_router = create_query_router(mongo_uri, db_name)
        
        # Simple session storage
        self.chat_history = []
        self.current_tenant = None
        self.schema_context = {}
    
    def start_chat(self, tenant_id: str):
        """Start new chat session"""
        self.current_tenant = tenant_id
        self.chat_history = []
        
        # Get schema context
        try:
            self.schema_context = get_tenant_schema("mongodb://localhost:27017", "my_database", tenant_id)
        except:
            self.schema_context = {}
        
        return "Hi! I'm your content advisor. Ask me anything about your content strategy!"
    
    def ask(self, question: str) -> str:
        """
        Main method - ask any question and get intelligent response
        LLM handles everything: analysis, insights, recommendations
        """
        if not self.current_tenant:
            return "Please start a chat session first with start_chat(tenant_id)"
        
        # Step 1: Get data using your existing query router
        query_result = self.query_router.execute_query(question, self.current_tenant)
        print(query_result)
        
        # Step 2: Let LLM handle EVERYTHING
        response = self._ask_llm(question, query_result)
        
        # Step 3: Save to chat history
        self.chat_history.append({"user": question, "assistant": response, "timestamp": datetime.now()})
        
        return response
    
    def _ask_llm(self, question: str, query_result) -> str:
        """Let LLM handle all the logic and analysis"""
        
        # Build context
        categories = self.schema_context.get("categories", {})
        recent_chat = self.chat_history[-3:] if self.chat_history else []
        
        # Create simple data summary for LLM
        if query_result.success:
            if query_result.operation_type == "distribution":
                data_info = f"Distribution query returned {len(query_result.data) if query_result.data else 0} categories with total {query_result.total_count} items. Data: {query_result.data}"
            elif query_result.operation_type == "list":
                data_info = f"Found {query_result.total_count} matching content items. Sample data: {query_result.data[:2] if isinstance(query_result.data, list) else 'No preview available'}"
            elif query_result.operation_type == "semantic":
                data_info = f"Semantic search found {query_result.total_count} relevant items"
            else:
                data_info = f"Query executed successfully with {query_result.total_count} results"
        else:
            data_info = f"Query failed: {query_result.error_message}"
        
        # Single LLM call handles everything
        prompt = f"""You are a content strategy advisor. Answer the user's question intelligently based on the data provided.

CONTEXT:
- Available categories: {json.dumps(categories)}  
- Recent conversation: {recent_chat}
- Current query data: {data_info}

USER QUESTION: "{question}"

INSTRUCTIONS:
- Provide direct, helpful answers
- Use specific numbers from the data when available
- Give strategic insights and recommendations
- If it's a distribution query, analyze balance and identify gaps
- If it's a list query, summarize findings and offer next steps  
- If no data or query failed, still try to help with general advice
- Be conversational and concise
- Suggest relevant follow-up questions naturally

Answer the user's question now:"""

        try:
            completion = self.client.chat.completions.create(
                model="gpt-4.1",
                messages=[{"role": "user", "content": prompt}],
                # temperature=0.7,
                # max_tokens=400
            )
            return completion.choices[0].message.content
        except Exception as e:
            return f"I had trouble analyzing your data, but I'm here to help with your content strategy questions. Could you try asking again? Error: {str(e)}"
    
    def show_data(self) -> str:
        """Show raw data from last query"""
        if not self.chat_history:
            return "No recent queries to show data for."
        
        # Get last user question and re-run query
        last_question = self.chat_history[-1]["user"]
        query_result = self.query_router.execute_query(last_question, self.current_tenant)
        
        if query_result.success and query_result.data:
            return f"Raw data from your last query:\n{json.dumps(query_result.data, indent=2, default=str)}"
        else:
            return "No data available from your last query."

# Simple usage
def create_advisor(mongo_uri: str, db_name: str) -> SimpleAdvisoryAgent:
    return SimpleAdvisoryAgent(mongo_uri, db_name)

# Demo
if __name__ == "__main__":
    # Create advisor
    advisor = create_advisor("mongodb://localhost:27017", "Test_database")
    
    # Start chat
    print("🤖", advisor.start_chat("6875f3afc8337606d54a7f37"))
    
    # Simple conversation
    questions = [
        # "What is the distribution of funnel stages?",
        # "Which stage needs more content?", 
        # "Show me content for investors",
        # "Are we too focused on TOFU?",
        # "What should our content strategy prioritize?"
        'Which personas are underrepresented in our content library?'
    ]
    
    for q in questions:
        print(f"\n👤 {q}")
        answer = advisor.ask(q)
        print(f"🤖 {answer}")
        
        # Optionally show raw data
        # print(f"\n📊 Raw data: {advisor.show_data()[:200]}...")
    
    print(f"\n💬 Chat history: {len(advisor.chat_history)} exchanges")