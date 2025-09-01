import streamlit as st
import json
import pandas as pd
from datetime import datetime
from advisory_answer import SimpleAdvisoryAgent, create_advisor

# Configure page
st.set_page_config(page_title="Content Advisory Chat", layout="wide")

# Initialize session state
for key in ['advisor', 'chat_history', 'debug_logs', 'last_query_result']:
    if key not in st.session_state:
        st.session_state[key] = None if key in ['advisor', 'last_query_result'] else []

def log_debug(step: str, data: str):
    """Add debug log entry"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.debug_logs.append(f"[{timestamp}] {step}: {data}")
    st.session_state.debug_logs = st.session_state.debug_logs[-20:]  # Keep last 20

def stream_response(question: str, placeholder):
    """Stream LLM response with debug logging"""
    try:
        log_debug("QUESTION", question)
        
        # Get query result for data display
        query_result = st.session_state.advisor.query_router.execute_query(
            question, st.session_state.advisor.current_tenant
        )
        log_debug("QUERY", f"Success: {query_result.success}, Type: {query_result.operation_type}, Count: {query_result.total_count}")
        st.session_state.last_query_result = query_result
        
        # Use advisory agent's context and logic
        advisor = st.session_state.advisor
        categories = advisor.schema_context.get("categories", {})
        recent_chat = advisor.chat_history[-3:] if advisor.chat_history else []
        
        # Data summary (same logic as advisory agent)
        if query_result.success:
            data_info = {
                "distribution": f"Distribution query returned {len(query_result.data) if query_result.data else 0} categories with total {query_result.total_count} items. Data: {query_result.data}",
                "fetch_by_filter": f"Found {query_result.total_count} matching content items. Sample data: {query_result.data[:2] if isinstance(query_result.data, list) else 'No preview available'}",
                "semantic": f"Semantic search found {query_result.total_count} relevant items"
            }.get(query_result.operation_type, f"Query executed successfully with {query_result.total_count} results")
        else:
            data_info = f"Query failed: {query_result.error_message}"


        
        
        # Build prompt (same as advisory agent)
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

        log_debug("LLM", "Streaming response...")
        
        # Stream response
        full_response = ""
        stream = advisor.client.chat.completions.create(
            model="gpt-4.1",
            messages=[{"role": "user", "content": prompt}],
            stream=True,
            temperature=0
        )
        
        for chunk in stream:
            if chunk.choices[0].delta.content:
                full_response += chunk.choices[0].delta.content
                placeholder.write(full_response + "▌")
        
        placeholder.write(full_response)
        
        # Update advisory agent's history
        advisor.chat_history.append({
            "user": question, 
            "assistant": full_response, 
            "timestamp": datetime.now()
        })
        
        log_debug("COMPLETE", f"{len(full_response)} characters")
        return full_response
        
    except Exception as e:
        error_msg = f"Error: {str(e)}"
        log_debug("ERROR", error_msg)
        placeholder.write(error_msg)
        return error_msg

def show_data_table():
    """Display data in tabular format with resolved lookup names (no ObjectIds).
       Category attributes are expanded into separate columns with clean headers.
    """
    try:
        data = st.session_state.last_query_result.data

        def clean_mongo_data(doc):
            """Flatten MongoDB lookup fields and category attributes"""
            if not isinstance(doc, dict):
                return doc

            row = {}
            for k, v in doc.items():
                # Handle lookup fields (topics, content types, tags)
                if k.endswith("_info") and isinstance(v, list) and v:
                    if k == "category_info":
                        # Expand each category into its own column
                        for cat in v:
                            category_name = cat.get("categoryName", "Unknown Category")
                            attribute_value = cat.get("name", "")
                            row[category_name] = attribute_value
                    else:
                        # Normal lookup → just use the first 'name'
                        col_name = k.replace("_info", "").replace("_", " ").title()
                        row[col_name] = v[0].get("name", "")
                elif isinstance(v, dict):
                    row[k] = clean_mongo_data(v)
                elif isinstance(v, list):
                    # Flatten multi-value lists into comma-separated strings
                    row[k] = ", ".join(str(item) for item in v)
                elif type(v).__name__ == "ObjectId":
                    # Skip ObjectIds entirely
                    continue
                else:
                    row[k] = v
            return row

        # Clean all rows
        if isinstance(data, list):
            cleaned_data = [clean_mongo_data(d) for d in data]
        elif isinstance(data, dict):
            cleaned_data = [clean_mongo_data(data)]
        else:
            cleaned_data = [{"Value": str(data)}]

        # Convert to DataFrame
        df = pd.DataFrame(cleaned_data)

        # Ensure everything is string for Streamlit safety
        for col in df.columns:
            df[col] = df[col].astype(str)

        st.dataframe(df, use_container_width=True)
        st.caption(f"Showing {len(df)} rows × {len(df.columns)} columns")
        log_debug("TABLE", f"{len(df)} rows × {len(df.columns)} columns")

    except Exception as e:
        st.error(f"Error creating table: {str(e)}")
        st.subheader("Raw Data (JSON Format)")
        st.json(json.loads(json.dumps(st.session_state.last_query_result.data, default=str)))
        log_debug("TABLE ERROR", str(e))

def main():
    st.title("🤖 Content Advisory Chat")
    
    # Sidebar
    with st.sidebar:
        st.header("Setup")
        
        # Initialize advisor
        if not st.session_state.advisor:
            if st.button("Initialize Advisor"):
                try:
                    st.session_state.advisor = create_advisor("mongodb://localhost:27017", "my_database")
                    log_debug("INIT", "Advisor ready")
                    st.success("Advisor initialized!")
                except Exception as e:
                    log_debug("INIT ERROR", str(e))
                    st.error(f"Failed: {str(e)}")
        
        # Start chat
        if st.session_state.advisor and not getattr(st.session_state.advisor, 'current_tenant', None):
            tenant_id = st.text_input("Tenant ID", "6875f3afc8337606d54a7f37")
            if st.button("Start Chat"):
                try:
                    welcome = st.session_state.advisor.start_chat(tenant_id)
                    st.session_state.chat_history.append({"role": "assistant", "content": welcome})
                    log_debug("CHAT", f"Started for {tenant_id}")
                    st.success("Chat started!")
                    st.rerun()
                except Exception as e:
                    log_debug("CHAT ERROR", str(e))
                    st.error(f"Failed: {str(e)}")
        
        # Debug logs
        st.header("Debug Logs")
        if st.button("Clear Logs"):
            st.session_state.debug_logs = []
            st.rerun()
        
        with st.container(height=400):
            for log in reversed(st.session_state.debug_logs[-10:]):
                st.text(log)

    # Main content
    col1, col2 = st.columns([4, 2])
    
    with col1:
        # Chat history
        for msg in st.session_state.chat_history:
            with st.chat_message(msg["role"]):
                st.write(msg["content"])
        
        # Chat input
        if st.session_state.advisor and getattr(st.session_state.advisor, 'current_tenant', None):
            if prompt := st.chat_input("Ask about your content strategy..."):
                # Add user message
                st.session_state.chat_history.append({"role": "user", "content": prompt})
                with st.chat_message("user"):
                    st.write(prompt)
                
                # Stream assistant response
                with st.chat_message("assistant"):
                    response = stream_response(prompt, st.empty())
                    st.session_state.chat_history.append({"role": "assistant", "content": response})
        else:
            st.info("👈 Please initialize advisor and start chat session")
    
    with col2:
        # Data display
        result = st.session_state.last_query_result
        if result and result.success and result.data:
            st.subheader("Data Available")
            st.info(f"Found {result.total_count} items")
            st.caption(f"Query: {result.operation_type}")
            
            if st.button("📋 Show Data Table", type="primary"):
                st.subheader("Query Results")
                show_data_table()
        else:
            status = "Last query failed" if result and not result.success else "Ask a question to see data!"
            st.info(status)

if __name__ == "__main__":
    main()