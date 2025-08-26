# # Main Streamlit application

# def main():
#     """Main Streamlit app entry point"""

# def setup_page_config():
#     """Configure Streamlit page settings"""

# def create_layout():
#     """Create two-column layout (data table + chat)"""

# def display_data_table():
#     """Display MongoDB collections data in table format"""

# def display_chat_interface():
#     """Display chat interface and handle user queries"""

# def handle_chat_query(query: str):
#     """Process user query through ControlFlow and update display"""

# def filter_and_display_data(response_data):
#     """Filter and highlight data based on chat query results"""

# main.py - Streamlit demo app
"""
Main entry point for the Streamlit demo application.

Sets up logging, loads configs, tests DB connection,
and simulates queries for testing updated schema + cleaned doc fields.
"""

import time
import streamlit as st
from config.settings import load_environment_variables, get_tenant_config
from utils.logger import (
    setup_logging,
    get_logger,
    log_query_processing,
    log_controlflow_execution,
    log_error,
)
from database.connection import get_database, test_connection, close_connection
from database.queries import (
    fetch_content,
    fetch_content_by_filters,
    fetch_content_count,
    fetch_analytics_data,
    fetch_tags_for_tenant,
    fetch_metadata_for_tenant,
)


def run_demo_queries(tenant_id: str):
    """
    Run a series of demo queries to test updated schema + _clean_content_doc
    """
    st.subheader("🔍 Demo Queries")

    try:
        st.write("### Fetching sample content")
        content = fetch_content(tenant_id, limit=5)
        st.json(content)

        st.write("### Fetching content with filters")
        filtered = fetch_content_by_filters(tenant_id, filters={"industry": "Healthcare"}, limit=5)
        st.json(filtered)

        st.write("### Fetching content count")
        count = fetch_content_count(tenant_id)
        st.write(count)

        st.write("### Fetching analytics")
        analytics = fetch_analytics_data(tenant_id)
        st.json(analytics)

        st.write("### Fetching tags")
        tags = fetch_tags_for_tenant(tenant_id)
        st.json(tags)

        st.write("### Fetching metadata")
        metadata = fetch_metadata_for_tenant(tenant_id)
        st.json(metadata)

    except Exception as e:
        log_error(e, {"tenant_id": tenant_id})
        st.error(f"Error running demo queries: {e}")


def dummy_chatbot_request(query: str) -> dict:
    """
    Simulate chatbot request flow (placeholder for ControlFlow).
    """
    tenant_id = get_tenant_config()["tenant_id"]
    logger = get_logger("chatbot_demo")

    try:
        # Step 1: Log incoming query
        log_query_processing(tenant_id, query, response_type="chat")

        # Step 2: Simulate ControlFlow task execution
        start_time = time.time()
        time.sleep(1.2)  # simulate LLM thinking
        log_controlflow_execution("dummy_task", time.time() - start_time)

        # Step 3: Return fake response
        response = {
            "response_type": "chat",
            "message": f"Echo: {query}",
            "metadata": {"tenant_id": tenant_id},
        }

        logger.info("Dummy chatbot response generated", response=response)
        return response

    except Exception as e:
        log_error(e, {"tenant_id": tenant_id, "query": query})
        return {"response_type": "error", "message": "Something went wrong"}


def main():
    """Main Streamlit app entry point."""
    st.title("📊 Tenant Content Explorer (Demo)")
    setup_logging()
    logger = get_logger("main")

    # --- Load configs ---
    settings = load_environment_variables()
    tenant_id = get_tenant_config()["tenant_id"]
    st.sidebar.success(f"Loaded tenant: {tenant_id}")

    logger.info(
        "Environment loaded",
        tenant=settings.tenant,
        db=settings.database,
    )

    # --- Test DB connection ---
    if test_connection():
        db = get_database()
        logger.info("Database ready", collections=db.list_collection_names())
        st.sidebar.success("✅ MongoDB Connected")
    else:
        logger.error("MongoDB not available")
        st.sidebar.error("❌ MongoDB not available")
        return

    # --- Tabs for demo ---
    tab1, tab2 = st.tabs(["Chatbot Demo", "Database Demo"])

    with tab1:
        st.subheader("💬 Chatbot Echo")
        user_input = st.text_input("Enter a query")
        if user_input:
            response = dummy_chatbot_request(user_input)
            st.json(response)

    with tab2:
        run_demo_queries(tenant_id)

    # --- Close DB connection (optional for demo) ---
    close_connection()


if __name__ == "__main__":
    main()

