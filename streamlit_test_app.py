# streamlit_app.py

import streamlit as st
import json
import pandas as pd
from test_query import execute_query
from database.try_query_parser import parse_query_with_enhanced_tools

st.set_page_config(page_title="Multi-tenant Query Executor", layout="wide")

st.title("🔎 Multi-tenant Query Executor")

# Input box for query
query = st.text_input("Enter your query:", "what is distribution of funnel stage")

if st.button("Run Query"):
    with st.spinner("Parsing and executing..."):
        # Step 1: Parse the query
        parsed_result = parse_query_with_enhanced_tools(query)

        st.subheader("Parsed Result (Raw JSON)")
        st.json(parsed_result)

        # Step 2: Execute query only if executable
        if parsed_result.get("is_executable", False):
            result = execute_query(parsed_result)

            st.subheader("Execution Result (Structured)")

            # Try converting result into a table
            if isinstance(result, list):
                # List of dicts → DataFrame
                df = pd.DataFrame(result)
                st.dataframe(df, use_container_width=True)
            elif isinstance(result, dict):
                # Dict → flatten into table
                df = pd.DataFrame([result])
                st.dataframe(df, use_container_width=True)
            else:
                st.write(result)

        else:
            st.warning("⚠️ Query not executable (advisory or incomplete).")
