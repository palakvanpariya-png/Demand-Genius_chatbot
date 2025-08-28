import streamlit as st
import pandas as pd
from pymongo import MongoClient

from query_parser import create_parser
from query_builder import create_query_builder
from schema_extractor import create_schema_util
from analytics_engine import create_analytics_engine
from advisory_answers import create_llm_advisor, get_complete_advisory_response

# -----------------------------
# Streamlit App Config
# -----------------------------
st.set_page_config(page_title="LLM Business Advisor", layout="wide")

st.title("📊 LLM Business Advisor")
st.markdown("Ask business/content strategy questions and get advisory responses powered by analytics + LLM.")

# -----------------------------
# MongoDB Connection
# -----------------------------
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "my_database"
TENANT_ID = "6875f3afc8337606d54a7f37"

client = MongoClient(MONGO_URI)
mongo_db = client[DB_NAME]

# -----------------------------
# Initialize Components
# -----------------------------
schema_util = create_schema_util(MONGO_URI, DB_NAME)
query_parser = create_parser(schema_util)
query_builder = create_query_builder(schema_util)
analytics_engine = create_analytics_engine()
llm_advisor = create_llm_advisor()

# -----------------------------
# UI Input
# -----------------------------
user_query = st.text_input("💬 Enter your question:", placeholder="e.g., How many BOFU pages do we have?")

if st.button("Run Analysis") and user_query:
    with st.spinner("Analyzing..."):
        response = get_complete_advisory_response(
            query_text=user_query,
            tenant_id=TENANT_ID,
            query_parser=query_parser,
            query_builder=query_builder,
            analytics_engine=analytics_engine,
            mongo_db=mongo_db,
            llm_advisor=llm_advisor
        )

    if response["success"]:
        st.success("✅ Advisory response generated")
        
        # Show advisory
        st.subheader("📌 Advisory Response")
        st.write(response["advisory_response"])
        
        # Show analytics summary
        st.subheader("📈 Analytics Overview")
        st.json(response["analytics"])
        
        # Button to show filtered data
        if response["data_found"] > 0:
            with st.expander(f"🔎 View Filtered Data ({response['data_found']} documents)"):
                df = pd.DataFrame(response["parsed_result"].results if hasattr(response["parsed_result"], "results") else response.get("raw_data", []))
                if not df.empty:
                    st.dataframe(df)
                else:
                    st.warning("No structured results available to display.")
    else:
        st.error(f"❌ Error: {response['error']}")
