import streamlit as st
import pandas as pd
import json
from datetime import datetime
from advisory_answer import query_parser, query_builder, query_router  # adjust import paths
from openai import OpenAI

# Initialize LLM client
client = OpenAI()

st.set_page_config(page_title="Content Advisory Chat (Simplified)", layout="wide")
st.title("🤖 Content Advisory Chat")

# Keep chat history in session
if "history" not in st.session_state:
    st.session_state.history = []
if "last_query" not in st.session_state:
    st.session_state.last_query = None

# --- helper to clean mongo docs ---
def clean_doc(doc):
    """Flatten categories and lookups into readable table rows"""
    row = {}
    for k, v in doc.items():
        if k.endswith("_info") and isinstance(v, list) and v:
            if k == "category_info":
                for cat in v:
                    row[cat.get("categoryName", "Unknown")] = cat.get("name", "")
            else:
                row[k.replace("_info", "").title()] = v[0].get("name", "") if v else ""
        elif isinstance(v, list):
            row[k] = ", ".join(str(x) for x in v)
        elif isinstance(v, dict):
            row[k] = str(v)
        else:
            row[k] = v
    return row

# --- chat input ---
user_input = st.chat_input("Ask me about your content...")

if user_input:
    # Step 1: Parse & build query
    parsed = query_parser.parse(user_input)
    built_query = query_builder.build(parsed)
    result = query_router.execute_query(built_query)

    st.session_state.last_query = result

    # Step 2: Prepare few samples for LLM
    samples = []
    if result.success and isinstance(result.data, list):
        cleaned = [clean_doc(d) for d in result.data[:3]]  # just 3 samples
        samples = cleaned
    categories = result.categories if hasattr(result, "categories") else {}

    # Step 3: Prompt LLM
    prompt = f"""
You are a content strategy advisor. The user asked: "{user_input}"

Here are the available categories:
{json.dumps(categories, indent=2)}

Here are a few sample results from the database:
{json.dumps(samples, indent=2)}

Task:
- Give a direct, strategic, helpful answer
- Use numbers from the data when possible
- Summarize trends and gaps
- Suggest next steps naturally
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    answer = response.choices[0].message.content

    # Save to chat history
    st.session_state.history.append({"role": "user", "content": user_input})
    st.session_state.history.append({"role": "assistant", "content": answer})

# --- show chat history ---
for msg in st.session_state.history:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

# --- show results ---
if st.session_state.last_query and st.session_state.last_query.success:
    result = st.session_state.last_query
    st.subheader("📋 Query Results")
    st.info(f"Found {result.total_count} items")

    if isinstance(result.data, list):
        df = pd.DataFrame([clean_doc(d) for d in result.data])
        st.dataframe(df, use_container_width=True)
