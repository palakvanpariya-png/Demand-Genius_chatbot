# from services.chatbot_service import create_chatbot_service
# from bson import ObjectId

# # Example usage
# if __name__ == "__main__":
#     # Example of how to use the service
#     DEMO_TENANT_ID = ObjectId("6875f3afc8337606d54a7f37")
    
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
    
#     session_id = 'demo_session'
    
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

import streamlit as st
from services.chatbot_service import create_chatbot_service
from bson import ObjectId

# --- Setup ---
DEMO_TENANT_ID = ObjectId("6875f3afc8337606d54a7f37")
service = create_chatbot_service(str(DEMO_TENANT_ID))  # pass as str
SESSION_ID = "demo_session"

# --- Streamlit UI ---
st.set_page_config(page_title="ControlFlow Chatbot", page_icon="🤖", layout="centered")

st.title("🤖 ControlFlow Chatbot Demo")
st.write("Ask me something about your tenant's content.")

# Session state to hold conversation
if "history" not in st.session_state:
    st.session_state.history = []

# Input box
user_query = st.text_input("💬 Your message", placeholder="Type a question and hit Enter...")

if user_query:
    # Process query
    result = service.process_message(user_query, SESSION_ID)

    if result["success"]:
        response = result["response"]
        bot_reply = response.get("message", "⚠️ No response text")
    else:
        bot_reply = f"❌ Error: {result['error']}"

    # Save to history
    st.session_state.history.append({"user": user_query, "bot": bot_reply})

# Display conversation history
for chat in st.session_state.history:
    st.markdown(f"**🧑 You:** {chat['user']}")
    st.markdown(f"**🤖 Bot:** {chat['bot']}")
    st.markdown("---")

# Sidebar with session stats
st.sidebar.header("📊 Session Info")
st.sidebar.json(service.get_session_history(SESSION_ID))
st.sidebar.header("⚙️ Service Stats")
st.sidebar.json(service.get_service_stats())
