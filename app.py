import streamlit as st
from external_data_gallery.agent import DataAgent
import pandas as pd

st.set_page_config(page_title="Environmental Data Assistant", layout="wide")

with st.sidebar:
    anthropic_api_key = st.text_input(
        "Anthropic API Key",
        type="password",
        help="Enter your Anthropic API key to use the AI agent.",
    )

    if st.button("🗑️ Clear Conversation"):
        st.session_state.messages = []
        st.session_state.query_context = []
        st.rerun()

st.title("🌍 Environmental Data Assistant")
st.caption("💬 Ask questions about environmental data in natural language")

if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "Hello! How can I assist you today?"}
    ]

if "query_context" not in st.session_state:
    st.session_state.query_context = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        if isinstance(msg["content"], dict):
            if "response" in msg["content"] and msg["content"]["response"]:
                st.write(msg["content"]["response"])

            if "external_query" in msg["content"] and msg["content"]["external_query"]:
                with st.expander("🔍 External Query Details"):
                    st.json(msg["content"]["external_query"])

            if "code" in msg["content"] and msg["content"]["code"]:
                with st.expander("💻 Generated Code"):
                    st.code(msg["content"]["code"], language="python")

            if "results" in msg["content"] and msg["content"]["results"] is not None:
                results = msg["content"]["results"]

                # Display dataframe results
                if isinstance(results, pd.DataFrame) and not results.empty:
                    st.subheader("📊 Query Results")
                    st.dataframe(results, use_container_width=True)

                    csv = results.to_csv(index=False)
                    st.download_button(
                        "Download Results as CSV",
                        csv,
                        "query_results.csv",
                        "text/csv"
                    )
                else:
                    with st.expander("📊 Query Results"):
                        st.write(msg["content"]["results"])

            if "logs" in msg["content"] and msg["content"]["logs"]:
                with st.expander("📝 Logs"):
                    for log in msg["content"]["logs"]:
                        st.text(log)
        else:
            st.write(msg["content"])

if prompt := st.chat_input():
    if not anthropic_api_key:
        st.info("Please enter your Anthropic API key in the sidebar to proceed.")
        st.stop()

    st.session_state.messages.append({"role": "user", "content": prompt})
    st.chat_message("user").markdown(prompt)

    with st.spinner("Thinking ...    (not really, but let's pretend)"):
        agent = DataAgent(api_key=anthropic_api_key)
        response = agent.query(
            natural_language_query=prompt,
            conversation_history=st.session_state.query_context
        )

    st.session_state.query_context.append({
        "query": prompt,
        "response": response.get("response", ""),
        "external_query": response.get("external_query", {}),
        "code": response.get("code", ""),
        "results": response.get("results", ""),
        "logs": response.get("logs", [])
    })

    st.session_state.messages.append({"role": "assistant", "content": response})

    st.rerun()
