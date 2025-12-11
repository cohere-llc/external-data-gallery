import streamlit as st
from external_data_gallery.agent import DataAgent

st.set_page_config(page_title="Environmental Data Assistant", layout="wide")

with st.sidebar:
    anthropic_api_key = st.text_input(
        "Anthropic API Key",
        type="password",
        help="Enter your Anthropic API key to use the AI agent.",
    )

st.title("🌍 Environmental Data Assistant")
st.caption("💬 Ask questions about environmental data in natural language")

if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "assistant", "content": "Hello! How can I assist you with environmental data today?"}]

for msg in st.session_state.messages:
    st.chat_message(msg["role"]).write(msg["content"])

if prompt := st.chat_input():
    if not anthropic_api_key:
        st.info("Please enter your Anthropic API key in the sidebar to proceed.")
        st.stop()

    agent = DataAgent(api_key=anthropic_api_key)
    response = agent.query(natural_language_query=prompt)

    st.session_state.messages.append({"role": "user", "content": prompt})
    st.session_state.messages.append({"role": "assistant", "content": response})
    st.chat_message("user").write(prompt)
    st.chat_message("assistant").write(response)