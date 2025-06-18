import os
import streamlit as st
from functions.catalog_connector import set_connection, get_catalog_metadata, list_schemas

REQUIRED_KEYS = ["openai_api_key", "databricks_host", "http_path", "databricks_token", "catalog_name"]
st.set_page_config(page_title="Databricks SQL Assistant", layout="wide")

for key in REQUIRED_KEYS + ["schema_metadata", "messages", "schema_options", "selected_schemas", "schemas_loaded"]:
    if key not in st.session_state:
        st.session_state[key] = "" if key not in ["messages", "schema_options", "selected_schemas"] else ([] if key != "messages" else [])

def configure_app():
    with st.sidebar:
        st.header("App Configuration")
        st.subheader("🔐 OpenAI API Settings")
        st.text_input("OpenAI API Key", type="password", key="openai_api_key")
        st.selectbox(
            "OpenAI model:",
            options=["openai:gpt-4.1-mini", "openai:gpt-4o-mini"],
            key="model_choice"
        )

        st.subheader("🧱 Databricks Connection Settings")
        st.text_input("Databricks Workspace Host", key="databricks_host", placeholder="e.g., dbc-1234.cloud.databricks.com")
        st.text_input("Databricks SQL HTTP Path", key="http_path")
        st.text_input("Databricks Access Token", type="password", key="databricks_token")
        st.text_input("Catalog name", key='catalog_name')

        # --- Schema selection logic ---
        ready_for_schemas = all([
            st.session_state.get("databricks_host"),
            st.session_state.get("http_path"),
            st.session_state.get("databricks_token"),
            st.session_state.get("catalog_name")
        ])
        # Button to load schemas
        if ready_for_schemas and not st.session_state.get("schemas_loaded", False):
            if st.button("Choose Schemas"):
                try:
                    connection = set_connection(
                        server_hostname=st.session_state['databricks_host'],
                        http_path=st.session_state['http_path'],
                        access_token=st.session_state['databricks_token'],
                    )
                    st.session_state["schema_options"] = list_schemas(
                        st.session_state['catalog_name'], connection
                    )
                    st.session_state["schemas_loaded"] = True
                    st.session_state["selected_schemas"] = []
                except Exception as e:
                    st.session_state["schema_options"] = []
                    st.session_state["schemas_loaded"] = False
                    st.error(f"Failed to load schemas: {e}")

        # Multiselect and Save Configuration (after loading schemas)
        if st.session_state.get("schemas_loaded", False):
            st.multiselect(
                "Select one or more schemas",
                st.session_state["schema_options"],
                key="selected_schemas"
            )

            if len(st.session_state.get("selected_schemas", [])) > 5:
                st.warning("Selecting many schemas may slow down query generation.")

            # Always show Save Configurations button after schemas loaded
            if st.button("Save Configurations"):
                if all([st.session_state.get("openai_api_key"),
                        st.session_state.get("databricks_host"),
                        st.session_state.get("http_path"),
                        st.session_state.get("databricks_token"),
                        st.session_state.get("catalog_name"),
                        st.session_state.get("selected_schemas")]):
                    os.environ['OPENAI_API_KEY'] = st.session_state.get("openai_api_key")
                    try:
                        with st.spinner("Fetching schema metadata..."):
                            connection = set_connection(
                                server_hostname=st.session_state['databricks_host'],
                                http_path=st.session_state['http_path'],
                                access_token=st.session_state['databricks_token']
                            )
                            schema_metadata = get_catalog_metadata(
                                catalog_name=st.session_state['catalog_name'],
                                schema_names=st.session_state['selected_schemas'],
                                connection=connection
                            )
                            st.session_state['schema_metadata'] = schema_metadata
                            st.session_state['agent'] = None  # reset agent if schema changes
                    except Exception as e:
                        st.error(f"❌ Failed to connect or fetch metadata: {e}")
                    st.success("✅ Configuration saved!")
                else:
                    st.error("❌ Please fill in all fields.")

configure_app()

st.title("💬 Databricks SQL Chat Assistant")

missing_keys = [key for key in REQUIRED_KEYS if not st.session_state.get(key)]
if missing_keys or not st.session_state["schema_metadata"]:
    st.warning("⚠️ Please configure your environment in the 'Configuration' tab.")
else:
    if "messages" not in st.session_state:
        st.session_state["messages"] = [{"role": "assistant", "content": "How can I help you with your data?"}]

    for msg in st.session_state.messages:
        st.chat_message(msg["role"]).write(msg["content"])

    if prompt := st.chat_input():
        import asyncio
        from functions.query_assistant import system_prompt, assistant_prompt, catalog_metadata_agent

        # Agent caching: only rebuild if metadata or model changes
        if ("agent" not in st.session_state or
            st.session_state.get("agent_metadata") != st.session_state.get("schema_metadata") or
            st.session_state.get("agent_model") != st.session_state.get("model_choice")):
            st.session_state["agent"] = catalog_metadata_agent(
                system_prompt.format(summary=st.session_state['schema_metadata']),
                model=st.session_state.get("model_choice", "openai:gpt-4.1-mini")
            )
            st.session_state["agent_metadata"] = st.session_state["schema_metadata"]
            st.session_state["agent_model"] = st.session_state.get("model_choice", "openai:gpt-4.1-mini")

        agent = st.session_state["agent"]

        st.session_state.messages.append({"role": "user", "content": prompt})

        async def run_agent(prompt):
            msg = await agent.run(prompt)
            return msg.output

        st.chat_message("user").write(prompt)
        try:
            with st.spinner("Generating SQL query..."):
                content = asyncio.run(run_agent(assistant_prompt.format(question=prompt)))

            if content and content.code:
                st.chat_message("assistant").write(f"```sql\n{content.code}\n```")
                st.session_state["messages"].append({"role": "assistant", "content": content.code})
            else:
                st.chat_message("assistant").write("❓ Something went wrong. Please try rephrasing your question.")
        except Exception as e:
            st.chat_message("assistant").write(f"❌ Error generating response: {e}")
