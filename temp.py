"""
AI Agent for Environmental Data Extraction
Helps researchers query data sources using natural language
"""

from anthropic import Anthropic
import dask.dataframe as dd
import json
from typing import Dict, List, Any

class EnvironmentalDataAgent:
    """Agent that translates natural language to data queries"""
    
    def __init__(self, api_key: str):
        self.client = Anthropic(api_key=api_key)
        self.data_sources = self._load_data_sources()
        
    def _load_data_sources(self) -> Dict[str, Any]:
        """Load available data sources and their schemas"""
        return {
            "gbif": {
                "name": "Global Biodiversity Information Facility",
                "type": "parquet",
                "location": "s3://gbif-open-data-af-south-1/occurrence/",
                "columns": ["gbifid", "countrycode", "species", "decimallatitude", 
                           "decimallongitude", "eventdate", "year", "month"],
                "description": "Species occurrence data worldwide"
            },
            # Add other data sources from your notebooks
        }
    
    def query(self, natural_language_query: str) -> Dict[str, Any]:
        """Convert natural language to executable query"""
        
        # Step 1: Understand the user's intent
        intent = self._parse_intent(natural_language_query)
        
        # Step 2: Generate executable code
        code = self._generate_query_code(intent)
        
        # Step 3: Execute safely and return results
        results = self._execute_query(code)
        
        return {
            "query": natural_language_query,
            "interpretation": intent,
            "code": code,
            "results": results
        }
    
    def _parse_intent(self, query: str) -> Dict[str, Any]:
        """Use Claude to parse user intent"""
        
        system_prompt = f"""You are an expert in environmental data analysis.
Parse user queries and identify:
1. Data source (from: {list(self.data_sources.keys())})
2. Geographic filters (country, region, coordinates)
3. Time filters (date range, year, month)
4. Species/variables of interest
5. Aggregation type (count, average, sum, list)

Return JSON with this structure:
{{
    "data_source": "gbif",
    "filters": {{"countrycode": "US", "year": 2021}},
    "variables": ["species"],
    "aggregation": "count",
    "group_by": ["countrycode"]
}}
"""
        
        response = self.client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            system=system_prompt,
            messages=[{
                "role": "user",
                "content": query
            }]
        )
        
        return json.loads(response.content[0].text)
    
    def _generate_query_code(self, intent: Dict[str, Any]) -> str:
        """Generate Dask/Pandas code from intent"""
        
        system_prompt = """You are a Python data science expert.
Generate safe, efficient Dask code to query environmental data.
Use only: dask.dataframe, pandas, numpy
Return ONLY the Python code, no explanations."""
        
        user_prompt = f"""
Data source info: {self.data_sources[intent['data_source']]}
Query intent: {json.dumps(intent, indent=2)}

Generate Python code using Dask to:
1. Read from the data source
2. Apply filters
3. Select variables
4. Perform aggregation
5. Return results as a pandas DataFrame

Code template:
```python
import dask.dataframe as dd

df = dd.read_parquet(
    "s3://...",
    storage_options={{"anon": True}},
    engine="pyarrow",
    parquet_file_extension=""
)

# Apply filters and aggregation
result = df[...].compute()
```
"""
        
        response = self.client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2048,
            system=system_prompt,
            messages=[{
                "role": "user",
                "content": user_prompt
            }]
        )
        
        code = response.content[0].text
        # Extract code from markdown if present
        if "```python" in code:
            code = code.split("```python")[1].split("```")[0].strip()
        
        return code
    
    def _execute_query(self, code: str) -> Any:
        """Safely execute generated code"""
        
        # Create safe execution environment
        safe_globals = {
            "dd": dd,
            "__builtins__": {
                "print": print,
                "len": len,
                "range": range,
                "str": str,
                "int": int,
                "float": float,
                "list": list,
                "dict": dict,
            }
        }
        
        local_vars = {}
        
        try:
            exec(code, safe_globals, local_vars)
            return local_vars.get("result", "No result variable found")
        except Exception as e:
            return {"error": str(e), "code": code}
        


  # Web Interface (Streamlit)
  import streamlit as st
from agent import EnvironmentalDataAgent
import os

st.set_page_config(page_title="Environmental Data Assistant", layout="wide")

st.title("🌍 Environmental Data Assistant")
st.markdown("Ask questions about environmental data in natural language")

# Initialize agent
if "agent" not in st.session_state:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    st.session_state.agent = EnvironmentalDataAgent(api_key)

# Example queries
st.sidebar.header("Example Queries")
examples = [
    "How many species observations in the US in 2021?",
    "Show me bird sightings in California last year",
    "What's the average temperature in Brazil for June?",
    "List countries with most biodiversity records"
]
for example in examples:
    if st.sidebar.button(example):
        st.session_state.query = example

# Main query interface
query = st.text_area(
    "What would you like to know?",
    value=st.session_state.get("query", ""),
    height=100
)

if st.button("Search", type="primary"):
    with st.spinner("Analyzing your question..."):
        response = st.session_state.agent.query(query)
    
    # Show interpretation
    st.subheader("Understanding your question")
    st.json(response["interpretation"])
    
    # Show generated code
    with st.expander("View generated code"):
        st.code(response["code"], language="python")
    
    # Show results
    st.subheader("Results")
    if "error" in response["results"]:
        st.error(response["results"]["error"])
    else:
        st.dataframe(response["results"])
        
        # Download button
        csv = response["results"].to_csv(index=False)
        st.download_button(
            "Download as CSV",
            csv,
            "data.csv",
            "text/csv"
        )