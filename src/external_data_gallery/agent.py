"""
Prototype AI Agent for External Data
Attempts to query external data sources to answer researcher's questions.
"""

import re
from anthropic import Anthropic
import json
from typing import Dict, Any
from .sources import nasa_zarr, gbif_parquet

class DataAgent:
    """Agent that translates natural language into data queries and executes them."""

    def __init__(self, api_key: str):
        self.client = Anthropic(api_key=api_key)
        self.data_sources = self._load_data_sources()

    def _load_data_sources(self) -> Dict[str, Any]:
        """Load available data source schemas"""
        return {
            "nasa_zarr": nasa_zarr.schema(),
            "gbif_parquet": gbif_parquet.schema(),
        }
    
    def __repr__(self) -> str:
        return json.dumps({k: v for k, v in self.data_sources.items()}, indent=2)
    
    def query(self, natural_language_query: str) -> Dict[str, Any]:
        """Convert natural language to executable query and return results"""

        # Keep track of AI responses and errors
        logs: list[str] = []

        # Step 1: Understand the user's intent
        intent = self._parse_intent(natural_language_query, logs)

        # Step 2: Generate executable code (not implemented here)
        code = "Generated code (placeholder)"

        # Step 3: Execute safely and return results (not implemented here)
        results = "Query results (placeholder)"

        # Placeholder for code generation and execution
        return {
            "query": natural_language_query,
            "intent": intent,
            "code": code,
            "results": results,
            "logs": logs
        }

    def _parse_intent(self, query: str, logs: list[str]) -> Dict[str, Any]:
        """Use Claude to parse user intent"""
        
        system_prompt = f"""You are an expert in environmental data analysis.
Your task is to understand the user's query and extract the intent.
User query: {query}

Specifically, you should identify:
1. Data sources (complex queries may involve multiple sources)
  - Available sources: {list(self.data_sources.keys())}
  - Source details: {json.dumps(self.data_sources, indent=2)}
2. Geographic filters (country, region, coordinates)
3. Time filters (date range, year, month)
4. Species/variables of interest
5. Aggregation type (count, average, sum, list)

Provide the parsed intent as a JSON object with keys:
- sub_queries: List of sub-queries with data source and filters following schema from data source definitions.
- final_aggregation: Plain language description of final aggregation to perform on sub-query results.

Return only the JSON object, without any additional text.
"""
        response = self.client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            system=system_prompt,
            messages=[
                {"role": "user", "content": query}
            ]
        )

        full_text = response.content[0].text
        logs.append(f"Intent parsing full response: {full_text}")

        intent: Dict[str, Any] = {}

        # extract JSON from markdown code block embedded in response
        json_text_match = re.search(r"```json\s*(.*?)\s*```", full_text, re.DOTALL)
        if not json_text_match:
            logs.append("Failed to extract JSON from response.")
            return {}
        try:
            intent = json.loads(json_text_match.group(1))
        except json.JSONDecodeError as e:
            logs.append(f"JSON decoding error: {e}")
            logs.append(f"Extracted text: {json_text_match.group(1)}")

        return intent
    