"""
Prototype AI Agent for External Data
Attempts to query external data sources to answer researcher's questions.
"""

import re
from anthropic import Anthropic
import json
from typing import Dict, Any, List, Tuple
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
    
    def query(
        self,
        natural_language_query: str,
        conversation_history: List[Dict[str, Any]] | None = None
    ) -> Dict[str, Any]:
        """Convert natural language to executable query and return results"""

        if conversation_history is None:
            conversation_history = []

        # Keep track of AI responses and errors
        logs: list[str] = []

        # Step 1: Understand the user's intent
        external_query, response = self._parse_intent(
            natural_language_query,
            conversation_history,
            logs
        )

        code = ""
        results = ""

        if external_query is not None:

            # Step 2: Generate executable code (not implemented here)
            code = "Generated code (placeholder)"

            # Step 3: Execute safely and return results (not implemented here)
            results = "Query results (placeholder)"


        # Placeholder for code generation and execution
        return {
            "query": natural_language_query,
            "response": response,
            "external_query": external_query,
            "code": code,
            "results": results,
            "logs": logs
        }

    def _parse_intent(
        self,
        query: str,
        conversation_history: List[Dict[str, Any]],
        logs: list[str]
    ) -> Tuple[Dict[str, Any] | None, str | None]:
        """
        Use Claude to parse user intent
        
        Returns either a direct answer (no external query needed) or
        a structured external query.
        """

        context_text = ""
        if conversation_history:
            context_text = "\n\nPrevious conversation history:\n"
            for turn in conversation_history[-5:]:  # last 5 turns
                context_text += f"User asked: {turn['query']}\n"
                if turn.get("response"):
                    context_text += f"Agent responded: {turn['response']}\n"
                if turn.get("external_query"):
                    context_text += f"External query: {json.dumps(turn['external_query'], indent=2)}\n"
                if turn.get("results"):
                    context_text += f"Results: {turn['results']}\n"
        
        system_prompt = f"""You are an expert in environmental data analysis.
Your task is to understand the user's query and extract the intent.

{context_text}

Current query: {query}

If the query can be answered without querying external data sources, respond with a natural language answer.

If you determine that responding to the query should involve querying external data sources,
break down the intent into one or more structured sub-queries and a final aggregation step.

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

Return only the JSON object preceded by `EXTERNAL_QUERY`, without any additional text.
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

        # Determine if external data query is needed
        if "EXTERNAL_QUERY" not in full_text:
            logs.append("No external query needed; providing direct answer.")
            return None, full_text  # direct answer, no query needed

        # remove EXTERNAL_QUERY marker
        full_text = full_text.split("EXTERNAL_QUERY", 1)[1].strip()        

        # extract JSON from markdown code block embedded in response
        external_query: Dict[str, Any] = {}
        json_text_match = re.search(r"```json\s*(.*?)\s*```", full_text, re.DOTALL)
        if json_text_match:
            json_str = json_text_match.group(1)
            logs.append("Extracted JSON from markdown code block.")
        else:
            json_str = full_text
            logs.append("No markdown code block found; using full text as JSON.")
        try:
            external_query = json.loads(json_str)
        except json.JSONDecodeError as e:
            logs.append(f"JSON decoding error: {e}")
            logs.append(f"Extracted text: {json_str}")
            return None, None

        logs.append("Determined response with "f"{len(external_query.get('sub_queries', []))} sub-queries")
        return external_query, None
    