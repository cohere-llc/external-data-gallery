"""
Prototype AI Agent for External Data
Attempts to query external data sources to answer researcher's questions.
"""

import re
from anthropic import Anthropic
import json
from typing import Dict, Any, List, Tuple
from .sources import nasa_zarr, gbif_parquet
import pandas as pd

# Pre-imported modules for safe execution environment
import dask
import dask.dataframe as dd # pyright: ignore[reportUnusedImport]
from dask import array as da # pyright: ignore[reportUnusedImport]
import s3fs # pyright: ignore[reportUnusedImport]
import zarr # pyright: ignore[reportUnusedImport]
from zarr.storage import FsspecStore, MemoryStore # pyright: ignore[reportUnusedImport]
from zarr.experimental.cache_store import CacheStore # pyright: ignore[reportUnusedImport]
import numpy as np # pyright: ignore[reportUnusedImport]

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
        """
        Convert natural language to executable query and return results
        
        This is the only high-level method that should be called externally.
        Internally, it calls:
        1. _parse_intent() to understand user intent
        2. _generate_query_code() to generate executable code
        3. _execute_query() to run the code and get results
        4. _summarize() is run to provide a plain language summary of the whole process
        4. It also manages conversation history and logging.
        """

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

        code: str | None = None
        results: pd.DataFrame | None = None
        previous_errors: List[Dict[str, Any]] = []
        previous_analyses: List[str] = []

        if external_query is not None:

            # Iteratively refine and execute the query
            max_retries = 3
            for i in range(max_retries):

                # Step 2: Generate executable code (not implemented here)
                code = self._generate_query_code(
                    external_query,
                    conversation_history,
                    previous_errors,
                    previous_analyses,
                    logs
                )

                # Step 3: Execute safely and return results (not implemented here)
                results, err = self._execute_query(code, logs)
                if err:
                    logs.append(f"Error during query execution: {err}")
                    previous_errors.append(err)
                    continue

                response, do_retry = self._summarize(
                    natural_language_query,
                    conversation_history,
                    external_query,
                    code,
                    results,
                    logs,
                    previous_errors,
                    i < max_retries - 1
                )
                previous_analyses.append(response)
                if not do_retry:
                    logs.append(f"Query completed successfully after {i + 1} attempts.")
                    break
                else:
                    logs.append(f"Query executed, but results look problematic. Retrying")


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
        logs: list[str],
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
                if turn.get("results") is not None:
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

Each sub-query object should have a "data_source" key that is one of the available data sources,
and any relevant query parameters (filters, variables, etc) for each specific data source.

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

        logs.append(f"Determined response with {len(external_query.get('sub_queries', []))} sub-queries")
        return external_query, None
    
    def _generate_query_code(
        self,
        external_query: Dict[str, Any],
        conversation_history: List[Dict[str, Any]],
        previous_errors: List[Dict[str, Any]] | None,
        previous_analyses: List[str] | None,
        logs: list[str]
    ) -> str:
        """Generate Dask code from intent"""

        context_text = ""
        if conversation_history:
            context_text = "\n\nPrevious conversation history:\n"
            for turn in conversation_history[-5:]:  # last 5 turns
                context_text += f"User asked: {turn['query']}\n"
                if turn.get("response"):
                    context_text += f"Agent responded: {turn['response']}\n"
                if turn.get("external_query"):
                    context_text += f"External query: {json.dumps(turn['external_query'], indent=2)}\n"
                if turn.get("results") is not None:
                    context_text += f"Results: {turn['results']}\n"
        if previous_errors is not None and len(previous_errors) > 0:
            context_text += f"""
One or more errors were encountered when attempting to run code generated on previous attempts at this query.
Ensure that you address these errors when re-generating the query code.

Previous errors during execution with generated code (from earliest to most recent attempt): {json.dumps(previous_errors, indent=2)}
"""
        if previous_analyses is not None and len(previous_analyses) > 0:
            context_text += f"""
One or more analyses of the results determined that the query code should be regenerated. Ensure you
address these issues when re-generating the query code.

Previous analyses (from earliest to most recent): {json.dumps(previous_analyses, indent=2)}
"""

        system_prompt = """You are a Python data science expert.
Given a structured external query intent, generate safe, efficient
Python code using Dask to execute the query.

IMPORTANT: Do NOT include import statements. The following modules are already available:
- dask (with dask.config)
- dask.dataframe as dd
- dask.array as da
- pandas as pd
- s3fs
- zarr
- zarr.storage.FsspecStore as FsspecStore
- zarr.experimental.cache_store.CacheStore as CacheStore
- numpy as np

DO NOT INCLUDE ANY LINES SIMILAR TO THESE:
- from foo import Bar
- import baz

The word "import" MUST NOT APPEAR IN YOUR CODE

The safe globals you will have available are:
        safe_globals: Dict[str, Any] = {
            "pd": pd,
            "np": np,
            "dask": dask,
            "dd": dd,
            "da": da,
            "s3fs": s3fs,
            "zarr": zarr,
            "FsspecStore": FsspecStore,
            "MemoryStore": MemoryStore,
            "CacheStore": CacheStore,
            "__builtins__": {
                "print": print,
                "len": len,
                "range": range,
                "min": min,
                "max": max,
                "sum": sum,
                "abs": abs,
                "str": str,
                "int": int,
                "float": float,
                "list": list,
                "dict": dict,
                "set": set,
                "tuple": tuple,
                "any": any,
                "all": all,
                "enumerate": enumerate,
                "sorted": sorted,
                "isinstance": isinstance,
                "hasattr": hasattr,
                "getattr": getattr,
                "type": type,                
            }
        }

Do not use print statements to debug because you will no way to view them.
        
Your code must define a function named `execute_query()`:
```python
def execute_query() -> pd.DataFrame:
    # Your Dask code here
    return result_df
```

Return ONLY the code, without any additional text.
"""
        user_prompt = f"""
Data source info: {self.data_sources}
Structured external query intent: {json.dumps(external_query, indent=2)}

{context_text}

Generate Python code using Dask to execute a series of sub-queries
and a final aggregation step, as specified in the intent. The
code must be in a function named `execute_query()` that returns the final results
as a pandas or Dask DataFrame.

Remember that each sub-query may involve different data sources,
and that datasets may be large and require caching and efficient parallel
processing.

IMPORTANT: Do NOT include import statements. The following modules are already available:
- dask (with dask.config)
- dask.dataframe as dd
- dask.array as da
- pandas as pd
- s3fs
- zarr
- zarr.storage.FsspecStore as FsspecStore
- zarr.experimental.cache_store.CacheStore as CacheStore
- numpy as np

DO NOT INCLUDE ANY LINES SIMILAR TO THESE:
- from foo import Bar
- import baz

The word "import" MUST NOT APPEAR IN YOUR CODE

The safe globals you will have available are:
        safe_globals: Dict[str, Any] = {{
            "pd": pd,
            "np": np,
            "dask": dask,
            "dd": dd,
            "da": da,
            "s3fs": s3fs,
            "zarr": zarr,
            "FsspecStore": FsspecStore,
            "MemoryStore": MemoryStore,
            "CacheStore": CacheStore,
            "__builtins__": {{
                "print": print,
                "len": len,
                "range": range,
                "min": min,
                "max": max,
                "sum": sum,
                "abs": abs,
                "str": str,
                "int": int,
                "float": float,
                "list": list,
                "dict": dict,
                "set": set,
                "tuple": tuple,
                "any": any,
                "all": all,
                "enumerate": enumerate,
                "sorted": sorted,
                "isinstance": isinstance,
                "hasattr": hasattr,
                "getattr": getattr,
                "type": type,                
            }}
        }}

Do not use print statements to debug because you will no way to view them.
        
Your code must define a function named `execute_query()`:
```python
def execute_query() -> pd.DataFrame:
    # Your Dask code here
    return result_df
```

Return ONLY the code, without any additional text.
"""
        response = self.client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2048,
            system=system_prompt,
            messages=[
                {"role": "user", "content": user_prompt}
            ]
        )

        code = response.content[0].text
        if "```python" in code:
            code = code.split("```python", 1)[1].rsplit("```", 1)[0].strip()

        logs.append("Generated query code.")
        return code


    def _execute_query(self, code: str, logs: list[str]) -> Tuple[pd.DataFrame | None, Dict[str, Any] | None]:
        """Execute generated code safely and return results"""

        # WARNING: Executing arbitrary code can be dangerous.
        # In production, use a secure sandboxed environment.
        
        # Create safe execution environment
        safe_globals: Dict[str, Any] = {
            "pd": pd,
            "np": np,
            "dask": dask,
            "dd": dd,
            "da": da,
            "s3fs": s3fs,
            "zarr": zarr,
            "FsspecStore": FsspecStore,
            "MemoryStore": MemoryStore,
            "CacheStore": CacheStore,
            "__builtins__": {
                "print": print,
                "len": len,
                "range": range,
                "min": min,
                "max": max,
                "sum": sum,
                "abs": abs,
                "str": str,
                "int": int,
                "float": float,
                "list": list,
                "dict": dict,
                "set": set,
                "tuple": tuple,
                "any": any,
                "all": all,
                "enumerate": enumerate,
                "sorted": sorted,
                "isinstance": isinstance,
                "hasattr": hasattr,
                "getattr": getattr,
                "type": type,                
            }
        }

        local_vars: Dict[str, Any] = {}

        try:
            exec(code, safe_globals, local_vars)
            if "execute_query" not in local_vars:
                return None, {"error": "No execute_query function defined in generated code.", "code": code}

            result = local_vars["execute_query"]()
            logs.append("Executed query code successfully.")

            # Compute Dask DataFrame if needed
            if hasattr(result, "compute"):
                logs.append("Computing Dask DataFrame result.")
                result = result.compute()

            return result, None
        
        except Exception as e:
            return None, {"error": str(e), "code": code}
        
    def _summarize(
        self,
        natural_language_query: str,
        conversation_history: List[Dict[str, Any]],
        external_query: Dict[str, Any] | None,
        code: str | None,
        results: pd.DataFrame | None,
        logs: List[str],
        previous_errors: List[Dict[str, Any]],
        retry_if_needed: bool
    ) -> Tuple[str, bool]:
        """
        Generate a plain language summary of the attempt at figuring out a query request
        """

        system_prompt = f"""You are the supervisor of the team of AI agents that
respond to requests involving external environmental data queries
"""
        
        retry_text = """
Do NOT include "RETRY_QUERY" in the response.

"""
        if retry_if_needed:
            retry_text = """
If you think there was a problem with the queries, include a
recommendation for how to fix them and include "RETRY_QUERY" in the response.

"""
        query = f"""Please summarize the process and results from your team's
attempt to respond to this request: {natural_language_query}

Generate a plain-language summary of how things have gone.

{retry_text}

The summary should include:
- The original request
- A high-level overview of what data sources were queried and how
- Any errors encountered
- An overview of the results (if determined)
- Recommendations for next steps (if warranted)



Here is the audit trail:

External queries: {external_query}

Generated code: {code}

Results: {results}

Logs: {logs}

Query errors encountered along the way: {previous_errors}

Conversation history: {conversation_history}
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

        if "RETRY_QUERY" in full_text and retry_if_needed:
            return full_text, True

        return full_text, False
