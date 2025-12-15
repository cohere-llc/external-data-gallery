"""
Tests for the DataAgent class and its methods.
"""

import pytest
from external_data_gallery import DataAgent

def test_agent_initialization():
    """Test that the DataAgent initializes correctly with data sources."""
    agent = DataAgent(api_key="test_key")
    as_json = agent.__repr__()
    assert "nasa_zarr" in as_json
    assert "gbif_parquet" in as_json

if __name__ == "__main__":
    pytest.main([__file__])