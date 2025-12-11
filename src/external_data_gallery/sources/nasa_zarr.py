"""
Prompts and schemas for NASA Zarr data
"""

import zarr
from zarr.storage import FsspecStore
from typing import Any, Dict


def schema() -> Dict[str, Any]:
    """Returns the schema for the NASA Zarr dataset."""
    
    store = FsspecStore.from_url(
        "s3://nasa-power/syn1deg/spatial/power_syn1deg_daily_spatial_utc.zarr",
        read_only=True,
        storage_options={"anon": True}
    )
    group = zarr.open_group(store, mode="r") # pyright: ignore[reportUnknownMemberType]
    fields = {name: array.metadata.attributes['long_name'] for name, array in group.arrays()}

    return {
      "name": "NASA POWER Synoptic 1-Degree Daily Data",
      "description": "A dataset containing daily synoptic data from NASA's Prediction of Worldwide Energy Resources (POWER) project in Zarr format.",
      "format": "zarr",
      "source type": "aws s3",
      "location": "s3://nasa-power/syn1deg/spatial/power_syn1deg_daily_spatial_utc.zarr",
      "fields": fields,
      "partitions": None,
      "notes": None
    }


