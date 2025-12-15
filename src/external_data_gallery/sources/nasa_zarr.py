"""
Prompts and schemas for NASA Zarr data
"""

import zarr
from zarr.storage import FsspecStore
from typing import Any, Dict


def schema() -> Dict[str, Any]:
    """Returns the schema for the NASA Zarr dataset."""
    
    store = FsspecStore.from_url(
        "s3://nasa-power/merra2/spatial/power_merra2_daily_spatial_utc.zarr",
        read_only=True,
        storage_options={"anon": True}
    )
    group = zarr.open_group(store, mode="r") # pyright: ignore[reportUnknownMemberType]
    fields = {name: array.metadata.attributes['long_name'] for name, array in group.arrays()}
    query_template = """
```python
import zarr
from zarr.storage import FsspecStore, MemoryStore
from zarr.experimental.cache_store import CacheStore
import numpy as np
import pandas as pd

def execute_query():
    source_store = FsspecStore.from_url(
        's3://nasa-power/merra2/spatial/power_merra2_daily_spatial_utc.zarr',
        read_only=True,
        storage_options={'anon': True}
    )

    cache_store = MemoryStore()
    cached_store = CacheStore(
        store=source_store,
        cache_store=cache_store,
        max_size=256 * 1024 * 1024  # 256 MB cache
    )
    group = zarr.open_group(store=cached_store, mode='r')

    def extract_property_values(array_name, lat, lon, start_date, end_date):
        array = group[array_name]
        latitudes = group['lat'][:]
        longitudes = group['lon'][:]
        times = pd.to_datetime(group['time'][:], unit='D')

        lat_idx = (np.abs(latitudes - lat)).argmin()
        lon_idx = (np.abs(longitudes - lon)).argmin()

        time_mask = (times >= pd.to_datetime(start_date)) & (times <= pd.to_datetime(end_date))
        time_indices = np.where(time_mask)[0]
        selected_times = times[time_mask]

        values = array[time_indices, lat_idx, lon_idx]

        units = array.attrs.get('units', 'unknown')

        return pd.DataFrame({
            'date': selected_times,
            array_name: values,
            'units': units
        })

    for date in pd.date_range(start='{{START_DATE}}', end='{{END_DATE}}'):
        df = extract_property_values(
            array_name='{{VARIABLE_NAME}}',
            lat={{LATITUDE}},
            lon={{LONGITUDE}},
            start_date=date.strftime('%Y-%m-%d'),
            end_date=date.strftime('%Y-%m-%d')
        )

    return df
```
"""


    return {
      "name": "NASA POWER Modern-Era Retrospective analysis for Research and Applications, Version 2 (MERRA-2) ",
      "description": "A dataset containing MERRA-2 data from NASA's Prediction of Worldwide Energy Resources (POWER) project in Zarr format.",
      "format": "zarr",
      "source type": "aws s3",
      "location": "s3://nasa-power/merra2/spatial/power_merra2_daily_spatial_utc.zarr",
      "fields": fields,
      "partitions": None,
      "notes": "IMPORTANT: Follow the example code for creating FsspecStore and CacheStore VERY CLOSELY.",
      "query_template": query_template
    }


