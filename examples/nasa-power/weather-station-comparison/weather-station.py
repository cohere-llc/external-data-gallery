# Comparing weather station data with NASA POWER data
import zarr
from zarr.storage import MemoryStore, FsspecStore
from zarr import Group
import numpy as np
import pandas as pd
from zarr.experimental.cache_store import CacheStore

def load_store(path: str) -> Group:
    """Load a Zarr store from the given path."""
    source_store = FsspecStore.from_url(
        path,
        read_only=True,
        storage_options={'anon': True}
    )
    cache_store = MemoryStore()
    cached_store = CacheStore(
        store=source_store,
        cache_store=cache_store,
        max_size=256 * 1024 * 1024  # 256 MB cache
    )
    return zarr.open_group(store=cached_store, mode='r')

data_paths = {
    "flashflux (hourly LST)": "s3://nasa-power/flashflux/spatial/power_flashflux_daily_spatial_lst.zarr",
    "geosit (hourly UTC)": "s3://nasa-power/geosit/spatial/power_geosit_daily_spatial_utc.zarr",
    "merra2 (hourly UTC)": "s3://nasa-power/merra2/spatial/power_merra2_daily_spatial_utc.zarr",
    "syn1deg (hourly UTC)": "s3://nasa-power/syn1deg/spatial/power_syn1deg_daily_spatial_utc.zarr",
}


for dataset_name, path in data_paths.items():
    print("========================================")
    store = load_store(path)
    print(f"Variables in {dataset_name}:")
    for name, array in store.arrays():
        print(f"- {name}: {array.metadata.attributes['long_name']} [{array.metadata.attributes['units']}]")
        