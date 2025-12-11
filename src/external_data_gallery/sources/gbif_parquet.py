"""
Prompts and schemas for GBIF Parquet data
"""

import dask.dataframe as dd
from typing import Any, Dict
import s3fs


def schema() -> Dict[str, Any]:
    """Returns the schema for the GBIF Parquet dataset."""

    df = dd.read_parquet( # pyright: ignore[reportUnknownMemberType]
        "s3://gbif-open-data-af-south-1/occurrence/2021-06-01/occurrence.parquet/",
        storage_options={"anon": True},
        engine="pyarrow",
        parquet_file_extension=""
    )
    fields = {field:field for field in df.columns.tolist()}
    fs = s3fs.S3FileSystem(anon=True)
    folders = fs.ls("gbif-open-data-af-south-1/occurrence/")
    dates = [folder.split("/")[-1] for folder in folders if folder.split("/")[-1].count("-") == 2]

    return {
      "name": "Global Biodiversity Information Facility Species Occurrence Data",
      "description": "A dataset containing species occurrence records from the Global Biodiversity Information Facility (GBIF) in Parquet format.",
      "format": "parquet",
      "source type": "aws s3",
      "location": "s3://gbif-open-data-parquet/occurrence/",
      "fields": fields,
      "partitions": dates,
      "notes": "Data is stored in Parquet format on AWS S3. Each monthly partition contains occurrence records for that month. Use Dask to efficiently query and analyze the data. Folder structure is s3://gbif-open-data-af-south-1/occurrence/{YYYY-MM-DD}/occurrence.parquet/"
    }
