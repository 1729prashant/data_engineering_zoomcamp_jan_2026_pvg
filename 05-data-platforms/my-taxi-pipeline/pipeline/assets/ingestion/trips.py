"""@bruin

name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: append
image: python:3.11

columns:
  - name: pickup_datetime
    type: timestamp
    description: When the meter was engaged
  - name: dropoff_datetime
    type: timestamp
    description: When the meter was disengaged

@bruin"""

# Required imports for ingestion
import os
import json
import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pyarrow.parquet as pq
import io


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.

def materialize():
  """
  Ingest NYC taxi trip data from TLC public endpoint for the configured taxi_types and date window.
  Uses Bruin's environment variables for date range and pipeline variables for taxi_types.
  Downloads Parquet files, concatenates them, adds an extracted_at column, and returns a DataFrame.
  """
  # Get Bruin date window and variables
  start_date = os.environ["BRUIN_START_DATE"]  # YYYY-MM-DD
  end_date = os.environ["BRUIN_END_DATE"]      # YYYY-MM-DD (exclusive)
  bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
  taxi_types = bruin_vars.get("taxi_types", ["yellow"])  # default to yellow if not set

  # TLC endpoint and file pattern
  TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
  FILE_PATTERN = "{taxi_type}_tripdata_{year}-{month:02d}.parquet"

  # Generate list of (taxi_type, year, month) for the run window
  start = datetime.strptime(start_date, "%Y-%m-%d")
  end = datetime.strptime(end_date, "%Y-%m-%d")
  months = []
  current = start.replace(day=1)
  while current < end:
    months.append((current.year, current.month))
    current += relativedelta(months=1)

  # Download and load all relevant files
  dfs = []
  for taxi_type in taxi_types:
    for year, month in months:
      file_name = FILE_PATTERN.format(taxi_type=taxi_type, year=year, month=month)
      url = TLC_BASE_URL + file_name
      try:
        resp = requests.get(url)
        resp.raise_for_status()
        table = pq.read_table(io.BytesIO(resp.content))
        df = table.to_pandas()
        df["taxi_type"] = taxi_type
        dfs.append(df)
      except Exception as e:
        print(f"Warning: Could not fetch {url}: {e}")

  if not dfs:
    # Return empty DataFrame with expected columns if nothing was fetched
    return pd.DataFrame(columns=["pickup_datetime", "dropoff_datetime", "taxi_type"])

  # Concatenate all DataFrames
  final_df = pd.concat(dfs, ignore_index=True)

  # Add extracted_at column for lineage/debugging
  final_df["extracted_at"] = datetime.utcnow()

  return final_df
