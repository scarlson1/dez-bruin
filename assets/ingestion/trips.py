"""@bruin

# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.nyc_taxi_trips

# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# Pick a Python image version (Bruin runs Python in isolated environments).
image: python:3.13

# Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # choose `table` or `view` (ingestion generally should be a table)
  type: table
  # suggested strategy: append
  strategy: append

parameters:
  enforce_schema: true

# Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
# green
columns:
  - name: vendorid
    type: string
    description: "Taxi technology provider (1 = Creative Mobile Technologies, 2 = VeriFone Inc.) - Note: Raw data may contain nulls, filtered in staging"
  - name: pickup_datetime
    type: timestamp
    description: Date and time when the meter was engaged
  - name: dropoff_datetime
    type: timestamp
    description: Date and time when the meter was disengaged
  - name: passenger_count
    type: integer
    description: Number of passengers in the vehicle
  - name: trip_distance
    type: float
    description: Trip distance in miles
  - name: pulocationid
    type: string
    description: TLC Taxi Zone where the meter was engaged
  - name: dolocationid
    type: string
    description: TLC Taxi Zone where the meter was disengaged
  - name: ratecodeid
    type: integer
    description: Rate code (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group)
  - name: store_and_fwd_flag
    type: string
    description: Trip record held in vehicle memory (Y/N)
  - name: payment_type
    type: integer
    description: Payment method (1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided)
  - name: fare_amount
    type: float
    description: Time and distance fare
  - name: extra
    type: float
    description: Miscellaneous extras and surcharges
  - name: mta_tax
    type: float
    description: MTA tax
  - name: tip_amount
    type: float
    description: Tip amount (credit card only)
  - name: tolls_amount
    type: float
    description: Total tolls paid
  - name: improvement_surcharge
    type: float
    description: Improvement surcharge
  - name: total_amount
    type: float
    description: Total amount charged
  - name: trip_type
    type: integer
    description: Trip type (1=Street-hail, 2=Dispatch)
  - name: ehail_fee
    type: string
    description: E-hail fee
  - name: extracted_at
    type: timestamp
    description: datetime bruin loaded the data
@bruin"""

# Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python

import io
import json
import os
from datetime import date

import pandas as pd
import pyarrow.parquet as pq
import requests
from dateutil.relativedelta import relativedelta

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
start_date = os.environ.get("BRUIN_START_DATE")
end_date = os.environ.get("BRUIN_END_DATE")
taxi_types = vars.get("taxi_types") or ["yellow"]
if isinstance(taxi_types, str):
    taxi_types = [taxi_types]


def _parse_date(value: str, name: str) -> date:
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return date.fromisoformat(value)


def _month_starts(start: date, end: date):
    current = date(start.year, start.month, 1)
    end_month = date(end.year, end.month, 1)
    while current <= end_month:
        yield current
        current += relativedelta(months=1)


def _fetch_parquet(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    table = pq.read_table(io.BytesIO(response.content))
    return table.to_pandas()


# Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """

    start = _parse_date(start_date, "BRUIN_START_DATE")
    end = _parse_date(end_date, "BRUIN_END_DATE")

    dataframes = []
    extracted_at = pd.Timestamp.utcnow()

    for month_start in _month_starts(start, end):
        year = month_start.year
        month = month_start.month
        for taxi_type in taxi_types:
            filename = f"{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
            url = f"{TLC_BASE_URL}{filename}"
            df = _fetch_parquet(url)
            df["extracted_at"] = extracted_at
            dataframes.append(df)

    if not dataframes:
        return pd.DataFrame()

    return pd.concat(dataframes, ignore_index=True)
