/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# Set the asset name (recommended: staging.trips).
name: staging.trips
# Docs: https://getbruin.com/docs/bruin/assets/sql
type: duckdb.sql

# Declare dependencies so `bruin run ... --downstream` and lineage work.
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  # set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies (what does "incremental" mean?):
  # Incremental means you update only part of the destination instead of rebuilding everything every run.
  # In Bruin, this is controlled by `strategy` plus keys like `incremental_key` and `time_granularity`.
  #
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
  strategy: time_interval
  # set incremental_key to your event time column (DATE or TIMESTAMP).
  incremental_key: lpep_pickup_datetime
  # choose `date` vs `timestamp` based on the incremental_key type.
  time_granularity: timestamp

# TODO: Define output columns, mark primary keys, and add a few checks.
columns:
  - name: composite_key
    type: string
    description: Unique key - composite of DO location, PU location, PU datetime, DO datetime, total_amount
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

# Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: location_not_null
    description: Pick up and drop off location are not null
    query: |
      SELECT COUNT(*) FROM staging.trips WHERE pulocationid IS NULL OR pulocationid IS NULL;
    value: 0
  - name: unique_composite
    description: composite key should be unique
    query: |
      SELECT CASE
          WHEN COUNT(DISTINCT composite_key) = COUNT(composite_key) THEN 0
          ELSE 1
      END AS UniquenessCheck
      FROM staging.trips;
    value: 0

@bruin */

-- query: |
--   SELECT COUNT(*) FROM staging.trips GROUP BY composite_key HAVING COUNT(composite_key) > 1;

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

SELECT
    MD5(CONCAT(
      pulocationid, 
      dolocationid, 
      CAST(lpep_pickup_datetime AS STRING),
      CAST(lpep_dropoff_datetime AS STRING),
      CAST(total_amount AS STRING)
    ))AS composite_key,
    vendorid,
    lpep_pickup_datetime AS pickup_datetime,
    lpep_dropoff_datetime AS dropoff_datetime,
    passenger_count,
    trip_distance,
    pulocationid,
    dolocationid,
    ratecodeid,
    store_and_fwd_flag,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    trip_type,
    ehail_fee,
    extracted_at,
FROM ingestion.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
