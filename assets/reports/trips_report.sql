/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# Docs: https://getbruin.com/docs/bruin/assets/sql
type: duckdb.sql

# Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  strategy: time_interval
  # set to your report's date column
  incremental_key: pickup_datetime
  # set to `date` or `timestamp`
  time_granularity: timestamp

# Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: composite_key
    type: string
    description: Unique key - composite of DO location, PU location, PU datetime, DO datetime, total_amount
    primary_key: true
  - name: pickup_datetime
    type: timestamp
    description: trip pickup datetime
  - name: dropoff_datetime
    type: timestamp
    description: trip dropoff datetime
  - name: pulocationid
    type: string
    description: TLC Taxi Zone where the meter was engaged
  - name: dolocationid
    type: string
    description: TLC Taxi Zone where the meter was disengaged
  - name: date_hour_trunc
    type: BIGINT
    description: Trips originating from PU location within hour
    checks:
      - name: non_negative
  - name: pu_hourly_trips
    type: BIGINT
    description: Trips originating from PU location within hour
    checks:
      - name: non_negative
  - name: pu_hourly_rev
    type: FLOAT
    description: Revenue originating from PU location within hour

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
  composite_key,
  pickup_datetime,
  dropoff_datetime,
  pulocationid,
  dolocationid,
  DATE_TRUNC('hour', pickup_datetime) AS date_hour_trunc,
  COUNT(*) AS pu_hourly_trips,
  SUM(total_amount) AS pu_hourly_rev,
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 6,4
