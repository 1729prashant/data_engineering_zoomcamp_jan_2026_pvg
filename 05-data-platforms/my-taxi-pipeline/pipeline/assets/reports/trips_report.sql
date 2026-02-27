/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks


# Asset metadata
name: reports.trips_report
type: duckdb.sql
depends:
  - staging.trips
materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_date
    type: date
    description: "Date of trip pickup"
    primary_key: true
  - name: taxi_type
    type: string
    description: "Taxi type (yellow/green)"
    primary_key: true
  - name: payment_type_name
    type: string
    description: "Payment type name"
    primary_key: true
  - name: trip_count
    type: integer
    description: "Number of trips"
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns


-- Aggregate report: trips by date, taxi type, payment type
SELECT
  CAST(pickup_datetime AS DATE) AS pickup_date,
  taxi_type,
  payment_type_name,
  COUNT(*) AS trip_count,
  SUM(fare_amount) AS total_fare
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY pickup_date, taxi_type, payment_type_name
