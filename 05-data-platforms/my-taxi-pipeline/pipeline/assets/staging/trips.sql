/* @bruin

name: staging.trips
type: duckdb.sql

depends:
   - ingestion.trips
   - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp


columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: "Pickup location zone ID"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: "Dropoff location zone ID"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: payment_type
    type: integer
    description: "Payment type code"
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Payment type name (lookup)"
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Taxi type (yellow/green)"
    checks:
      - name: not_null
  - name: extracted_at
    type: timestamp
    description: "Extraction timestamp"
    checks:
      - name: not_null

custom_checks:
  - name: deduplication_check
    description: "Ensure no duplicate trips for composite key in staging layer"
    query: |
      SELECT COUNT(*) - COUNT(DISTINCT pickup_datetime || dropoff_datetime || pickup_location_id || dropoff_location_id || fare_amount)
      FROM staging.trips
      WHERE pickup_datetime >= '{{ start_datetime }}' AND pickup_datetime < '{{ end_datetime }}'
    value: 0

@bruin */

WITH deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY tpep_pickup_datetime, tpep_dropoff_datetime, pu_location_id, do_location_id, fare_amount
      ORDER BY extracted_at DESC
    ) AS rn
  FROM ingestion.trips
  WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
    AND tpep_pickup_datetime < '{{ end_datetime }}'
)
SELECT
  d.tpep_pickup_datetime as pickup_datetime,
  d.tpep_dropoff_datetime as dropoff_datetime,
  d.pu_location_id as pickup_location_id,
  d.do_location_id as dropoff_location_id,
  d.fare_amount,
  d.payment_type,
  p.payment_type_name,
  d.taxi_type,
  d.extracted_at
FROM deduped d
LEFT JOIN ingestion.payment_lookup p
  ON d.payment_type = p.payment_type
WHERE d.rn = 1
