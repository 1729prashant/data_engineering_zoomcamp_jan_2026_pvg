-- This test will fail if any rows are returned
SELECT *
FROM {{ ref('fct_trips') }}
WHERE trip_distance is NULL
