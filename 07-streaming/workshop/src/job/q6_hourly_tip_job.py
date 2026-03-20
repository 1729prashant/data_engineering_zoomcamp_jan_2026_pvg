import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
def hourly_tip_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    table_env = StreamTableEnvironment.create(env)
    # The JDBC connector JAR is automatically loaded by Flink from the /opt/flink/lib/ directory.
    # We no longer need to specify the JAR path manually.
    # --- Source Table (Kafka) ---
    # We need to include the `tip_amount` field in our source table definition.
    table_env.execute_sql("""
        CREATE TABLE green_trips (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INT,
            DOLocationID INT,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'flink-group-q6',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true' -- Ignore records that cause parsing errors (e.g., NaN values)
        );
    """)
    # --- Sink Table (PostgreSQL) ---
    table_env.execute_sql("""
        CREATE TABLE q6_hourly_tip_results (
            window_end TIMESTAMP(3),
            total_tip DOUBLE,
            PRIMARY KEY (window_end) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'q6_hourly_tip_results',
            'username' = 'postgres',
            'password' = 'postgres'
        );
    """)
    # --- Streaming Query ---
    # This query calculates the sum of `tip_amount` over 1-hour tumbling windows.
    # Note that we are not grouping by any location ID, so the sum is across all trips
    # that fall within each 1-hour window.
    # We use `TUMBLE_END` to identify the window, as is common in reporting.
    table_env.execute_sql("""
        INSERT INTO q6_hourly_tip_results
        SELECT
            TUMBLE_END(event_timestamp, INTERVAL '1' HOUR) as window_end,
            SUM(tip_amount) as total_tip
        FROM
            green_trips
        GROUP BY
            TUMBLE(event_timestamp, INTERVAL '1' HOUR);
    """)
if __name__ == '__main__':
    hourly_tip_job()