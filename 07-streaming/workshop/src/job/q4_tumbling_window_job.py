import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
  # --- Data Engineering Perspective: Flink SQL Jobs ---
# This script is a PyFlink job that uses the high-level Table API and Flink SQL.
# Flink SQL allows you to define and run streaming analytics queries using standard SQL syntax,
# which is highly accessible and powerful for data engineers. It abstracts away much of the
# complexity of low-level stream processing.
def tumbling_window_job():
    # --- Environment Setup ---
    # Set up the Flink execution environment. This is the entry point for any Flink job.
    env = StreamExecutionEnvironment.get_execution_environment()
    # As per the homework instructions, set parallelism to 1.
    # For this specific dataset and topic (with 1 partition), this prevents idle subtasks
    # from holding back the watermark, ensuring timely processing.
    env.set_parallelism(1)
    # Create a StreamTableEnvironment for using the Table API and Flink SQL.
    table_env = StreamTableEnvironment.create(env)
    # --- JDBC Connector ---
    # The JDBC connector JAR is automatically loaded by Flink from the /opt/flink/lib/ directory.
    # We no longer need to specify the JAR path manually.
    # --- Source Table (Kafka) ---
    # Define the source table using Flink SQL DDL (Data Definition Language).
    # This statement describes how to connect to Kafka and interpret the data.
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
            -- `event_timestamp` is a computed column that converts the string timestamp
            -- into a proper Flink TIMESTAMP(3) type. This is crucial for event-time processing.
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            -- `WATERMARK` is the core concept for handling late events in a stream.
            -- It tells Flink how much out-of-orderness to expect in the event times.
            -- `event_timestamp - INTERVAL '5' SECOND` means we assume events will not be more
            -- than 5 seconds late. The watermark for a window will only advance when Flink is
            -- confident that no more events for that window will arrive.
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092', -- Using the internal Docker network address
            'properties.group.id' = 'flink-group-q4',
            'scan.startup.mode' = 'earliest-offset', -- Start reading from the beginning of the topic
            'format' = 'json',
            'json.ignore-parse-errors' = 'true' -- Ignore records that cause parsing errors (e.g., NaN values)
        );
    """)
    # --- Sink Table (PostgreSQL) ---
    # Define the sink table, which is our PostgreSQL table.
    # Flink will use the JDBC connector to write the results of our query to this table.
    table_env.execute_sql("""
        CREATE TABLE q4_tumbling_window_results (
            window_start TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'q4_tumbling_window_results',
            'username' = 'postgres',
            'password' = 'postgres'
        );
    """)
    # --- Streaming Query ---
    # This is the main processing logic, written in Flink SQL.
    # - `TUMBLE(event_timestamp, INTERVAL '5' MINUTE)`: This function groups events into
    #   fixed-size, 5-minute, non-overlapping windows based on their event time.
    # - `GROUP BY`: We group by the window and the pickup location ID.
    # - `COUNT(PULocationID)`: We count the number of trips in each group.
    # The results are then inserted into our PostgreSQL sink table.
    table_env.execute_sql("""
        INSERT INTO q4_tumbling_window_results
        SELECT
            TUMBLE_START(event_timestamp, INTERVAL '5' MINUTE) as window_start,
            PULocationID,
            COUNT(PULocationID) as num_trips
        FROM
            green_trips
        GROUP BY
            TUMBLE(event_timestamp, INTERVAL '5' MINUTE),
            PULocationID;
    """)
if __name__ == '__main__':
    tumbling_window_job()