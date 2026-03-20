import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
# --- Data Engineering Perspective: Session Windows ---
# Session windows are different from tumbling or sliding windows because they are not based on time
# but on activity. A session window groups events together until a period of inactivity (a "gap")
# occurs. This is extremely useful for user behavior analysis, like tracking a user's actions on a
# website or, in this case, tracking a continuous streak of taxi pickups in a location.
def session_window_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    table_env = StreamTableEnvironment.create(env)
    # The JDBC connector JAR is automatically loaded by Flink from the /opt/flink/lib/ directory.
    # We no longer need to specify the JAR path manually.
    # --- Source Table (Kafka) ---
    # The source table definition is identical to the previous job, as we are reading
    # from the same 'green-trips' topic and using the same event time and watermark strategy.
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
            'properties.group.id' = 'flink-group-q5',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true' -- Ignore records that cause parsing errors (e.g., NaN values)
        );
    """)
    # --- Sink Table (PostgreSQL) ---
    # We define a new sink table for the results of this specific question.
    table_env.execute_sql("""
        CREATE TABLE q5_session_window_results (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            PULocationID INT,
            trips_in_session BIGINT,
            PRIMARY KEY (session_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'q5_session_window_results',
            'username' = 'postgres',
            'password' = 'postgres'
        );
    """)
    # --- Streaming Query with Session Window ---
    # - `SESSION(event_timestamp, INTERVAL '5' MINUTE)`: This is the key function. It creates
    #   session windows. The `INTERVAL '5' MINUTE` defines the "inactivity gap." If two events
    #   for the same `PULocationID` arrive more than 5 minutes apart, they will belong to
    #   different sessions.
    # - `SESSION_START` and `SESSION_END`: These functions return the start and end times of a session window.
    table_env.execute_sql("""
        INSERT INTO q5_session_window_results
        SELECT
            SESSION_START(event_timestamp, INTERVAL '5' MINUTE) as session_start,
            SESSION_END(event_timestamp, INTERVAL '5' MINUTE) as session_end,
            PULocationID,
            COUNT(PULocationID) as trips_in_session
        FROM
            green_trips
        GROUP BY
            SESSION(event_timestamp, INTERVAL '5' MINUTE),
            PULocationID;
    """)
if __name__ == '__main__':
    session_window_job()