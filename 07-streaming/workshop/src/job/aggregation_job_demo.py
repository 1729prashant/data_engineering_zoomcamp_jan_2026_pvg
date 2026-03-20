"""
Demo aggregation job with 10-second tumbling windows.

Use with producer_realtime.py to observe watermark behavior:
- Watermark = event_timestamp - 5 seconds
- Late events (<=5s) arrive before the watermark closes the window -> included
- Late events (>5s) may arrive after the watermark closes the window -> dropped
"""

# Import necessary PyFlink modules for stream processing.
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# This script defines a PyFlink job similar to `aggregation_job.py`, but it uses
# shorter 10-second tumbling windows. This is specifically designed for demonstration
# purposes, allowing quicker observation of watermarking and late event handling
# when used with `producer_realtime.py`.

# --- Flink Table DDL for Kafka Source with Watermarks ---
# This function defines the schema and connection details for the Kafka topic
# from which taxi ride events will be read. It includes event_timestamp and WATERMARK
# definitions, crucial for time-windowed processing.
            # Define a computed column 'event_timestamp' from 'tpep_pickup_datetime'.
            # Define a watermark strategy. This tells Flink how to handle out-of-order events.
            # 'event_timestamp - INTERVAL '5' SECOND' means Flink will wait for 5 seconds
            # of "lateness" for events before closing a window and emitting results.
            # 'scan.startup.mode' = 'latest-offset', # Start reading from the latest available offset.
            # 'properties.auto.offset.reset' = 'latest', # Kafka consumer property for offset reset.
            # 'format' = 'json' # Specify that the data in Kafka is in JSON format.
def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_distance DOUBLE,
            total_amount DOUBLE,
            tpep_pickup_datetime BIGINT,
            
            event_timestamp AS TO_TIMESTAMP_LTZ(tpep_pickup_datetime, 3),

            WATERMARK for event_timestamp as event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'rides',
            'scan.startup.mode' = 'latest-offset', 
            'properties.auto.offset.reset' = 'latest', 
            'format' = 'json' 
        );
        """
    # Execute the DDL statement to register the source table with Flink's Table Environment.
    t_env.execute_sql(source_ddl)
    return table_name


# --- Flink Table DDL for Aggregated PostgreSQL Sink ---
# This function defines the schema and connection details for the PostgreSQL table
# where aggregated events will be stored. It includes a primary key for upsert behavior.
            # window_start TIMESTAMP(3), # The start timestamp of the aggregation window.
            # PULocationID INT,          # Pickup location ID, used as a grouping key.
            # num_trips BIGINT,          # Count of trips within the window for that location.
            # total_revenue DOUBLE,      # Sum of total_amount within the window for that location.
            # PRIMARY KEY (window_start, PULocationID) NOT ENFORCED # Defines a primary key for upserts.
def create_events_aggregated_sink(t_env):
    table_name = 'processed_events_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3), 
            PULocationID INT,          
            num_trips BIGINT,          
            total_revenue DOUBLE,      
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED 
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    # Execute the DDL statement to register the sink table with Flink's Table Environment.
    t_env.execute_sql(sink_ddl)
    return table_name


# --- Main Flink Job Logic for Aggregation Demo ---
# This function orchestrates the Flink streaming job for aggregation with short windows.
def log_aggregation():
    # Set up the Stream Execution Environment.
    env = StreamExecutionEnvironment.get_execution_environment()
    # Enable checkpointing every 10 seconds for fault tolerance.
    env.enable_checkpointing(10 * 1000)
    # Set parallelism to 1 for easier observation in a demo scenario.
    env.set_parallelism(1)

    # Set up the Table Environment for SQL-based stream processing.
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create and register the Kafka source table.
        source_table = create_events_source_kafka(t_env)
        # Create and register the PostgreSQL sink table for aggregated results.
        aggregated_table = create_events_aggregated_sink(t_env)

        # Define and execute the Flink SQL query for time-windowed aggregation.
        # This uses 10-second tumbling windows (instead of 1 hour) to quickly observe
        # window closing and how late events are handled by the watermark.
        t_env.execute_sql(f"""
        INSERT INTO {aggregated_table}
        SELECT
            window_start,
            PULocationID,
            COUNT(*) AS num_trips,
            SUM(total_amount) AS total_revenue
        FROM TABLE(
            TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '10' SECOND)
        )
        GROUP BY window_start, PULocationID;

        """).wait() # .wait() makes the job run synchronously.

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


# Entry point for the script.
if __name__ == '__main__':
    log_aggregation()
