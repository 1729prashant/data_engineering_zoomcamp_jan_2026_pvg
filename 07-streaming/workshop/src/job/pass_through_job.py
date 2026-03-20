# Import necessary PyFlink modules for stream processing.
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# This script defines a PyFlink job that reads taxi ride events from a Kafka topic,
# processes them (in this case, it's a "pass-through" job, meaning minimal transformation),
# and then writes the processed events to a PostgreSQL database.

# --- Flink Table DDL for PostgreSQL Sink ---
# This function defines the schema and connection details for the PostgreSQL table
# where processed events will be stored. Flink uses this DDL to understand how to
# write data to the JDBC sink.
def create_processed_events_sink_postgres(t_env):
    table_name = 'processed_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_distance DOUBLE,
            total_amount DOUBLE,
            pickup_datetime TIMESTAMP
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


# --- Flink Table DDL for Kafka Source ---
# This function defines the schema and connection details for the Kafka topic
# from which taxi ride events will be read. Flink uses this DDL to understand
# how to consume data from the Kafka source.
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
            tpep_pickup_datetime BIGINT
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

# --- Main Flink Job Logic ---
# This function orchestrates the entire Flink streaming job.
def log_processing():
    # Set up the Stream Execution Environment. This is the entry point for Flink streaming applications.
    env = StreamExecutionEnvironment.get_execution_environment()
    # Enable checkpointing every 10 seconds. Checkpointing is crucial for fault tolerance;
    # Flink periodically saves the state of the job, allowing it to recover from failures
    # without losing data or reprocessing everything.
    env.enable_checkpointing(10 * 1000)

    # Set up the Table Environment. This is used for defining and executing SQL queries on streams.
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    try:
        # Create and register the Kafka source table.
        source_table = create_events_source_kafka(t_env)
        # Create and register the PostgreSQL sink table.
        postgres_sink = create_processed_events_sink_postgres(t_env)
        
        # Define and execute the Flink SQL query.
        # This query reads data from the Kafka source, transforms the timestamp (from epoch milliseconds
        # to a SQL TIMESTAMP type), and inserts the results into the PostgreSQL sink.
        # This is a "pass-through" operation as it primarily moves data with minimal transformation.
        # .wait() makes the job run synchronously for local testing/debugging.
        t_env.execute_sql(
            f"""
                    INSERT INTO {postgres_sink}
                    SELECT
                        PULocationID,
                        DOLocationID,
                        trip_distance,
                        total_amount,
                        TO_TIMESTAMP_LTZ(tpep_pickup_datetime, 3) as pickup_datetime
                    FROM {source_table}
                    """
        ).wait() 

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


# Entry point for the script. When this Python file is executed, it will run the Flink job.
if __name__ == '__main__':
    log_processing()
