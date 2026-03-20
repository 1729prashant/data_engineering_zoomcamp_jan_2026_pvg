# Import necessary libraries.
# sys, Path: To modify the system path for module imports.
# datetime: To convert timestamps into a human-readable format.
import sys
from datetime import datetime
from pathlib import Path

# Add the parent directory to the system path to allow importing modules from 'src'.
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import third-party libraries.
import psycopg2 # PostgreSQL adapter for Python.
from kafka import KafkaConsumer

# Import the 'ride_deserializer' function from our 'models' module.
from models import ride_deserializer

# This script demonstrates a Kafka consumer that reads messages from a topic
# and writes them into a PostgreSQL database.

# Define the address of the Kafka bootstrap server.
server = 'localhost:9092'
# Define the name of the Kafka topic to consume from.
topic_name = 'rides'

# --- PostgreSQL Connection Setup ---
# Establish a connection to the PostgreSQL database.
# The host is 'localhost' because this script runs on the host machine,
# connecting to the 'postgres' container via its exposed port.
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='postgres',
    user='postgres',
    password='postgres'
)
# Set autocommit to True. This means every SQL statement (like INSERT) will be
# automatically committed to the database without needing an explicit conn.commit().
conn.autocommit = True
# Create a cursor object. Cursors allow you to execute SQL queries.
cur = conn.cursor()

# --- Kafka Consumer Setup ---
# Initialize the KafkaConsumer.
# - topic_name: The topic to subscribe to.
# - bootstrap_servers: The list of broker addresses.
# - auto_offset_reset='earliest': Start reading from the beginning of the topic.
# - group_id='rides-to-postgres': A unique consumer group ID for this consumer.
#   This allows Kafka to track the offset for this specific consumer group independently.
# - value_deserializer=ride_deserializer: The function to deserialize message values into 'Ride' objects.
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='rides-to-postgres',
    value_deserializer=ride_deserializer
)

print(f"Listening to {topic_name} and writing to PostgreSQL...")

# Loop through the messages received from the Kafka topic.
count = 0
for message in consumer:
    # 'message.value' is already a 'Ride' object due to the deserializer.
    ride = message.value
    
    # Convert the epoch milliseconds timestamp to a human-readable datetime object for storage.
    pickup_dt = datetime.fromtimestamp(ride.tpep_pickup_datetime / 1000)
    
    # Execute an INSERT SQL statement to store the ride data into the 'processed_events' table.
    # Using parameterized queries (%s) helps prevent SQL injection.
    cur.execute(
        """INSERT INTO processed_events
           (PULocationID, DOLocationID, trip_distance, total_amount, pickup_datetime)
           VALUES (%s, %s, %s, %s, %s)""",
        (ride.PULocationID, ride.DOLocationID,
         ride.trip_distance, ride.total_amount, pickup_dt)
    )
    
    count += 1
    # Print a progress message every 100 rows inserted.
    if count % 100 == 0:
        print(f"Inserted {count} rows...")

# --- Cleanup ---
# Close the consumer, cursor, and database connection to release resources.
consumer.close()
cur.close()
conn.close()
