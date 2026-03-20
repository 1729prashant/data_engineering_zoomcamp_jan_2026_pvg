# Import necessary libraries.
# json: To serialize data into JSON format.
# sys, Path: To modify the system path for module imports.
# time: To measure execution time.
import json
import sys
from pathlib import Path
from time import time

# Add the parent directory to the system path to allow importing modules from 'src'.
# This is a common pattern for structuring Python projects.
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import third-party libraries.
import pandas as pd
from kafka import KafkaProducer

# --- Data Engineering Perspective: The Role of a Producer ---
# A producer is a fundamental component in a streaming architecture. Its primary role is to ingest data
# from various sources into a central streaming platform like Kafka or Redpanda.
# This script acts as a "batch producer," reading a static dataset (a Parquet file) and
# publishing its contents as a stream of events. In real-world scenarios, producers might
# capture data from web servers, IoT devices, database change-data-capture (CDC) logs, or other microservices.

# --- Data Source ---
# The data is from the NYC Green Taxi dataset. For this homework, you need to download it first.
# URL: https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet
# Place the downloaded file in the 'workshop' directory.
file_path = "data/green_tripdata_2025-10.parquet"

# Define the columns we want to keep from the dataset.
# Selecting specific columns reduces memory usage and network bandwidth, which is a best practice.
columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount',
]

# Read the Parquet file into a pandas DataFrame.
# Parquet is a columnar storage format, which is highly efficient for reading specific columns,
# making it a popular choice in data engineering pipelines.
try:
    df = pd.read_parquet(file_path, columns=columns)
except FileNotFoundError:
    print(f"Error: The file '{file_path}' was not found.")
    print("Please download it from 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet' and place it in the current directory.")
    sys.exit(1)


# --- Serialization: Preparing Data for the Stream ---
# Kafka messages are transmitted as byte arrays. To send structured data, we must serialize it.
# JSON is a common choice for serialization because it is language-agnostic, human-readable, and widely supported.
# This function converts a Python dictionary to a JSON-formatted string and then encodes it to UTF-8 bytes.
def json_serializer(data):
    return json.dumps(data).encode('utf-8')


# --- Kafka Producer Configuration ---
# Define the address of the Kafka bootstrap server.
# 'localhost:9092' is used because this script runs on the host machine, connecting to the
# Redpanda container via its exposed port.
server = 'localhost:9092'

# Initialize the KafkaProducer.
# - bootstrap_servers: The list of broker addresses to connect to.
# - value_serializer: The function to call to serialize the message value before sending.
# The producer handles connection pooling, batching of messages, and other optimizations.
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

# --- Sending Data to a Topic ---
# Record the start time to measure ingestion performance.
t0 = time()

# Define the name of the Kafka topic. A topic is a logical channel to which producers write
# events and from which consumers read.
topic_name = 'green-trips'

# Iterate over each row of the DataFrame to create and send events.
for _, row in df.iterrows():
    # Convert the DataFrame row to a dictionary.
    row_dict = row.to_dict()

    # --- Handling Timestamps ---
    # Here, we explicitly format the datetime objects into strings with a
    # specific format that our Flink job will be able to parse reliably.
    row_dict['lpep_pickup_datetime'] = row['lpep_pickup_datetime'].strftime('%Y-%m-%d %H:%M:%S')
    row_dict['lpep_dropoff_datetime'] = row['lpep_dropoff_datetime'].strftime('%Y-%m-%d %H:%M:%S')

    # Send the dictionary to the specified Kafka topic.
    # The producer sends messages asynchronously by default. It adds the message to an internal
    # buffer and sends it to the broker in a background thread. This improves throughput.
    # We are not specifying a message key, so the producer will use a round-robin policy
    # to distribute messages across the topic's partitions.
    producer.send(topic_name, value=row_dict)

# --- Ensuring Data Delivery ---
# The .flush() method is crucial. It blocks until all buffered messages have been successfully
# sent to the Kafka broker. Without this, the script might exit before all messages are delivered,
# leading to data loss.
print("Flushing messages...")
producer.flush()
print("All messages sent.")

# Record the end time and print the total duration.
t1 = time()
print(f'Took {(t1 - t0):.2f} seconds to send {len(df)} records.')

# Close the producer connection to release network resources.
producer.close()