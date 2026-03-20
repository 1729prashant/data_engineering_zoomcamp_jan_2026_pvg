# Import necessary libraries.
# sys, Path: To modify the system path for module imports.
# json: To deserialize JSON messages from Kafka.
import sys
import json
from pathlib import Path

# Add the parent directory to the system path to allow importing modules from 'src'.
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import the KafkaConsumer class from the kafka-python library.
from kafka import KafkaConsumer

# --- Data Engineering Perspective: The Role of a Consumer ---
# A consumer is an application that subscribes to one or more Kafka topics and processes the
# stream of records produced to them. Consumers are the "read" side of a streaming pipeline.
# They can be used for various purposes: writing data to a database, feeding a real-time
# dashboard, or performing stream processing and analytics.

# --- Deserialization: Interpreting Data from the Stream ---
# Since producers serialize data into bytes, consumers must deserialize it back into a usable format.
# This function takes the raw byte value from a Kafka message, decodes it from UTF-8, and
# parses the JSON string into a Python dictionary.
def json_deserializer(data):
    return json.loads(data.decode('utf-8'))


# --- Kafka Consumer Configuration ---
# Define the address of the Kafka bootstrap server.
server = 'localhost:9092'
# Define the name of the Kafka topic to consume from.
topic_name = 'green-trips'

# Initialize the KafkaConsumer.
# - topic_name: The topic to subscribe to.
# - bootstrap_servers: The list of broker addresses.
# - auto_offset_reset='earliest': This is a critical setting. It tells the consumer where to
#   start reading if it's a new consumer group or if its last known offset is invalid.
#   'earliest': Start from the very first message in the topic. Use this when you need to
#               process all historical data in the topic.
#   'latest' (default): Start from the most recent message. Use this when you only care
#                       about new data arriving after the consumer starts.
# - group_id='green-trips-homework': This ID identifies the consumer group. Kafka uses this to
#   track the progress (the "offset") of this group for each topic partition. If you run
#   multiple instances of this consumer with the same group_id, they will automatically
#   balance the load, with each instance reading from a different subset of partitions.
# - value_deserializer: The function to use for deserializing the message value.
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-homework',
    value_deserializer=json_deserializer
)

print(f"Listening to topic '{topic_name}' from the beginning...")

# --- Processing the Stream ---
# This is the core logic of the consumer. It continuously polls Kafka for new messages.
count = 0
trips_with_distance_gt_5 = 0

try:
    # The consumer object is an iterator. This loop will block and wait for new messages
    # if it reaches the end of the topic, making it suitable for continuous streaming.
    # For a batch-style job like this (reading a finite dataset), we could add a timeout
    # to automatically exit the loop, but for this homework, we'll manually stop it.
    for message in consumer:
        # The 'message.value' is already a dictionary because of the 'value_deserializer'.
        trip = message.value
        count += 1

        # Apply the business logic: count trips where the distance is greater than 5.0.
        if trip.get('trip_distance', 0) > 5.0:
            trips_with_distance_gt_5 += 1

        # Print progress every 10,000 messages to provide feedback without overwhelming the console.
        if count % 10000 == 0:
            print(f"Processed {count} messages. Found {trips_with_distance_gt_5} trips with distance > 5.0 so far.")

except KeyboardInterrupt:
    # This block allows for a graceful shutdown. If you press Ctrl+C, the script will
    # exit the loop and print the final results instead of crashing.
    print("\nInterrupted by user. Finalizing...")

finally:
    # Print the final results.
    print(f"\n--- Final Results ---")
    print(f"Total messages processed: {count}")
    print(f"Number of trips with distance > 5.0: {trips_with_distance_gt_5}")

    # Close the consumer connection to release resources and ensure a clean shutdown.
    # This also triggers a final commit of offsets to Kafka if auto-commit is enabled.
    consumer.close()
    print("Consumer closed.")