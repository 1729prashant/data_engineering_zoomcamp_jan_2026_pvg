# Import necessary libraries.
# dataclasses: To work with the Ride data class.
# json: To serialize data into JSON format.
# random: To generate random data for ride events and simulate delays.
# sys, Path: To modify the system path for module imports.
# time, datetime, timezone: To handle timestamps and simulate real-time events.
import dataclasses
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add the parent directory to the system path to allow importing modules from 'src'.
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import third-party libraries.
from kafka import KafkaProducer

# Import the 'Ride' data class from our 'models' module.
from models import Ride

# This script simulates a real-time producer that generates synthetic taxi ride data.
# It includes logic to occasionally create "late" events to demonstrate how stream processors handle them.

# A list of popular pickup locations based on real NYC taxi data.
# PULocationID is a taxi zone ID (1-263) defined by the NYC TLC.
# For more details, see: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
PICKUP_LOCATIONS = [
    79,   # East Village, Manhattan
    107,  # Gramercy, Manhattan
    48,   # Clinton East (Hell's Kitchen), Manhattan
    132,  # JFK Airport
    234,  # Union Sq, Manhattan
    148,  # Lower East Side, Manhattan
    249,  # West Village, Manhattan
    68,   # East Chelsea, Manhattan
    90,   # Flatiron, Manhattan
    263,  # Yorkville West, Manhattan
    138,  # LaGuardia Airport
    230,  # Times Sq/Theatre District, Manhattan
    161,  # Midtown Center, Manhattan
    162,  # Midtown East, Manhattan
    170,  # Murray Hill, Manhattan
    237,  # Upper East Side South, Manhattan
    239,  # Upper West Side South, Manhattan
    186,  # Penn Station/Madison Sq West, Manhattan
    164,  # Midtown South, Manhattan
    236,  # Upper East Side North, Manhattan
]

# For simplicity, we use the same set of locations for drop-offs.
DROPOFF_LOCATIONS = PICKUP_LOCATIONS


# A function to generate a synthetic 'Ride' event.
# It can also create events with a past timestamp to simulate network delays or late-arriving data.
def make_ride(delay_seconds=0):
    # Get the current time in epoch milliseconds.
    # If a delay is specified, subtract it to create a timestamp in the past.
    now_ms = int(time.time() * 1000) - delay_seconds * 1000
    return Ride(
        PULocationID=random.choice(PICKUP_LOCATIONS),
        DOLocationID=random.choice(DROPOFF_LOCATIONS),
        trip_distance=round(random.uniform(0.5, 20.0), 2),
        total_amount=round(random.uniform(5.0, 100.0), 2),
        tpep_pickup_datetime=now_ms,
    )


# Define a serializer function for Kafka messages.
# This is similar to the one in the batch producer, converting a Ride object to a JSON byte string.
def ride_serializer(ride):
    return json.dumps(dataclasses.asdict(ride)).encode('utf-8')


# Define the Kafka bootstrap server address.
server = 'localhost:9092'

# Initialize the KafkaProducer.
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer,
)

# Define the Kafka topic name.
topic_name = 'rides'
count = 0

print("Sending events (Ctrl+C to stop)...")
print()

try:
    # Loop indefinitely to continuously generate and send events.
    while True:
        # With a ~20% probability, create a "late" event.
        # This is crucial for testing how Flink's watermarking and windowing logic handles out-of-order data.
        if random.random() < 0.2:
            # Generate a random delay between 3 and 10 seconds.
            delay = random.randint(3, 10)
            ride = make_ride(delay_seconds=delay)
            ts = datetime.fromtimestamp(ride.tpep_pickup_datetime / 1000, tz=timezone.utc)
            print(f"  LATE ({delay}s) -> PU={ride.PULocationID} ts={ts:%H:%M:%S}")
        else:
            # Otherwise, create an on-time event.
            ride = make_ride()
            ts = datetime.fromtimestamp(ride.tpep_pickup_datetime / 1000, tz=timezone.utc)
            print(f"  on time   -> PU={ride.PULocationID} ts={ts:%H:%M:%S}")

        # Send the ride event to the Kafka topic.
        producer.send(topic_name, value=ride)
        count += 1
        
        # Wait for half a second before sending the next event.
        time.sleep(0.5)

# Handle graceful shutdown on keyboard interrupt (Ctrl+C).
except KeyboardInterrupt:
    # Ensure all buffered messages are sent.
    producer.flush()
    print(f"\nSent {count} events")
