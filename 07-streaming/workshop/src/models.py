# Import necessary libraries.
# json is used for encoding and decoding data structures.
# dataclass is a decorator that automatically generates special methods like __init__ and __repr__ for classes.
import json
from dataclasses import dataclass


# Define a data class 'Ride' to represent the structure of a taxi ride event.
# This provides a clear, typed schema for the data we'll be sending and receiving.
@dataclass
class Ride:
    PULocationID: int  # Pickup location ID
    DOLocationID: int  # Dropoff location ID
    trip_distance: float  # The distance of the trip in miles
    total_amount: float  # The total cost of the trip
    tpep_pickup_datetime: int  # Pickup timestamp in epoch milliseconds, used for event time processing


# A utility function to create a Ride object from a row of a pandas DataFrame.
# This is used by the producer to convert raw data into our structured 'Ride' format.
def ride_from_row(row):
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        # Convert pandas Timestamp to epoch milliseconds, which is a common format for event time in streaming systems.
        tpep_pickup_datetime=int(row['tpep_pickup_datetime'].timestamp() * 1000),
    )


# A deserializer function for the Kafka consumer.
# Kafka messages are received as raw bytes. This function decodes the bytes into a UTF-8 string,
# parses the JSON string into a Python dictionary, and then constructs a 'Ride' object from it.
def ride_deserializer(data):
    # Decode the byte string to a UTF-8 string.
    json_str = data.decode('utf-8')
    # Load the JSON string into a Python dictionary.
    ride_dict = json.loads(json_str)
    # Use dictionary unpacking to create a Ride object. This is a clean way to map dictionary keys to class attributes.
    return Ride(**ride_dict)
