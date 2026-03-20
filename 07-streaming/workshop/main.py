from curses import raw

import pandas as pd
from dataclasses import dataclass
import dataclasses
import time

import json
from kafka import KafkaProducer

# Define a dataclass for our message. This gives us a clear schema for each taxi trip:
@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    tpep_pickup_datetime: int  # epoch milliseconds

# Write a function to convert a DataFrame row into a Ride. We convert the pandas Timestamp to epoch milliseconds - that's the format Flink expects later:
def ride_from_row(row):
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        tpep_pickup_datetime=int(row['tpep_pickup_datetime'].timestamp() * 1000),
    )


def main():

    # First, download the data. We read a parquet file of yellow taxi trips and take the first 1000 rows:
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
    columns = ['PULocationID', 'DOLocationID', 'trip_distance', 'total_amount', 'tpep_pickup_datetime']
    df = pd.read_parquet(url, columns=columns).head(1000)
    df.head()

    ride = ride_from_row(df.iloc[0])
    print(ride)

    # Next, connect to Kafka. 
    # The bootstrap_servers is where the broker accepts connections - localhost:9092 
    # because we're running this from our laptop (outside Docker). 
    # In production with multiple brokers, you'd list several for redundancy - if one is down, 
    # the client connects through another.
    # Kafka works with raw bytes, so we need a serializer that converts Python dicts to JSON:
    def json_serializer(data):
        return json.dumps(data).encode('utf-8')
    server = 'localhost:9092'
    producer = KafkaProducer(
        bootstrap_servers=[server],
        value_serializer=json_serializer
    )


    # Let's send a single ride to try it out. dataclasses.asdict(ride) converts the dataclass 
    # to a plain dict, which the serializer turns into JSON bytes. The broker auto-creates 
    # the rides topic on first use:
    topic_name = 'rides'

    producer.send(topic_name, value=dataclasses.asdict(ride))
    producer.flush()


    # This works, but calling dataclasses.asdict() every time is tedious. 
    # We can make a serializer that handles dataclasses directly:
    def ride_serializer(ride):
        ride_dict = dataclasses.asdict(ride)
        json_str = json.dumps(ride_dict)
        return json_str.encode('utf-8')



    # Now recreate the producer with the new serializer - we can pass Ride objects 
    # directly without converting them to dicts first:
    producer = KafkaProducer(
        bootstrap_servers=[server],
        value_serializer=ride_serializer
    )   

    # Send one ride to verify:
    producer.send(topic_name, value=ride)
    producer.flush()

    # verify topic exists inside the Redpanda container:
    # rpk topic list

    # consume messages from the topic to verify they are being produced correctly:
    # rpk topic consume rides -n 5
    # If you see JSON messages → your producer works correctly.

    # check topic metadata
    # rpk topic describe rides


    # That sent one record. Now let's send all 1000 rides in a loop:

    t0 = time.time()

    for _, row in df.iterrows():
        ride = ride_from_row(row)
        producer.send(topic_name, value=ride)
        print(f"Sent: {ride}")
        time.sleep(0.01)

    producer.flush()

    t1 = time.time()
    print(f'took {(t1 - t0):.2f} seconds')
    

if __name__ == "__main__":
    main()
