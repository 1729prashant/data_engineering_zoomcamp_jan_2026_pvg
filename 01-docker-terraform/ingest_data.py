#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine
import pyarrow.parquet as pq


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

zone_dtype = {
    "LocationID":"Int64",
    "Borough":"string",
    "Zone":"string",
    "service_zone":"string"
}

# does not matter for parquet
# green_taxi_dtype = {
# "VendorID":"Int64",
# "store_and_fwd_flag":"string",
# "RatecodeID":"Int64",
# "PULocationID":"Int64",
# "DOLocationID":"Int64",
# "passenger_count":"Int64",	
# "trip_distance":"float64",
# "fare_amount":"float64",
# "extra":"float64",
# "mta_tax":"float64",
# "tip_amount":"float64",
# "tolls_amount":"float64",
# "ehail_fee":"float64",
# "improvement_surcharge":"float64",
# "total_amount":"float64",
# "payment_type":"Int64",
# "trip_type":"Int64",
# "congestion_surcharge":"float64"
# }


# green_taxi_parse_dates = {
#     "lpep_pickup_datetime",
#     "lpep_dropoff_datetime"
# }

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
def ingest_nyc_dataset(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize):
    """Import NYC taxi data into postgres sql"""
    # Read a sample of the data
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz' # yellow_tripdata_2021-01.csv.gz
    zone_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}') # postgresql://root:root@localhost:5432/ny_taxi
    
    # Monthly data yellow trips
    df_iter = pd.read_csv(url, dtype=dtype, parse_dates=parse_dates, iterator=True, chunksize=chunksize)
    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace")
            first = False

        # Insert chunk
        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")

        print(f"Populated {len(df_chunk)} chunks into table {target_table}")


    # Monthly data green trips
    green_taxi_trip_url = "/data/green_tripdata_2025-11.parquet"
    green_trip_table = "green_taxi_trips"
    parquet_file = pq.ParquetFile(green_taxi_trip_url)  # Use PyArrow to create a chunked reader
    first = True
    for batch in tqdm(parquet_file.iter_batches(batch_size=chunksize)): # iterate_batches allows us to stream the file without loading it all into RAM
        df_chunk = batch.to_pandas()
        
        if first:
            # Equivalent logic: Create schema (head 0) and replace existing table
            df_chunk.head(0).to_sql(name=green_trip_table, con=engine, if_exists="replace")
            first = False

        # Insert chunk
        df_chunk.to_sql(name=green_trip_table, con=engine, if_exists="append")

        print(f"Populated {len(df_chunk)} rows into table {green_trip_table}")

    # Zone data
    df_zone_iter = pd.read_csv(zone_url, dtype=zone_dtype, iterator=True, chunksize=chunksize)
    first = True
    zone_table = "zones"
    for df_zone_chunk in tqdm(df_zone_iter):
        if first:
            # Create table schema (no data)
            df_zone_chunk.head(0).to_sql(name=zone_table, con=engine, if_exists="replace")
            first = False

        # Insert chunk
        df_zone_chunk.to_sql(name=zone_table, con=engine, if_exists="append")

        print(f"Populated {len(df_zone_chunk)} chunks into table {zone_table}")

if __name__ == '__main__':
    ingest_nyc_dataset()