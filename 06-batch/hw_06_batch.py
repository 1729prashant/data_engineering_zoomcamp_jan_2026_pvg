import os
from pyspark.sql import SparkSession

base_dir = os.path.abspath("data")   # resolves relative → absolute at runtime
input_path = os.path.join(base_dir, "yellow_tripdata_2025-11.parquet")
output_path = os.path.join(base_dir, "output")
# print(f"base_dir is {base_dir}")
# print(f"input_path is {input_path}")
# print(f"output_path is {output_path}")


# 1. Create a Spark session
spark = SparkSession.builder \
    .appName("ParquetRepartition") \
    .getOrCreate()

print(f"spark version is {spark.version}")

# 2. Read the parquet file into a Spark DataFrame
df = spark.read.parquet(input_path)


# # Optional: inspect it
# df.printSchema()
# df.show(5)
# print(f"Partition count before: {df.rdd.getNumPartitions()}")

# ---- QUESTION 2 START --------------
# # 3. Repartition to 4 partitions and save
# df.repartition(4).write.mode("overwrite").parquet(output_path)

# print("Done. Output written to 4 partition files.")
# ---- QUESTION 2 END   --------------



# ---- QUESTION 3 START --------------
# Register the DataFrame as a temp view (gives it a SQL table name)
df.createOrReplaceTempView("trips")

# How many taxi trips were there on the 15th of November?
# Consider only trips that started on the 15th of November.
nov15_trips = """
                   SELECT 
                        COUNT(*)
                   FROM trips
                   WHERE DATE_TRUNC('day', tpep_pickup_datetime) = DATE '2025-11-15'
                   """
# print(f'sql query is: {nov15_trips}')
result = spark.sql(nov15_trips)
# result.show()
# ---- QUESTION 3 END.  --------------


# ---- QUESTION 4 START --------------
# What is the length of the longest trip in the dataset in hours?
longest_trip_query = """
    SELECT 
        (UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime)) / 3600.0 
        AS longest_trip_hours
    FROM trips
    ORDER BY longest_trip_hours DESC
    LIMIT 1
                   """
# print(f'sql query is: {longest_trip_query}')
result = spark.sql(longest_trip_query)
# result.show()
# ---- QUESTION 4 END.  --------------


# ---- QUESTION 6 START --------------
df_zones = spark.read \
.option("header", "true") \
.option("inferSchema", "true") \
.csv("data/taxi_zone_lookup.csv")

df_zones.createOrReplaceTempView("zones")

# Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?
least_frequent_pickup_zone = """
    SELECT 
        pz.Zone   AS pickup_zone,
        count(*)
    FROM trips t
    JOIN zones pz ON t.PULocationID = pz.LocationID
    GROUP BY pz.Zone
    ORDER BY 2 ASC
    LIMIT 5
"""
print(f'sql query is: {least_frequent_pickup_zone}')
result = spark.sql(least_frequent_pickup_zone)
result.show()
# ---- QUESTION 6 END.  --------------


# ---- QUESTION 5 START --------------
print(f"UI available at: {spark.sparkContext.uiWebUrl}")
input("Press Enter to exit and close Spark UI...")  # holds the program open

spark.stop()
# ---- QUESTION 5 END.  --------------
