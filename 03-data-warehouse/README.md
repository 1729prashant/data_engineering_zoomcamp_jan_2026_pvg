# Module 3 Homework: Data Warehousing & BigQuery

In this homework we'll practice working with BigQuery and Google Cloud Storage.

When submitting your homework, you will also need to include
a link to your GitHub repository or other public code-hosting
site.

This repository should contain the code for solving the homework.

When your solution has SQL or shell commands and not code
(e.g. python files) file format, include them directly in
the README file of your repository.

## Data

For this homework we will be using the Yellow Taxi Trip Records for January 2024 - June 2024 (not the entire year of data).

Parquet Files are available from the New York City Taxi Data found here:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Loading the data

You can use the following scripts to load the data into your GCS bucket:

- Python script: [load_yellow_taxi_data.py](./load_yellow_taxi_data.py)
- Jupyter notebook with DLT: [DLT_upload_to_GCP.ipynb](./DLT_upload_to_GCP.ipynb)

You will need to generate a Service Account with GCS Admin privileges or be authenticated with the Google SDK, and update the bucket name in the script.

If you are using orchestration tools such as Kestra, Mage, Airflow, or Prefect, do not load the data into BigQuery using the orchestrator.

Make sure that all 6 files show in your GCS bucket before beginning.

Note: You will need to use the PARQUET option when creating an external table.


## BigQuery Setup

Create an external table using the Yellow Taxi Trip Records. 

```
CREATE OR REPLACE EXTERNAL TABLE `<GCP_PROJECT_ID>.nytaxi.external_table_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://<GCP_BUCKET_ID>/yellow_tripdata_2024-*.parquet']
);
```
Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table). 

```
-- note: this is a regular table, this is not materialized
CREATE OR REPLACE TABLE `<GCP_PROJECT_ID>.nytaxi.nonpartitioned_table_yellow_tripdata`
AS SELECT * FROM `<GCP_PROJECT_ID>.nytaxi.external_table_yellow_tripdata`;
```
![](screenshots/BigQuery Setup.png)
---

## Question 1. Counting records

What is count of records for the 2024 Yellow Taxi Data?

- 65,623
- 840,402
- 20,332,093
- 85,431,289

```
Ans --> 20,332,093 
REASON:
SELECT count(*) 
FROM `<GCP_PROJECT_ID>.nytaxi.external_table_yellow_tripdata`;
see screenshot

```
![](screenshots/1.png)
---

## Question 2. Data read estimation

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

```
SELECT COUNT(DISTINCT(PULocationID)) 
FROM `<GCP_PROJECT_ID>.nytaxi.external_table_yellow_tripdata`;

SELECT COUNT(DISTINCT(PULocationID)) 
FROM `<GCP_PROJECT_ID>.nytaxi.nonpartitioned_table_yellow_tripdata`;
```
 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 155.12 MB for the Materialized Table
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

```
Ans --> 0 MB for the External Table and 155.12 MB for the Materialized Table
REASON: see screenshot
```
![](screenshots/2_1.png)
![](screenshots/2_2.png)

---

## Question 3. Understanding columnar storage

Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.

Why are the estimated number of Bytes different?

- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires 
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, 
doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

```
Ans --> BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires 
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
REASON: see https://cloud.google.com/blog/products/bigquery/inside-capacitor-bigquerys-next-generation-columnar-storage-format

```

## Question 4. Counting zero fare trips

How many records have a fare_amount of 0?

- 128,210
- 546,578
- 20,188,016
- 8,333

```
Ans --> 8,333
REASON: see screenshot
select COUNT(*) 
FROM `<GCP_PROJECT_ID>.nytaxi.nonpartitioned_table_yellow_tripdata`
where fare_amount = 0; 
-- note: 0 nd 0.0 give same result
```
![](screenshots/4.png)
---

## Question 5. Partitioning and clustering

What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

- Partition by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on tpep_dropoff_datetime Partition by VendorID
- Partition by tpep_dropoff_datetime and Partition by VendorID

```
Ans --> Partition by tpep_dropoff_datetime and Cluster on VendorID
REASON:
Partition = High-cardinality filter columns (dates/timestamps)
Cluster = Low-to-medium cardinality columns used in ORDER BY/GROUP BY

CREATE OR REPLACE TABLE `<GCP_PROJECT_ID>.nytaxi.partitioned_table_yellow_tripdata`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `<GCP_PROJECT_ID>.nytaxi.external_table_yellow_tripdata`
);

```
![](screenshots/5.png)

---

## Question 6. Partition benefits

Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime
2024-03-01 and 2024-03-15 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? 

Choose the answer which most closely matches.

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

```
Ans --> 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
REASON: see screenshot
SELECT DISTINCT(VendorID) 
FROM  `<GCP_PROJECT_ID>.nytaxi.nonpartitioned_table_yellow_tripdata`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT(VendorID) 
FROM `<GCP_PROJECT_ID>.nytaxi.partitioned_table_yellow_tripdata`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```
![](screenshots/6_1.png)
![](screenshots/6_2.png)


---

## Question 7. External table storage

Where is the data stored in the External Table you created?

- Big Query
- Container Registry
- GCP Bucket
- Big Table

```
Ans --> GCP Bucket
REASON: External Table -
Data location: Stays in Cloud Storage (GCS)
Query performance: Slower (reads from GCS each time)
Cost: Pay for queries (scans all data each time)
Storage cost: Only pay for GCS storage (cheaper)
Use case: Data that changes frequently in GCS, or when you want to save on BigQuery storage costs
see: https://docs.cloud.google.com/bigquery/docs/external-tables
```


## Question 8. Clustering best practices

It is best practice in Big Query to always cluster your data:

- True
- False

```
Ans --> False
REASON: 
Clustering depends on data size (< 1 GB sees minimal benefit)
Depends on query patterns (needs filtering/sorting on cluster columns)

Good for clustering:
--country (200 values)
--VendorID (2-10 values)
--payment_type (5 values)
--status (ACTIVE, INACTIVE, PENDING)

Bad for clustering:
--user_id (millions of unique values) ← too high cardinality
--timestamp (every value unique) ← too high cardinality
--transaction_id (UUIDs) ← too high cardinality


Additional reasons NOT to cluster:
--Table < 1 GB
--No common filter/sort columns
--Fully scanning table anyway (no WHERE clause)
--Ad-hoc queries with random filter columns
--Cost of reclustering > query savings
```

## Question 9. Understanding table scans

No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

```
Ans --> 0 bytes, for the partitioned table created in Question 6

Note: question is ambiguous. question should be clearly framed given there are a few tables created throughout the assignment. my answer is for the partitioned table. 
REASON: 
Because BigQuery stores table metadata that includes:
Total row count
Number of partitions
Rows per partition

When you run SELECT COUNT(*) on a partitioned or non-partitioned native table, BigQuery reads this metadata instead of scanning actual data.
Result: 0 bytes processed, instant response.

This works on: Partitioned tables, Non-partitioned tables
Does not work on: External tables (no metadata, must scan files)

The partitioning itself doesn't enable this - it's a feature of all native BigQuery tables.
```
